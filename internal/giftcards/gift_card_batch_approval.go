package giftcards

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// GiftCardBatchApprovalServer implements `acme.giftcards.v1.GiftCardBatchApprovalService`.
// Routes gift card batches through approval, with escalation and delegation.
type GiftCardBatchApprovalServer struct {
	giftcardsv1.UnimplementedGiftCardBatchApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	ledgerAccountValidationClient   ledgerv1.LedgerAccountValidationServiceClient
	giftCardQueryClient             giftcardsv1.GiftCardQueryServiceClient
	notificationValidationOnce      sync.Once
	notificationValidationCached    notificationsv1.NotificationValidationServiceClient
	meteredGiftCardBatchCalculation meteredGiftCardBatchCalculation
	clients                         *registry.Registry
	giftCardBatchValidationClient   giftcardsv1.GiftCardBatchValidationServiceClient
	consentRecordValidationClient   customersv1.ConsentRecordValidationServiceClient
	featureFlagClient               featureflagsv1.FeatureFlagServiceClient
	giftCardValidationClient        giftcardsv1.GiftCardValidationServiceClient
}

// NewGiftCardBatchApprovalServer wires a GiftCardBatchApprovalServer.
func NewGiftCardBatchApprovalServer(deps platform.Deps) *GiftCardBatchApprovalServer {
	return &GiftCardBatchApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.giftcards.v1.GiftCardBatchApprovalService"),
		conn:                            deps.Conn,
		ledgerAccountValidationClient:   ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		giftCardQueryClient:             giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
		meteredGiftCardBatchCalculation: meteredGiftCardBatchCalculation{GiftCardBatchCalculationServiceClient: giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                         deps.Clients,
		giftCardBatchValidationClient:   giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
		consentRecordValidationClient:   customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		featureFlagClient:               featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		giftCardValidationClient:        giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
	}
}

// SubmitGiftCardBatch submits a gift card batch.
func (s *GiftCardBatchApprovalServer) SubmitGiftCardBatch(ctx context.Context, req *giftcardsv1.SubmitGiftCardBatchRequest) (*giftcardsv1.SubmitGiftCardBatchResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "submit gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetGiftCardBatch())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scheduleGiftCardTransactionExpiryRequest := &giftcardsv1.ScheduleGiftCardTransactionExpiryRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				GiftCardBatchId: req.GetGiftCardBatch().GetGiftCardBatchId(),
			},
			Detail: packed,
		},
	}
	scheduleGiftCardTransactionExpiryReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardTransactionLifecycleServiceClient).ScheduleGiftCardTransactionExpiry(ctx, scheduleGiftCardTransactionExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionLifecycleService/ScheduleGiftCardTransactionExpiry")
	}
	return &giftcardsv1.SubmitGiftCardBatchResponse{
		Outcome: &giftcardsv1.SubmitGiftCardBatchResponse_Outcome{
			Message: scheduleGiftCardTransactionExpiryReply.GetOutcome().GetMessage(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Notes:           req.GetGiftCardBatch().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveGiftCardBatch approves a gift card batch.
func (s *GiftCardBatchApprovalServer) ApproveGiftCardBatch(ctx context.Context, req *giftcardsv1.ApproveGiftCardBatchRequest) (*giftcardsv1.ApproveGiftCardBatchResponse, error) {
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "approve gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	lintLedgerAccountsReply, err := s.ledgerAccountValidationClient.LintLedgerAccounts(ctx, &ledgerv1.LintLedgerAccountsRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Funding: &ledgerv1.LedgerAccount_Card{Card: &ledgerv1.LedgerAccountCardFunding{
				Token: req.GetGiftCardBatch().GetCard().GetToken(),
				Last4: req.GetGiftCardBatch().GetCard().GetLast4(),
			}},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetGiftCardBatch().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts")
	}
	return &giftcardsv1.ApproveGiftCardBatchResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintLedgerAccountsReply.GetLedgerAccount().GetAudit().GetCreatedBy(),
			},
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: lintLedgerAccountsReply.GetLedgerAccount().GetHolder().GetDisplayName(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Notes:           req.GetGiftCardBatch().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateGiftCardBatch escalates a gift card batch.
func (s *GiftCardBatchApprovalServer) EscalateGiftCardBatch(ctx context.Context, req *giftcardsv1.EscalateGiftCardBatchRequest) (*giftcardsv1.EscalateGiftCardBatchResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "escalate gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	summarizeGiftCardsRequest := newSummarizeGiftCardsRequest(withSummarizeGiftCardsContext(req.GetContext()), withSummarizeGiftCardsGiftCardId(req.GetGiftCardBatch().GetCounterparty().GetGiftCardId()))
	summarizeGiftCardsReply, err := s.giftCardQueryClient.SummarizeGiftCards(ctx, summarizeGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/SummarizeGiftCards")
	}
	return &giftcardsv1.EscalateGiftCardBatchResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeGiftCardsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Status:          req.GetGiftCardBatch().GetStatus(),
		},
	}, nil
}

// ListPendingGiftCardBatches lists gift card batches a page at a time.
func (s *GiftCardBatchApprovalServer) ListPendingGiftCardBatches(ctx context.Context, req *giftcardsv1.ListPendingGiftCardBatchesRequest) (*giftcardsv1.ListPendingGiftCardBatchesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending gift card batches", "tenant", req.GetContext().GetTenantId())
	return &giftcardsv1.ListPendingGiftCardBatchesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateGiftCardBatchApproval delegates a gift card batch.
func (s *GiftCardBatchApprovalServer) DelegateGiftCardBatchApproval(ctx context.Context, req *giftcardsv1.DelegateGiftCardBatchApprovalRequest) (*giftcardsv1.DelegateGiftCardBatchApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "delegate gift card batch approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	lintNotificationsRequest := &notificationsv1.LintNotificationsRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetGiftCardBatch().GetAudit().GetCreatedBy(),
			},
		},
		Details: &notificationsv1.LintNotificationsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	lintNotificationsReply, err := s.notificationValidationLazy().LintNotifications(ctx, lintNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/LintNotifications")
	}
	var last4 string
	switch choice := req.GetGiftCardBatch().GetSettlement().(type) {
	case *giftcardsv1.GiftCardBatch_Card:
		last4 = choice.Card.GetLast4()
	case *giftcardsv1.GiftCardBatch_Bank:
		last4 = choice.Bank.GetIban()
	case *giftcardsv1.GiftCardBatch_VoucherCode:
		last4 = choice.VoucherCode
	}
	quoteGiftCardBatchRequest := &giftcardsv1.QuoteGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: lintNotificationsReply.GetNotification().GetCustomer().GetDisplayName(),
			},
			Settlement: &giftcardsv1.GiftCardBatch_Card{Card: &giftcardsv1.GiftCardBatchCardSettlement{Last4: last4}},
		},
	}
	quoteGiftCardBatchReply, err := s.meteredGiftCardBatchCalculation.QuoteGiftCardBatch(ctx, quoteGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch")
	}
	return &giftcardsv1.DelegateGiftCardBatchApprovalResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			DueAt: lintNotificationsReply.GetNotification().GetSla().GetDueAt(),
			Audit: &commonv1.AuditInfo{
				Etag: lintNotificationsReply.GetNotification().GetAudit().GetEtag(),
			},
			Total:           quoteGiftCardBatchReply.GetGiftCardBatch().GetTotal(),
			Number:          quoteGiftCardBatchReply.GetGiftCardBatch().GetNumber(),
			GiftCardBatchId: req.GetGiftCardBatchId(),
			BillingAddress:  req.GetGiftCardBatch().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallGiftCardBatch recalls a gift card batch.
func (s *GiftCardBatchApprovalServer) RecallGiftCardBatch(ctx context.Context, req *giftcardsv1.RecallGiftCardBatchRequest) (*giftcardsv1.RecallGiftCardBatchResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallGiftCardBatch", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "recall gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	notificationValidation, ok := s.clients.Get("acme.notifications.v1.NotificationValidationService").(notificationsv1.NotificationValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.notifications.v1.NotificationValidationService")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetApprover().GetDisplayName())
	scoreNotificationRequest := &notificationsv1.ScoreNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Assignee: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Audit:    &commonv1.AuditInfo{Etag: req.GetGiftCardBatch().GetAudit().GetEtag()},
		},
	}
	scoreNotificationReply, err := notificationValidation.ScoreNotification(ctx, scoreNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ScoreNotification")
	}
	for line := range iterx.Each(req.GetGiftCardBatch().GetLines(), func(candidate *giftcardsv1.GiftCardBatchLine) bool { return candidate != nil }) {
		if _, err := s.giftCardBatchValidationClient.ValidateGiftCardBatch(ctx, &giftcardsv1.ValidateGiftCardBatchRequest{
			Context: req.GetContext(),
			GiftCardBatch: &giftcardsv1.GiftCardBatch{
				Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{
					Tax: &giftcardsv1.GiftCardBatchLineTax{Exemptions: line.GetTax().GetExemptions()},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/ValidateGiftCardBatch")
		}
	}
	packed, err := anypb.New(req.GetGiftCardBatch())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	step := &giftCardBatchApprovalServerRecallGiftCardBatchValidateConsentRecordStep{request: &customersv1.ValidateConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.ValidateConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            scoreNotificationReply.GetNotification().GetRequester().GetEmail(),
				PreferredChannel: scoreNotificationReply.GetNotification().GetRequester().GetPreferredChannel(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	}}
	if err := s.runRecallGiftCardBatchValidateConsentRecord(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ValidateConsentRecord")
	}
	validateConsentRecordReply := step.reply
	batchGetFeatureFlagsRequest := buildBatchGetFeatureFlagsRequestForRecallGiftCardBatch(req)
	batchGetFeatureFlagsReply, err := s.featureFlagClient.BatchGetFeatureFlags(ctx, batchGetFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags")
	}
	_ = batchGetFeatureFlagsReply
	return &giftcardsv1.RecallGiftCardBatchResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreNotificationReply.GetNotification().GetAudit().GetCreatedBy(),
			},
			Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{
				Attributes: validateConsentRecordReply.GetConsentRecord().GetAttributes(),
			}},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Counterparty:    req.GetGiftCardBatch().GetCounterparty(),
		},
		Outcome: &giftcardsv1.RecallGiftCardBatchResponse_Outcome{Message: scoreNotificationReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetGiftCardBatchApprovalStatus returns a gift card batch.
func (s *GiftCardBatchApprovalServer) GetGiftCardBatchApprovalStatus(ctx context.Context, req *giftcardsv1.GetGiftCardBatchApprovalStatusRequest) (*giftcardsv1.GetGiftCardBatchApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_batch", req.GetGiftCardBatchId(), "get_gift_card_batch_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get gift card batch approval status", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Locale string }
	if record := req.GetContext(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Locale"); err != nil {
			s.log.WarnContext(ctx, "copy locale", "error", err)
		}
	}
	previewGiftCardRequest := &giftcardsv1.PreviewGiftCardRequest{}
	previewGiftCardRequest.Context = req.GetContext()
	previewGiftCardRequest.GiftCard = &giftcardsv1.GiftCard{
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	}
	previewGiftCardRequest.Details = &giftcardsv1.PreviewGiftCardRequest_Details{Locale: holder.Locale}
	previewGiftCardReply, err := s.giftCardValidationClient.PreviewGiftCard(ctx, previewGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/PreviewGiftCard")
	}
	giftCardValidation, err := registry.Lookup[giftcardsv1.GiftCardValidationServiceClient](s.clients, "acme.giftcards.v1.GiftCardValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	checkGiftCardEligibilityRequest := &giftcardsv1.CheckGiftCardEligibilityRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				Last4: previewGiftCardReply.GetGiftCard().GetCard().GetLast4(),
			}},
		},
		Details: &giftcardsv1.CheckGiftCardEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: previewGiftCardReply.GetGiftCard().GetContact().GetEmail(),
			},
		},
	}
	checkGiftCardEligibilityReply, err := giftCardValidation.CheckGiftCardEligibility(ctx, checkGiftCardEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/CheckGiftCardEligibility")
	}
	return &giftcardsv1.GetGiftCardBatchApprovalStatusResponse{
		Result: &commonv1.OperationResult{Status: previewGiftCardReply.GetOutcome().GetStatus()},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: checkGiftCardEligibilityReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
		},
	}, nil
}

// notificationValidationLazy dials acme.notifications.v1.NotificationValidationService the first time a handler needs
// it.
func (s *GiftCardBatchApprovalServer) notificationValidationLazy() notificationsv1.NotificationValidationServiceClient {
	s.notificationValidationOnce.Do(func() {
		s.notificationValidationCached = notificationsv1.NewNotificationValidationServiceClient(s.conn)
	})
	return s.notificationValidationCached
}

// giftCardBatchApprovalServerRecallGiftCardBatchValidateConsentRecordStep carries the request of one step of
// RecallGiftCardBatch and, once it ran, its reply.
type giftCardBatchApprovalServerRecallGiftCardBatchValidateConsentRecordStep struct {
	request *customersv1.ValidateConsentRecordRequest
	reply   *customersv1.ValidateConsentRecordResponse
}

func (s *GiftCardBatchApprovalServer) runRecallGiftCardBatchValidateConsentRecord(ctx context.Context, step *giftCardBatchApprovalServerRecallGiftCardBatchValidateConsentRecordStep) error {
	reply, err := s.consentRecordValidationClient.ValidateConsentRecord(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
