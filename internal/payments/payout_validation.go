package payments

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PayoutValidationServer implements `acme.payments.v1.PayoutValidationService`.
// Checks payouts against the rules before anything acts on them.
type PayoutValidationServer struct {
	paymentsv1.UnsafePayoutValidationServiceServer
	store                             *Store
	log                               *slog.Logger
	paymentInstrumentValidationClient paymentsv1.PaymentInstrumentValidationServiceClient
	refundValidationClient            paymentsv1.RefundValidationServiceClient
	clients                           *registry.Registry
	sellerQueryClient                 sellersv1.SellerQueryServiceClient
	stockReservationQueryClient       inventoryv1.StockReservationQueryServiceClient
	roleNotificationClient            identityv1.RoleNotificationServiceClient
	roleAdminClient                   identityv1.RoleAdminServiceClient
	conn                              *grpc.ClientConn
	refundCalculationClient           paymentsv1.RefundCalculationServiceClient
}

// NewPayoutValidationServer wires a PayoutValidationServer.
func NewPayoutValidationServer(deps platform.Deps) *PayoutValidationServer {
	return &PayoutValidationServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.payments.v1.PayoutValidationService"),
		paymentInstrumentValidationClient: paymentsv1.NewPaymentInstrumentValidationServiceClient(deps.Conn),
		refundValidationClient:            paymentsv1.NewRefundValidationServiceClient(deps.Conn),
		clients:                           deps.Clients,
		sellerQueryClient:                 sellersv1.NewSellerQueryServiceClient(deps.Conn),
		stockReservationQueryClient:       inventoryv1.NewStockReservationQueryServiceClient(deps.Conn),
		roleNotificationClient:            identityv1.NewRoleNotificationServiceClient(deps.Conn),
		roleAdminClient:                   identityv1.NewRoleAdminServiceClient(deps.Conn),
		conn:                              deps.Conn,
		refundCalculationClient:           paymentsv1.NewRefundCalculationServiceClient(deps.Conn),
	}
}

// ValidatePayout validates a payout.
func (s *PayoutValidationServer) ValidatePayout(ctx context.Context, req *paymentsv1.ValidatePayoutRequest) (*paymentsv1.ValidatePayoutResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "validate payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetPayout().GetLines(), func(candidate *paymentsv1.PayoutLine) bool { return candidate != nil }) {
		if _, err := s.paymentInstrumentValidationClient.NormalizePaymentInstrument(ctx, &paymentsv1.NormalizePaymentInstrumentRequest{
			Context: req.GetContext(),
			PaymentInstrument: &paymentsv1.PaymentInstrument{
				Limits:  []*paymentsv1.PaymentInstrumentLimit{&paymentsv1.PaymentInstrumentLimit{Amount: line.GetTax().GetAmount()}},
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
				Funding: &paymentsv1.PaymentInstrument_Bank{Bank: &paymentsv1.PaymentInstrumentBankFunding{AccountHolder: req.GetPayout().GetBank().GetAccountHolder()}},
				Holder: &customersv1.CustomerRef{
					DisplayName: req.GetPayout().GetCounterparty().GetDisplayName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentValidationService/NormalizePaymentInstrument")
		}
	}
	return &paymentsv1.ValidatePayoutResponse{
		Payout: &paymentsv1.Payout{
			PayoutId: req.GetPayoutId(),
			Notes:    req.GetPayout().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewPayout previews a payout.
func (s *PayoutValidationServer) PreviewPayout(ctx context.Context, req *paymentsv1.PreviewPayoutRequest) (*paymentsv1.PreviewPayoutResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "preview payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPayout()).(*paymentsv1.Payout)
	normalizeRefundRequest := &paymentsv1.NormalizeRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetPayout().GetCounterparty().GetDisplayName(),
			},
			Settlement: &paymentsv1.Refund_Bank{Bank: &paymentsv1.RefundBankSettlement{Bic: req.GetPayout().GetBank().GetBic()}},
			Audit:      &commonv1.AuditInfo{CreatedBy: cloned.GetAudit().GetCreatedBy()},
			Number:     req.GetPayout().GetCounterparty().GetNumber(),
		},
	}
	normalizeRefundReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*paymentsv1.NormalizeRefundResponse, error) {
		return s.refundValidationClient.NormalizeRefund(ctx, normalizeRefundRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/NormalizeRefund")
	}
	searchIndexHistory, ok := s.clients.Get("acme.search.v1.SearchIndexHistoryService").(searchv1.SearchIndexHistoryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SearchIndexHistoryService")
	}
	snapshot, err := protojson.Marshal(req.GetPayout())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareSearchIndexVersionsRequest := &searchv1.CompareSearchIndexVersionsRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{
				CountryCode: req.GetPayout().GetBillingAddress().GetCountryCode(),
			}},
		},
		Details: &searchv1.CompareSearchIndexVersionsRequest_Details{Snapshot: snapshot},
	}
	compareSearchIndexVersionsReply, err := searchIndexHistory.CompareSearchIndexVersions(ctx, compareSearchIndexVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions")
	}
	source := req.GetDetails().GetContact().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	explainPaymentInstrumentDecisionRequest := &paymentsv1.ExplainPaymentInstrumentDecisionRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{
				Email: normalizeRefundReply.GetRefund().GetCounterparty().GetEmail(),
			},
			Funding: &paymentsv1.PaymentInstrument_Card{Card: &paymentsv1.PaymentInstrumentCardFunding{
				Token: normalizeRefundReply.GetRefund().GetCard().GetToken(),
			}},
		},
		Details: &paymentsv1.ExplainPaymentInstrumentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: email,
				Phone: req.GetDetails().GetContact().GetPhone(),
			},
		},
	}
	explainPaymentInstrumentDecisionReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*paymentsv1.ExplainPaymentInstrumentDecisionResponse, error) {
		return s.paymentInstrumentValidationClient.ExplainPaymentInstrumentDecision(ctx, explainPaymentInstrumentDecisionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentValidationService/ExplainPaymentInstrumentDecision")
	}
	var token string
	switch choice := req.GetPayout().GetSettlement().(type) {
	case *paymentsv1.Payout_Card:
		token = choice.Card.GetToken()
	case *paymentsv1.Payout_Bank:
		token = choice.Bank.GetIban()
	case *paymentsv1.Payout_VoucherCode:
		token = choice.VoucherCode
	}
	normalizePaymentInstrumentReply, err := s.paymentInstrumentValidationClient.NormalizePaymentInstrument(ctx, &paymentsv1.NormalizePaymentInstrumentRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Funding: &paymentsv1.PaymentInstrument_Card{Card: &paymentsv1.PaymentInstrumentCardFunding{Token: token}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainPaymentInstrumentDecisionReply.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &paymentsv1.NormalizePaymentInstrumentRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: explainPaymentInstrumentDecisionReply.GetPaymentInstrument().GetContact().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentValidationService/NormalizePaymentInstrument")
	}
	return &paymentsv1.PreviewPayoutResponse{
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				Etag:      normalizeRefundReply.GetRefund().GetAudit().GetEtag(),
				CreatedBy: explainPaymentInstrumentDecisionReply.GetPaymentInstrument().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: compareSearchIndexVersionsReply.GetSearchIndex().GetAddress().GetRecipient(),
			},
			PayoutId: req.GetPayoutId(),
			DueAt:    req.GetPayout().GetDueAt(),
		},
		Outcome: &paymentsv1.PreviewPayoutResponse_Outcome{Message: normalizeRefundReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Details: normalizePaymentInstrumentReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// CheckPayoutEligibility checks a payout.
func (s *PayoutValidationServer) CheckPayoutEligibility(ctx context.Context, req *paymentsv1.CheckPayoutEligibilityRequest) (*paymentsv1.CheckPayoutEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "check payout eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPayout())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Payout
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	ctx = tenancy.With(ctx, "refund_id", req.GetPayout().GetCounterparty().GetRefundId())
	findSellersReply, err := s.sellerQueryClient.FindSellers(ctx, &sellersv1.FindSellersRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.FindSellersRequest_Filter{
			Region:       req.GetPayout().GetBillingAddress().GetRegion(),
			CreatedAfter: (&decoded).GetAudit().GetCreatedAt(),
			OwnerId:      tenancy.Value(ctx, "refund_id"),
			Labels:       req.GetPayout().GetMetadata(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/FindSellers")
	}
	for _, line := range req.GetPayout().GetLines() {
		if _, err := s.stockReservationQueryClient.SummarizeStockReservations(ctx, &inventoryv1.SummarizeStockReservationsRequest{
			Context: req.GetContext(),
			Details: &inventoryv1.SummarizeStockReservationsRequest_Details{Filters: line.GetAttributes()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/SummarizeStockReservations")
		}
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := payoutValidationServerSendRoleTestNoticeSteps[mode]
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.CheckPayoutEligibilityRequest_Details)
	packed, err := anypb.New(req.GetPayout())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendRoleTestNoticeRequest := &identityv1.SendRoleTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendRoleTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{PreferredChannel: cloned.GetContact().GetPreferredChannel()},
			Payload:   packed,
		},
	}
	sendRoleTestNoticeReply, err := run(s, ctx, sendRoleTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/SendRoleTestNotice")
	}
	return &paymentsv1.CheckPayoutEligibilityResponse{
		Outcome: &paymentsv1.CheckPayoutEligibilityResponse_Outcome{
			Status:      findSellersReply.GetResult().GetStatus(),
			ReferenceId: sendRoleTestNoticeReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: findSellersReply.GetResult().GetCorrelationId(),
			Message:       sendRoleTestNoticeReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Payout: &paymentsv1.Payout{
			PayoutId: req.GetPayoutId(),
			Metadata: req.GetPayout().GetMetadata(),
		},
	}, nil
}

// NormalizePayout normalizes a payout.
func (s *PayoutValidationServer) NormalizePayout(ctx context.Context, req *paymentsv1.NormalizePayoutRequest) (*paymentsv1.NormalizePayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizePayout", "error", err)
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "normalize payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetPayout().GetCounterparty().GetDisplayName())
	createRiskAssessmentRequest := &fraudv1.CreateRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit:    &commonv1.AuditInfo{CreatedAt: req.GetPayout().GetAudit().GetCreatedAt()},
			Assignee: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	}
	createRiskAssessmentReply, err := createRiskAssessmentCall(ctx, createRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentService/CreateRiskAssessment")
	}
	jobs := make(chan *paymentsv1.PayoutAttachment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attachment := range jobs {
				if _, err := s.roleAdminClient.RepairRole(workCtx, &identityv1.RepairRoleRequest{
					Context: req.GetContext(),
					Details: &identityv1.RepairRoleRequest_Details{Checksum: attachment.GetChecksum()},
					Role: &identityv1.Role{
						Audit: &commonv1.AuditInfo{
							CreatedBy: createRiskAssessmentReply.GetRiskAssessment().GetAudit().GetCreatedBy(),
						},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPayout().GetAttachments() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleAdminService/RepairRole")
	}
	return &paymentsv1.NormalizePayoutResponse{
		Outcome: &paymentsv1.NormalizePayoutResponse_Outcome{Status: createRiskAssessmentReply.GetResult().GetStatus()},
		Payout: &paymentsv1.Payout{
			PayoutId: req.GetPayoutId(),
			Metadata: req.GetPayout().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScorePayout scores a payout.
func (s *PayoutValidationServer) ScorePayout(ctx context.Context, req *paymentsv1.ScorePayoutRequest) (*paymentsv1.ScorePayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScorePayout", "error", err)
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "score payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	comparePayoutOptionsRequest := &paymentsv1.ComparePayoutOptionsRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ComparePayoutOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetPayout().GetBillingAddress().GetRecipient(),
			},
		},
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetPayout().GetAudit().GetCreatedBy()},
		},
	}
	comparePayoutOptionsReply := new(paymentsv1.ComparePayoutOptionsResponse)
	if err := grpc.Invoke(ctx, "/acme.payments.v1.PayoutCalculationService/ComparePayoutOptions", comparePayoutOptionsRequest, comparePayoutOptionsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/ComparePayoutOptions")
	}
	return &paymentsv1.ScorePayoutResponse{
		Payout: &paymentsv1.Payout{
			Status:   comparePayoutOptionsReply.GetPayout().GetStatus(),
			PayoutId: req.GetPayoutId(),
			Number:   req.GetPayout().GetNumber(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintPayouts lints a payout.
func (s *PayoutValidationServer) LintPayouts(ctx context.Context, req *paymentsv1.LintPayoutsRequest) (*paymentsv1.LintPayoutsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "lint payouts", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	var holder struct{ PreferredChannel string }
	if record := req.GetDetails().GetContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PreferredChannel"); err != nil {
			s.log.WarnContext(ctx, "copy preferred_channel", "error", err)
		}
	}
	explainPaymentInstrumentDecisionRequest := &paymentsv1.ExplainPaymentInstrumentDecisionRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ExplainPaymentInstrumentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{PreferredChannel: holder.PreferredChannel},
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{Etag: req.GetPayout().GetAudit().GetEtag()},
		},
	}
	explainPaymentInstrumentDecisionReply, err := grpcx.Call(ctx, s.paymentInstrumentValidationClient.ExplainPaymentInstrumentDecision, explainPaymentInstrumentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentValidationService/ExplainPaymentInstrumentDecision")
	}
	return &paymentsv1.LintPayoutsResponse{
		Outcome: &paymentsv1.LintPayoutsResponse_Outcome{
			Status: explainPaymentInstrumentDecisionReply.GetOutcome().GetStatus(),
		},
		Payout: &paymentsv1.Payout{
			PayoutId: req.GetPayoutId(),
			Number:   req.GetPayout().GetNumber(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPayoutRules returns a payout.
func (s *PayoutValidationServer) GetPayoutRules(ctx context.Context, req *paymentsv1.GetPayoutRulesRequest) (*paymentsv1.GetPayoutRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "get_payout_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payout rules", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	compareTierOptionsRequest := &loyaltyv1.CompareTierOptionsRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: cloned.GetActor().GetUserId(),
			},
		},
	}
	compareTierOptionsReply := new(loyaltyv1.CompareTierOptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.TierCalculationService_CompareTierOptions_FullMethodName, compareTierOptionsRequest, compareTierOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/CompareTierOptions")
	}
	explainRefundCalculationRequest := &paymentsv1.ExplainRefundCalculationRequest{}
	explainRefundCalculationRequest.Context = req.GetContext()
	explainRefundCalculationRequest.Refund = &paymentsv1.Refund{
		Audit: &commonv1.AuditInfo{
			CreatedAt: compareTierOptionsReply.GetTier().GetAudit().GetCreatedAt(),
		},
	}
	explainRefundCalculationRequest.Details = &paymentsv1.ExplainRefundCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: compareTierOptionsReply.GetTier().GetOwner().GetEmail(),
		},
	}
	explainRefundCalculationReply, err := s.refundCalculationClient.ExplainRefundCalculation(ctx, explainRefundCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/ExplainRefundCalculation")
	}
	return &paymentsv1.GetPayoutRulesResponse{
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareTierOptionsReply.GetTier().GetAudit().GetCreatedAt(),
			},
			Counterparty: &paymentsv1.RefundRef{
				Number: explainRefundCalculationReply.GetRefund().GetNumber(),
			},
			PayoutId: req.GetPayoutId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// payoutValidationServerSendRoleTestNoticeSteps are the ways to run
// acme.identity.v1.RoleNotificationService/SendRoleTestNotice, by name.
var payoutValidationServerSendRoleTestNoticeSteps = map[string]func(*PayoutValidationServer, context.Context, *identityv1.SendRoleTestNoticeRequest) (*identityv1.SendRoleTestNoticeResponse, error){
	"default": (*PayoutValidationServer).sendRoleTestNoticeStep,
	"audited": (*PayoutValidationServer).sendRoleTestNoticeAuditedStep,
}

func (s *PayoutValidationServer) sendRoleTestNoticeStep(ctx context.Context, request *identityv1.SendRoleTestNoticeRequest) (*identityv1.SendRoleTestNoticeResponse, error) {
	return s.roleNotificationClient.SendRoleTestNotice(ctx, request)
}

func (s *PayoutValidationServer) sendRoleTestNoticeAuditedStep(ctx context.Context, request *identityv1.SendRoleTestNoticeRequest) (*identityv1.SendRoleTestNoticeResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.RoleNotificationService/SendRoleTestNotice")
	return s.roleNotificationClient.SendRoleTestNotice(ctx, request)
}
