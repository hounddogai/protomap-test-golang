package giftcards

import (
	"context"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	concessionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/concessions/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// GiftCardTransactionLifecycleServer implements `acme.giftcards.v1.GiftCardTransactionLifecycleService`.
// Moves a gift card transaction between the states of its lifecycle.
type GiftCardTransactionLifecycleServer struct {
	giftcardsv1.UnimplementedGiftCardTransactionLifecycleServiceServer
	store                               *Store
	log                                 *slog.Logger
	giftCardTransactionCalculationPort  giftCardTransactionCalculationPort
	giftCardTransactionValidationClient giftcardsv1.GiftCardTransactionValidationServiceClient
	tierValidationClient                loyaltyv1.TierValidationServiceClient
	conn                                *grpc.ClientConn
	meteredConcessionStandCalculation   meteredConcessionStandCalculation
	giftCardBatchValidationClient       giftcardsv1.GiftCardBatchValidationServiceClient
	giftCardCalculationPort             giftCardCalculationPort
	searchQueryNotificationClient       searchv1.SearchQueryNotificationServiceClient
	auditEventClient                    auditv1.AuditEventServiceClient
	productOnce                         sync.Once
	productCached                       catalogv1.ProductServiceClient
	giftCardBatchValidationOnce         sync.Once
	giftCardBatchValidationCached       giftcardsv1.GiftCardBatchValidationServiceClient
	giftCardCalculationClient           giftcardsv1.GiftCardCalculationServiceClient
	giftCardQueryClient                 giftcardsv1.GiftCardQueryServiceClient
}

// NewGiftCardTransactionLifecycleServer wires a GiftCardTransactionLifecycleServer.
func NewGiftCardTransactionLifecycleServer(deps platform.Deps) *GiftCardTransactionLifecycleServer {
	return &GiftCardTransactionLifecycleServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.giftcards.v1.GiftCardTransactionLifecycleService"),
		giftCardTransactionCalculationPort:  newGiftCardTransactionCalculationPort(deps),
		giftCardTransactionValidationClient: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
		tierValidationClient:                loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		conn:                                deps.Conn,
		meteredConcessionStandCalculation:   meteredConcessionStandCalculation{ConcessionStandCalculationServiceClient: concessionsv1.NewConcessionStandCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		giftCardBatchValidationClient:       giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
		giftCardCalculationPort:             newGiftCardCalculationPort(deps),
		searchQueryNotificationClient:       searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		auditEventClient:                    auditv1.NewAuditEventServiceClient(deps.Conn),
		giftCardCalculationClient:           giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		giftCardQueryClient:                 giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
	}
}

// ActivateGiftCardTransaction activates a gift card transaction.
func (s *GiftCardTransactionLifecycleServer) ActivateGiftCardTransaction(ctx context.Context, req *giftcardsv1.ActivateGiftCardTransactionRequest) (*giftcardsv1.ActivateGiftCardTransactionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "activate_gift_card_transaction"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate gift card transaction", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	estimateGiftCardTransactionRequest := &giftcardsv1.EstimateGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject:               &giftcardsv1.GiftCardBatchRef{DisplayName: tenancy.Value(ctx, "display_name")},
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
		},
		Reason: req.GetReason(),
	}
	estimateGiftCardTransactionReply, err := s.giftCardTransactionCalculationPort.EstimateGiftCardTransaction(ctx, estimateGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/EstimateGiftCardTransaction")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := giftCardTransactionLifecycleServerCheckGiftCardTransactionEligibilitySteps[mode]
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	checkGiftCardTransactionEligibilityRequest := &giftcardsv1.CheckGiftCardTransactionEligibilityRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CheckGiftCardTransactionEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: tenancy.Value(ctx, "email"),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	checkGiftCardTransactionEligibilityReply, err := run(s, ctx, checkGiftCardTransactionEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/CheckGiftCardTransactionEligibility")
	}
	validateTierReply, err := s.tierValidationClient.ValidateTier(ctx, &loyaltyv1.ValidateTierRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ValidateTierRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/ValidateTier")
	}
	calculateGiftCardRequest := &giftcardsv1.CalculateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validateTierReply.GetTier().GetAudit().GetUpdatedAt(),
				Etag:      validateTierReply.GetTier().GetAudit().GetEtag(),
			},
			Contact: &commonv1.ContactInfo{Email: validateTierReply.GetTier().GetOwner().GetEmail()},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				Kind: estimateGiftCardTransactionReply.GetGiftCardTransaction().GetKind(),
			},
		},
	}
	calculateGiftCardReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardCalculationServiceClient).CalculateGiftCard(ctx, calculateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard")
	}
	return &giftcardsv1.ActivateGiftCardTransactionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateGiftCardTransactionReply.GetResult().GetCorrelationId(),
			Status:        checkGiftCardTransactionEligibilityReply.GetOutcome().GetStatus(),
			ReferenceIds:  validateTierReply.GetResult().GetReferenceIds(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				GiftCardBatchId: estimateGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId(),
			},
			CorrelationId:         validateTierReply.GetResult().GetCorrelationId(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
		},
		Outcome: &giftcardsv1.ActivateGiftCardTransactionResponse_Outcome{
			Message:     calculateGiftCardReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// SuspendGiftCardTransaction suspends a gift card transaction.
func (s *GiftCardTransactionLifecycleServer) SuspendGiftCardTransaction(ctx context.Context, req *giftcardsv1.SuspendGiftCardTransactionRequest) (*giftcardsv1.SuspendGiftCardTransactionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendGiftCardTransaction", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "suspend_gift_card_transaction"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend gift card transaction", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotifyContact().GetEmail())
	explainConcessionStandCalculationRequest := &concessionsv1.ExplainConcessionStandCalculationRequest{
		Context: req.GetContext(),
		ConcessionStand: &concessionsv1.ConcessionStand{
			Owner: &identityv1.UserRef{
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
				Email:  tenancy.Value(ctx, "email"),
			},
		},
		Details: &concessionsv1.ExplainConcessionStandCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	explainConcessionStandCalculationReply, err := s.meteredConcessionStandCalculation.ExplainConcessionStandCalculation(ctx, explainConcessionStandCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.concessions.v1.ConcessionStandCalculationService/ExplainConcessionStandCalculation")
	}
	return &giftcardsv1.SuspendGiftCardTransactionResponse{
		Result: &commonv1.OperationResult{
			Message: explainConcessionStandCalculationReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{GiftCardTransactionId: req.GetGiftCardTransactionId()},
		Outcome:             &giftcardsv1.SuspendGiftCardTransactionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateGiftCardTransaction reinstates a gift card transaction.
func (s *GiftCardTransactionLifecycleServer) ReinstateGiftCardTransaction(ctx context.Context, req *giftcardsv1.ReinstateGiftCardTransactionRequest) (*giftcardsv1.ReinstateGiftCardTransactionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "reinstate_gift_card_transaction"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate gift card transaction", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded giftcardsv1.ReinstateGiftCardTransactionRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	validateGiftCardBatchRequest := newValidateGiftCardBatchRequest(withValidateGiftCardBatchContext(req.GetContext()), withValidateGiftCardBatchDetails(&giftcardsv1.ValidateGiftCardBatchRequest_Details{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: (&decoded).GetNotifyContact().GetPreferredChannel(),
			Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
		},
	}))
	validateGiftCardBatchReply, err := s.giftCardBatchValidationClient.ValidateGiftCardBatch(ctx, validateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/ValidateGiftCardBatch")
	}
	compareGiftCardOptionsRequest := &giftcardsv1.CompareGiftCardOptionsRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				Token: validateGiftCardBatchReply.GetGiftCardBatch().GetCard().GetToken(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
		},
	}
	compareGiftCardOptionsReply, err := s.giftCardCalculationPort.CompareGiftCardOptions(ctx, compareGiftCardOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CompareGiftCardOptions")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewGiftCardTransactionRequest := &giftcardsv1.PreviewGiftCardTransactionRequest{}
	previewGiftCardTransactionRequest.Context = req.GetContext()
	previewGiftCardTransactionRequest.GiftCardTransaction = &giftcardsv1.GiftCardTransaction{
		Subject: &giftcardsv1.GiftCardBatchRef{
			DisplayName: validateGiftCardBatchReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			Number:      validateGiftCardBatchReply.GetGiftCardBatch().GetNumber(),
		},
		GiftCardTransactionId: compareGiftCardOptionsReply.GetGiftCard().GetHolder().GetGiftCardTransactionId(),
		Detail:                packed,
	}
	previewGiftCardTransactionRequest.Details = &giftcardsv1.PreviewGiftCardTransactionRequest_Details{
		Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
	}
	previewGiftCardTransactionReply, err := s.giftCardTransactionValidationClient.PreviewGiftCardTransaction(ctx, previewGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/PreviewGiftCardTransaction")
	}
	return &giftcardsv1.ReinstateGiftCardTransactionResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: validateGiftCardBatchReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
			Kind:                  compareGiftCardOptionsReply.GetGiftCard().GetHolder().GetKind(),
			CorrelationId:         previewGiftCardTransactionReply.GetResult().GetCorrelationId(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
		},
		Outcome: &giftcardsv1.ReinstateGiftCardTransactionResponse_Outcome{
			Status:      compareGiftCardOptionsReply.GetResult().GetStatus(),
			NextSteps:   previewGiftCardTransactionReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TerminateGiftCardTransaction terminates a gift card transaction.
func (s *GiftCardTransactionLifecycleServer) TerminateGiftCardTransaction(ctx context.Context, req *giftcardsv1.TerminateGiftCardTransactionRequest) (*giftcardsv1.TerminateGiftCardTransactionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "terminate_gift_card_transaction"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate gift card transaction", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeNotificationsRequest := &notificationsv1.SummarizeNotificationsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	summarizeNotificationsReply := new(notificationsv1.SummarizeNotificationsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.notifications.v1.NotificationQueryService/SummarizeNotifications", summarizeNotificationsRequest, summarizeNotificationsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/SummarizeNotifications")
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetNotifyContact().GetPreferredChannel())
	unsubscribeSearchQueryUpdatesReply, err := s.searchQueryNotificationClient.UnsubscribeSearchQueryUpdates(ctx, &searchv1.UnsubscribeSearchQueryUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSearchQueryUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNotifyContact().GetEmail(),
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
				Name:             req.GetDetails().GetNotifyContact().GetName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/UnsubscribeSearchQueryUpdates")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	estimateGiftCardTransactionRequest := &giftcardsv1.EstimateGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Sequence:   unsubscribeSearchQueryUpdatesReply.GetSearchQuery().GetSequence(),
			OccurredAt: unsubscribeSearchQueryUpdatesReply.GetSearchQuery().GetOccurredAt(),
			Detail:     packed,
		},
	}
	estimateGiftCardTransactionReply := new(giftcardsv1.EstimateGiftCardTransactionResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardTransactionCalculationService_EstimateGiftCardTransaction_FullMethodName, estimateGiftCardTransactionRequest, estimateGiftCardTransactionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/EstimateGiftCardTransaction")
	}
	return &giftcardsv1.TerminateGiftCardTransactionResponse{
		Result: &commonv1.OperationResult{
			Status:  summarizeNotificationsReply.GetResult().GetStatus(),
			Message: summarizeNotificationsReply.GetResult().GetMessage(),
		},
		Outcome: &giftcardsv1.TerminateGiftCardTransactionResponse_Outcome{
			Status:      unsubscribeSearchQueryUpdatesReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{GiftCardTransactionId: req.GetGiftCardTransactionId()},
	}, nil
}

// RenewGiftCardTransaction renews a gift card transaction.
func (s *GiftCardTransactionLifecycleServer) RenewGiftCardTransaction(ctx context.Context, req *giftcardsv1.RenewGiftCardTransactionRequest) (*giftcardsv1.RenewGiftCardTransactionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewGiftCardTransaction", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "renew_gift_card_transaction"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew gift card transaction", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	updateAuditEventRequest := &auditv1.UpdateAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Measurements: []*auditv1.AuditEventMeasurement{&auditv1.AuditEventMeasurement{ObservedAt: req.GetDetails().GetEffectiveAt()}},
			Detail:       packed,
		},
	}
	var updateAuditEventReply *auditv1.UpdateAuditEventResponse
	if err := pipeline.Run(ctx, "acme.audit.v1.AuditEventService/UpdateAuditEvent", func(ctx context.Context) error {
		answer, err := s.auditEventClient.UpdateAuditEvent(ctx, updateAuditEventRequest)
		updateAuditEventReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/UpdateAuditEvent")
	}
	return &giftcardsv1.RenewGiftCardTransactionResponse{
		Outcome: &giftcardsv1.RenewGiftCardTransactionResponse_Outcome{
			Message:     updateAuditEventReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			OccurredAt:            updateAuditEventReply.GetAuditEvent().GetOccurredAt(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireGiftCardTransaction expires a gift card transaction.
func (s *GiftCardTransactionLifecycleServer) ExpireGiftCardTransaction(ctx context.Context, req *giftcardsv1.ExpireGiftCardTransactionRequest) (*giftcardsv1.ExpireGiftCardTransactionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpireGiftCardTransaction", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "expire_gift_card_transaction"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire gift card transaction", "tenant", req.GetContext().GetTenantId())
	batchGetProductsRequest := &catalogv1.BatchGetProductsRequest{Context: req.GetContext()}
	batchGetProductsReply, err := s.productLazy().BatchGetProducts(ctx, batchGetProductsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductService/BatchGetProducts")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	countGiftCardsRequest := &giftcardsv1.CountGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.CountGiftCardsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      batchGetProductsReply.GetResult().GetCorrelationId(),
			Labels:       annotations,
		},
	}
	countGiftCardsReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardQueryServiceClient).CountGiftCards(ctx, countGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/CountGiftCards")
	}
	return &giftcardsv1.ExpireGiftCardTransactionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetProductsReply.GetResult().GetCorrelationId(),
			Status:        countGiftCardsReply.GetResult().GetStatus(),
		},
		Outcome: &giftcardsv1.ExpireGiftCardTransactionResponse_Outcome{
			Status:      batchGetProductsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{GiftCardTransactionId: req.GetGiftCardTransactionId()},
	}, nil
}

// GetGiftCardTransactionLifecycle returns a gift card transaction.
func (s *GiftCardTransactionLifecycleServer) GetGiftCardTransactionLifecycle(ctx context.Context, req *giftcardsv1.GetGiftCardTransactionLifecycleRequest) (*giftcardsv1.GetGiftCardTransactionLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetGiftCardTransactionLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "get_gift_card_transaction_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get gift card transaction lifecycle", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	explainGiftCardBatchDecisionRequest := &giftcardsv1.ExplainGiftCardBatchDecisionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ExplainGiftCardBatchDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: email},
			Locale:  req.GetContext().GetLocale(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			DueAt:    req.GetContext().GetDeadline(),
			Metadata: req.GetContext().GetBaggage(),
		},
	}
	explainGiftCardBatchDecisionReply, err := s.giftCardBatchValidationLazy().ExplainGiftCardBatchDecision(ctx, explainGiftCardBatchDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/ExplainGiftCardBatchDecision")
	}
	return &giftcardsv1.GetGiftCardTransactionLifecycleResponse{
		Result: &commonv1.OperationResult{
			Status:  explainGiftCardBatchDecisionReply.GetResult().GetStatus(),
			Message: explainGiftCardBatchDecisionReply.GetOutcome().GetMessage(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{GiftCardTransactionId: req.GetGiftCardTransactionId()},
	}, nil
}

// ScheduleGiftCardTransactionExpiry schedules a gift card transaction.
func (s *GiftCardTransactionLifecycleServer) ScheduleGiftCardTransactionExpiry(ctx context.Context, req *giftcardsv1.ScheduleGiftCardTransactionExpiryRequest) (*giftcardsv1.ScheduleGiftCardTransactionExpiryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "schedule gift card transaction expiry", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetGiftCardTransaction().GetMeasurements() {
		if _, err := s.giftCardCalculationClient.CompareGiftCardOptions(ctx, &giftcardsv1.CompareGiftCardOptionsRequest{
			Context: req.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
				Holder: &giftcardsv1.GiftCardTransactionRef{
					DisplayName:           req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
					GiftCardTransactionId: req.GetGiftCardTransaction().GetGiftCardTransactionId(),
				},
				Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CompareGiftCardOptions")
		}
	}
	resolveGiftCardsReply, err := s.giftCardQueryClient.ResolveGiftCards(ctx, &giftcardsv1.ResolveGiftCardsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/ResolveGiftCards")
	}
	return &giftcardsv1.ScheduleGiftCardTransactionExpiryResponse{
		Outcome: &giftcardsv1.ScheduleGiftCardTransactionExpiryResponse_Outcome{Message: resolveGiftCardsReply.GetResult().GetMessage()},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId:         resolveGiftCardsReply.GetResult().GetCorrelationId(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Kind:                  req.GetGiftCardTransaction().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// giftCardTransactionLifecycleServerCheckGiftCardTransactionEligibilitySteps are the ways to run
// acme.giftcards.v1.GiftCardTransactionValidationService/CheckGiftCardTransactionEligibility, by name.
var giftCardTransactionLifecycleServerCheckGiftCardTransactionEligibilitySteps = map[string]func(*GiftCardTransactionLifecycleServer, context.Context, *giftcardsv1.CheckGiftCardTransactionEligibilityRequest) (*giftcardsv1.CheckGiftCardTransactionEligibilityResponse, error){
	"default": (*GiftCardTransactionLifecycleServer).checkGiftCardTransactionEligibilityStep,
	"audited": (*GiftCardTransactionLifecycleServer).checkGiftCardTransactionEligibilityAuditedStep,
}

func (s *GiftCardTransactionLifecycleServer) checkGiftCardTransactionEligibilityStep(ctx context.Context, request *giftcardsv1.CheckGiftCardTransactionEligibilityRequest) (*giftcardsv1.CheckGiftCardTransactionEligibilityResponse, error) {
	return s.giftCardTransactionValidationClient.CheckGiftCardTransactionEligibility(ctx, request)
}

func (s *GiftCardTransactionLifecycleServer) checkGiftCardTransactionEligibilityAuditedStep(ctx context.Context, request *giftcardsv1.CheckGiftCardTransactionEligibilityRequest) (*giftcardsv1.CheckGiftCardTransactionEligibilityResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.giftcards.v1.GiftCardTransactionValidationService/CheckGiftCardTransactionEligibility")
	return s.giftCardTransactionValidationClient.CheckGiftCardTransactionEligibility(ctx, request)
}

// productLazy dials acme.catalog.v1.ProductService the first time a handler needs it.
func (s *GiftCardTransactionLifecycleServer) productLazy() catalogv1.ProductServiceClient {
	s.productOnce.Do(func() { s.productCached = catalogv1.NewProductServiceClient(s.conn) })
	return s.productCached
}

// giftCardBatchValidationLazy dials acme.giftcards.v1.GiftCardBatchValidationService the first time a handler needs it.
func (s *GiftCardTransactionLifecycleServer) giftCardBatchValidationLazy() giftcardsv1.GiftCardBatchValidationServiceClient {
	s.giftCardBatchValidationOnce.Do(func() { s.giftCardBatchValidationCached = giftcardsv1.NewGiftCardBatchValidationServiceClient(s.conn) })
	return s.giftCardBatchValidationCached
}
