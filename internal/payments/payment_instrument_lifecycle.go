package payments

import (
	"context"
	"log/slog"

	chatv1 "github.com/hounddogai/protomap-test-golang/gen/acme/chat/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	itsmv1 "github.com/hounddogai/protomap-test-golang/gen/acme/itsm/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentInstrumentLifecycleServer implements `acme.payments.v1.PaymentInstrumentLifecycleService`.
// Moves a payment instrument between the states of its lifecycle.
type PaymentInstrumentLifecycleServer struct {
	paymentsv1.UnimplementedPaymentInstrumentLifecycleServiceServer
	store                          *Store
	log                            *slog.Logger
	sellerScorecardReportingClient sellersv1.SellerScorecardReportingServiceClient
	consentRecordValidationClient  customersv1.ConsentRecordValidationServiceClient
	conn                           *grpc.ClientConn
	incidentClient                 itsmv1.IncidentServiceClient
	bus                            *eventbus.Bus
	rewardApprovalClient           loyaltyv1.RewardApprovalServiceClient
	paymentInstrumentSyncClient    paymentsv1.PaymentInstrumentSyncServiceClient
	clients                        *registry.Registry
}

// NewPaymentInstrumentLifecycleServer wires a PaymentInstrumentLifecycleServer.
func NewPaymentInstrumentLifecycleServer(deps platform.Deps) *PaymentInstrumentLifecycleServer {
	return &PaymentInstrumentLifecycleServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.payments.v1.PaymentInstrumentLifecycleService"),
		sellerScorecardReportingClient: sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
		consentRecordValidationClient:  customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		incidentClient:                 itsmv1.NewIncidentServiceClient(deps.Conn),
		bus:                            deps.Bus,
		rewardApprovalClient:           loyaltyv1.NewRewardApprovalServiceClient(deps.Conn),
		paymentInstrumentSyncClient:    paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn),
		clients:                        deps.Clients,
	}
}

// ActivatePaymentInstrument activates a payment instrument.
func (s *PaymentInstrumentLifecycleServer) ActivatePaymentInstrument(ctx context.Context, req *paymentsv1.ActivatePaymentInstrumentRequest) (*paymentsv1.ActivatePaymentInstrumentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "activate_payment_instrument"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate payment instrument", "tenant", req.GetContext().GetTenantId())
	countSellerScorecardTotalsRequest := buildCountSellerScorecardTotalsRequestForActivatePaymentInstrument(req)
	countSellerScorecardTotalsReply, err := s.sellerScorecardReportingClient.CountSellerScorecardTotals(ctx, countSellerScorecardTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/CountSellerScorecardTotals")
	}
	return &paymentsv1.ActivatePaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			Message:       countSellerScorecardTotalsReply.GetResult().GetMessage(),
			CorrelationId: countSellerScorecardTotalsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{PaymentInstrumentId: req.GetPaymentInstrumentId()},
		Outcome:           &paymentsv1.ActivatePaymentInstrumentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendPaymentInstrument suspends a payment instrument.
func (s *PaymentInstrumentLifecycleServer) SuspendPaymentInstrument(ctx context.Context, req *paymentsv1.SuspendPaymentInstrumentRequest) (*paymentsv1.SuspendPaymentInstrumentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "suspend_payment_instrument"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend payment instrument", "tenant", req.GetContext().GetTenantId())
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{}
	explainConsentRecordDecisionRequest.Context = req.GetContext()
	explainConsentRecordDecisionRequest.Details = &customersv1.ExplainConsentRecordDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
		},
	}
	explainConsentRecordDecisionRequest.ConsentRecord = &customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{
			DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
		},
	}
	explainConsentRecordDecisionReply, err := s.consentRecordValidationClient.ExplainConsentRecordDecision(ctx, explainConsentRecordDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	return &paymentsv1.SuspendPaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			Status: explainConsentRecordDecisionReply.GetOutcome().GetStatus(),
		},
		Outcome: &paymentsv1.SuspendPaymentInstrumentResponse_Outcome{
			ReferenceId: explainConsentRecordDecisionReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{PaymentInstrumentId: req.GetPaymentInstrumentId()},
	}, nil
}

// ReinstatePaymentInstrument reinstates a payment instrument.
func (s *PaymentInstrumentLifecycleServer) ReinstatePaymentInstrument(ctx context.Context, req *paymentsv1.ReinstatePaymentInstrumentRequest) (*paymentsv1.ReinstatePaymentInstrumentResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "reinstate_payment_instrument"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate payment instrument", "tenant", req.GetContext().GetTenantId())
	listChatRoomsRequest := &chatv1.ListChatRoomsRequest{
		Context: req.GetContext(),
		Filter: &chatv1.ListChatRoomsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetPaymentInstrumentId(),
			Labels:       req.GetAnnotations(),
		},
	}
	listChatRoomsReply, err := grpcx.NewClient(s.conn, chatv1.NewChatRoomServiceClient).ListChatRooms(ctx, listChatRoomsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.chat.v1.ChatRoomService/ListChatRooms")
	}
	return &paymentsv1.ReinstatePaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			Status:  listChatRoomsReply.GetResult().GetStatus(),
			Details: listChatRoomsReply.GetResult().GetDetails(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{PaymentInstrumentId: req.GetPaymentInstrumentId()},
		Outcome:           &paymentsv1.ReinstatePaymentInstrumentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminatePaymentInstrument terminates a payment instrument.
func (s *PaymentInstrumentLifecycleServer) TerminatePaymentInstrument(ctx context.Context, req *paymentsv1.TerminatePaymentInstrumentRequest) (*paymentsv1.TerminatePaymentInstrumentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "terminate_payment_instrument"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate payment instrument", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.TerminatePaymentInstrumentRequest_Details)
	searchIncidentsRequest := &itsmv1.SearchIncidentsRequest{
		Context: req.GetContext(),
		Filter:  &itsmv1.SearchIncidentsRequest_Filter{OwnerId: cloned.GetNewOwner().GetUserId()},
	}
	send := s.searchIncidentsSender(searchIncidentsRequest)
	searchIncidentsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.itsm.v1.IncidentService/SearchIncidents")
	}
	return &paymentsv1.TerminatePaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: searchIncidentsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{PaymentInstrumentId: req.GetPaymentInstrumentId()},
		Outcome:           &paymentsv1.TerminatePaymentInstrumentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewPaymentInstrument renews a payment instrument.
func (s *PaymentInstrumentLifecycleServer) RenewPaymentInstrument(ctx context.Context, req *paymentsv1.RenewPaymentInstrumentRequest) (*paymentsv1.RenewPaymentInstrumentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewPaymentInstrument", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "renew_payment_instrument"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew payment instrument", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "payments.renew_payment_instrument.update_customer",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"customer.relationship_manager.email": req.GetDetails().GetNewOwner().GetEmail(),
			"customer.contact.email":              req.GetDetails().GetNotifyContact().GetEmail(),
			"customer.contact.preferred_channel":  req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			"context.tenant_id":                   req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	approveStockMovementRequest := &inventoryv1.ApproveStockMovementRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.ApproveStockMovementRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		StockMovement: &inventoryv1.StockMovement{Detail: packed},
	}
	approveStockMovementReply, err := approveStockMovementCall(ctx, approveStockMovementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementApprovalService/ApproveStockMovement")
	}
	return &paymentsv1.RenewPaymentInstrumentResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				CustomerId: approveStockMovementReply.GetStockMovement().GetSubject().GetCustomerId(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
		},
		Outcome: &paymentsv1.RenewPaymentInstrumentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpirePaymentInstrument expires a payment instrument.
func (s *PaymentInstrumentLifecycleServer) ExpirePaymentInstrument(ctx context.Context, req *paymentsv1.ExpirePaymentInstrumentRequest) (*paymentsv1.ExpirePaymentInstrumentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpirePaymentInstrument", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "expire_payment_instrument"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire payment instrument", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	delegateRewardApprovalRequest := &loyaltyv1.DelegateRewardApprovalRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.DelegateRewardApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				Email:       req.GetDetails().GetNewOwner().GetEmail(),
				DisplayName: tenancy.Value(ctx, "display_name"),
			},
		},
	}
	delegateRewardApprovalReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*loyaltyv1.DelegateRewardApprovalResponse, error) {
		return s.rewardApprovalClient.DelegateRewardApproval(ctx, delegateRewardApprovalRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/DelegateRewardApproval")
	}
	return &paymentsv1.ExpirePaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			Status: delegateRewardApprovalReply.GetOutcome().GetStatus(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{PaymentInstrumentId: req.GetPaymentInstrumentId()},
		Outcome:           &paymentsv1.ExpirePaymentInstrumentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetPaymentInstrumentLifecycle returns a payment instrument.
func (s *PaymentInstrumentLifecycleServer) GetPaymentInstrumentLifecycle(ctx context.Context, req *paymentsv1.GetPaymentInstrumentLifecycleRequest) (*paymentsv1.GetPaymentInstrumentLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPaymentInstrumentLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "get_payment_instrument_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment instrument lifecycle", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.GetPaymentInstrumentLifecycleResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{PaymentInstrumentId: req.GetPaymentInstrumentId()},
		Result:            &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SchedulePaymentInstrumentExpiry schedules a payment instrument.
func (s *PaymentInstrumentLifecycleServer) SchedulePaymentInstrumentExpiry(ctx context.Context, req *paymentsv1.SchedulePaymentInstrumentExpiryRequest) (*paymentsv1.SchedulePaymentInstrumentExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SchedulePaymentInstrumentExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "schedule payment instrument expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	pullPaymentInstrumentsRequest := &paymentsv1.PullPaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Funding: &paymentsv1.PaymentInstrument_Bank{Bank: &paymentsv1.PaymentInstrumentBankFunding{Bic: req.GetPaymentInstrument().GetBank().GetBic()}},
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotify().GetName(),
				Email: req.GetPaymentInstrument().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPaymentInstrument().GetAudit().GetCreatedAt(),
			},
		},
	}
	pullPaymentInstrumentsReply, err := grpcx.Call(ctx, s.paymentInstrumentSyncClient.PullPaymentInstruments, pullPaymentInstrumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/PullPaymentInstruments")
	}
	rewardApproval, err := registry.Lookup[loyaltyv1.RewardApprovalServiceClient](s.clients, "acme.loyalty.v1.RewardApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetPaymentInstrument().GetAudit().GetUpdatedBy())
	submitRewardRequest := &loyaltyv1.SubmitRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: tenancy.Value(ctx, "updated_by"),
				Etag:      pullPaymentInstrumentsReply.GetPaymentInstrument().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				Email: pullPaymentInstrumentsReply.GetPaymentInstrument().GetHolder().GetEmail(),
			},
		},
	}
	submitRewardReply, err := rewardApproval.SubmitReward(ctx, submitRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/SubmitReward")
	}
	customerCalculation, err := registry.Lookup[customersv1.CustomerCalculationServiceClient](s.clients, "acme.customers.v1.CustomerCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email:  pullPaymentInstrumentsReply.GetPaymentInstrument().GetContact().GetEmail(),
				UserId: submitRewardReply.GetReward().GetOwner().GetUserId(),
			},
		},
	}
	explainCustomerCalculationReply, err := customerCalculation.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	return &paymentsv1.SchedulePaymentInstrumentExpiryResponse{
		Result: &commonv1.OperationResult{
			Details:       pullPaymentInstrumentsReply.GetResult().GetDetails(),
			Status:        submitRewardReply.GetOutcome().GetStatus(),
			CorrelationId: submitRewardReply.GetResult().GetCorrelationId(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{
				Email: explainCustomerCalculationReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			OpenedAt:            req.GetPaymentInstrument().GetOpenedAt(),
		},
	}, nil
}

// searchIncidentsSender returns a function that sends one request to acme.itsm.v1.IncidentService/SearchIncidents.
func (s *PaymentInstrumentLifecycleServer) searchIncidentsSender(request *itsmv1.SearchIncidentsRequest) func(context.Context) (*itsmv1.SearchIncidentsResponse, error) {
	return func(ctx context.Context) (*itsmv1.SearchIncidentsResponse, error) {
		return s.incidentClient.SearchIncidents(ctx, request)
	}
}
