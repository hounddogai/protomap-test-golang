package fraud

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
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

// FraudSignalLifecycleServer implements `acme.fraud.v1.FraudSignalLifecycleService`.
// Moves a fraud signal between the states of its lifecycle.
type FraudSignalLifecycleServer struct {
	fraudv1.UnimplementedFraudSignalLifecycleServiceServer
	store                              *Store
	log                                *slog.Logger
	clients                            *registry.Registry
	watchlistCalculationClient         fraudv1.WatchlistCalculationServiceClient
	compareFraudSignalOptionsFn        func(context.Context, *fraudv1.CompareFraudSignalOptionsRequest, ...grpc.CallOption) (*fraudv1.CompareFraudSignalOptionsResponse, error)
	loyaltyMemberWorkflowClient        loyaltyv1.LoyaltyMemberWorkflowServiceClient
	searchIndexNotificationClient      searchv1.SearchIndexNotificationServiceClient
	rejectSubscriptionPlanFn           func(context.Context, *subscriptionsv1.RejectSubscriptionPlanRequest, ...grpc.CallOption) (*subscriptionsv1.RejectSubscriptionPlanResponse, error)
	fraudRuleCalculationClient         fraudv1.FraudRuleCalculationServiceClient
	fraudRuleCalculationClientFallback fraudv1.FraudRuleCalculationServiceClient
	fraudSignalCalculationClient       fraudv1.FraudSignalCalculationServiceClient
	riskAssessmentClient               fraudv1.RiskAssessmentServiceClient
	subscriptionPlanWorkflowClient     subscriptionsv1.SubscriptionPlanWorkflowServiceClient
	conn                               *grpc.ClientConn
}

// NewFraudSignalLifecycleServer wires a FraudSignalLifecycleServer.
func NewFraudSignalLifecycleServer(deps platform.Deps) FraudSignalLifecycleServer {
	fraudSignalCalculation := fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn)
	subscriptionPlanApproval := subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn)
	return FraudSignalLifecycleServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.fraud.v1.FraudSignalLifecycleService"),
		clients:                            deps.Clients,
		watchlistCalculationClient:         fraudv1.NewWatchlistCalculationServiceClient(deps.Conn),
		compareFraudSignalOptionsFn:        fraudSignalCalculation.CompareFraudSignalOptions,
		loyaltyMemberWorkflowClient:        loyaltyv1.NewLoyaltyMemberWorkflowServiceClient(deps.Conn),
		searchIndexNotificationClient:      searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		rejectSubscriptionPlanFn:           subscriptionPlanApproval.RejectSubscriptionPlan,
		fraudRuleCalculationClient:         fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn),
		fraudRuleCalculationClientFallback: fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn),
		fraudSignalCalculationClient:       fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn),
		riskAssessmentClient:               fraudv1.NewRiskAssessmentServiceClient(deps.Conn),
		subscriptionPlanWorkflowClient:     subscriptionsv1.NewSubscriptionPlanWorkflowServiceClient(deps.Conn),
		conn:                               deps.Conn,
	}
}

// ActivateFraudSignal activates a fraud signal.
func (s FraudSignalLifecycleServer) ActivateFraudSignal(ctx context.Context, req *fraudv1.ActivateFraudSignalRequest) (*fraudv1.ActivateFraudSignalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "activate_fraud_signal"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate fraud signal", "tenant", req.GetContext().GetTenantId())
	fraudSignalQuery, err := registry.Lookup[fraudv1.FraudSignalQueryServiceClient](s.clients, "acme.fraud.v1.FraudSignalQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	lookupFraudSignalRequest := &fraudv1.LookupFraudSignalRequest{
		Context:       req.GetContext(),
		FraudSignalId: req.GetFraudSignalId(),
	}
	lookupFraudSignalReply, err := fraudSignalQuery.LookupFraudSignal(ctx, lookupFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalQueryService/LookupFraudSignal")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	estimateWatchlistRequest := newEstimateWatchlistRequest(withEstimateWatchlistContext(req.GetContext()), withEstimateWatchlistWatchlist(&fraudv1.Watchlist{
		Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Manager: &identityv1.UserRef{
					Email:       req.GetDetails().GetNotifyContact().GetEmail(),
					DisplayName: tenancy.Value(ctx, "display_name"),
				},
			}},
		}},
	}))
	estimateWatchlistReply, err := s.watchlistCalculationClient.EstimateWatchlist(ctx, estimateWatchlistRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/EstimateWatchlist")
	}
	return &fraudv1.ActivateFraudSignalResponse{
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: lookupFraudSignalReply.GetResult().GetCorrelationId(),
			FraudSignalId: req.GetFraudSignalId(),
		},
		Result: &commonv1.OperationResult{
			Details: estimateWatchlistReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &fraudv1.ActivateFraudSignalResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendFraudSignal suspends a fraud signal.
func (s FraudSignalLifecycleServer) SuspendFraudSignal(ctx context.Context, req *fraudv1.SuspendFraudSignalRequest) (*fraudv1.SuspendFraudSignalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "suspend_fraud_signal"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend fraud signal", "tenant", req.GetContext().GetTenantId())
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareFraudSignalOptionsRequest := &fraudv1.CompareFraudSignalOptionsRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject:       &fraudv1.FraudRuleRef{DisplayName: displayName},
			FraudSignalId: req.GetFraudSignalId(),
			Detail:        packed,
		},
		Reason: req.GetReason(),
	}
	compareFraudSignalOptionsReply, err := s.compareFraudSignalOptionsFn(ctx, compareFraudSignalOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/CompareFraudSignalOptions")
	}
	return &fraudv1.SuspendFraudSignalResponse{
		FraudSignal: &fraudv1.FraudSignal{
			OccurredAt:    compareFraudSignalOptionsReply.GetFraudSignal().GetOccurredAt(),
			Sequence:      compareFraudSignalOptionsReply.GetFraudSignal().GetSequence(),
			FraudSignalId: req.GetFraudSignalId(),
		},
		Outcome: &fraudv1.SuspendFraudSignalResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TerminateFraudSignal terminates a fraud signal.
func (s FraudSignalLifecycleServer) TerminateFraudSignal(ctx context.Context, req *fraudv1.TerminateFraudSignalRequest) (*fraudv1.TerminateFraudSignalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "terminate_fraud_signal"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate fraud signal", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*fraudv1.TerminateFraudSignalRequest_Details)
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetNewOwner().GetUserId())
	step := &fraudSignalLifecycleServerTerminateFraudSignalSimulateWatchlistStep{request: &fraudv1.SimulateWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						Email:       req.GetDetails().GetNotifyContact().GetEmail(),
						DisplayName: cloned.GetNewOwner().GetDisplayName(),
						UserId:      tenancy.Value(ctx, "user_id"),
					},
				}},
			}},
		},
	}}
	if err := s.runTerminateFraudSignalSimulateWatchlist(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/SimulateWatchlist")
	}
	simulateWatchlistReply := step.reply
	cancelLoyaltyMemberWorkflowRequest := &loyaltyv1.CancelLoyaltyMemberWorkflowRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}
	cancelLoyaltyMemberWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*loyaltyv1.CancelLoyaltyMemberWorkflowResponse, error) {
		return s.loyaltyMemberWorkflowClient.CancelLoyaltyMemberWorkflow(ctx, cancelLoyaltyMemberWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberWorkflowService/CancelLoyaltyMemberWorkflow")
	}
	return &fraudv1.TerminateFraudSignalResponse{
		Result: &commonv1.OperationResult{
			Details:      simulateWatchlistReply.GetResult().GetDetails(),
			ReferenceIds: cancelLoyaltyMemberWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: simulateWatchlistReply.GetResult().GetCorrelationId(),
			FraudSignalId: req.GetFraudSignalId(),
		},
		Outcome: &fraudv1.TerminateFraudSignalResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewFraudSignal renews a fraud signal.
func (s FraudSignalLifecycleServer) RenewFraudSignal(ctx context.Context, req *fraudv1.RenewFraudSignalRequest) (*fraudv1.RenewFraudSignalResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "renew_fraud_signal"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew fraud signal", "tenant", req.GetContext().GetTenantId())
	unsubscribeSearchIndexUpdatesReply, err := s.searchIndexNotificationClient.UnsubscribeSearchIndexUpdates(ctx, &searchv1.UnsubscribeSearchIndexUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSearchIndexUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/UnsubscribeSearchIndexUpdates")
	}
	rejectSubscriptionPlanRequest := &subscriptionsv1.RejectSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: unsubscribeSearchIndexUpdatesReply.GetSearchIndex().GetAudit().GetUpdatedAt(),
			},
		},
	}
	rejectSubscriptionPlanReply, err := s.rejectSubscriptionPlanFn(ctx, rejectSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/RejectSubscriptionPlan")
	}
	return &fraudv1.RenewFraudSignalResponse{
		Result: &commonv1.OperationResult{
			Message: unsubscribeSearchIndexUpdatesReply.GetOutcome().GetMessage(),
			Status:  rejectSubscriptionPlanReply.GetOutcome().GetStatus(),
		},
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: rejectSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetDisplayName(),
			},
			FraudSignalId: req.GetFraudSignalId(),
		},
		Outcome: &fraudv1.RenewFraudSignalResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireFraudSignal expires a fraud signal.
func (s FraudSignalLifecycleServer) ExpireFraudSignal(ctx context.Context, req *fraudv1.ExpireFraudSignalRequest) (*fraudv1.ExpireFraudSignalResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "expire_fraud_signal"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire fraud signal", "tenant", req.GetContext().GetTenantId())
	calculateWatchlistRequest := buildCalculateWatchlistRequestForExpireFraudSignal(req)
	calculateWatchlistReply, err := s.watchlistCalculationClient.CalculateWatchlist(ctx, calculateWatchlistRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/CalculateWatchlist")
	}
	routes := map[string]func(context.Context, *fraudv1.RecalculateFraudRulesRequest, ...grpc.CallOption) (*fraudv1.RecalculateFraudRulesResponse, error){
		"primary":  s.fraudRuleCalculationClient.RecalculateFraudRules,
		"fallback": s.fraudRuleCalculationClientFallback.RecalculateFraudRules,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	recalculateFraudRulesRequest := &fraudv1.RecalculateFraudRulesRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				Email:  calculateWatchlistReply.GetWatchlist().GetManager().GetEmail(),
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
	}
	recalculateFraudRulesReply, err := route(ctx, recalculateFraudRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleCalculationService/RecalculateFraudRules")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareFraudSignalOptionsRequest := &fraudv1.CompareFraudSignalOptionsRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &fraudv1.CompareFraudSignalOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: calculateWatchlistReply.GetWatchlist().GetLocation().GetPostalCode(),
			},
		},
	}
	type compareFraudSignalOptionsResult struct {
		reply *fraudv1.CompareFraudSignalOptionsResponse
		err   error
	}
	compareFraudSignalOptionsResults := make(chan compareFraudSignalOptionsResult, 1)
	go func() {
		reply, err := s.fraudSignalCalculationClient.CompareFraudSignalOptions(ctx, compareFraudSignalOptionsRequest)
		compareFraudSignalOptionsResults <- compareFraudSignalOptionsResult{reply, err}
	}()
	var compareFraudSignalOptionsReply *fraudv1.CompareFraudSignalOptionsResponse
	select {
	case result := <-compareFraudSignalOptionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fraud.v1.FraudSignalCalculationService/CompareFraudSignalOptions")
		}
		compareFraudSignalOptionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &fraudv1.ExpireFraudSignalResponse{
		Result: &commonv1.OperationResult{
			Status:        calculateWatchlistReply.GetResult().GetStatus(),
			Details:       calculateWatchlistReply.GetResult().GetDetails(),
			CorrelationId: recalculateFraudRulesReply.GetResult().GetCorrelationId(),
		},
		FraudSignal: &fraudv1.FraudSignal{
			Attributes: compareFraudSignalOptionsReply.GetFraudSignal().GetAttributes(),
			Subject: &fraudv1.FraudRuleRef{
				FraudRuleId: compareFraudSignalOptionsReply.GetFraudSignal().GetSubject().GetFraudRuleId(),
			},
			FraudSignalId: req.GetFraudSignalId(),
		},
		Outcome: &fraudv1.ExpireFraudSignalResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetFraudSignalLifecycle returns a fraud signal.
func (s FraudSignalLifecycleServer) GetFraudSignalLifecycle(ctx context.Context, req *fraudv1.GetFraudSignalLifecycleRequest) (*fraudv1.GetFraudSignalLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetFraudSignalLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "get_fraud_signal_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get fraud signal lifecycle", "tenant", req.GetContext().GetTenantId())
	searchRiskAssessmentsRequest := &fraudv1.SearchRiskAssessmentsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.SearchRiskAssessmentsRequest_Filter{
			OwnerId:      req.GetFraudSignalId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	searchRiskAssessmentsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*fraudv1.SearchRiskAssessmentsResponse, error) {
		return s.riskAssessmentClient.SearchRiskAssessments(ctx, searchRiskAssessmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentService/SearchRiskAssessments")
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	startSubscriptionPlanWorkflowRequest := &subscriptionsv1.StartSubscriptionPlanWorkflowRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.StartSubscriptionPlanWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: (&decoded).GetActor().GetEmail()},
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner:      &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Parameters: searchRiskAssessmentsReply.GetResult().GetDetails(),
			Audit:      &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
	}
	startSubscriptionPlanWorkflowReply, err := subscriptionsv1.SubscriptionPlanWorkflowServiceClient.StartSubscriptionPlanWorkflow(s.subscriptionPlanWorkflowClient, ctx, startSubscriptionPlanWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/StartSubscriptionPlanWorkflow")
	}
	return &fraudv1.GetFraudSignalLifecycleResponse{
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: searchRiskAssessmentsReply.GetResult().GetCorrelationId(),
			Subject: &fraudv1.FraudRuleRef{
				Version: startSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetVersion(),
			},
			FraudSignalId: req.GetFraudSignalId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: searchRiskAssessmentsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ScheduleFraudSignalExpiry schedules a fraud signal.
func (s FraudSignalLifecycleServer) ScheduleFraudSignalExpiry(ctx context.Context, req *fraudv1.ScheduleFraudSignalExpiryRequest) (*fraudv1.ScheduleFraudSignalExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "schedule fraud signal expiry", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetFraudSignal().GetAttributes()))
	for key, value := range req.GetFraudSignal().GetAttributes() {
		attributes[key] = value
	}
	findRiskAssessmentsRequest := &fraudv1.FindRiskAssessmentsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.FindRiskAssessmentsRequest_Filter{
			CreatedAfter: req.GetFraudSignal().GetOccurredAt(),
			Labels:       attributes,
		},
	}
	findRiskAssessmentsReply := new(fraudv1.FindRiskAssessmentsResponse)
	if err := s.conn.Invoke(ctx, fraudRiskAssessmentQueryServiceFindRiskAssessmentsMethod, findRiskAssessmentsRequest, findRiskAssessmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentQueryService/FindRiskAssessments")
	}
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listCarrierRateWorkflowsRequest := &shippingv1.ListCarrierRateWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListCarrierRateWorkflowsRequest_Filter{
			CreatedAfter: req.GetFraudSignal().GetOccurredAt(),
			OwnerId:      findRiskAssessmentsReply.GetResult().GetCorrelationId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listCarrierRateWorkflowsReply := new(shippingv1.ListCarrierRateWorkflowsResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.CarrierRateWorkflowService_ListCarrierRateWorkflows_FullMethodName, listCarrierRateWorkflowsRequest, listCarrierRateWorkflowsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateWorkflowService/ListCarrierRateWorkflows")
	}
	return &fraudv1.ScheduleFraudSignalExpiryResponse{
		Result: &commonv1.OperationResult{Status: findRiskAssessmentsReply.GetResult().GetStatus()},
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			OccurredAt:    req.GetFraudSignal().GetOccurredAt(),
		},
	}, nil
}

// fraudSignalLifecycleServerTerminateFraudSignalSimulateWatchlistStep carries the request of one step of
// TerminateFraudSignal and, once it ran, its reply.
type fraudSignalLifecycleServerTerminateFraudSignalSimulateWatchlistStep struct {
	request *fraudv1.SimulateWatchlistRequest
	reply   *fraudv1.SimulateWatchlistResponse
}

func (s FraudSignalLifecycleServer) runTerminateFraudSignalSimulateWatchlist(ctx context.Context, step *fraudSignalLifecycleServerTerminateFraudSignalSimulateWatchlistStep) error {
	reply, err := s.watchlistCalculationClient.SimulateWatchlist(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
