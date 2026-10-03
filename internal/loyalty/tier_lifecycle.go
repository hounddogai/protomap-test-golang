package loyalty

import (
	"context"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	schedulingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/scheduling/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TierLifecycleServer implements `acme.loyalty.v1.TierLifecycleService`.
// Moves a tier between the states of its lifecycle.
type TierLifecycleServer struct {
	loyaltyv1.UnimplementedTierLifecycleServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	tierCalculationOnce           sync.Once
	tierCalculationCached         loyaltyv1.TierCalculationServiceClient
	tierCalculationClient         loyaltyv1.TierCalculationServiceClient
	tierValidationClient          loyaltyv1.TierValidationServiceClient
	pointsLedgerCalculationClient loyaltyv1.PointsLedgerCalculationServiceClient
	tierReportingClient           loyaltyv1.TierReportingServiceClient
	metricSeriesValidationClient  metricsv1.MetricSeriesValidationServiceClient
	shiftQueryOnce                sync.Once
	shiftQueryCached              schedulingv1.ShiftQueryServiceClient
	loyaltyMemberQueryClient      loyaltyv1.LoyaltyMemberQueryServiceClient
	sellerApprovalClient          sellersv1.SellerApprovalServiceClient
}

// NewTierLifecycleServer wires a TierLifecycleServer.
func NewTierLifecycleServer(deps platform.Deps) *TierLifecycleServer {
	return &TierLifecycleServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.loyalty.v1.TierLifecycleService"),
		conn:                          deps.Conn,
		tierCalculationClient:         loyaltyv1.NewTierCalculationServiceClient(deps.Conn),
		tierValidationClient:          loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		pointsLedgerCalculationClient: loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		tierReportingClient:           loyaltyv1.NewTierReportingServiceClient(deps.Conn),
		metricSeriesValidationClient:  metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		loyaltyMemberQueryClient:      loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn),
		sellerApprovalClient:          sellersv1.NewSellerApprovalServiceClient(deps.Conn),
	}
}

// ActivateTier activates a tier.
func (s *TierLifecycleServer) ActivateTier(ctx context.Context, req *loyaltyv1.ActivateTierRequest) (*loyaltyv1.ActivateTierResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "activate_tier"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate tier", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key2, value2 := range req.GetContext().GetBaggage() {
		baggage[key2] = value2
	}
	summarizeAccessPoliciesRequest := &authorizationv1.SummarizeAccessPoliciesRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &authorizationv1.SummarizeAccessPoliciesRequest_Details{Filters: baggage},
	}
	summarizeAccessPoliciesReply, err := grpcx.NewClient(s.conn, authorizationv1.NewAccessPolicyQueryServiceClient).SummarizeAccessPolicies(ctx, summarizeAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/SummarizeAccessPolicies")
	}
	return &loyaltyv1.ActivateTierResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeAccessPoliciesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Tier:    &loyaltyv1.Tier{TierId: req.GetTierId()},
		Outcome: &loyaltyv1.ActivateTierResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendTier suspends a tier.
func (s *TierLifecycleServer) SuspendTier(ctx context.Context, req *loyaltyv1.SuspendTierRequest) (*loyaltyv1.SuspendTierResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "suspend_tier"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend tier", "tenant", req.GetContext().GetTenantId())
	estimateTierRequest := &loyaltyv1.EstimateTierRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
	}
	estimateTierReply, err := s.tierCalculationLazy().EstimateTier(ctx, estimateTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/EstimateTier")
	}
	simulateTierRequest := &loyaltyv1.SimulateTierRequest{}
	simulateTierRequest.Context = req.GetContext()
	simulateTierRequest.Tier = &loyaltyv1.Tier{
		Action: &loyaltyv1.Tier_Discount{Discount: &loyaltyv1.TierDiscountAction{
			Amount: estimateTierReply.GetTier().GetDiscount().GetAmount(),
		}},
		Audit: &commonv1.AuditInfo{
			UpdatedAt: estimateTierReply.GetTier().GetAudit().GetUpdatedAt(),
		},
	}
	simulateTierReply, err := s.tierCalculationClient.SimulateTier(ctx, simulateTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/SimulateTier")
	}
	return &loyaltyv1.SuspendTierResponse{
		Tier: &loyaltyv1.Tier{
			TierId: estimateTierReply.GetTier().GetTierId(),
			Owner: &identityv1.UserRef{
				Email:       estimateTierReply.GetTier().GetOwner().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: simulateTierReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &loyaltyv1.SuspendTierResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateTier reinstates a tier.
func (s *TierLifecycleServer) ReinstateTier(ctx context.Context, req *loyaltyv1.ReinstateTierRequest) (*loyaltyv1.ReinstateTierResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "reinstate_tier"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate tier", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*loyaltyv1.ReinstateTierRequest_Details)
	detailsCache := cache.New[string, *loyaltyv1.ReinstateTierRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	previewTierRequest := &loyaltyv1.PreviewTierRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				Email:       cloned.GetNewOwner().GetEmail(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				DisplayName: cachedDetails.GetNewOwner().GetDisplayName(),
			},
		},
	}
	previewTierReply, err := grpcx.Call(ctx, s.tierValidationClient.PreviewTier, previewTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/PreviewTier")
	}
	return &loyaltyv1.ReinstateTierResponse{
		Tier: &loyaltyv1.Tier{
			Name:   previewTierReply.GetTier().GetName(),
			Owner:  &identityv1.UserRef{UserId: previewTierReply.GetTier().GetOwner().GetUserId()},
			TierId: req.GetTierId(),
		},
		Outcome: &loyaltyv1.ReinstateTierResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TerminateTier terminates a tier.
func (s *TierLifecycleServer) TerminateTier(ctx context.Context, req *loyaltyv1.TerminateTierRequest) (*loyaltyv1.TerminateTierResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "terminate_tier"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate tier", "tenant", req.GetContext().GetTenantId())
	explainPointsLedgerCalculationReply, err := s.pointsLedgerCalculationClient.ExplainPointsLedgerCalculation(ctx, &loyaltyv1.ExplainPointsLedgerCalculationRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
				Email:            req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/ExplainPointsLedgerCalculation")
	}
	simulatePointsLedgerRequest := &loyaltyv1.SimulatePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: explainPointsLedgerCalculationReply.GetPointsLedger().GetContact().GetPreferredChannel(),
				Name:             explainPointsLedgerCalculationReply.GetPointsLedger().GetContact().GetName(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
			},
			Holder: &loyaltyv1.RewardRef{
				RewardId: explainPointsLedgerCalculationReply.GetPointsLedger().GetHolder().GetRewardId(),
			},
		},
	}
	simulatePointsLedgerReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewPointsLedgerCalculationServiceClient).SimulatePointsLedger(ctx, simulatePointsLedgerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/SimulatePointsLedger")
	}
	return &loyaltyv1.TerminateTierResponse{
		Result: &commonv1.OperationResult{
			Message: explainPointsLedgerCalculationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				DisplayName: simulatePointsLedgerReply.GetPointsLedger().GetHolder().GetDisplayName(),
				Email:       simulatePointsLedgerReply.GetPointsLedger().GetContact().GetEmail(),
			},
			TierId: req.GetTierId(),
		},
		Outcome: &loyaltyv1.TerminateTierResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewTier renews a tier.
func (s *TierLifecycleServer) RenewTier(ctx context.Context, req *loyaltyv1.RenewTierRequest) (*loyaltyv1.RenewTierResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "renew_tier"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew tier", "tenant", req.GetContext().GetTenantId())
	scheduleTierReportReply, err := s.tierReportingClient.ScheduleTierReport(ctx, &loyaltyv1.ScheduleTierReportRequest{
		Context:     req.GetContext(),
		TierId:      req.GetTierId(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/ScheduleTierReport")
	}
	previewMetricSeriesRequest := &metricsv1.PreviewMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
		Details: &metricsv1.PreviewMetricSeriesRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
		},
	}
	previewMetricSeriesReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*metricsv1.PreviewMetricSeriesResponse, error) {
		return s.metricSeriesValidationClient.PreviewMetricSeries(ctx, previewMetricSeriesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/PreviewMetricSeries")
	}
	batchGetAccessPoliciesRequest := &authorizationv1.BatchGetAccessPoliciesRequest{Context: req.GetContext()}
	batchGetAccessPoliciesReply := new(authorizationv1.BatchGetAccessPoliciesResponse)
	if err := s.conn.Invoke(ctx, authorizationAccessPolicyServiceBatchGetAccessPoliciesMethod, batchGetAccessPoliciesRequest, batchGetAccessPoliciesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/BatchGetAccessPolicies")
	}
	return &loyaltyv1.RenewTierResponse{
		Result: &commonv1.OperationResult{
			Status:        scheduleTierReportReply.GetResult().GetStatus(),
			Details:       previewMetricSeriesReply.GetResult().GetDetails(),
			CorrelationId: batchGetAccessPoliciesReply.GetResult().GetCorrelationId(),
			ReferenceIds:  batchGetAccessPoliciesReply.GetResult().GetReferenceIds(),
		},
		Tier:    &loyaltyv1.Tier{TierId: req.GetTierId()},
		Outcome: &loyaltyv1.RenewTierResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireTier expires a tier.
func (s *TierLifecycleServer) ExpireTier(ctx context.Context, req *loyaltyv1.ExpireTierRequest) (*loyaltyv1.ExpireTierResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "expire_tier"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire tier", "tenant", req.GetContext().GetTenantId())
	suggestShiftsRequest := &schedulingv1.SuggestShiftsRequest{
		Context: req.GetContext(),
		Filter:  &schedulingv1.SuggestShiftsRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()},
	}
	suggestShiftsReply, err := s.shiftQueryLazy().SuggestShifts(ctx, suggestShiftsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.scheduling.v1.ShiftQueryService/SuggestShifts")
	}
	detailsCache := cache.New[string, *loyaltyv1.ExpireTierRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	suggestLoyaltyMembersRequest := newSuggestLoyaltyMembersRequest(withSuggestLoyaltyMembersContext(req.GetContext()), withSuggestLoyaltyMembersFilter(&loyaltyv1.SuggestLoyaltyMembersRequest_Filter{
		CreatedAfter: cachedDetails.GetEffectiveAt(),
		OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		Labels:       suggestShiftsReply.GetResult().GetDetails(),
	}))
	suggestLoyaltyMembersReply, err := s.loyaltyMemberQueryClient.SuggestLoyaltyMembers(ctx, suggestLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/SuggestLoyaltyMembers")
	}
	explainTierDecisionRequest := &loyaltyv1.ExplainTierDecisionRequest{}
	explainTierDecisionRequest.Context = req.GetContext()
	explainTierDecisionRequest.Details = &loyaltyv1.ExplainTierDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
	}
	explainTierDecisionReply, err := s.tierValidationClient.ExplainTierDecision(ctx, explainTierDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/ExplainTierDecision")
	}
	delegateSellerApprovalRequest := &sellersv1.DelegateSellerApprovalRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				UserId: explainTierDecisionReply.GetTier().GetOwner().GetUserId(),
			},
		},
	}
	send := s.delegateSellerApprovalSender(delegateSellerApprovalRequest)
	delegateSellerApprovalReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/DelegateSellerApproval")
	}
	return &loyaltyv1.ExpireTierResponse{
		Outcome: &loyaltyv1.ExpireTierResponse_Outcome{
			Status:      suggestShiftsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status:       suggestShiftsReply.GetResult().GetStatus(),
			ReferenceIds: suggestLoyaltyMembersReply.GetResult().GetReferenceIds(),
			Message:      delegateSellerApprovalReply.GetOutcome().GetMessage(),
		},
		Tier: &loyaltyv1.Tier{TierId: req.GetTierId()},
	}, nil
}

// GetTierLifecycle returns a tier.
func (s *TierLifecycleServer) GetTierLifecycle(ctx context.Context, req *loyaltyv1.GetTierLifecycleRequest) (*loyaltyv1.GetTierLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetTierLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "get_tier_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get tier lifecycle", "tenant", req.GetContext().GetTenantId())
	scheduleTierReportRequest := &loyaltyv1.ScheduleTierReportRequest{
		Context:     req.GetContext(),
		TierId:      req.GetTierId(),
		Annotations: req.GetContext().GetBaggage(),
	}
	scheduleTierReportReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*loyaltyv1.ScheduleTierReportResponse, error) {
		return s.tierReportingClient.ScheduleTierReport(ctx, scheduleTierReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/ScheduleTierReport")
	}
	return &loyaltyv1.GetTierLifecycleResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scheduleTierReportReply.GetResult().GetReferenceIds(),
			Details:      scheduleTierReportReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Tier: &loyaltyv1.Tier{TierId: req.GetTierId()},
	}, nil
}

// tierCalculationLazy dials acme.loyalty.v1.TierCalculationService the first time a handler needs it.
func (s *TierLifecycleServer) tierCalculationLazy() loyaltyv1.TierCalculationServiceClient {
	s.tierCalculationOnce.Do(func() { s.tierCalculationCached = loyaltyv1.NewTierCalculationServiceClient(s.conn) })
	return s.tierCalculationCached
}

// shiftQueryLazy dials acme.scheduling.v1.ShiftQueryService the first time a handler needs it.
func (s *TierLifecycleServer) shiftQueryLazy() schedulingv1.ShiftQueryServiceClient {
	s.shiftQueryOnce.Do(func() { s.shiftQueryCached = schedulingv1.NewShiftQueryServiceClient(s.conn) })
	return s.shiftQueryCached
}

// delegateSellerApprovalSender returns a function that sends one request to
// acme.sellers.v1.SellerApprovalService/DelegateSellerApproval.
func (s *TierLifecycleServer) delegateSellerApprovalSender(request *sellersv1.DelegateSellerApprovalRequest) func(context.Context) (*sellersv1.DelegateSellerApprovalResponse, error) {
	return func(ctx context.Context) (*sellersv1.DelegateSellerApprovalResponse, error) {
		return s.sellerApprovalClient.DelegateSellerApproval(ctx, request)
	}
}
