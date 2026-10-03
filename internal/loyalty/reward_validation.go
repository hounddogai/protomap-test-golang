package loyalty

import (
	"context"
	"fmt"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RewardValidationServer implements `acme.loyalty.v1.RewardValidationService`.
// Checks rewards against the rules before anything acts on them.
type RewardValidationServer struct {
	loyaltyv1.UnimplementedRewardValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	pointsLedgerValidationClient    loyaltyv1.PointsLedgerValidationServiceClient
	metricSeriesQueryPort           metricSeriesQueryPort
	accessPolicyQueryClient         authorizationv1.AccessPolicyQueryServiceClient
	conn                            *grpc.ClientConn
	reviewModerationQueryClient     reviewsv1.ReviewModerationQueryServiceClient
	meteredPointsLedgerCalculation  meteredPointsLedgerCalculation
	loyaltyMemberCalculationPort    loyaltyMemberCalculationPort
	sellerScorecardValidationClient sellersv1.SellerScorecardValidationServiceClient
	loyaltyMemberCalculationClient  loyaltyv1.LoyaltyMemberCalculationServiceClient
	driverClient                    driversv1.DriverServiceClient
}

// NewRewardValidationServer wires a RewardValidationServer.
func NewRewardValidationServer(deps platform.Deps) *RewardValidationServer {
	return &RewardValidationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.loyalty.v1.RewardValidationService"),
		pointsLedgerValidationClient:    loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
		metricSeriesQueryPort:           newMetricSeriesQueryPort(deps),
		accessPolicyQueryClient:         authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		conn:                            deps.Conn,
		reviewModerationQueryClient:     reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn),
		meteredPointsLedgerCalculation:  meteredPointsLedgerCalculation{PointsLedgerCalculationServiceClient: loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		loyaltyMemberCalculationPort:    newLoyaltyMemberCalculationPort(deps),
		sellerScorecardValidationClient: sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		loyaltyMemberCalculationClient:  loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		driverClient:                    driversv1.NewDriverServiceClient(deps.Conn),
	}
}

// ValidateReward validates a reward.
func (s *RewardValidationServer) ValidateReward(ctx context.Context, req *loyaltyv1.ValidateRewardRequest) (*loyaltyv1.ValidateRewardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "validate reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	previewPointsLedgerReply, err := s.pointsLedgerValidationClient.PreviewPointsLedger(ctx, &loyaltyv1.PreviewPointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetReward().GetAudit().GetUpdatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/PreviewPointsLedger")
	}
	return &loyaltyv1.ValidateRewardResponse{
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: previewPointsLedgerReply.GetPointsLedger().GetAudit().GetUpdatedAt(),
			},
			Version:  previewPointsLedgerReply.GetPointsLedger().GetHolder().GetVersion(),
			RewardId: req.GetRewardId(),
			Name:     req.GetReward().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewReward previews a reward.
func (s *RewardValidationServer) PreviewReward(ctx context.Context, req *loyaltyv1.PreviewRewardRequest) (*loyaltyv1.PreviewRewardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "preview reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reward_id", req.GetReward().GetRewardId())
	parameters := make(map[string]string, len(req.GetReward().GetParameters()))
	for key, value := range req.GetReward().GetParameters() {
		parameters[key] = value
	}
	suggestMetricSeriesesRequest := &metricsv1.SuggestMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.SuggestMetricSeriesesRequest_Filter{
			CreatedAfter: req.GetReward().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "reward_id"),
			Labels:       parameters,
		},
	}
	suggestMetricSeriesesReply, err := s.metricSeriesQueryPort.SuggestMetricSerieses(ctx, suggestMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SuggestMetricSerieses")
	}
	rewardCache := cache.New[string, *loyaltyv1.Reward](1)
	rewardCache.Put(req.GetContext().GetTraceId(), req.GetReward())
	cachedReward, _ := rewardCache.Get(req.GetContext().GetTraceId())
	countAccessPoliciesReply, err := s.accessPolicyQueryClient.CountAccessPolicies(ctx, &authorizationv1.CountAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.CountAccessPoliciesRequest_Filter{
			CreatedAfter: req.GetReward().GetAudit().GetCreatedAt(),
			OwnerId:      cachedReward.GetOwner().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/CountAccessPolicies")
	}
	return &loyaltyv1.PreviewRewardResponse{
		Result: &commonv1.OperationResult{
			Status:  suggestMetricSeriesesReply.GetResult().GetStatus(),
			Message: countAccessPoliciesReply.GetResult().GetMessage(),
		},
		Reward: &loyaltyv1.Reward{
			RewardId:   req.GetRewardId(),
			Parameters: req.GetReward().GetParameters(),
		},
	}, nil
}

// CheckRewardEligibility checks a reward.
func (s *RewardValidationServer) CheckRewardEligibility(ctx context.Context, req *loyaltyv1.CheckRewardEligibilityRequest) (*loyaltyv1.CheckRewardEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "check reward eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "updated_by", req.GetReward().GetAudit().GetUpdatedBy())
	cloned := proto.Clone(req.GetDetails()).(*loyaltyv1.CheckRewardEligibilityRequest_Details)
	scorePointsLedgerRequest := &loyaltyv1.ScorePointsLedgerRequest{}
	scorePointsLedgerRequest.Context = req.GetContext()
	scorePointsLedgerRequest.PointsLedger = &loyaltyv1.PointsLedger{
		Audit: &commonv1.AuditInfo{
			CreatedAt: req.GetReward().GetAudit().GetCreatedAt(),
			UpdatedBy: tenancy.Value(ctx, "updated_by"),
		},
		Holder:  &loyaltyv1.RewardRef{DisplayName: req.GetReward().GetOwner().GetDisplayName()},
		Contact: &commonv1.ContactInfo{Name: cloned.GetContact().GetName()},
	}
	scorePointsLedgerReply, err := s.pointsLedgerValidationClient.ScorePointsLedger(ctx, scorePointsLedgerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ScorePointsLedger")
	}
	recalculateLoyaltyMembersRequest := &loyaltyv1.RecalculateLoyaltyMembersRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	recalculateLoyaltyMembersReply := new(loyaltyv1.RecalculateLoyaltyMembersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", loyaltyLoyaltyMemberCalculationServiceName, "RecalculateLoyaltyMembers"), recalculateLoyaltyMembersRequest, recalculateLoyaltyMembersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/RecalculateLoyaltyMembers")
	}
	explainPointsLedgerDecisionRequest := &loyaltyv1.ExplainPointsLedgerDecisionRequest{}
	explainPointsLedgerDecisionRequest.Context = req.GetContext()
	explainPointsLedgerDecisionRequest.PointsLedger = &loyaltyv1.PointsLedger{
		Funding: &loyaltyv1.PointsLedger_Bank{Bank: &loyaltyv1.PointsLedgerBankFunding{
			Iban: recalculateLoyaltyMembersReply.GetLoyaltyMember().GetBank().GetIban(),
		}},
		Contact: &commonv1.ContactInfo{
			PreferredChannel: recalculateLoyaltyMembersReply.GetLoyaltyMember().GetContact().GetPreferredChannel(),
		},
	}
	explainPointsLedgerDecisionReply, err := s.pointsLedgerValidationClient.ExplainPointsLedgerDecision(ctx, explainPointsLedgerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ExplainPointsLedgerDecision")
	}
	findReviewModerationsRequest := buildFindReviewModerationsRequestForCheckRewardEligibility(req, scorePointsLedgerReply)
	findReviewModerationsReply, err := s.reviewModerationQueryClient.FindReviewModerations(ctx, findReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/FindReviewModerations")
	}
	return &loyaltyv1.CheckRewardEligibilityResponse{
		Result: &commonv1.OperationResult{
			Details: scorePointsLedgerReply.GetResult().GetDetails(),
			Message: recalculateLoyaltyMembersReply.GetResult().GetMessage(),
			Status:  explainPointsLedgerDecisionReply.GetOutcome().GetStatus(),
		},
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainPointsLedgerDecisionReply.GetPointsLedger().GetAudit().GetUpdatedBy(),
			},
			RewardId:  req.GetRewardId(),
			Effective: req.GetReward().GetEffective(),
		},
		Outcome: &loyaltyv1.CheckRewardEligibilityResponse_Outcome{
			Message: findReviewModerationsReply.GetResult().GetMessage(),
		},
	}, nil
}

// NormalizeReward normalizes a reward.
func (s *RewardValidationServer) NormalizeReward(ctx context.Context, req *loyaltyv1.NormalizeRewardRequest) (*loyaltyv1.NormalizeRewardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "normalize reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	parameters := make(map[string]string, len(req.GetReward().GetParameters()))
	for key, value := range req.GetReward().GetParameters() {
		parameters[key] = value
	}
	countGiftCardsRequest := &giftcardsv1.CountGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.CountGiftCardsRequest_Filter{
			CreatedAfter: req.GetReward().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetReward().GetRewardId(),
			Labels:       parameters,
		},
	}
	countGiftCardsReply := new(giftcardsv1.CountGiftCardsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", giftcardsGiftCardQueryServiceName, "CountGiftCards"), countGiftCardsRequest, countGiftCardsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/CountGiftCards")
	}
	simulatePointsLedgerRequest := &loyaltyv1.SimulatePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetReward().GetAudit().GetUpdatedAt()},
		},
	}
	simulatePointsLedgerReply, err := s.meteredPointsLedgerCalculation.SimulatePointsLedger(ctx, simulatePointsLedgerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/SimulatePointsLedger")
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetReward().GetAudit().GetUpdatedBy())
	calculateLoyaltyMemberRequest := &loyaltyv1.CalculateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				Phone: simulatePointsLedgerReply.GetPointsLedger().GetContact().GetPhone(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: tenancy.Value(ctx, "updated_by")},
		},
	}
	calculateLoyaltyMemberReply, err := s.loyaltyMemberCalculationPort.CalculateLoyaltyMember(ctx, calculateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	return &loyaltyv1.NormalizeRewardResponse{
		Result: &commonv1.OperationResult{
			Message: countGiftCardsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &loyaltyv1.NormalizeRewardResponse_Outcome{
			Message: countGiftCardsReply.GetResult().GetMessage(),
			Status:  calculateLoyaltyMemberReply.GetResult().GetStatus(),
		},
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulatePointsLedgerReply.GetPointsLedger().GetAudit().GetCreatedAt(),
				UpdatedAt: calculateLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
			},
			RewardId:    req.GetRewardId(),
			Description: req.GetReward().GetDescription(),
		},
	}, nil
}

// ExplainRewardDecision explains a reward.
func (s *RewardValidationServer) ExplainRewardDecision(ctx context.Context, req *loyaltyv1.ExplainRewardDecisionRequest) (*loyaltyv1.ExplainRewardDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "explain reward decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	scoreSellerScorecardRequest := buildScoreSellerScorecardRequestForExplainRewardDecision(req)
	scoreSellerScorecardReply, err := s.sellerScorecardValidationClient.ScoreSellerScorecard(ctx, scoreSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ScoreSellerScorecard")
	}
	explainLoyaltyMemberCalculationReply, err := s.loyaltyMemberCalculationClient.ExplainLoyaltyMemberCalculation(ctx, &loyaltyv1.ExplainLoyaltyMemberCalculationRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{
				Network: scoreSellerScorecardReply.GetSellerScorecard().GetCard().GetNetwork(),
			}},
			Limits:  []*loyaltyv1.LoyaltyMemberLimit{&loyaltyv1.LoyaltyMemberLimit{Amount: req.GetReward().GetDiscount().GetAmount()}},
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
			Audit: &commonv1.AuditInfo{
				Etag: scoreSellerScorecardReply.GetSellerScorecard().GetAudit().GetEtag(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/ExplainLoyaltyMemberCalculation")
	}
	return &loyaltyv1.ExplainRewardDecisionResponse{
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				DisplayName: scoreSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreSellerScorecardReply.GetSellerScorecard().GetAudit().GetUpdatedBy(),
			},
			RewardId: req.GetRewardId(),
			Version:  req.GetReward().GetVersion(),
		},
		Outcome: &loyaltyv1.ExplainRewardDecisionResponse_Outcome{
			Message:     explainLoyaltyMemberCalculationReply.GetResult().GetMessage(),
			ReferenceId: explainLoyaltyMemberCalculationReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreReward scores a reward.
func (s *RewardValidationServer) ScoreReward(ctx context.Context, req *loyaltyv1.ScoreRewardRequest) (*loyaltyv1.ScoreRewardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreReward", "error", err)
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "score reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	detailsCache := cache.New[string, *loyaltyv1.ScoreRewardRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	batchCreateDriversStream, err := s.driverClient.BatchCreateDrivers(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverService/BatchCreateDrivers")
	}
	if err := batchCreateDriversStream.Send(&driversv1.BatchCreateDriversRequest{
		Context: req.GetContext(),
		Chunk: &driversv1.BatchCreateDriversRequest_Record{Record: &driversv1.Driver{
			PrimaryAddress: &commonv1.PostalAddress{Region: cachedDetails.GetDestination().GetRegion()},
			Audit:          &commonv1.AuditInfo{CreatedBy: req.GetReward().GetAudit().GetCreatedBy()},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverService/BatchCreateDrivers")
	}
	if _, err := batchCreateDriversStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverService/BatchCreateDrivers")
	}
	countSearchQueriesRequest := &searchv1.CountSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.CountSearchQueriesRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetReward().GetAudit().GetCreatedAt(),
		},
	}
	countSearchQueriesReply := new(searchv1.CountSearchQueriesResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SearchQueryQueryService_CountSearchQueries_FullMethodName, countSearchQueriesRequest, countSearchQueriesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/CountSearchQueries")
	}
	return &loyaltyv1.ScoreRewardResponse{
		Result: &commonv1.OperationResult{
			Details: countSearchQueriesReply.GetResult().GetDetails(),
			Message: countSearchQueriesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Reward: &loyaltyv1.Reward{
			RewardId: req.GetRewardId(),
			Status:   req.GetReward().GetStatus(),
		},
	}, nil
}

// GetRewardRules returns a reward.
func (s *RewardValidationServer) GetRewardRules(ctx context.Context, req *loyaltyv1.GetRewardRulesRequest) (*loyaltyv1.GetRewardRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetRewardRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "get_reward_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get reward rules", "tenant", req.GetContext().GetTenantId())
	return &loyaltyv1.GetRewardRulesResponse{
		Reward: &loyaltyv1.Reward{RewardId: req.GetRewardId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
