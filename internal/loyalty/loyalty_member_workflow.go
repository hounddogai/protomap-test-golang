package loyalty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	performancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	performersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/performers/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// LoyaltyMemberWorkflowServer implements `acme.loyalty.v1.LoyaltyMemberWorkflowService`.
// Runs the long-lived workflow that takes a loyalty member from start to finish.
type LoyaltyMemberWorkflowServer struct {
	loyaltyv1.UnimplementedLoyaltyMemberWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	performerClient                 performersv1.PerformerServiceClient
	loyaltyMemberQueryClient        loyaltyv1.LoyaltyMemberQueryServiceClient
	metricSeriesQueryClient         metricsv1.MetricSeriesQueryServiceClient
	userClient                      identityv1.UserServiceClient
	rewardLifecycleClient           loyaltyv1.RewardLifecycleServiceClient
	conn                            *grpc.ClientConn
	roleHistoryClient               identityv1.RoleHistoryServiceClient
	clients                         *registry.Registry
	onboardingPlanCalculationClient onboardingv1.OnboardingPlanCalculationServiceClient
	bus                             *eventbus.Bus
	reviewVoteWorkflowClient        reviewsv1.ReviewVoteWorkflowServiceClient
	pointsLedgerQueryClient         loyaltyv1.PointsLedgerQueryServiceClient
}

// NewLoyaltyMemberWorkflowServer wires a LoyaltyMemberWorkflowServer.
func NewLoyaltyMemberWorkflowServer(deps platform.Deps) *LoyaltyMemberWorkflowServer {
	return &LoyaltyMemberWorkflowServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.loyalty.v1.LoyaltyMemberWorkflowService"),
		performerClient:                 performersv1.NewPerformerServiceClient(deps.Conn),
		loyaltyMemberQueryClient:        loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn),
		metricSeriesQueryClient:         metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		userClient:                      identityv1.NewUserServiceClient(deps.Conn),
		rewardLifecycleClient:           loyaltyv1.NewRewardLifecycleServiceClient(deps.Conn),
		conn:                            deps.Conn,
		roleHistoryClient:               identityv1.NewRoleHistoryServiceClient(deps.Conn),
		clients:                         deps.Clients,
		onboardingPlanCalculationClient: onboardingv1.NewOnboardingPlanCalculationServiceClient(deps.Conn),
		bus:                             deps.Bus,
		reviewVoteWorkflowClient:        reviewsv1.NewReviewVoteWorkflowServiceClient(deps.Conn),
		pointsLedgerQueryClient:         loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
	}
}

// StartLoyaltyMemberWorkflow starts a loyalty member.
func (s *LoyaltyMemberWorkflowServer) StartLoyaltyMemberWorkflow(ctx context.Context, req *loyaltyv1.StartLoyaltyMemberWorkflowRequest) (*loyaltyv1.StartLoyaltyMemberWorkflowResponse, error) {
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "start loyalty member workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetLoyaltyMember().GetLabels()))
	for key, value := range req.GetLoyaltyMember().GetLabels() {
		labels[key] = value
	}
	encoded, err := protojson.Marshal(req.GetLoyaltyMember())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded loyaltyv1.LoyaltyMember
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	searchPerformersReply, err := s.performerClient.SearchPerformers(ctx, &performersv1.SearchPerformersRequest{
		Context: req.GetContext(),
		Filter: &performersv1.SearchPerformersRequest_Filter{
			Labels:       labels,
			CreatedAfter: (&decoded).GetAudit().GetCreatedAt(),
			OwnerId:      req.GetLoyaltyMemberId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.performers.v1.PerformerService/SearchPerformers")
	}
	exportLoyaltyMembersStream, err := s.loyaltyMemberQueryClient.ExportLoyaltyMembers(ctx, &loyaltyv1.ExportLoyaltyMembersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ExportLoyaltyMembersRequest_Filter{
			Labels:  req.GetLoyaltyMember().GetLabels(),
			OwnerId: req.GetLoyaltyMember().GetHolder().GetRewardId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/ExportLoyaltyMembers")
	}
	for {
		received, err := exportLoyaltyMembersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/ExportLoyaltyMembers")
		}
		_ = received
	}
	return &loyaltyv1.StartLoyaltyMemberWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: searchPerformersReply.GetResult().GetCorrelationId(),
			Message:       searchPerformersReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Holder:          req.GetLoyaltyMember().GetHolder(),
		},
	}, nil
}

// AdvanceLoyaltyMemberWorkflow advances a loyalty member.
func (s *LoyaltyMemberWorkflowServer) AdvanceLoyaltyMemberWorkflow(ctx context.Context, req *loyaltyv1.AdvanceLoyaltyMemberWorkflowRequest) (*loyaltyv1.AdvanceLoyaltyMemberWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "advance loyalty member workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetLoyaltyMember().GetLabels()))
	for key, value := range req.GetLoyaltyMember().GetLabels() {
		labels[key] = value
	}
	countMetricSeriesesRequest := &metricsv1.CountMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter:  &metricsv1.CountMetricSeriesesRequest_Filter{Labels: labels},
	}
	var countMetricSeriesesReply *metricsv1.CountMetricSeriesesResponse
	if err := pipeline.Run(ctx, "acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses", func(ctx context.Context) error {
		answer, err := s.metricSeriesQueryClient.CountMetricSerieses(ctx, countMetricSeriesesRequest)
		countMetricSeriesesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses")
	}
	watchUsersStream, err := s.userClient.WatchUsers(ctx, &identityv1.WatchUsersRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: req.GetLoyaltyMember().GetContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
	}
	for {
		received, err := watchUsersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
		}
		_ = received
	}
	cloned := proto.Clone(req.GetLoyaltyMember()).(*loyaltyv1.LoyaltyMember)
	activateRewardRequest := &loyaltyv1.ActivateRewardRequest{
		Context:  req.GetContext(),
		RewardId: cloned.GetHolder().GetRewardId(),
		Details: &loyaltyv1.ActivateRewardRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetLoyaltyMember().GetHolder().GetDisplayName(),
			},
		},
	}
	send := s.activateRewardSender(activateRewardRequest)
	activateRewardReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/ActivateReward")
	}
	_ = activateRewardReply
	return &loyaltyv1.AdvanceLoyaltyMemberWorkflowResponse{
		Outcome: &loyaltyv1.AdvanceLoyaltyMemberWorkflowResponse_Outcome{Message: countMetricSeriesesReply.GetResult().GetMessage()},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			CurrencyCode:    req.GetLoyaltyMember().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelLoyaltyMemberWorkflow cancels a loyalty member.
func (s *LoyaltyMemberWorkflowServer) CancelLoyaltyMemberWorkflow(ctx context.Context, req *loyaltyv1.CancelLoyaltyMemberWorkflowRequest) (*loyaltyv1.CancelLoyaltyMemberWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "cancel loyalty member workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	recallLoyaltyMemberRequest := &loyaltyv1.RecallLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Name: req.GetLoyaltyMember().GetContact().GetName()},
		},
		Details: &loyaltyv1.RecallLoyaltyMemberRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	recallLoyaltyMemberReply := new(loyaltyv1.RecallLoyaltyMemberResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.loyalty.v1.LoyaltyMemberApprovalService/RecallLoyaltyMember", recallLoyaltyMemberRequest, recallLoyaltyMemberReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberApprovalService/RecallLoyaltyMember")
	}
	reinstateRewardRequest := &loyaltyv1.ReinstateRewardRequest{
		Context:  req.GetContext(),
		RewardId: req.GetLoyaltyMember().GetHolder().GetRewardId(),
		Details: &loyaltyv1.ReinstateRewardRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Phone: recallLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetPhone(),
				Name:  req.GetLoyaltyMember().GetContact().GetName(),
			},
		},
	}
	reinstateRewardReply, err := reinstateRewardCall(ctx, reinstateRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/ReinstateReward")
	}
	cloned := proto.Clone(req.GetLoyaltyMember()).(*loyaltyv1.LoyaltyMember)
	listRoleVersionsRequest := &identityv1.ListRoleVersionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListRoleVersionsRequest_Filter{
			Labels:       recallLoyaltyMemberReply.GetLoyaltyMember().GetLabels(),
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
		},
	}
	listRoleVersionsReply, err := grpcx.Call(ctx, s.roleHistoryClient.ListRoleVersions, listRoleVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/ListRoleVersions")
	}
	return &loyaltyv1.CancelLoyaltyMemberWorkflowResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact:         recallLoyaltyMemberReply.GetLoyaltyMember().GetContact(),
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			CurrencyCode:    req.GetLoyaltyMember().GetCurrencyCode(),
		},
		Outcome: &loyaltyv1.CancelLoyaltyMemberWorkflowResponse_Outcome{NextSteps: reinstateRewardReply.GetOutcome().GetNextSteps()},
		Result: &commonv1.OperationResult{
			Details: listRoleVersionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// RetryLoyaltyMemberWorkflow retrys a loyalty member.
func (s *LoyaltyMemberWorkflowServer) RetryLoyaltyMemberWorkflow(ctx context.Context, req *loyaltyv1.RetryLoyaltyMemberWorkflowRequest) (*loyaltyv1.RetryLoyaltyMemberWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "retry loyalty member workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	reviewQuery, err := registry.Lookup[performancev1.ReviewQueryServiceClient](s.clients, "acme.performance.v1.ReviewQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resolveReviewsRequest := &performancev1.ResolveReviewsRequest{Context: req.GetContext()}
	resolveReviewsReply, err := reviewQuery.ResolveReviews(ctx, resolveReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.performance.v1.ReviewQueryService/ResolveReviews")
	}
	var accountHolder string
	switch choice := req.GetLoyaltyMember().GetFunding().(type) {
	case *loyaltyv1.LoyaltyMember_Card:
		accountHolder = choice.Card.GetHolderName()
	case *loyaltyv1.LoyaltyMember_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *loyaltyv1.LoyaltyMember_ExternalAccountId:
		accountHolder = choice.ExternalAccountId
	}
	explainOnboardingPlanCalculationRequest := &onboardingv1.ExplainOnboardingPlanCalculationRequest{}
	explainOnboardingPlanCalculationRequest.Context = req.GetContext()
	explainOnboardingPlanCalculationRequest.OnboardingPlan = &onboardingv1.OnboardingPlan{
		Settlement: &onboardingv1.OnboardingPlan_Bank{Bank: &onboardingv1.OnboardingPlanBankSettlement{
			Bic:           req.GetLoyaltyMember().GetBank().GetBic(),
			AccountHolder: accountHolder,
		}},
		Audit: &commonv1.AuditInfo{CreatedAt: req.GetLoyaltyMember().GetAudit().GetCreatedAt()},
	}
	explainOnboardingPlanCalculationRequest.Details = &onboardingv1.ExplainOnboardingPlanCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
	}
	explainOnboardingPlanCalculationReply, err := s.onboardingPlanCalculationClient.ExplainOnboardingPlanCalculation(ctx, explainOnboardingPlanCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanCalculationService/ExplainOnboardingPlanCalculation")
	}
	return &loyaltyv1.RetryLoyaltyMemberWorkflowResponse{
		Result: &commonv1.OperationResult{Status: resolveReviewsReply.GetResult().GetStatus()},
		Outcome: &loyaltyv1.RetryLoyaltyMemberWorkflowResponse_Outcome{
			ReferenceId: explainOnboardingPlanCalculationReply.GetOutcome().GetReferenceId(),
			Message:     explainOnboardingPlanCalculationReply.GetResult().GetMessage(),
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Balance:         req.GetLoyaltyMember().GetBalance(),
		},
	}, nil
}

// PauseLoyaltyMemberWorkflow pauses a loyalty member.
func (s *LoyaltyMemberWorkflowServer) PauseLoyaltyMemberWorkflow(ctx context.Context, req *loyaltyv1.PauseLoyaltyMemberWorkflowRequest) (*loyaltyv1.PauseLoyaltyMemberWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "pause loyalty member workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	venueValidation, err := registry.Lookup[venuesv1.VenueValidationServiceClient](s.clients, "acme.venues.v1.VenueValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	checkVenueEligibilityRequest := &venuesv1.CheckVenueEligibilityRequest{
		Context: req.GetContext(),
		Details: &venuesv1.CheckVenueEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetLoyaltyMember().GetContact().GetPhone()},
		},
		Venue: &venuesv1.Venue{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetLoyaltyMember().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	checkVenueEligibilityReply, err := venueValidation.CheckVenueEligibility(ctx, checkVenueEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/CheckVenueEligibility")
	}
	suggestPointsLedgersRequest := &loyaltyv1.SuggestPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.SuggestPointsLedgersRequest_Filter{
			Region: checkVenueEligibilityReply.GetVenue().GetAddress().GetRegion(),
		},
	}
	suggestPointsLedgersReply := new(loyaltyv1.SuggestPointsLedgersResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", loyaltyPointsLedgerQueryServiceName, "SuggestPointsLedgers"), suggestPointsLedgersRequest, suggestPointsLedgersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/SuggestPointsLedgers")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "loyalty.pause_loyalty_member_workflow.retry_gift_card_workflow",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"gift_card.audit.created_by": checkVenueEligibilityReply.GetVenue().GetAudit().GetCreatedBy(),
			"details.notify.phone":       req.GetDetails().GetNotify().GetPhone(),
			"gift_card.audit.updated_by": req.GetLoyaltyMember().GetAudit().GetUpdatedBy(),
			"context.tenant_id":          req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	cloned := proto.Clone(req.GetDetails()).(*loyaltyv1.PauseLoyaltyMemberWorkflowRequest_Details)
	startReviewVoteWorkflowRequest := &reviewsv1.StartReviewVoteWorkflowRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.StartReviewVoteWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: cloned.GetNotify().GetEmail()},
		},
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{Email: req.GetLoyaltyMember().GetContact().GetEmail()},
		},
	}
	startReviewVoteWorkflowReply, err := grpcx.Call(ctx, s.reviewVoteWorkflowClient.StartReviewVoteWorkflow, startReviewVoteWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteWorkflowService/StartReviewVoteWorkflow")
	}
	_ = startReviewVoteWorkflowReply
	return &loyaltyv1.PauseLoyaltyMemberWorkflowResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: checkVenueEligibilityReply.GetVenue().GetOwner().GetDisplayName(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Status:          req.GetLoyaltyMember().GetStatus(),
		},
		Outcome: &loyaltyv1.PauseLoyaltyMemberWorkflowResponse_Outcome{Status: suggestPointsLedgersReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			CorrelationId: suggestPointsLedgersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ResumeLoyaltyMemberWorkflow resumes a loyalty member.
func (s *LoyaltyMemberWorkflowServer) ResumeLoyaltyMemberWorkflow(ctx context.Context, req *loyaltyv1.ResumeLoyaltyMemberWorkflowRequest) (*loyaltyv1.ResumeLoyaltyMemberWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "resume loyalty member workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	browsePointsLedgersReply, err := s.pointsLedgerQueryClient.BrowsePointsLedgers(ctx, &loyaltyv1.BrowsePointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.BrowsePointsLedgersRequest_Filter{
			Labels:       req.GetLoyaltyMember().GetLabels(),
			CreatedAfter: req.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/BrowsePointsLedgers")
	}
	explainLedgerAccountDecisionRequest := &ledgerv1.ExplainLedgerAccountDecisionRequest{
		Context: req.GetContext(),
		Details: &ledgerv1.ExplainLedgerAccountDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotify().GetName(),
				Email: req.GetDetails().GetNotify().GetEmail(),
			},
		},
	}
	explainLedgerAccountDecisionReply := new(ledgerv1.ExplainLedgerAccountDecisionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", ledgerLedgerAccountValidationServiceName, "ExplainLedgerAccountDecision"), explainLedgerAccountDecisionRequest, explainLedgerAccountDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ExplainLedgerAccountDecision")
	}
	return &loyaltyv1.ResumeLoyaltyMemberWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browsePointsLedgersReply.GetResult().GetReferenceIds(),
			Status:       explainLedgerAccountDecisionReply.GetOutcome().GetStatus(),
			Message:      explainLedgerAccountDecisionReply.GetResult().GetMessage(),
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Labels:          req.GetLoyaltyMember().GetLabels(),
		},
	}, nil
}

// activateRewardSender returns a function that sends one request to
// acme.loyalty.v1.RewardLifecycleService/ActivateReward.
func (s *LoyaltyMemberWorkflowServer) activateRewardSender(request *loyaltyv1.ActivateRewardRequest) func(context.Context) (*loyaltyv1.ActivateRewardResponse, error) {
	return func(ctx context.Context) (*loyaltyv1.ActivateRewardResponse, error) {
		return s.rewardLifecycleClient.ActivateReward(ctx, request)
	}
}
