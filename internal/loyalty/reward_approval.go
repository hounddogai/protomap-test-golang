package loyalty

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RewardApprovalServer implements `acme.loyalty.v1.RewardApprovalService`.
// Routes rewards through approval, with escalation and delegation.
type RewardApprovalServer struct {
	loyaltyv1.UnimplementedRewardApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	searchQueryNotificationClient  searchv1.SearchQueryNotificationServiceClient
	bus                            *eventbus.Bus
	conn                           *grpc.ClientConn
	tenantExportClient             identityv1.TenantExportServiceClient
	userQueryClient                identityv1.UserQueryServiceClient
	loyaltyMemberCalculationClient loyaltyv1.LoyaltyMemberCalculationServiceClient
	tierReportingClient            loyaltyv1.TierReportingServiceClient
	policyQueryClient              insurancev1.PolicyQueryServiceClient
	synonymNotificationClient      searchv1.SynonymNotificationServiceClient
	sellerScorecardReportingClient sellersv1.SellerScorecardReportingServiceClient
}

// NewRewardApprovalServer wires a RewardApprovalServer.
func NewRewardApprovalServer(deps platform.Deps) *RewardApprovalServer {
	return &RewardApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.loyalty.v1.RewardApprovalService"),
		searchQueryNotificationClient:  searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		bus:                            deps.Bus,
		conn:                           deps.Conn,
		tenantExportClient:             identityv1.NewTenantExportServiceClient(deps.Conn),
		userQueryClient:                identityv1.NewUserQueryServiceClient(deps.Conn),
		loyaltyMemberCalculationClient: loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		tierReportingClient:            loyaltyv1.NewTierReportingServiceClient(deps.Conn),
		policyQueryClient:              insurancev1.NewPolicyQueryServiceClient(deps.Conn),
		synonymNotificationClient:      searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		sellerScorecardReportingClient: sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
	}
}

// SubmitReward submits a reward.
func (s *RewardApprovalServer) SubmitReward(ctx context.Context, req *loyaltyv1.SubmitRewardRequest) (*loyaltyv1.SubmitRewardResponse, error) {
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "submit reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReward()).(*loyaltyv1.Reward)
	streamSearchQueryNoticesStream, err := s.searchQueryNotificationClient.StreamSearchQueryNotices(ctx, &searchv1.StreamSearchQueryNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		Since:      req.GetReward().GetAudit().GetCreatedAt(),
		Filter: &searchv1.StreamSearchQueryNoticesRequest_Filter{
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
			Labels:       req.GetReward().GetParameters(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
	}
	for {
		received, err := streamSearchQueryNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
		}
		_ = received
	}
	var holder struct{ Email string }
	if record := req.GetDetails().GetApprover(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	commentReviewVoteApprovalRequest := &reviewsv1.CommentReviewVoteApprovalRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.CommentReviewVoteApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  holder.Email,
				UserId: req.GetReward().GetOwner().GetUserId(),
			},
		},
	}
	commentReviewVoteApprovalReply, err := commentReviewVoteApprovalCall(ctx, commentReviewVoteApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/CommentReviewVoteApproval")
	}
	return &loyaltyv1.SubmitRewardResponse{
		Result: &commonv1.OperationResult{
			Message: commentReviewVoteApprovalReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Reward: &loyaltyv1.Reward{
			RewardId: req.GetRewardId(),
			Status:   req.GetReward().GetStatus(),
		},
	}, nil
}

// ApproveReward approves a reward.
func (s *RewardApprovalServer) ApproveReward(ctx context.Context, req *loyaltyv1.ApproveRewardRequest) (*loyaltyv1.ApproveRewardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "approve reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "loyalty.approve_reward.unsubscribe_synonym_updates",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.recipient.email": req.GetReward().GetOwner().GetEmail(),
			"context.tenant_id":       req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	purgeTenantRequest := &identityv1.PurgeTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	purgeTenantReply, err := grpcx.NewClient(s.conn, identityv1.NewTenantAdminServiceClient).PurgeTenant(ctx, purgeTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/PurgeTenant")
	}
	return &loyaltyv1.ApproveRewardResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: purgeTenantReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Reward: &loyaltyv1.Reward{
			RewardId:  req.GetRewardId(),
			Effective: req.GetReward().GetEffective(),
		},
	}, nil
}

// RejectReward rejects a reward.
func (s *RewardApprovalServer) RejectReward(ctx context.Context, req *loyaltyv1.RejectRewardRequest) (*loyaltyv1.RejectRewardResponse, error) {
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "reject reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	step := &rewardApprovalServerRejectRewardGetTenantExportStep{request: &identityv1.GetTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}}
	if err := s.runRejectRewardGetTenantExport(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/GetTenantExport")
	}
	getTenantExportReply := step.reply
	resolveUsersReply, err := s.userQueryClient.ResolveUsers(ctx, &identityv1.ResolveUsersRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ResolveUsers")
	}
	return &loyaltyv1.RejectRewardResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: getTenantExportReply.GetResult().GetCorrelationId(),
			Message:       getTenantExportReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Outcome: &loyaltyv1.RejectRewardResponse_Outcome{Message: resolveUsersReply.GetResult().GetMessage()},
		Reward: &loyaltyv1.Reward{
			RewardId:  req.GetRewardId(),
			Effective: req.GetReward().GetEffective(),
		},
	}, nil
}

// EscalateReward escalates a reward.
func (s *RewardApprovalServer) EscalateReward(ctx context.Context, req *loyaltyv1.EscalateRewardRequest) (*loyaltyv1.EscalateRewardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "escalate reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReward()).(*loyaltyv1.Reward)
	peekUserRequest := &identityv1.PeekUserRequest{}
	peekUserRequest.Context = req.GetContext()
	peekUserRequest.UserId = cloned.GetOwner().GetUserId()
	peekUserReply, err := s.userQueryClient.PeekUser(ctx, peekUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/PeekUser")
	}
	return &loyaltyv1.EscalateRewardResponse{
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				CreatedBy: peekUserReply.GetUser().GetAudit().GetCreatedBy(),
				CreatedAt: peekUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			RewardId:  req.GetRewardId(),
			Effective: req.GetReward().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingRewards lists rewards a page at a time.
func (s *RewardApprovalServer) ListPendingRewards(ctx context.Context, req *loyaltyv1.ListPendingRewardsRequest) (*loyaltyv1.ListPendingRewardsResponse, error) {
	s.log.InfoContext(ctx, "list pending rewards", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	explainLoyaltyMemberCalculationRequest := &loyaltyv1.ExplainLoyaltyMemberCalculationRequest{
		Context:       req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{Labels: labels},
		Details: &loyaltyv1.ExplainLoyaltyMemberCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	explainLoyaltyMemberCalculationReply, err := grpcx.Call(ctx, s.loyaltyMemberCalculationClient.ExplainLoyaltyMemberCalculation, explainLoyaltyMemberCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/ExplainLoyaltyMemberCalculation")
	}
	step := &rewardApprovalServerListPendingRewardsScheduleTierReportStep{request: &loyaltyv1.ScheduleTierReportRequest{
		Context:     req.GetContext(),
		Annotations: explainLoyaltyMemberCalculationReply.GetResult().GetDetails(),
	}}
	if err := s.runListPendingRewardsScheduleTierReport(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/ScheduleTierReport")
	}
	scheduleTierReportReply := step.reply
	return &loyaltyv1.ListPendingRewardsResponse{
		Rewards: []*loyaltyv1.Reward{&loyaltyv1.Reward{
			Version: explainLoyaltyMemberCalculationReply.GetLoyaltyMember().GetHolder().GetVersion(),
		}},
		Result: &commonv1.OperationResult{
			Status:        scheduleTierReportReply.GetResult().GetStatus(),
			CorrelationId: scheduleTierReportReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// DelegateRewardApproval delegates a reward.
func (s *RewardApprovalServer) DelegateRewardApproval(ctx context.Context, req *loyaltyv1.DelegateRewardApprovalRequest) (*loyaltyv1.DelegateRewardApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "delegate reward approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	summarizePoliciesRequest := &insurancev1.SummarizePoliciesRequest{}
	summarizePoliciesRequest.Context = req.GetContext()
	summarizePoliciesRequest.Reason = req.GetReason()
	summarizePoliciesRequest.Annotations = req.GetAnnotations()
	summarizePoliciesRequest.Details = &insurancev1.SummarizePoliciesRequest_Details{Period: req.GetReward().GetEffective()}
	summarizePoliciesReply, err := s.policyQueryClient.SummarizePolicies(ctx, summarizePoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.insurance.v1.PolicyQueryService/SummarizePolicies")
	}
	cloned := proto.Clone(req.GetReward()).(*loyaltyv1.Reward)
	streamTenantExportStream, err := s.tenantExportClient.StreamTenantExport(ctx, &identityv1.StreamTenantExportRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamTenantExportRequest_Filter{
			OwnerId:      cloned.GetOwner().GetUserId(),
			CreatedAfter: req.GetReward().GetAudit().GetUpdatedAt(),
			Labels:       summarizePoliciesReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
	}
	for {
		received, err := streamTenantExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
		}
		_ = received
	}
	return &loyaltyv1.DelegateRewardApprovalResponse{
		Result: &commonv1.OperationResult{Status: summarizePoliciesReply.GetResult().GetStatus()},
		Reward: &loyaltyv1.Reward{
			RewardId: req.GetRewardId(),
			Version:  req.GetReward().GetVersion(),
		},
	}, nil
}

// RecallReward recalls a reward.
func (s *RewardApprovalServer) RecallReward(ctx context.Context, req *loyaltyv1.RecallRewardRequest) (*loyaltyv1.RecallRewardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "recall reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	lookupUserReply, err := s.userQueryClient.LookupUser(ctx, &identityv1.LookupUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetReward().GetOwner().GetUserId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/LookupUser")
	}
	listSynonymSubscriptionsRequest := &searchv1.ListSynonymSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymSubscriptionsRequest_Filter{
			Region: lookupUserReply.GetUser().GetPrimaryAddress().GetRegion(),
		},
	}
	var listSynonymSubscriptionsReply *searchv1.ListSynonymSubscriptionsResponse
	if err := pipeline.Run(ctx, "acme.search.v1.SynonymNotificationService/ListSynonymSubscriptions", func(ctx context.Context) error {
		answer, err := s.synonymNotificationClient.ListSynonymSubscriptions(ctx, listSynonymSubscriptionsRequest)
		listSynonymSubscriptionsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/ListSynonymSubscriptions")
	}
	return &loyaltyv1.RecallRewardResponse{
		Outcome: &loyaltyv1.RecallRewardResponse_Outcome{Message: lookupUserReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			ReferenceIds: listSynonymSubscriptionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Reward: &loyaltyv1.Reward{
			RewardId:   req.GetRewardId(),
			Parameters: req.GetReward().GetParameters(),
		},
	}, nil
}

// GetRewardApprovalStatus returns a reward.
func (s *RewardApprovalServer) GetRewardApprovalStatus(ctx context.Context, req *loyaltyv1.GetRewardApprovalStatusRequest) (*loyaltyv1.GetRewardApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "get_reward_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get reward approval status", "tenant", req.GetContext().GetTenantId())
	step := &rewardApprovalServerGetRewardApprovalStatusResolveUsersStep{request: &identityv1.ResolveUsersRequest{Context: req.GetContext()}}
	if err := s.runGetRewardApprovalStatusResolveUsers(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ResolveUsers")
	}
	resolveUsersReply := step.reply
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	streamSellerScorecardMetricsStream, err := s.sellerScorecardReportingClient.StreamSellerScorecardMetrics(ctx, &sellersv1.StreamSellerScorecardMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: holder.Email},
		Filter: &sellersv1.StreamSellerScorecardMetricsRequest_Filter{
			OwnerId:      resolveUsersReply.GetResult().GetCorrelationId(),
			Labels:       resolveUsersReply.GetResult().GetDetails(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/StreamSellerScorecardMetrics")
	}
	for {
		received, err := streamSellerScorecardMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/StreamSellerScorecardMetrics")
		}
		_ = received
	}
	return &loyaltyv1.GetRewardApprovalStatusResponse{
		Result: &commonv1.OperationResult{Status: resolveUsersReply.GetResult().GetStatus()},
		Reward: &loyaltyv1.Reward{RewardId: req.GetRewardId()},
	}, nil
}

// rewardApprovalServerRejectRewardGetTenantExportStep carries the request of one step of RejectReward and, once it ran,
// its reply.
type rewardApprovalServerRejectRewardGetTenantExportStep struct {
	request *identityv1.GetTenantExportRequest
	reply   *identityv1.GetTenantExportResponse
}

func (s *RewardApprovalServer) runRejectRewardGetTenantExport(ctx context.Context, step *rewardApprovalServerRejectRewardGetTenantExportStep) error {
	reply, err := s.tenantExportClient.GetTenantExport(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// rewardApprovalServerListPendingRewardsScheduleTierReportStep carries the request of one step of ListPendingRewards
// and, once it ran, its reply.
type rewardApprovalServerListPendingRewardsScheduleTierReportStep struct {
	request *loyaltyv1.ScheduleTierReportRequest
	reply   *loyaltyv1.ScheduleTierReportResponse
}

func (s *RewardApprovalServer) runListPendingRewardsScheduleTierReport(ctx context.Context, step *rewardApprovalServerListPendingRewardsScheduleTierReportStep) error {
	reply, err := s.tierReportingClient.ScheduleTierReport(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// rewardApprovalServerGetRewardApprovalStatusResolveUsersStep carries the request of one step of
// GetRewardApprovalStatus and, once it ran, its reply.
type rewardApprovalServerGetRewardApprovalStatusResolveUsersStep struct {
	request *identityv1.ResolveUsersRequest
	reply   *identityv1.ResolveUsersResponse
}

func (s *RewardApprovalServer) runGetRewardApprovalStatusResolveUsers(ctx context.Context, step *rewardApprovalServerGetRewardApprovalStatusResolveUsersStep) error {
	reply, err := s.userQueryClient.ResolveUsers(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
