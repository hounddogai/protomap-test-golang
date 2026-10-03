package loyalty

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RewardLifecycleServer implements `acme.loyalty.v1.RewardLifecycleService`.
// Moves a reward between the states of its lifecycle.
type RewardLifecycleServer struct {
	loyaltyv1.UnimplementedRewardLifecycleServiceServer
	store                              *Store
	log                                *slog.Logger
	pointsLedgerCalculationClient      loyaltyv1.PointsLedgerCalculationServiceClient
	giftCardApprovalClient             giftcardsv1.GiftCardApprovalServiceClient
	rewardValidationClient             loyaltyv1.RewardValidationServiceClient
	loyaltyMemberCalculationClient     loyaltyv1.LoyaltyMemberCalculationServiceClient
	sellerPayoutScheduleWorkflowClient sellersv1.SellerPayoutScheduleWorkflowServiceClient
	tenantExportClient                 identityv1.TenantExportServiceClient
	loyaltyMemberClient                loyaltyv1.LoyaltyMemberServiceClient
	tierReportingClient                loyaltyv1.TierReportingServiceClient
	synonymValidationClient            searchv1.SynonymValidationServiceClient
	reviewVoteApprovalClient           reviewsv1.ReviewVoteApprovalServiceClient
	tierValidationClient               loyaltyv1.TierValidationServiceClient
	suggestVenuesFn                    func(context.Context, *venuesv1.SuggestVenuesRequest, ...grpc.CallOption) (*venuesv1.SuggestVenuesResponse, error)
	pointsLedgerQueryClient            loyaltyv1.PointsLedgerQueryServiceClient
	bus                                *eventbus.Bus
}

// NewRewardLifecycleServer wires a RewardLifecycleServer.
func NewRewardLifecycleServer(deps platform.Deps) *RewardLifecycleServer {
	venueQuery := venuesv1.NewVenueQueryServiceClient(deps.Conn)
	return &RewardLifecycleServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.loyalty.v1.RewardLifecycleService"),
		pointsLedgerCalculationClient:      loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		giftCardApprovalClient:             giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		rewardValidationClient:             loyaltyv1.NewRewardValidationServiceClient(deps.Conn),
		loyaltyMemberCalculationClient:     loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		sellerPayoutScheduleWorkflowClient: sellersv1.NewSellerPayoutScheduleWorkflowServiceClient(deps.Conn),
		tenantExportClient:                 identityv1.NewTenantExportServiceClient(deps.Conn),
		loyaltyMemberClient:                loyaltyv1.NewLoyaltyMemberServiceClient(deps.Conn),
		tierReportingClient:                loyaltyv1.NewTierReportingServiceClient(deps.Conn),
		synonymValidationClient:            searchv1.NewSynonymValidationServiceClient(deps.Conn),
		reviewVoteApprovalClient:           reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn),
		tierValidationClient:               loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		suggestVenuesFn:                    venueQuery.SuggestVenues,
		pointsLedgerQueryClient:            loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		bus:                                deps.Bus,
	}
}

// ActivateReward activates a reward.
func (s *RewardLifecycleServer) ActivateReward(ctx context.Context, req *loyaltyv1.ActivateRewardRequest) (*loyaltyv1.ActivateRewardResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "activate_reward"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate reward", "tenant", req.GetContext().GetTenantId())
	comparePointsLedgerOptionsRequest := newComparePointsLedgerOptionsRequest(withComparePointsLedgerOptionsContext(req.GetContext()), withComparePointsLedgerOptionsPointsLedger(&loyaltyv1.PointsLedger{
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
	}))
	comparePointsLedgerOptionsReply, err := s.pointsLedgerCalculationClient.ComparePointsLedgerOptions(ctx, comparePointsLedgerOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/ComparePointsLedgerOptions")
	}
	approveGiftCardRequest := &giftcardsv1.ApproveGiftCardRequest{}
	approveGiftCardRequest.Context = req.GetContext()
	approveGiftCardRequest.GiftCard = &giftcardsv1.GiftCard{
		Contact: &commonv1.ContactInfo{
			Name: comparePointsLedgerOptionsReply.GetPointsLedger().GetContact().GetName(),
		},
	}
	approveGiftCardReply, err := s.giftCardApprovalClient.ApproveGiftCard(ctx, approveGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/ApproveGiftCard")
	}
	scoreRewardReply, err := s.rewardValidationClient.ScoreReward(ctx, &loyaltyv1.ScoreRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: approveGiftCardReply.GetGiftCard().GetAudit().GetUpdatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/ScoreReward")
	}
	_ = scoreRewardReply
	return &loyaltyv1.ActivateRewardResponse{
		Result: &commonv1.OperationResult{
			Message:      comparePointsLedgerOptionsReply.GetResult().GetMessage(),
			ReferenceIds: comparePointsLedgerOptionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				Email: approveGiftCardReply.GetGiftCard().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: approveGiftCardReply.GetGiftCard().GetAudit().GetCreatedBy(),
			},
			RewardId: req.GetRewardId(),
		},
		Outcome: &loyaltyv1.ActivateRewardResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendReward suspends a reward.
func (s *RewardLifecycleServer) SuspendReward(ctx context.Context, req *loyaltyv1.SuspendRewardRequest) (*loyaltyv1.SuspendRewardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "suspend_reward"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend reward", "tenant", req.GetContext().GetTenantId())
	simulateLoyaltyMemberRequest := &loyaltyv1.SimulateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotifyContact().GetName(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	type simulateLoyaltyMemberResult struct {
		reply *loyaltyv1.SimulateLoyaltyMemberResponse
		err   error
	}
	simulateLoyaltyMemberResults := make(chan simulateLoyaltyMemberResult, 1)
	go func() {
		reply, err := s.loyaltyMemberCalculationClient.SimulateLoyaltyMember(ctx, simulateLoyaltyMemberRequest)
		simulateLoyaltyMemberResults <- simulateLoyaltyMemberResult{reply, err}
	}()
	var simulateLoyaltyMemberReply *loyaltyv1.SimulateLoyaltyMemberResponse
	select {
	case result := <-simulateLoyaltyMemberResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.loyalty.v1.LoyaltyMemberCalculationService/SimulateLoyaltyMember")
		}
		simulateLoyaltyMemberReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	resumeSellerPayoutScheduleWorkflowRequest := &sellersv1.ResumeSellerPayoutScheduleWorkflowRequest{}
	resumeSellerPayoutScheduleWorkflowRequest.Context = req.GetContext()
	resumeSellerPayoutScheduleWorkflowRequest.Details = &sellersv1.ResumeSellerPayoutScheduleWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{
			Name: simulateLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetName(),
		},
	}
	resumeSellerPayoutScheduleWorkflowRequest.SellerPayoutSchedule = &sellersv1.SellerPayoutSchedule{
		Organizer: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
	}
	resumeSellerPayoutScheduleWorkflowReply, err := s.sellerPayoutScheduleWorkflowClient.ResumeSellerPayoutScheduleWorkflow(ctx, resumeSellerPayoutScheduleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleWorkflowService/ResumeSellerPayoutScheduleWorkflow")
	}
	return &loyaltyv1.SuspendRewardResponse{
		Result: &commonv1.OperationResult{
			Message:      simulateLoyaltyMemberReply.GetResult().GetMessage(),
			ReferenceIds: resumeSellerPayoutScheduleWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email: resumeSellerPayoutScheduleWorkflowReply.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
			},
			RewardId: req.GetRewardId(),
		},
		Outcome: &loyaltyv1.SuspendRewardResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateReward reinstates a reward.
func (s *RewardLifecycleServer) ReinstateReward(ctx context.Context, req *loyaltyv1.ReinstateRewardRequest) (*loyaltyv1.ReinstateRewardResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "reinstate_reward"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate reward", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*loyaltyv1.ReinstateRewardRequest_Details)
	streamTenantExportStream, err := s.tenantExportClient.StreamTenantExport(ctx, &identityv1.StreamTenantExportRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.StreamTenantExportRequest_Filter{CreatedAfter: cloned.GetEffectiveAt()},
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
	return &loyaltyv1.ReinstateRewardResponse{
		Reward:  &loyaltyv1.Reward{RewardId: req.GetRewardId()},
		Outcome: &loyaltyv1.ReinstateRewardResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TerminateReward terminates a reward.
func (s *RewardLifecycleServer) TerminateReward(ctx context.Context, req *loyaltyv1.TerminateRewardRequest) (*loyaltyv1.TerminateRewardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "terminate_reward"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate reward", "tenant", req.GetContext().GetTenantId())
	searchLoyaltyMembersRequest := buildSearchLoyaltyMembersRequestForTerminateReward(req)
	searchLoyaltyMembersReply, err := s.loyaltyMemberClient.SearchLoyaltyMembers(ctx, searchLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberService/SearchLoyaltyMembers")
	}
	return &loyaltyv1.TerminateRewardResponse{
		Result: &commonv1.OperationResult{
			Message: searchLoyaltyMembersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Reward:  &loyaltyv1.Reward{RewardId: req.GetRewardId()},
		Outcome: &loyaltyv1.TerminateRewardResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewReward renews a reward.
func (s *RewardLifecycleServer) RenewReward(ctx context.Context, req *loyaltyv1.RenewRewardRequest) (*loyaltyv1.RenewRewardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "renew_reward"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew reward", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*loyaltyv1.RenewRewardRequest_Details)
	countTierTotalsRequest := &loyaltyv1.CountTierTotalsRequest{
		Context: req.GetContext(),
		Filter:  &loyaltyv1.CountTierTotalsRequest_Filter{CreatedAfter: cloned.GetEffectiveAt()},
	}
	send := s.countTierTotalsSender(countTierTotalsRequest)
	countTierTotalsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/CountTierTotals")
	}
	cloned2 := proto.Clone(req.GetDetails()).(*loyaltyv1.RenewRewardRequest_Details)
	normalizeSynonymRequest := &searchv1.NormalizeSynonymRequest{}
	normalizeSynonymRequest.Context = req.GetContext()
	normalizeSynonymRequest.Details = &searchv1.NormalizeSynonymRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: req.GetDetails().GetNotifyContact().GetEmail(),
			Name:  cloned2.GetNotifyContact().GetName(),
		},
	}
	normalizeSynonymRequest.Synonym = &searchv1.Synonym{
		Owner:  &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
		Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{NotifyEmail: req.GetDetails().GetNewOwner().GetEmail()}},
	}
	normalizeSynonymReply, err := s.synonymValidationClient.NormalizeSynonym(ctx, normalizeSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/NormalizeSynonym")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	commentReviewVoteApprovalRequest := &reviewsv1.CommentReviewVoteApprovalRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: normalizeSynonymReply.GetSynonym().GetOwner().GetDisplayName(),
				Email:       normalizeSynonymReply.GetSynonym().GetOwner().GetEmail(),
			},
			Detail: packed,
		},
		Details: &reviewsv1.CommentReviewVoteApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: normalizeSynonymReply.GetSynonym().GetOwner().GetUserId(),
			},
		},
	}
	commentReviewVoteApprovalReply, err := grpcx.Call(ctx, s.reviewVoteApprovalClient.CommentReviewVoteApproval, commentReviewVoteApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/CommentReviewVoteApproval")
	}
	return &loyaltyv1.RenewRewardResponse{
		Outcome: &loyaltyv1.RenewRewardResponse_Outcome{
			Status:      countTierTotalsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message:      countTierTotalsReply.GetResult().GetMessage(),
			Details:      normalizeSynonymReply.GetResult().GetDetails(),
			ReferenceIds: commentReviewVoteApprovalReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Reward: &loyaltyv1.Reward{RewardId: req.GetRewardId()},
	}, nil
}

// ExpireReward expires a reward.
func (s *RewardLifecycleServer) ExpireReward(ctx context.Context, req *loyaltyv1.ExpireRewardRequest) (*loyaltyv1.ExpireRewardResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "expire_reward"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire reward", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	rejectReviewVoteRequest := &reviewsv1.RejectReviewVoteRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.RejectReviewVoteRequest_Details{
			Approver: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	rejectReviewVoteReply, err := grpcx.Call(ctx, s.reviewVoteApprovalClient.RejectReviewVote, rejectReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/RejectReviewVote")
	}
	normalizeTierRequest := &loyaltyv1.NormalizeTierRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.NormalizeTierRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            rejectReviewVoteReply.GetReviewVote().GetSubject().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	normalizeTierReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*loyaltyv1.NormalizeTierResponse, error) {
		return s.tierValidationClient.NormalizeTier(ctx, normalizeTierRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/NormalizeTier")
	}
	suggestVenuesRequest := &venuesv1.SuggestVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.SuggestVenuesRequest_Filter{
			CreatedAfter: normalizeTierReply.GetTier().GetAudit().GetCreatedAt(),
			OwnerId:      rejectReviewVoteReply.GetReviewVote().GetSubject().GetCustomerId(),
		},
	}
	suggestVenuesReply, err := s.suggestVenuesFn(ctx, suggestVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/SuggestVenues")
	}
	return &loyaltyv1.ExpireRewardResponse{
		Result: &commonv1.OperationResult{
			Message:       rejectReviewVoteReply.GetResult().GetMessage(),
			ReferenceIds:  suggestVenuesReply.GetResult().GetReferenceIds(),
			CorrelationId: suggestVenuesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Reward: &loyaltyv1.Reward{
			Name:     normalizeTierReply.GetTier().GetName(),
			RewardId: req.GetRewardId(),
		},
		Outcome: &loyaltyv1.ExpireRewardResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetRewardLifecycle returns a reward.
func (s *RewardLifecycleServer) GetRewardLifecycle(ctx context.Context, req *loyaltyv1.GetRewardLifecycleRequest) (*loyaltyv1.GetRewardLifecycleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "get_reward_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get reward lifecycle", "tenant", req.GetContext().GetTenantId())
	scoreRewardReply, err := s.rewardValidationClient.ScoreReward(ctx, &loyaltyv1.ScoreRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			RewardId: req.GetRewardId(),
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/ScoreReward")
	}
	return &loyaltyv1.GetRewardLifecycleResponse{
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreRewardReply.GetReward().GetAudit().GetUpdatedBy(),
			},
			RewardId: req.GetRewardId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleRewardExpiry schedules a reward.
func (s *RewardLifecycleServer) ScheduleRewardExpiry(ctx context.Context, req *loyaltyv1.ScheduleRewardExpiryRequest) (*loyaltyv1.ScheduleRewardExpiryResponse, error) {
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "schedule reward expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	exportPointsLedgersStream, err := s.pointsLedgerQueryClient.ExportPointsLedgers(ctx, &loyaltyv1.ExportPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ExportPointsLedgersRequest_Filter{
			CreatedAfter: req.GetReward().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetReward().GetOwner().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/ExportPointsLedgers")
	}
	for {
		received, err := exportPointsLedgersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/ExportPointsLedgers")
		}
		_ = received
	}
	for _, condition := range req.GetReward().GetConditions() {
		if _, err := s.rewardValidationClient.ScoreReward(ctx, &loyaltyv1.ScoreRewardRequest{
			Context: req.GetContext(),
			Reward: &loyaltyv1.Reward{
				Conditions: []*loyaltyv1.RewardCondition{&loyaltyv1.RewardCondition{
					Operator:  condition.GetOperator(),
					Attribute: condition.GetAttribute(),
					Value:     condition.GetValue(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/ScoreReward")
		}
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "loyalty.schedule_reward_expiry.suggest_points_ledgers",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   req.GetReward().GetOwner().GetUserId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &loyaltyv1.ScheduleRewardExpiryResponse{
		Reward: &loyaltyv1.Reward{
			RewardId:    req.GetRewardId(),
			Description: req.GetReward().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// countTierTotalsSender returns a function that sends one request to
// acme.loyalty.v1.TierReportingService/CountTierTotals.
func (s *RewardLifecycleServer) countTierTotalsSender(request *loyaltyv1.CountTierTotalsRequest) func(context.Context) (*loyaltyv1.CountTierTotalsResponse, error) {
	return func(ctx context.Context) (*loyaltyv1.CountTierTotalsResponse, error) {
		return s.tierReportingClient.CountTierTotals(ctx, request)
	}
}
