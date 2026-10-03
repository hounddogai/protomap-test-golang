package loyalty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// TierApprovalServer implements `acme.loyalty.v1.TierApprovalService`.
// Routes tiers through approval, with escalation and delegation.
type TierApprovalServer struct {
	loyaltyv1.UnimplementedTierApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	rewardLifecycleClient           loyaltyv1.RewardLifecycleServiceClient
	meteredTierReporting            meteredTierReporting
	loyaltyMemberApprovalClient     loyaltyv1.LoyaltyMemberApprovalServiceClient
	roleNotificationClient          identityv1.RoleNotificationServiceClient
	conn                            *grpc.ClientConn
	searchIndexClient               searchv1.SearchIndexServiceClient
	pointsLedgerQueryClient         loyaltyv1.PointsLedgerQueryServiceClient
	pointsLedgerQueryClientFallback loyaltyv1.PointsLedgerQueryServiceClient
	pointsLedgerValidationClient    loyaltyv1.PointsLedgerValidationServiceClient
	loyaltyMemberQueryClient        loyaltyv1.LoyaltyMemberQueryServiceClient
	userHistoryPort                 userHistoryPort
	meteredPointsLedgerWorkflow     meteredPointsLedgerWorkflow
	mediaAssetQueryOnce             sync.Once
	mediaAssetQueryCached           mediav1.MediaAssetQueryServiceClient
	rewardApprovalClient            loyaltyv1.RewardApprovalServiceClient
	rewardApprovalPort              rewardApprovalPort
	tierReportingClient             loyaltyv1.TierReportingServiceClient
	sellerListingApprovalClient     sellersv1.SellerListingApprovalServiceClient
	accessPolicyValidationClient    authorizationv1.AccessPolicyValidationServiceClient
	giftCardBatchWorkflowClient     giftcardsv1.GiftCardBatchWorkflowServiceClient
	accessPolicyValidationOnce      sync.Once
	accessPolicyValidationCached    authorizationv1.AccessPolicyValidationServiceClient
}

// NewTierApprovalServer wires a TierApprovalServer.
func NewTierApprovalServer(deps platform.Deps) *TierApprovalServer {
	return &TierApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.loyalty.v1.TierApprovalService"),
		rewardLifecycleClient:           loyaltyv1.NewRewardLifecycleServiceClient(deps.Conn),
		meteredTierReporting:            meteredTierReporting{TierReportingServiceClient: loyaltyv1.NewTierReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		loyaltyMemberApprovalClient:     loyaltyv1.NewLoyaltyMemberApprovalServiceClient(deps.Conn),
		roleNotificationClient:          identityv1.NewRoleNotificationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		searchIndexClient:               searchv1.NewSearchIndexServiceClient(deps.Conn),
		pointsLedgerQueryClient:         loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		pointsLedgerQueryClientFallback: loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		pointsLedgerValidationClient:    loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
		loyaltyMemberQueryClient:        loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn),
		userHistoryPort:                 newUserHistoryPort(deps),
		meteredPointsLedgerWorkflow:     meteredPointsLedgerWorkflow{PointsLedgerWorkflowServiceClient: loyaltyv1.NewPointsLedgerWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		rewardApprovalClient:            loyaltyv1.NewRewardApprovalServiceClient(deps.Conn),
		rewardApprovalPort:              newRewardApprovalPort(deps),
		tierReportingClient:             loyaltyv1.NewTierReportingServiceClient(deps.Conn),
		sellerListingApprovalClient:     sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
		accessPolicyValidationClient:    authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		giftCardBatchWorkflowClient:     giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
	}
}

// SubmitTier submits a tier.
func (s *TierApprovalServer) SubmitTier(ctx context.Context, req *loyaltyv1.SubmitTierRequest) (*loyaltyv1.SubmitTierResponse, error) {
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "submit tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	for _, condition := range req.GetTier().GetConditions() {
		if _, err := s.rewardLifecycleClient.ScheduleRewardExpiry(ctx, &loyaltyv1.ScheduleRewardExpiryRequest{
			Context: req.GetContext(),
			Reward: &loyaltyv1.Reward{
				Conditions: []*loyaltyv1.RewardCondition{&loyaltyv1.RewardCondition{
					Negate: condition.GetNegate(),
					Value:  condition.GetValue(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/ScheduleRewardExpiry")
		}
	}
	fetchTierReportRequest := &loyaltyv1.FetchTierReportRequest{
		Context: req.GetContext(),
		TierId:  req.GetTier().GetTierId(),
	}
	fetchTierReportReply, err := s.meteredTierReporting.FetchTierReport(ctx, fetchTierReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/FetchTierReport")
	}
	return &loyaltyv1.SubmitTierResponse{
		Outcome: &loyaltyv1.SubmitTierResponse_Outcome{Status: fetchTierReportReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: fetchTierReportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Tier: &loyaltyv1.Tier{
			TierId: req.GetTierId(),
			Owner:  req.GetTier().GetOwner(),
		},
	}, nil
}

// ApproveTier approves a tier.
func (s *TierApprovalServer) ApproveTier(ctx context.Context, req *loyaltyv1.ApproveTierRequest) (*loyaltyv1.ApproveTierResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveTier", "error", err)
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "approve tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	rejectLoyaltyMemberRequest := &loyaltyv1.RejectLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetTier().GetAudit().GetUpdatedBy()},
		},
	}
	var rejectLoyaltyMemberReply *loyaltyv1.RejectLoyaltyMemberResponse
	if err := pipeline.Run(ctx, "acme.loyalty.v1.LoyaltyMemberApprovalService/RejectLoyaltyMember", func(ctx context.Context) error {
		answer, err := s.loyaltyMemberApprovalClient.RejectLoyaltyMember(ctx, rejectLoyaltyMemberRequest)
		rejectLoyaltyMemberReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberApprovalService/RejectLoyaltyMember")
	}
	streamRoleNoticesStream, err := s.roleNotificationClient.StreamRoleNotices(ctx, &identityv1.StreamRoleNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: rejectLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/StreamRoleNotices")
	}
	for {
		received, err := streamRoleNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/StreamRoleNotices")
		}
		_ = received
	}
	signalGiftCardBatchWorkflowRequest := &giftcardsv1.SignalGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: rejectLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetCreatedBy(),
			},
		},
	}
	signalGiftCardBatchWorkflowReply := new(giftcardsv1.SignalGiftCardBatchWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", giftcardsGiftCardBatchWorkflowServiceName, "SignalGiftCardBatchWorkflow"), signalGiftCardBatchWorkflowRequest, signalGiftCardBatchWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/SignalGiftCardBatchWorkflow")
	}
	searchSearchIndexesRequest := &searchv1.SearchSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.SearchSearchIndexesRequest_Filter{
			Region: signalGiftCardBatchWorkflowReply.GetGiftCardBatch().GetBillingAddress().GetRegion(),
		},
	}
	searchSearchIndexesReply, err := grpcx.Call(ctx, s.searchIndexClient.SearchSearchIndexes, searchSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/SearchSearchIndexes")
	}
	return &loyaltyv1.ApproveTierResponse{
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: rejectLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
			},
			TierId:     req.GetTierId(),
			Parameters: req.GetTier().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  rejectLoyaltyMemberReply.GetResult().GetReferenceIds(),
			CorrelationId: searchSearchIndexesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &loyaltyv1.ApproveTierResponse_Outcome{
			Status: signalGiftCardBatchWorkflowReply.GetResult().GetStatus(),
		},
	}, nil
}

// RejectTier rejects a tier.
func (s *TierApprovalServer) RejectTier(ctx context.Context, req *loyaltyv1.RejectTierRequest) (*loyaltyv1.RejectTierResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "reject tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *loyaltyv1.CountPointsLedgersRequest, ...grpc.CallOption) (*loyaltyv1.CountPointsLedgersResponse, error){
		"primary":  s.pointsLedgerQueryClient.CountPointsLedgers,
		"fallback": s.pointsLedgerQueryClientFallback.CountPointsLedgers,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	countPointsLedgersRequest := &loyaltyv1.CountPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.CountPointsLedgersRequest_Filter{
			CreatedAfter: req.GetTier().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
	}
	countPointsLedgersReply, err := route(ctx, countPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/CountPointsLedgers")
	}
	previewPointsLedgerReply, err := s.pointsLedgerValidationClient.PreviewPointsLedger(ctx, &loyaltyv1.PreviewPointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			Limits:  []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{Amount: req.GetTier().GetDiscount().GetAmount()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/PreviewPointsLedger")
	}
	return &loyaltyv1.RejectTierResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: countPointsLedgersReply.GetResult().GetCorrelationId(),
			Details:       countPointsLedgersReply.GetResult().GetDetails(),
			Status:        previewPointsLedgerReply.GetResult().GetStatus(),
		},
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewPointsLedgerReply.GetPointsLedger().GetAudit().GetCreatedBy(),
			},
			TierId:    req.GetTierId(),
			Effective: req.GetTier().GetEffective(),
		},
	}, nil
}

// EscalateTier escalates a tier.
func (s *TierApprovalServer) EscalateTier(ctx context.Context, req *loyaltyv1.EscalateTierRequest) (*loyaltyv1.EscalateTierResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "escalate tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetApprover().GetUserId())
	parameters := make(map[string]string, len(req.GetTier().GetParameters()))
	for key, value := range req.GetTier().GetParameters() {
		parameters[key] = value
	}
	suggestLoyaltyMembersRequest := &loyaltyv1.SuggestLoyaltyMembersRequest{}
	suggestLoyaltyMembersRequest.Context = req.GetContext()
	suggestLoyaltyMembersRequest.Filter = &loyaltyv1.SuggestLoyaltyMembersRequest_Filter{
		CreatedAfter: req.GetTier().GetAudit().GetCreatedAt(),
		OwnerId:      tenancy.Value(ctx, "user_id"),
		Labels:       parameters,
	}
	suggestLoyaltyMembersReply, err := s.loyaltyMemberQueryClient.SuggestLoyaltyMembers(ctx, suggestLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/SuggestLoyaltyMembers")
	}
	cloned := proto.Clone(req.GetDetails()).(*loyaltyv1.EscalateTierRequest_Details)
	var reason string
	switch choice := req.GetTier().GetAction().(type) {
	case *loyaltyv1.Tier_Route:
		reason = choice.Route.GetTarget()
	case *loyaltyv1.Tier_Block:
		reason = choice.Block.GetReason()
	}
	pruneUserVersionsRequest := &identityv1.PruneUserVersionsRequest{
		Context: req.GetContext(),
		UserId:  cloned.GetApprover().GetUserId(),
		Etag:    req.GetTier().GetAudit().GetEtag(),
		Reason:  reason,
	}
	pruneUserVersionsReply, err := s.userHistoryPort.PruneUserVersions(ctx, pruneUserVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/PruneUserVersions")
	}
	pausePointsLedgerWorkflowRequest := &loyaltyv1.PausePointsLedgerWorkflowRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetTier().GetAudit().GetUpdatedAt()},
		},
	}
	pausePointsLedgerWorkflowReply, err := s.meteredPointsLedgerWorkflow.PausePointsLedgerWorkflow(ctx, pausePointsLedgerWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerWorkflowService/PausePointsLedgerWorkflow")
	}
	_ = pruneUserVersionsReply
	return &loyaltyv1.EscalateTierResponse{
		Result: &commonv1.OperationResult{
			Status:       suggestLoyaltyMembersReply.GetResult().GetStatus(),
			Message:      suggestLoyaltyMembersReply.GetResult().GetMessage(),
			ReferenceIds: pausePointsLedgerWorkflowReply.GetResult().GetReferenceIds(),
		},
		Outcome: &loyaltyv1.EscalateTierResponse_Outcome{
			Message: pausePointsLedgerWorkflowReply.GetResult().GetMessage(),
		},
		Tier: &loyaltyv1.Tier{
			TierId:  req.GetTierId(),
			Version: req.GetTier().GetVersion(),
		},
	}, nil
}

// ListPendingTiers lists tiers a page at a time.
func (s *TierApprovalServer) ListPendingTiers(ctx context.Context, req *loyaltyv1.ListPendingTiersRequest) (*loyaltyv1.ListPendingTiersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending tiers", "tenant", req.GetContext().GetTenantId())
	countMediaAssetsRequest := &mediav1.CountMediaAssetsRequest{
		Context: req.GetContext(),
		Filter:  &mediav1.CountMediaAssetsRequest_Filter{OwnerId: req.GetFilter().GetOwnerId()},
	}
	countMediaAssetsReply, err := s.mediaAssetQueryLazy().CountMediaAssets(ctx, countMediaAssetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetQueryService/CountMediaAssets")
	}
	return &loyaltyv1.ListPendingTiersResponse{
		Result: &commonv1.OperationResult{
			Details:      countMediaAssetsReply.GetResult().GetDetails(),
			ReferenceIds: countMediaAssetsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// DelegateTierApproval delegates a tier.
func (s *TierApprovalServer) DelegateTierApproval(ctx context.Context, req *loyaltyv1.DelegateTierApprovalRequest) (*loyaltyv1.DelegateTierApprovalResponse, error) {
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "delegate tier approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	for _, condition := range req.GetTier().GetConditions() {
		if _, err := s.rewardApprovalClient.RejectReward(ctx, &loyaltyv1.RejectRewardRequest{
			Context: req.GetContext(),
			Reward: &loyaltyv1.Reward{
				Conditions: []*loyaltyv1.RewardCondition{&loyaltyv1.RewardCondition{Value: condition.GetValue()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/RejectReward")
		}
	}
	recallRewardRequest := &loyaltyv1.RecallRewardRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.RecallRewardRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	recallRewardReply, err := s.rewardApprovalPort.RecallReward(ctx, recallRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/RecallReward")
	}
	packed, err := anypb.New(req.GetTier())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareTierReportReply, err := s.tierReportingClient.ShareTierReport(ctx, &loyaltyv1.ShareTierReportRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ShareTierReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetTier().GetOwner().GetEmail()},
			Payload:   packed,
		},
		TierId: req.GetTier().GetTierId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/ShareTierReport")
	}
	submitSellerListingReply, err := s.sellerListingApprovalClient.SubmitSellerListing(ctx, &sellersv1.SubmitSellerListingRequest{
		Context: req.GetContext(),
		Details: &sellersv1.SubmitSellerListingRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: shareTierReportReply.GetTier().GetOwner().GetUserId(),
			},
		},
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recallRewardReply.GetReward().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/SubmitSellerListing")
	}
	return &loyaltyv1.DelegateTierApprovalResponse{
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				UserId: recallRewardReply.GetReward().GetOwner().GetUserId(),
			},
			Effective: shareTierReportReply.GetTier().GetEffective(),
			Name:      submitSellerListingReply.GetSellerListing().GetName(),
			TierId:    req.GetTierId(),
			Status:    req.GetTier().GetStatus(),
		},
		Outcome: &loyaltyv1.DelegateTierApprovalResponse_Outcome{Message: recallRewardReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallTier recalls a tier.
func (s *TierApprovalServer) RecallTier(ctx context.Context, req *loyaltyv1.RecallTierRequest) (*loyaltyv1.RecallTierResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "recall tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	rejectLoyaltyMemberRequest := &loyaltyv1.RejectLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Limits:  []*loyaltyv1.LoyaltyMemberLimit{&loyaltyv1.LoyaltyMemberLimit{Amount: req.GetTier().GetDiscount().GetAmount()}},
			Contact: &commonv1.ContactInfo{Email: req.GetTier().GetOwner().GetEmail()},
		},
		Details: &loyaltyv1.RejectLoyaltyMemberRequest_Details{
			Approver: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	rejectLoyaltyMemberReply := new(loyaltyv1.RejectLoyaltyMemberResponse)
	if err := s.conn.Invoke(ctx, loyaltyLoyaltyMemberApprovalServiceRejectLoyaltyMemberMethod, rejectLoyaltyMemberRequest, rejectLoyaltyMemberReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberApprovalService/RejectLoyaltyMember")
	}
	for condition := range iterx.Each(req.GetTier().GetConditions(), func(candidate *loyaltyv1.TierCondition) bool { return candidate != nil }) {
		if _, err := s.accessPolicyValidationClient.CheckAccessPolicyEligibility(ctx, &authorizationv1.CheckAccessPolicyEligibilityRequest{
			Context: req.GetContext(),
			AccessPolicy: &authorizationv1.AccessPolicy{
				Conditions: []*authorizationv1.AccessPolicyCondition{&authorizationv1.AccessPolicyCondition{Negate: condition.GetNegate()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/CheckAccessPolicyEligibility")
		}
	}
	return &loyaltyv1.RecallTierResponse{
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				CreatedBy: rejectLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetCreatedBy(),
			},
			TierId: req.GetTierId(),
			Owner:  req.GetTier().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetTierApprovalStatus returns a tier.
func (s *TierApprovalServer) GetTierApprovalStatus(ctx context.Context, req *loyaltyv1.GetTierApprovalStatusRequest) (*loyaltyv1.GetTierApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "get_tier_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get tier approval status", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	startGiftCardBatchWorkflowRequest := &giftcardsv1.StartGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.StartGiftCardBatchWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{DueAt: req.GetContext().GetDeadline()},
	}
	startGiftCardBatchWorkflowReply, err := grpcx.Call(ctx, s.giftCardBatchWorkflowClient.StartGiftCardBatchWorkflow, startGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/StartGiftCardBatchWorkflow")
	}
	checkAccessPolicyEligibilityRequest := &authorizationv1.CheckAccessPolicyEligibilityRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{
				CreatedBy: startGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetCreatedBy(),
				UpdatedAt: startGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
				Etag:      startGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetEtag(),
			},
		},
	}
	checkAccessPolicyEligibilityReply, err := s.accessPolicyValidationLazy().CheckAccessPolicyEligibility(ctx, checkAccessPolicyEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/CheckAccessPolicyEligibility")
	}
	return &loyaltyv1.GetTierApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message: startGiftCardBatchWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: startGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
				CreatedBy: checkAccessPolicyEligibilityReply.GetAccessPolicy().GetAudit().GetCreatedBy(),
			},
			TierId: req.GetTierId(),
		},
	}, nil
}

// CommentTierApproval comments a tier.
func (s *TierApprovalServer) CommentTierApproval(ctx context.Context, req *loyaltyv1.CommentTierApprovalRequest) (*loyaltyv1.CommentTierApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "comment tier approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	var holder struct{ UserId string }
	if record := req.GetTier().GetOwner(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UserId"); err != nil {
			s.log.WarnContext(ctx, "copy user_id", "error", err)
		}
	}
	findPointsLedgersReply, err := s.pointsLedgerQueryClient.FindPointsLedgers(ctx, &loyaltyv1.FindPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.FindPointsLedgersRequest_Filter{
			CreatedAfter: req.GetTier().GetAudit().GetUpdatedAt(),
			OwnerId:      holder.UserId,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/FindPointsLedgers")
	}
	tierCache := cache.New[string, *loyaltyv1.Tier](1)
	tierCache.Put(req.GetContext().GetTraceId(), req.GetTier())
	cachedTier, _ := tierCache.Get(req.GetContext().GetTraceId())
	cloned := proto.Clone(req.GetTier()).(*loyaltyv1.Tier)
	var reason string
	switch choice := req.GetTier().GetAction().(type) {
	case *loyaltyv1.Tier_Route:
		reason = choice.Route.GetTarget()
	case *loyaltyv1.Tier_Block:
		reason = choice.Block.GetReason()
	}
	recallRewardRequest := &loyaltyv1.RecallRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit:  &commonv1.AuditInfo{UpdatedBy: cachedTier.GetAudit().GetUpdatedBy()},
			Action: &loyaltyv1.Reward_Block{Block: &loyaltyv1.RewardBlockAction{Reason: reason}},
		},
		Details: &loyaltyv1.RecallRewardRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       cloned.GetOwner().GetEmail(),
				DisplayName: req.GetTier().GetOwner().GetDisplayName(),
			},
		},
	}
	recallRewardReply := new(loyaltyv1.RecallRewardResponse)
	if err := s.conn.Invoke(ctx, loyaltyRewardApprovalServiceRecallRewardMethod, recallRewardRequest, recallRewardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/RecallReward")
	}
	return &loyaltyv1.CommentTierApprovalResponse{
		Outcome: &loyaltyv1.CommentTierApprovalResponse_Outcome{
			Message:   findPointsLedgersReply.GetResult().GetMessage(),
			NextSteps: recallRewardReply.GetOutcome().GetNextSteps(),
		},
		Tier: &loyaltyv1.Tier{
			TierId:     req.GetTierId(),
			Parameters: req.GetTier().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// mediaAssetQueryLazy dials acme.media.v1.MediaAssetQueryService the first time a handler needs it.
func (s *TierApprovalServer) mediaAssetQueryLazy() mediav1.MediaAssetQueryServiceClient {
	s.mediaAssetQueryOnce.Do(func() { s.mediaAssetQueryCached = mediav1.NewMediaAssetQueryServiceClient(s.conn) })
	return s.mediaAssetQueryCached
}

// accessPolicyValidationLazy dials acme.authorization.v1.AccessPolicyValidationService the first time a handler needs
// it.
func (s *TierApprovalServer) accessPolicyValidationLazy() authorizationv1.AccessPolicyValidationServiceClient {
	s.accessPolicyValidationOnce.Do(func() {
		s.accessPolicyValidationCached = authorizationv1.NewAccessPolicyValidationServiceClient(s.conn)
	})
	return s.accessPolicyValidationCached
}
