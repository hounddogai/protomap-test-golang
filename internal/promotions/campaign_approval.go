package promotions

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// CampaignApprovalServer implements `acme.promotions.v1.CampaignApprovalService`.
// Routes campaigns through approval, with escalation and delegation.
type CampaignApprovalServer struct {
	promotionsv1.UnimplementedCampaignApprovalServiceServer
	store                               *Store
	log                                 *slog.Logger
	conn                                *grpc.ClientConn
	redemptionCalculationClient         promotionsv1.RedemptionCalculationServiceClient
	redemptionCalculationClientFallback promotionsv1.RedemptionCalculationServiceClient
	bus                                 *eventbus.Bus
	meteredPickListApproval             meteredPickListApproval
	featureFlagClient                   featureflagsv1.FeatureFlagServiceClient
	userClient                          identityv1.UserServiceClient
	loyaltyMemberApprovalClient         loyaltyv1.LoyaltyMemberApprovalServiceClient
	redemptionReportingClient           promotionsv1.RedemptionReportingServiceClient
}

// NewCampaignApprovalServer wires a CampaignApprovalServer.
func NewCampaignApprovalServer(deps platform.Deps) *CampaignApprovalServer {
	return &CampaignApprovalServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.promotions.v1.CampaignApprovalService"),
		conn:                                deps.Conn,
		redemptionCalculationClient:         promotionsv1.NewRedemptionCalculationServiceClient(deps.Conn),
		redemptionCalculationClientFallback: promotionsv1.NewRedemptionCalculationServiceClient(deps.Conn),
		bus:                                 deps.Bus,
		meteredPickListApproval:             meteredPickListApproval{PickListApprovalServiceClient: warehousingv1.NewPickListApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		featureFlagClient:                   featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		userClient:                          identityv1.NewUserServiceClient(deps.Conn),
		loyaltyMemberApprovalClient:         loyaltyv1.NewLoyaltyMemberApprovalServiceClient(deps.Conn),
		redemptionReportingClient:           promotionsv1.NewRedemptionReportingServiceClient(deps.Conn),
	}
}

// SubmitCampaign submits a campaign.
func (s *CampaignApprovalServer) SubmitCampaign(ctx context.Context, req *promotionsv1.SubmitCampaignRequest) (*promotionsv1.SubmitCampaignResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "submit campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	normalizeAccessPolicyRequest := &authorizationv1.NormalizeAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				UserId: req.GetDetails().GetApprover().GetUserId(),
				Email:  req.GetCampaign().GetOrganizer().GetEmail(),
			},
		},
		Details: &authorizationv1.NormalizeAccessPolicyRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	normalizeAccessPolicyReply := new(authorizationv1.NormalizeAccessPolicyResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.authorization.v1.AccessPolicyValidationService/NormalizeAccessPolicy", normalizeAccessPolicyRequest, normalizeAccessPolicyReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/NormalizeAccessPolicy")
	}
	routes := map[string]func(context.Context, *promotionsv1.ExplainRedemptionCalculationRequest, ...grpc.CallOption) (*promotionsv1.ExplainRedemptionCalculationResponse, error){
		"primary":  s.redemptionCalculationClient.ExplainRedemptionCalculation,
		"fallback": s.redemptionCalculationClientFallback.ExplainRedemptionCalculation,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	explainRedemptionCalculationRequest := &promotionsv1.ExplainRedemptionCalculationRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ExplainRedemptionCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: normalizeAccessPolicyReply.GetAccessPolicy().GetOwner().GetDisplayName(),
			},
		},
	}
	explainRedemptionCalculationReply, err := route(ctx, explainRedemptionCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionCalculationService/ExplainRedemptionCalculation")
	}
	return &promotionsv1.SubmitCampaignResponse{
		Result: &commonv1.OperationResult{
			Details: normalizeAccessPolicyReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &promotionsv1.SubmitCampaignResponse_Outcome{
			ReferenceId: explainRedemptionCalculationReply.GetOutcome().GetReferenceId(),
		},
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Window:     req.GetCampaign().GetWindow(),
		},
	}, nil
}

// ApproveCampaign approves a campaign.
func (s *CampaignApprovalServer) ApproveCampaign(ctx context.Context, req *promotionsv1.ApproveCampaignRequest) (*promotionsv1.ApproveCampaignResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "approve campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "promotions.approve_campaign.list_promotion_workflows",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     req.GetCampaign().GetLocation().GetRegion(),
			"filter.owner_id":   req.GetDetails().GetApprover().GetUserId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	submitPickListRequest := &warehousingv1.SubmitPickListRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetCampaign().GetOrganizer().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: req.GetCampaign().GetLocation().GetLocality(),
				Region:   req.GetCampaign().GetLocation().GetRegion(),
			},
		},
	}
	submitPickListReply, err := s.meteredPickListApproval.SubmitPickList(ctx, submitPickListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListApprovalService/SubmitPickList")
	}
	return &promotionsv1.ApproveCampaignResponse{
		Campaign: &promotionsv1.Campaign{
			Metadata:   submitPickListReply.GetPickList().GetMetadata(),
			CampaignId: req.GetCampaignId(),
			Title:      req.GetCampaign().GetTitle(),
		},
		Outcome: &promotionsv1.ApproveCampaignResponse_Outcome{Status: submitPickListReply.GetOutcome().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectCampaign rejects a campaign.
func (s *CampaignApprovalServer) RejectCampaign(ctx context.Context, req *promotionsv1.RejectCampaignRequest) (*promotionsv1.RejectCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectCampaign", "error", err)
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "reject campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	for participant := range iterx.Each(req.GetCampaign().GetParticipants(), func(candidate *promotionsv1.CampaignParticipant) bool { return candidate != nil }) {
		if _, err := s.featureFlagClient.CloneFeatureFlag(ctx, &featureflagsv1.CloneFeatureFlagRequest{
			Context: req.GetContext(),
			FeatureFlag: &featureflagsv1.FeatureFlag{
				Owner: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
				Audit: &commonv1.AuditInfo{
					Etag:      req.GetCampaign().GetAudit().GetEtag(),
					CreatedAt: req.GetCampaign().GetAudit().GetCreatedAt(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/CloneFeatureFlag")
		}
	}
	step := &campaignApprovalServerRejectCampaignBatchGetUsersStep{request: &identityv1.BatchGetUsersRequest{Context: req.GetContext()}}
	if err := s.runRejectCampaignBatchGetUsers(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/BatchGetUsers")
	}
	batchGetUsersReply := step.reply
	commentCartApprovalRequest := &cartv1.CommentCartApprovalRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetCampaign().GetAudit().GetCreatedAt()},
		},
	}
	commentCartApprovalReply := new(cartv1.CommentCartApprovalResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.cart.v1.CartApprovalService/CommentCartApproval", commentCartApprovalRequest, commentCartApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartApprovalService/CommentCartApproval")
	}
	return &promotionsv1.RejectCampaignResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetUsersReply.GetResult().GetCorrelationId(),
			Message:       batchGetUsersReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Outcome: &promotionsv1.RejectCampaignResponse_Outcome{Status: commentCartApprovalReply.GetResult().GetStatus()},
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Metadata:   req.GetCampaign().GetMetadata(),
		},
	}, nil
}

// EscalateCampaign escalates a campaign.
func (s *CampaignApprovalServer) EscalateCampaign(ctx context.Context, req *promotionsv1.EscalateCampaignRequest) (*promotionsv1.EscalateCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateCampaign", "error", err)
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "escalate campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "created_by", req.GetCampaign().GetAudit().GetCreatedBy())
	escalateLoyaltyMemberRequest := &loyaltyv1.EscalateLoyaltyMemberRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.EscalateLoyaltyMemberRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetDetails().GetApprover().GetEmail(),
				UserId: req.GetDetails().GetApprover().GetUserId(),
			},
			ApproverEmail: req.GetDetails().GetApproverEmail(),
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
		},
	}
	escalateLoyaltyMemberReply, err := grpcx.Call(ctx, s.loyaltyMemberApprovalClient.EscalateLoyaltyMember, escalateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberApprovalService/EscalateLoyaltyMember")
	}
	return &promotionsv1.EscalateCampaignResponse{
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: escalateLoyaltyMemberReply.GetLoyaltyMember().GetContact(),
			}},
			CampaignId: req.GetCampaignId(),
			Location:   req.GetCampaign().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingCampaigns lists campaigns a page at a time.
func (s *CampaignApprovalServer) ListPendingCampaigns(ctx context.Context, req *promotionsv1.ListPendingCampaignsRequest) (*promotionsv1.ListPendingCampaignsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending campaigns", "tenant", req.GetContext().GetTenantId())
	return &promotionsv1.ListPendingCampaignsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CommentCampaignApproval comments a campaign.
func (s *CampaignApprovalServer) CommentCampaignApproval(ctx context.Context, req *promotionsv1.CommentCampaignApprovalRequest) (*promotionsv1.CommentCampaignApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentCampaignApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "comment campaign approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*promotionsv1.ShareRedemptionReportResponse, len(req.GetCampaign().GetParticipants()))
	for i, participant := range req.GetCampaign().GetParticipants() {
		group.Go(func() error {
			reply, err := s.redemptionReportingClient.ShareRedemptionReport(groupCtx, &promotionsv1.ShareRedemptionReportRequest{
				Context: req.GetContext(),
				Details: &promotionsv1.ShareRedemptionReportRequest_Details{
					Recipient: &commonv1.ContactInfo{
						Email: participant.GetContact().GetEmail(),
						Phone: participant.GetContact().GetPhone(),
						Name:  participant.GetContact().GetName(),
					},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionReportingService/ShareRedemptionReport")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &promotionsv1.CommentCampaignApprovalResponse{
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Metadata:   req.GetCampaign().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// campaignApprovalServerRejectCampaignBatchGetUsersStep carries the request of one step of RejectCampaign and, once it
// ran, its reply.
type campaignApprovalServerRejectCampaignBatchGetUsersStep struct {
	request *identityv1.BatchGetUsersRequest
	reply   *identityv1.BatchGetUsersResponse
}

func (s *CampaignApprovalServer) runRejectCampaignBatchGetUsers(ctx context.Context, step *campaignApprovalServerRejectCampaignBatchGetUsersStep) error {
	reply, err := s.userClient.BatchGetUsers(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
