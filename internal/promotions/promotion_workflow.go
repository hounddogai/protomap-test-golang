package promotions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	telehealthv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telehealth/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PromotionWorkflowServer implements `acme.promotions.v1.PromotionWorkflowService`.
// Runs the long-lived workflow that takes a promotion from start to finish.
type PromotionWorkflowServer struct {
	promotionsv1.UnimplementedPromotionWorkflowServiceServer
	store                             *Store
	log                               *slog.Logger
	telemetryReadingCalculationClient telematicsv1.TelemetryReadingCalculationServiceClient
	videoVisitCalculationClient       telehealthv1.VideoVisitCalculationServiceClient
	campaignSyncClient                promotionsv1.CampaignSyncServiceClient
	campaignCalculationClient         promotionsv1.CampaignCalculationServiceClient
	promotionApprovalClient           promotionsv1.PromotionApprovalServiceClient
	redemptionValidationPort          redemptionValidationPort
	campaignReportingClient           promotionsv1.CampaignReportingServiceClient
	tenantNotificationClient          identityv1.TenantNotificationServiceClient
	couponApprovalClient              promotionsv1.CouponApprovalServiceClient
	apiCredentialValidationClient     identityv1.ApiCredentialValidationServiceClient
	conn                              *grpc.ClientConn
}

// NewPromotionWorkflowServer wires a PromotionWorkflowServer.
func NewPromotionWorkflowServer(deps platform.Deps) *PromotionWorkflowServer {
	return &PromotionWorkflowServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.promotions.v1.PromotionWorkflowService"),
		telemetryReadingCalculationClient: telematicsv1.NewTelemetryReadingCalculationServiceClient(deps.Conn),
		videoVisitCalculationClient:       telehealthv1.NewVideoVisitCalculationServiceClient(deps.Conn),
		campaignSyncClient:                promotionsv1.NewCampaignSyncServiceClient(deps.Conn),
		campaignCalculationClient:         promotionsv1.NewCampaignCalculationServiceClient(deps.Conn),
		promotionApprovalClient:           promotionsv1.NewPromotionApprovalServiceClient(deps.Conn),
		redemptionValidationPort:          newRedemptionValidationPort(deps),
		campaignReportingClient:           promotionsv1.NewCampaignReportingServiceClient(deps.Conn),
		tenantNotificationClient:          identityv1.NewTenantNotificationServiceClient(deps.Conn),
		couponApprovalClient:              promotionsv1.NewCouponApprovalServiceClient(deps.Conn),
		apiCredentialValidationClient:     identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		conn:                              deps.Conn,
	}
}

// StartPromotionWorkflow starts a promotion.
func (s *PromotionWorkflowServer) StartPromotionWorkflow(ctx context.Context, req *promotionsv1.StartPromotionWorkflowRequest) (*promotionsv1.StartPromotionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "start promotion workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	packed, err := anypb.New(req.GetPromotion())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateTelemetryReadingRequest := &telematicsv1.CalculateTelemetryReadingRequest{
		Context: req.GetContext(),
		TelemetryReading: &telematicsv1.TelemetryReading{
			Subject: &telematicsv1.GeoFenceRef{
				DisplayName: req.GetPromotion().GetOwner().GetDisplayName(),
				Version:     req.GetPromotion().GetVersion(),
			},
			Detail: packed,
		},
		Reason:      tenancy.Value(ctx, "reason"),
		Annotations: req.GetAnnotations(),
	}
	calculateTelemetryReadingReply, err := grpcx.Call(ctx, s.telemetryReadingCalculationClient.CalculateTelemetryReading, calculateTelemetryReadingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingCalculationService/CalculateTelemetryReading")
	}
	return &promotionsv1.StartPromotionWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: calculateTelemetryReadingReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Promotion: &promotionsv1.Promotion{
			PromotionId: req.GetPromotionId(),
			Status:      req.GetPromotion().GetStatus(),
		},
	}, nil
}

// AdvancePromotionWorkflow advances a promotion.
func (s *PromotionWorkflowServer) AdvancePromotionWorkflow(ctx context.Context, req *promotionsv1.AdvancePromotionWorkflowRequest) (*promotionsv1.AdvancePromotionWorkflowResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "advance promotion workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	browseAccessPoliciesRequest := &authorizationv1.BrowseAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.BrowseAccessPoliciesRequest_Filter{
			CreatedAfter: req.GetPromotion().GetAudit().GetUpdatedAt(),
			Labels:       req.GetAnnotations(),
			OwnerId:      req.GetPromotionId(),
		},
	}
	browseAccessPoliciesReply, err := browseAccessPoliciesCall(ctx, browseAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/BrowseAccessPolicies")
	}
	return &promotionsv1.AdvancePromotionWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browseAccessPoliciesReply.GetResult().GetReferenceIds(),
			Details:      browseAccessPoliciesReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Promotion: &promotionsv1.Promotion{
			PromotionId: req.GetPromotionId(),
			Owner:       req.GetPromotion().GetOwner(),
		},
	}, nil
}

// CancelPromotionWorkflow cancels a promotion.
func (s *PromotionWorkflowServer) CancelPromotionWorkflow(ctx context.Context, req *promotionsv1.CancelPromotionWorkflowRequest) (*promotionsv1.CancelPromotionWorkflowResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "cancel promotion workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	step := &promotionWorkflowServerCancelPromotionWorkflowCalculateVideoVisitStep{request: &telehealthv1.CalculateVideoVisitRequest{
		Context: req.GetContext(),
		VideoVisit: &telehealthv1.VideoVisit{
			Participants: []*telehealthv1.VideoVisitParticipant{&telehealthv1.VideoVisitParticipant{
				Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			}},
		},
	}}
	if err := s.runCancelPromotionWorkflowCalculateVideoVisit(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.telehealth.v1.VideoVisitCalculationService/CalculateVideoVisit")
	}
	calculateVideoVisitReply := step.reply
	snapshot, err := protojson.Marshal(req.GetPromotion())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveCampaignConflictRequest := newResolveCampaignConflictRequest(withResolveCampaignConflictContext(req.GetContext()), withResolveCampaignConflictCampaign(&promotionsv1.Campaign{
		Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
			Contact: &commonv1.ContactInfo{Email: req.GetPromotion().GetOwner().GetEmail()},
		}},
		Organizer: &identityv1.UserRef{
			Email: calculateVideoVisitReply.GetVideoVisit().GetOrganizer().GetEmail(),
		},
	}), withResolveCampaignConflictDetails(&promotionsv1.ResolveCampaignConflictRequest_Details{Payload: snapshot}))
	resolveCampaignConflictReply, err := s.campaignSyncClient.ResolveCampaignConflict(ctx, resolveCampaignConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/ResolveCampaignConflict")
	}
	recalculateCampaignsRequest := &promotionsv1.RecalculateCampaignsRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			}},
		},
	}
	recalculateCampaignsReply, err := grpcx.Call(ctx, s.campaignCalculationClient.RecalculateCampaigns, recalculateCampaignsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignCalculationService/RecalculateCampaigns")
	}
	for condition := range iterx.Each(req.GetPromotion().GetConditions(), func(candidate *promotionsv1.PromotionCondition) bool { return candidate != nil }) {
		if _, err := s.promotionApprovalClient.RecallPromotion(ctx, &promotionsv1.RecallPromotionRequest{
			Context: req.GetContext(),
			Promotion: &promotionsv1.Promotion{
				Conditions: []*promotionsv1.PromotionCondition{&promotionsv1.PromotionCondition{Attribute: condition.GetAttribute()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionApprovalService/RecallPromotion")
		}
	}
	return &promotionsv1.CancelPromotionWorkflowResponse{
		Outcome: &promotionsv1.CancelPromotionWorkflowResponse_Outcome{Message: calculateVideoVisitReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Status: resolveCampaignConflictReply.GetResult().GetStatus(),
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: resolveCampaignConflictReply.GetCampaign().GetAudit().GetUpdatedBy(),
				CreatedAt: recalculateCampaignsReply.GetCampaign().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				Email: recalculateCampaignsReply.GetCampaign().GetOrganizer().GetEmail(),
			},
			PromotionId: req.GetPromotionId(),
			Parameters:  req.GetDetails().GetParameters(),
		},
	}, nil
}

// RetryPromotionWorkflow retrys a promotion.
func (s *PromotionWorkflowServer) RetryPromotionWorkflow(ctx context.Context, req *promotionsv1.RetryPromotionWorkflowRequest) (*promotionsv1.RetryPromotionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryPromotionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "retry promotion workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	lintRedemptionsRequest := &promotionsv1.LintRedemptionsRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.LintRedemptionsRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
	}
	lintRedemptionsReply, err := s.redemptionValidationPort.LintRedemptions(ctx, lintRedemptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionValidationService/LintRedemptions")
	}
	shareCampaignReportRequest := buildShareCampaignReportRequestForRetryPromotionWorkflow(req)
	shareCampaignReportReply, err := s.campaignReportingClient.ShareCampaignReport(ctx, shareCampaignReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignReportingService/ShareCampaignReport")
	}
	return &promotionsv1.RetryPromotionWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: lintRedemptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &promotionsv1.RetryPromotionWorkflowResponse_Outcome{
			EffectiveAt: lintRedemptionsReply.GetOutcome().GetEffectiveAt(),
		},
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				DisplayName: shareCampaignReportReply.GetCampaign().GetOrganizer().GetDisplayName(),
			},
			PromotionId: req.GetPromotionId(),
			Status:      req.GetPromotion().GetStatus(),
		},
	}, nil
}

// PausePromotionWorkflow pauses a promotion.
func (s *PromotionWorkflowServer) PausePromotionWorkflow(ctx context.Context, req *promotionsv1.PausePromotionWorkflowRequest) (*promotionsv1.PausePromotionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "pause promotion workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	compareCouponOptionsRequest := &promotionsv1.CompareCouponOptionsRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPromotion().GetAudit().GetEtag(),
				CreatedBy: req.GetPromotion().GetAudit().GetCreatedBy(),
			},
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
	}
	compareCouponOptionsReply, err := compareCouponOptionsCall(ctx, compareCouponOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/CompareCouponOptions")
	}
	packed, err := anypb.New(req.GetPromotion())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendTenantTestNoticeRequest := &identityv1.SendTenantTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendTenantTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetPromotion().GetOwner().GetEmail()},
			Payload:   packed,
		},
	}
	sendTenantTestNoticeReply, err := grpcx.Call(ctx, s.tenantNotificationClient.SendTenantTestNotice, sendTenantTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/SendTenantTestNotice")
	}
	approveCouponRequest := &promotionsv1.ApproveCouponRequest{}
	approveCouponRequest.Context = req.GetContext()
	approveCouponRequest.Coupon = &promotionsv1.Coupon{
		Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPromotion().GetAudit().GetUpdatedAt()},
		Holder: &promotionsv1.PromotionRef{
			DisplayName: compareCouponOptionsReply.GetCoupon().GetHolder().GetDisplayName(),
		},
	}
	approveCouponReply, err := s.couponApprovalClient.ApproveCoupon(ctx, approveCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponApprovalService/ApproveCoupon")
	}
	return &promotionsv1.PausePromotionWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: compareCouponOptionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				Etag:      sendTenantTestNoticeReply.GetTenant().GetAudit().GetEtag(),
				CreatedBy: approveCouponReply.GetCoupon().GetAudit().GetCreatedBy(),
			},
			PromotionId: approveCouponReply.GetCoupon().GetHolder().GetPromotionId(),
			Parameters:  req.GetDetails().GetParameters(),
			Priority:    req.GetPromotion().GetPriority(),
		},
	}, nil
}

// ResumePromotionWorkflow resumes a promotion.
func (s *PromotionWorkflowServer) ResumePromotionWorkflow(ctx context.Context, req *promotionsv1.ResumePromotionWorkflowRequest) (*promotionsv1.ResumePromotionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "resume promotion workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "updated_by", req.GetPromotion().GetAudit().GetUpdatedBy())
	scoreApiCredentialRequest := &identityv1.ScoreApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetPromotion().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetPromotion().GetAudit().GetCreatedAt(),
				UpdatedBy: tenancy.Value(ctx, "updated_by"),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	scoreApiCredentialReply, err := grpcx.Call(ctx, s.apiCredentialValidationClient.ScoreApiCredential, scoreApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/ScoreApiCredential")
	}
	return &promotionsv1.ResumePromotionWorkflowResponse{
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreApiCredentialReply.GetApiCredential().GetAudit().GetCreatedBy(),
			},
			PromotionId: req.GetPromotionId(),
			Effective:   req.GetPromotion().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectPromotionWorkflow returns a promotion.
func (s *PromotionWorkflowServer) InspectPromotionWorkflow(ctx context.Context, req *promotionsv1.InspectPromotionWorkflowRequest) (*promotionsv1.InspectPromotionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectPromotionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "inspect_promotion_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect promotion workflow", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	rejectCartRequest := &cartv1.RejectCartRequest{
		Context: req.GetContext(),
		Details: &cartv1.RejectCartRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
		Cart: &cartv1.Cart{
			IssuedAt: cloned.GetDeadline(),
			Lines:    []*cartv1.CartLine{&cartv1.CartLine{Attributes: baggage}},
		},
	}
	rejectCartReply := new(cartv1.RejectCartResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", cartCartApprovalServiceName, "RejectCart"), rejectCartRequest, rejectCartReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartApprovalService/RejectCart")
	}
	return &promotionsv1.InspectPromotionWorkflowResponse{
		Result:    &commonv1.OperationResult{Status: rejectCartReply.GetOutcome().GetStatus()},
		Promotion: &promotionsv1.Promotion{PromotionId: req.GetPromotionId()},
	}, nil
}

// ListPromotionWorkflows lists promotions a page at a time.
func (s *PromotionWorkflowServer) ListPromotionWorkflows(ctx context.Context, req *promotionsv1.ListPromotionWorkflowsRequest) (*promotionsv1.ListPromotionWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list promotion workflows", "tenant", req.GetContext().GetTenantId())
	return &promotionsv1.ListPromotionWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalPromotionWorkflow signals a promotion.
func (s *PromotionWorkflowServer) SignalPromotionWorkflow(ctx context.Context, req *promotionsv1.SignalPromotionWorkflowRequest) (*promotionsv1.SignalPromotionWorkflowResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "signal promotion workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPromotion())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.Promotion
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	streamCampaignMetricsStream, err := s.campaignReportingClient.StreamCampaignMetrics(ctx, &promotionsv1.StreamCampaignMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone: req.GetDetails().GetNotify().GetPhone(),
			Email: (&decoded).GetOwner().GetEmail(),
			Name:  req.GetDetails().GetNotify().GetName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignReportingService/StreamCampaignMetrics")
	}
	for {
		received, err := streamCampaignMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignReportingService/StreamCampaignMetrics")
		}
		_ = received
	}
	commentPromotionApprovalRequest := &promotionsv1.CommentPromotionApprovalRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{Email: req.GetPromotion().GetOwner().GetEmail()},
		},
	}
	commentPromotionApprovalReply, err := promotionsv1.PromotionApprovalServiceClient.CommentPromotionApproval(s.promotionApprovalClient, ctx, commentPromotionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionApprovalService/CommentPromotionApproval")
	}
	return &promotionsv1.SignalPromotionWorkflowResponse{
		Outcome: &promotionsv1.SignalPromotionWorkflowResponse_Outcome{
			Message: commentPromotionApprovalReply.GetResult().GetMessage(),
			Status:  commentPromotionApprovalReply.GetOutcome().GetStatus(),
		},
		Promotion: &promotionsv1.Promotion{
			PromotionId: req.GetPromotionId(),
			Owner:       req.GetPromotion().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// promotionWorkflowServerCancelPromotionWorkflowCalculateVideoVisitStep carries the request of one step of
// CancelPromotionWorkflow and, once it ran, its reply.
type promotionWorkflowServerCancelPromotionWorkflowCalculateVideoVisitStep struct {
	request *telehealthv1.CalculateVideoVisitRequest
	reply   *telehealthv1.CalculateVideoVisitResponse
}

func (s *PromotionWorkflowServer) runCancelPromotionWorkflowCalculateVideoVisit(ctx context.Context, step *promotionWorkflowServerCancelPromotionWorkflowCalculateVideoVisitStep) error {
	reply, err := s.videoVisitCalculationClient.CalculateVideoVisit(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
