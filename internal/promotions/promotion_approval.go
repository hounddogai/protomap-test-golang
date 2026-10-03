package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PromotionApprovalServer implements `acme.promotions.v1.PromotionApprovalService`.
// Routes promotions through approval, with escalation and delegation.
type PromotionApprovalServer struct {
	promotionsv1.UnimplementedPromotionApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	reviewModerationWorkflowClient  reviewsv1.ReviewModerationWorkflowServiceClient
	tierApprovalClient              loyaltyv1.TierApprovalServiceClient
	annotateRoleVersionFn           func(context.Context, *identityv1.AnnotateRoleVersionRequest, ...grpc.CallOption) (*identityv1.AnnotateRoleVersionResponse, error)
	pointsLedgerApprovalPort        pointsLedgerApprovalPort
	sellerScorecardReportingClient  sellersv1.SellerScorecardReportingServiceClient
	synonymHistoryClient            searchv1.SynonymHistoryServiceClient
	meteredParcelWorkflow           meteredParcelWorkflow
	shareRedemptionReportFn         func(context.Context, *promotionsv1.ShareRedemptionReportRequest, ...grpc.CallOption) (*promotionsv1.ShareRedemptionReportResponse, error)
	redemptionSyncClient            promotionsv1.RedemptionSyncServiceClient
	parcelWorkflowClient            shippingv1.ParcelWorkflowServiceClient
	conn                            *grpc.ClientConn
	metricSeriesValidationOnce      sync.Once
	metricSeriesValidationCached    metricsv1.MetricSeriesValidationServiceClient
	pointsLedgerApprovalClient      loyaltyv1.PointsLedgerApprovalServiceClient
	couponCalculationPort           couponCalculationPort
	synonymExportClient             searchv1.SynonymExportServiceClient
	carrierRateReportingClient      shippingv1.CarrierRateReportingServiceClient
	promotionValidationClient       promotionsv1.PromotionValidationServiceClient
	accessPolicyQueryClient         authorizationv1.AccessPolicyQueryServiceClient
	metricSeriesQueryClient         metricsv1.MetricSeriesQueryServiceClient
	metricSeriesQueryClientFallback metricsv1.MetricSeriesQueryServiceClient
}

// NewPromotionApprovalServer wires a PromotionApprovalServer.
func NewPromotionApprovalServer(deps platform.Deps) *PromotionApprovalServer {
	roleHistory := identityv1.NewRoleHistoryServiceClient(deps.Conn)
	redemptionReporting := promotionsv1.NewRedemptionReportingServiceClient(deps.Conn)
	return &PromotionApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.promotions.v1.PromotionApprovalService"),
		reviewModerationWorkflowClient:  reviewsv1.NewReviewModerationWorkflowServiceClient(deps.Conn),
		tierApprovalClient:              loyaltyv1.NewTierApprovalServiceClient(deps.Conn),
		annotateRoleVersionFn:           roleHistory.AnnotateRoleVersion,
		pointsLedgerApprovalPort:        newPointsLedgerApprovalPort(deps),
		sellerScorecardReportingClient:  sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
		synonymHistoryClient:            searchv1.NewSynonymHistoryServiceClient(deps.Conn),
		meteredParcelWorkflow:           meteredParcelWorkflow{ParcelWorkflowServiceClient: shippingv1.NewParcelWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		shareRedemptionReportFn:         redemptionReporting.ShareRedemptionReport,
		redemptionSyncClient:            promotionsv1.NewRedemptionSyncServiceClient(deps.Conn),
		parcelWorkflowClient:            shippingv1.NewParcelWorkflowServiceClient(deps.Conn),
		conn:                            deps.Conn,
		pointsLedgerApprovalClient:      loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn),
		couponCalculationPort:           newCouponCalculationPort(deps),
		synonymExportClient:             searchv1.NewSynonymExportServiceClient(deps.Conn),
		carrierRateReportingClient:      shippingv1.NewCarrierRateReportingServiceClient(deps.Conn),
		promotionValidationClient:       promotionsv1.NewPromotionValidationServiceClient(deps.Conn),
		accessPolicyQueryClient:         authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		metricSeriesQueryClient:         metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		metricSeriesQueryClientFallback: metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
	}
}

// SubmitPromotion submits a promotion.
func (s *PromotionApprovalServer) SubmitPromotion(ctx context.Context, req *promotionsv1.SubmitPromotionRequest) (*promotionsv1.SubmitPromotionResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "submit promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	resumeReviewModerationWorkflowRequest := &reviewsv1.ResumeReviewModerationWorkflowRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPromotion().GetAudit().GetUpdatedAt()},
		},
	}
	resumeReviewModerationWorkflowReply, err := reviewsv1.ReviewModerationWorkflowServiceClient.ResumeReviewModerationWorkflow(s.reviewModerationWorkflowClient, ctx, resumeReviewModerationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationWorkflowService/ResumeReviewModerationWorkflow")
	}
	rejectTierRequest := &loyaltyv1.RejectTierRequest{}
	rejectTierRequest.Context = req.GetContext()
	rejectTierRequest.Details = &loyaltyv1.RejectTierRequest_Details{
		Approver: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
	}
	rejectTierReply, err := s.tierApprovalClient.RejectTier(ctx, rejectTierRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierApprovalService/RejectTier")
	}
	return &promotionsv1.SubmitPromotionResponse{
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				UserId:      resumeReviewModerationWorkflowReply.GetReviewModeration().GetAssignee().GetUserId(),
				DisplayName: rejectTierReply.GetTier().GetOwner().GetDisplayName(),
			},
			PromotionId: req.GetPromotionId(),
			Effective:   req.GetPromotion().GetEffective(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: rejectTierReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ApprovePromotion approves a promotion.
func (s *PromotionApprovalServer) ApprovePromotion(ctx context.Context, req *promotionsv1.ApprovePromotionRequest) (*promotionsv1.ApprovePromotionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "approve promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetPromotion())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateRoleVersionRequest := &identityv1.AnnotateRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner:  &identityv1.UserRef{DisplayName: req.GetPromotion().GetOwner().GetDisplayName()},
			Action: &identityv1.Role_Route{Route: &identityv1.RoleRouteAction{Weight: req.GetPromotion().GetRoute().GetWeight()}},
		},
		Details: &identityv1.AnnotateRoleVersionRequest_Details{Snapshot: snapshot},
	}
	annotateRoleVersionReply, err := s.annotateRoleVersionFn(ctx, annotateRoleVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/AnnotateRoleVersion")
	}
	cloned := proto.Clone(req.GetPromotion()).(*promotionsv1.Promotion)
	listPendingPointsLedgersRequest := &loyaltyv1.ListPendingPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ListPendingPointsLedgersRequest_Filter{
			CreatedAfter: req.GetPromotion().GetAudit().GetCreatedAt(),
			OwnerId:      cloned.GetOwner().GetUserId(),
			Labels:       annotateRoleVersionReply.GetRole().GetParameters(),
		},
	}
	listPendingPointsLedgersReply, err := s.pointsLedgerApprovalPort.ListPendingPointsLedgers(ctx, listPendingPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/ListPendingPointsLedgers")
	}
	listSellerScorecardReportsRequest := &sellersv1.ListSellerScorecardReportsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerScorecardReportsRequest_Filter{
			CreatedAfter: annotateRoleVersionReply.GetRole().GetAudit().GetCreatedAt(),
			Labels:       listPendingPointsLedgersReply.GetResult().GetDetails(),
			OwnerId:      req.GetPromotion().GetPromotionId(),
		},
	}
	type listSellerScorecardReportsResult struct {
		reply *sellersv1.ListSellerScorecardReportsResponse
		err   error
	}
	listSellerScorecardReportsResults := make(chan listSellerScorecardReportsResult, 1)
	go func() {
		reply, err := s.sellerScorecardReportingClient.ListSellerScorecardReports(ctx, listSellerScorecardReportsRequest)
		listSellerScorecardReportsResults <- listSellerScorecardReportsResult{reply, err}
	}()
	var listSellerScorecardReportsReply *sellersv1.ListSellerScorecardReportsResponse
	select {
	case result := <-listSellerScorecardReportsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerScorecardReportingService/ListSellerScorecardReports")
		}
		listSellerScorecardReportsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	listSynonymVersionsRequest := &searchv1.ListSynonymVersionsRequest{}
	listSynonymVersionsRequest.Context = req.GetContext()
	listSynonymVersionsRequest.Filter = &searchv1.ListSynonymVersionsRequest_Filter{
		CreatedAfter: annotateRoleVersionReply.GetRole().GetAudit().GetUpdatedAt(),
		OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		Labels:       annotateRoleVersionReply.GetResult().GetDetails(),
	}
	listSynonymVersionsReply, err := s.synonymHistoryClient.ListSynonymVersions(ctx, listSynonymVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/ListSynonymVersions")
	}
	return &promotionsv1.ApprovePromotionResponse{
		Promotion: &promotionsv1.Promotion{
			Description: annotateRoleVersionReply.GetRole().GetDescription(),
			PromotionId: req.GetPromotionId(),
			Status:      req.GetPromotion().GetStatus(),
		},
		Outcome: &promotionsv1.ApprovePromotionResponse_Outcome{
			Status:  listPendingPointsLedgersReply.GetResult().GetStatus(),
			Message: listSynonymVersionsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: listSellerScorecardReportsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// RejectPromotion rejects a promotion.
func (s *PromotionApprovalServer) RejectPromotion(ctx context.Context, req *promotionsv1.RejectPromotionRequest) (*promotionsv1.RejectPromotionResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "reject promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	promotionCache := cache.New[string, *promotionsv1.Promotion](1)
	promotionCache.Put(req.GetContext().GetTraceId(), req.GetPromotion())
	cachedPromotion, _ := promotionCache.Get(req.GetContext().GetTraceId())
	cancelParcelWorkflowRequest := &shippingv1.CancelParcelWorkflowRequest{
		Context: req.GetContext(),
		Details: &shippingv1.CancelParcelWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: cachedPromotion.GetOwner().GetEmail()},
		},
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{
				UserId:      req.GetPromotion().GetOwner().GetUserId(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetPromotion().GetAudit().GetCreatedAt()},
		},
	}
	cancelParcelWorkflowReply, err := s.meteredParcelWorkflow.CancelParcelWorkflow(ctx, cancelParcelWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/CancelParcelWorkflow")
	}
	return &promotionsv1.RejectPromotionResponse{
		Promotion: &promotionsv1.Promotion{
			Name:        cancelParcelWorkflowReply.GetParcel().GetName(),
			Description: cancelParcelWorkflowReply.GetParcel().GetComponents()["primary"].GetDescription(),
			PromotionId: req.GetPromotionId(),
			Parameters:  req.GetPromotion().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalatePromotion escalates a promotion.
func (s *PromotionApprovalServer) EscalatePromotion(ctx context.Context, req *promotionsv1.EscalatePromotionRequest) (*promotionsv1.EscalatePromotionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalatePromotion", "error", err)
		return nil, err
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "escalate promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	promotionCache := cache.New[string, *promotionsv1.Promotion](1)
	promotionCache.Put(req.GetContext().GetTraceId(), req.GetPromotion())
	cachedPromotion, _ := promotionCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetPromotion())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareRedemptionReportRequest := &promotionsv1.ShareRedemptionReportRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ShareRedemptionReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cachedPromotion.GetOwner().GetEmail()},
			Payload:   packed,
		},
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
	}
	shareRedemptionReportReply, err := s.shareRedemptionReportFn(ctx, shareRedemptionReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionReportingService/ShareRedemptionReport")
	}
	snapshot, err := protojson.Marshal(req.GetPromotion())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileRedemptionsReply, err := s.redemptionSyncClient.ReconcileRedemptions(ctx, &promotionsv1.ReconcileRedemptionsRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: shareRedemptionReportReply.GetRedemption().GetSubject().GetDisplayName(),
			},
		},
		Details: &promotionsv1.ReconcileRedemptionsRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionSyncService/ReconcileRedemptions")
	}
	cancelParcelWorkflowRequest := &shippingv1.CancelParcelWorkflowRequest{
		Context: req.GetContext(),
		Details: &shippingv1.CancelParcelWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		Parcel: &shippingv1.Parcel{
			Owner: &identityv1.UserRef{UserId: req.GetPromotion().GetOwner().GetUserId()},
		},
	}
	cancelParcelWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*shippingv1.CancelParcelWorkflowResponse, error) {
		return s.parcelWorkflowClient.CancelParcelWorkflow(ctx, cancelParcelWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/CancelParcelWorkflow")
	}
	return &promotionsv1.EscalatePromotionResponse{
		Result: &commonv1.OperationResult{
			Details: shareRedemptionReportReply.GetResult().GetDetails(),
			Status:  reconcileRedemptionsReply.GetResult().GetStatus(),
			Message: cancelParcelWorkflowReply.GetResult().GetMessage(),
		},
		Outcome: &promotionsv1.EscalatePromotionResponse_Outcome{
			NextSteps: shareRedemptionReportReply.GetOutcome().GetNextSteps(),
			Message:   cancelParcelWorkflowReply.GetResult().GetMessage(),
		},
		Promotion: &promotionsv1.Promotion{
			PromotionId: req.GetPromotionId(),
			Priority:    req.GetPromotion().GetPriority(),
		},
	}, nil
}

// ListPendingPromotions lists promotions a page at a time.
func (s *PromotionApprovalServer) ListPendingPromotions(ctx context.Context, req *promotionsv1.ListPendingPromotionsRequest) (*promotionsv1.ListPendingPromotionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingPromotions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending promotions", "tenant", req.GetContext().GetTenantId())
	validateMetricSeriesRequest := &metricsv1.ValidateMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Locator: &metricsv1.MetricSeries_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
			Owner:   &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &metricsv1.ValidateMetricSeriesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	validateMetricSeriesReply, err := s.metricSeriesValidationLazy().ValidateMetricSeries(ctx, validateMetricSeriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries")
	}
	return &promotionsv1.ListPendingPromotionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: validateMetricSeriesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Promotions: []*promotionsv1.Promotion{&promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				Email: validateMetricSeriesReply.GetMetricSeries().GetOwner().GetEmail(),
			},
		}},
	}, nil
}

// DelegatePromotionApproval delegates a promotion.
func (s *PromotionApprovalServer) DelegatePromotionApproval(ctx context.Context, req *promotionsv1.DelegatePromotionApprovalRequest) (*promotionsv1.DelegatePromotionApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "delegate promotion approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	commentPointsLedgerApprovalRequest := &loyaltyv1.CommentPointsLedgerApprovalRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPromotion().GetAudit().GetUpdatedBy()},
		},
	}
	commentPointsLedgerApprovalReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*loyaltyv1.CommentPointsLedgerApprovalResponse, error) {
		return s.pointsLedgerApprovalClient.CommentPointsLedgerApproval(ctx, commentPointsLedgerApprovalRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/CommentPointsLedgerApproval")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateRedemptionReportRequest := &promotionsv1.GenerateRedemptionReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	generateRedemptionReportReply, err := generateRedemptionReportCall(ctx, generateRedemptionReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionReportingService/GenerateRedemptionReport")
	}
	estimateCouponRequest := &promotionsv1.EstimateCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				CreatedBy: commentPointsLedgerApprovalReply.GetPointsLedger().GetAudit().GetCreatedBy(),
				UpdatedBy: commentPointsLedgerApprovalReply.GetPointsLedger().GetAudit().GetUpdatedBy(),
			},
		},
	}
	estimateCouponReply, err := s.couponCalculationPort.EstimateCoupon(ctx, estimateCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/EstimateCoupon")
	}
	for _, condition := range req.GetPromotion().GetConditions() {
		if _, err := s.synonymExportClient.RetrySynonymExport(ctx, &searchv1.RetrySynonymExportRequest{
			Context: req.GetContext(),
			Synonym: &searchv1.Synonym{
				Conditions: []*searchv1.SynonymCondition{&searchv1.SynonymCondition{
					Negate: condition.GetNegate(),
					Value:  condition.GetValue(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/RetrySynonymExport")
		}
	}
	return &promotionsv1.DelegatePromotionApprovalResponse{
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				DisplayName: commentPointsLedgerApprovalReply.GetPointsLedger().GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: commentPointsLedgerApprovalReply.GetPointsLedger().GetAudit().GetCreatedBy(),
				Etag:      estimateCouponReply.GetCoupon().GetAudit().GetEtag(),
			},
			PromotionId: req.GetPromotionId(),
			Version:     req.GetPromotion().GetVersion(),
		},
		Result: &commonv1.OperationResult{
			Details: generateRedemptionReportReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &promotionsv1.DelegatePromotionApprovalResponse_Outcome{Message: estimateCouponReply.GetResult().GetMessage()},
	}, nil
}

// RecallPromotion recalls a promotion.
func (s *PromotionApprovalServer) RecallPromotion(ctx context.Context, req *promotionsv1.RecallPromotionRequest) (*promotionsv1.RecallPromotionResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "recall promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	delegateRiskAssessmentApprovalRequest := &fraudv1.DelegateRiskAssessmentApprovalRequest{
		Context: req.GetContext(),
		Details: &fraudv1.DelegateRiskAssessmentApprovalRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetPromotion().GetOwner().GetUserId()},
		},
	}
	delegateRiskAssessmentApprovalReply := new(fraudv1.DelegateRiskAssessmentApprovalResponse)
	if err := s.conn.Invoke(ctx, fraudRiskAssessmentApprovalServiceDelegateRiskAssessmentApprovalMethod, delegateRiskAssessmentApprovalRequest, delegateRiskAssessmentApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentApprovalService/DelegateRiskAssessmentApproval")
	}
	return &promotionsv1.RecallPromotionResponse{
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				Email: delegateRiskAssessmentApprovalReply.GetRiskAssessment().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: delegateRiskAssessmentApprovalReply.GetRiskAssessment().GetAudit().GetEtag(),
			},
			PromotionId: req.GetPromotionId(),
			Parameters:  req.GetPromotion().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPromotionApprovalStatus returns a promotion.
func (s *PromotionApprovalServer) GetPromotionApprovalStatus(ctx context.Context, req *promotionsv1.GetPromotionApprovalStatusRequest) (*promotionsv1.GetPromotionApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "get_promotion_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get promotion approval status", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	countCarrierRateTotalsRequest := &shippingv1.CountCarrierRateTotalsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.CountCarrierRateTotalsRequest_Filter{
			OwnerId:      req.GetPromotionId(),
			CreatedAfter: cachedContext.GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
	countCarrierRateTotalsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*shippingv1.CountCarrierRateTotalsResponse, error) {
		return s.carrierRateReportingClient.CountCarrierRateTotals(ctx, countCarrierRateTotalsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateReportingService/CountCarrierRateTotals")
	}
	lintPromotionsReply, err := s.promotionValidationClient.LintPromotions(ctx, &promotionsv1.LintPromotionsRequest{
		Context:     req.GetContext(),
		PromotionId: req.GetPromotionId(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/LintPromotions")
	}
	return &promotionsv1.GetPromotionApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Details:       countCarrierRateTotalsReply.GetResult().GetDetails(),
			CorrelationId: countCarrierRateTotalsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Promotion: &promotionsv1.Promotion{
			Description: lintPromotionsReply.GetPromotion().GetDescription(),
			Audit: &commonv1.AuditInfo{
				Etag: lintPromotionsReply.GetPromotion().GetAudit().GetEtag(),
			},
			PromotionId: req.GetPromotionId(),
		},
	}, nil
}

// CommentPromotionApproval comments a promotion.
func (s *PromotionApprovalServer) CommentPromotionApproval(ctx context.Context, req *promotionsv1.CommentPromotionApprovalRequest) (*promotionsv1.CommentPromotionApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentPromotionApproval", "error", err)
		return nil, err
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "comment promotion approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPromotion())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.Promotion
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	ctx = tenancy.With(ctx, "user_id", req.GetPromotion().GetOwner().GetUserId())
	parameters := make(map[string]string, len(req.GetPromotion().GetParameters()))
	for key, value := range req.GetPromotion().GetParameters() {
		parameters[key] = value
	}
	countRedemptionTotalsRequest := &promotionsv1.CountRedemptionTotalsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.CountRedemptionTotalsRequest_Filter{
			CreatedAfter: (&decoded).GetAudit().GetCreatedAt(),
			OwnerId:      tenancy.Value(ctx, "user_id"),
			Labels:       parameters,
		},
	}
	countRedemptionTotalsReply, err := countRedemptionTotalsCall(ctx, countRedemptionTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionReportingService/CountRedemptionTotals")
	}
	exportAccessPoliciesStream, err := s.accessPolicyQueryClient.ExportAccessPolicies(ctx, &authorizationv1.ExportAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.ExportAccessPoliciesRequest_Filter{
			CreatedAfter: req.GetPromotion().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/ExportAccessPolicies")
	}
	for {
		received, err := exportAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/ExportAccessPolicies")
		}
		_ = received
	}
	routes := map[string]func(context.Context, *metricsv1.BrowseMetricSeriesesRequest, ...grpc.CallOption) (*metricsv1.BrowseMetricSeriesesResponse, error){
		"primary":  s.metricSeriesQueryClient.BrowseMetricSerieses,
		"fallback": s.metricSeriesQueryClientFallback.BrowseMetricSerieses,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	browseMetricSeriesesRequest := &metricsv1.BrowseMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter:  &metricsv1.BrowseMetricSeriesesRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()},
	}
	browseMetricSeriesesReply, err := route(ctx, browseMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses")
	}
	return &promotionsv1.CommentPromotionApprovalResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: countRedemptionTotalsReply.GetResult().GetReferenceIds(),
			Message:      browseMetricSeriesesReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Promotion: &promotionsv1.Promotion{
			PromotionId: req.GetPromotionId(),
			Parameters:  req.GetPromotion().GetParameters(),
		},
	}, nil
}

// metricSeriesValidationLazy dials acme.metrics.v1.MetricSeriesValidationService the first time a handler needs it.
func (s *PromotionApprovalServer) metricSeriesValidationLazy() metricsv1.MetricSeriesValidationServiceClient {
	s.metricSeriesValidationOnce.Do(func() { s.metricSeriesValidationCached = metricsv1.NewMetricSeriesValidationServiceClient(s.conn) })
	return s.metricSeriesValidationCached
}
