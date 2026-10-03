package promotions

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	experimentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	toursv1 "github.com/hounddogai/protomap-test-golang/gen/acme/tours/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CampaignLifecycleServer implements `acme.promotions.v1.CampaignLifecycleService`.
// Moves a campaign between the states of its lifecycle.
type CampaignLifecycleServer struct {
	promotionsv1.UnimplementedCampaignLifecycleServiceServer
	store                           *Store
	log                             *slog.Logger
	experimentQueryClient           experimentsv1.ExperimentQueryServiceClient
	conn                            *grpc.ClientConn
	couponReportingClient           promotionsv1.CouponReportingServiceClient
	meteredReviewModerationApproval meteredReviewModerationApproval
	redemptionSyncClient            promotionsv1.RedemptionSyncServiceClient
	tourQueryClient                 toursv1.TourQueryServiceClient
	pointsLedgerApprovalClient      loyaltyv1.PointsLedgerApprovalServiceClient
	meteredRedemptionSync           meteredRedemptionSync
	redemptionImportClient          promotionsv1.RedemptionImportServiceClient
	promotionLifecycleClient        promotionsv1.PromotionLifecycleServiceClient
	synonymNotificationClient       searchv1.SynonymNotificationServiceClient
	couponValidationClient          promotionsv1.CouponValidationServiceClient
	promotionQueryClient            promotionsv1.PromotionQueryServiceClient
	campaignReportingClient         promotionsv1.CampaignReportingServiceClient
}

// NewCampaignLifecycleServer wires a CampaignLifecycleServer.
func NewCampaignLifecycleServer(deps platform.Deps) *CampaignLifecycleServer {
	return &CampaignLifecycleServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.promotions.v1.CampaignLifecycleService"),
		experimentQueryClient:           experimentsv1.NewExperimentQueryServiceClient(deps.Conn),
		conn:                            deps.Conn,
		couponReportingClient:           promotionsv1.NewCouponReportingServiceClient(deps.Conn),
		meteredReviewModerationApproval: meteredReviewModerationApproval{ReviewModerationApprovalServiceClient: reviewsv1.NewReviewModerationApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		redemptionSyncClient:            promotionsv1.NewRedemptionSyncServiceClient(deps.Conn),
		tourQueryClient:                 toursv1.NewTourQueryServiceClient(deps.Conn),
		pointsLedgerApprovalClient:      loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn),
		meteredRedemptionSync:           meteredRedemptionSync{RedemptionSyncServiceClient: promotionsv1.NewRedemptionSyncServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		redemptionImportClient:          promotionsv1.NewRedemptionImportServiceClient(deps.Conn),
		promotionLifecycleClient:        promotionsv1.NewPromotionLifecycleServiceClient(deps.Conn),
		synonymNotificationClient:       searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		couponValidationClient:          promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		promotionQueryClient:            promotionsv1.NewPromotionQueryServiceClient(deps.Conn),
		campaignReportingClient:         promotionsv1.NewCampaignReportingServiceClient(deps.Conn),
	}
}

// ActivateCampaign activates a campaign.
func (s *CampaignLifecycleServer) ActivateCampaign(ctx context.Context, req *promotionsv1.ActivateCampaignRequest) (*promotionsv1.ActivateCampaignResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "activate_campaign"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate campaign", "tenant", req.GetContext().GetTenantId())
	step := &campaignLifecycleServerActivateCampaignResolveExperimentsStep{request: &experimentsv1.ResolveExperimentsRequest{Context: req.GetContext()}}
	if err := s.runActivateCampaignResolveExperiments(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.experiments.v1.ExperimentQueryService/ResolveExperiments")
	}
	resolveExperimentsReply := step.reply
	resumeSubscriptionWorkflowRequest := &subscriptionsv1.ResumeSubscriptionWorkflowRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNotifyContact().GetEmail(),
				Name:  req.GetDetails().GetNotifyContact().GetName(),
			},
		},
	}
	resumeSubscriptionWorkflowReply := new(subscriptionsv1.ResumeSubscriptionWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, subscriptionsv1.SubscriptionWorkflowService_ResumeSubscriptionWorkflow_FullMethodName, resumeSubscriptionWorkflowRequest, resumeSubscriptionWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionWorkflowService/ResumeSubscriptionWorkflow")
	}
	return &promotionsv1.ActivateCampaignResponse{
		Result: &commonv1.OperationResult{
			Message: resolveExperimentsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: resumeSubscriptionWorkflowReply.GetSubscription().GetAudit().GetUpdatedAt(),
			},
			CampaignId: req.GetCampaignId(),
		},
		Outcome: &promotionsv1.ActivateCampaignResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendCampaign suspends a campaign.
func (s *CampaignLifecycleServer) SuspendCampaign(ctx context.Context, req *promotionsv1.SuspendCampaignRequest) (*promotionsv1.SuspendCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendCampaign", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "suspend_campaign"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend campaign", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainRedemptionDecisionRequest := &promotionsv1.ExplainRedemptionDecisionRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ExplainRedemptionDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotifyContact().GetName(),
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
		Redemption: &promotionsv1.Redemption{Detail: packed},
	}
	explainRedemptionDecisionReply := new(promotionsv1.ExplainRedemptionDecisionResponse)
	if err := s.conn.Invoke(ctx, promotionsRedemptionValidationServiceExplainRedemptionDecisionMethod, explainRedemptionDecisionRequest, explainRedemptionDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionValidationService/ExplainRedemptionDecision")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateCouponReportRequest := &promotionsv1.GenerateCouponReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		CouponId:    explainRedemptionDecisionReply.GetRedemption().GetSubject().GetCouponId(),
		Reason:      req.GetReason(),
		Details: &promotionsv1.GenerateCouponReportRequest_Details{
			Filters: explainRedemptionDecisionReply.GetResult().GetDetails(),
		},
	}
	type generateCouponReportResult struct {
		reply *promotionsv1.GenerateCouponReportResponse
		err   error
	}
	generateCouponReportResults := make(chan generateCouponReportResult, 1)
	go func() {
		reply, err := s.couponReportingClient.GenerateCouponReport(ctx, generateCouponReportRequest)
		generateCouponReportResults <- generateCouponReportResult{reply, err}
	}()
	var generateCouponReportReply *promotionsv1.GenerateCouponReportResponse
	select {
	case result := <-generateCouponReportResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.promotions.v1.CouponReportingService/GenerateCouponReport")
		}
		generateCouponReportReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	commentReviewModerationApprovalRequest := &reviewsv1.CommentReviewModerationApprovalRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	commentReviewModerationApprovalReply, err := s.meteredReviewModerationApproval.CommentReviewModerationApproval(ctx, commentReviewModerationApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationApprovalService/CommentReviewModerationApproval")
	}
	return &promotionsv1.SuspendCampaignResponse{
		Outcome: &promotionsv1.SuspendCampaignResponse_Outcome{
			Message:     explainRedemptionDecisionReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: generateCouponReportReply.GetResult().GetStatus()},
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: commentReviewModerationApprovalReply.GetReviewModeration().GetAudit().GetUpdatedBy(),
			},
			CampaignId: req.GetCampaignId(),
		},
	}, nil
}

// ReinstateCampaign reinstates a campaign.
func (s *CampaignLifecycleServer) ReinstateCampaign(ctx context.Context, req *promotionsv1.ReinstateCampaignRequest) (*promotionsv1.ReinstateCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReinstateCampaign", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "reinstate_campaign"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate campaign", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	resolveRedemptionConflictRequest := &promotionsv1.ResolveRedemptionConflictRequest{}
	resolveRedemptionConflictRequest.Context = req.GetContext()
	resolveRedemptionConflictRequest.Redemption = &promotionsv1.Redemption{
		Subject: &promotionsv1.CouponRef{
			DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
		},
		Detail: packed,
	}
	resolveRedemptionConflictRequest.Annotations = annotations
	resolveRedemptionConflictRequest.Reason = req.GetReason()
	resolveRedemptionConflictReply, err := s.redemptionSyncClient.ResolveRedemptionConflict(ctx, resolveRedemptionConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionSyncService/ResolveRedemptionConflict")
	}
	summarizeToursRequest := buildSummarizeToursRequestForReinstateCampaign(req)
	summarizeToursReply, err := s.tourQueryClient.SummarizeTours(ctx, summarizeToursRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.tours.v1.TourQueryService/SummarizeTours")
	}
	detailsCache := cache.New[string, *promotionsv1.ReinstateCampaignRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	commentPointsLedgerApprovalRequest := &loyaltyv1.CommentPointsLedgerApprovalRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: resolveRedemptionConflictReply.GetRedemption().GetSubject().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{Phone: cachedDetails.GetNotifyContact().GetPhone()},
		},
	}
	commentPointsLedgerApprovalReply, err := loyaltyv1.PointsLedgerApprovalServiceClient.CommentPointsLedgerApproval(s.pointsLedgerApprovalClient, ctx, commentPointsLedgerApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/CommentPointsLedgerApproval")
	}
	return &promotionsv1.ReinstateCampaignResponse{
		Result: &commonv1.OperationResult{
			Message: resolveRedemptionConflictReply.GetResult().GetMessage(),
			Status:  summarizeToursReply.GetResult().GetStatus(),
		},
		Outcome: &promotionsv1.ReinstateCampaignResponse_Outcome{
			NextSteps:   resolveRedemptionConflictReply.GetOutcome().GetNextSteps(),
			Status:      commentPointsLedgerApprovalReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Campaign: &promotionsv1.Campaign{CampaignId: req.GetCampaignId()},
	}, nil
}

// TerminateCampaign terminates a campaign.
func (s *CampaignLifecycleServer) TerminateCampaign(ctx context.Context, req *promotionsv1.TerminateCampaignRequest) (*promotionsv1.TerminateCampaignResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "terminate_campaign"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate campaign", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileRedemptionsRequest := &promotionsv1.ReconcileRedemptionsRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Reason:      tenancy.Value(ctx, "reason"),
		Annotations: req.GetAnnotations(),
		Details:     &promotionsv1.ReconcileRedemptionsRequest_Details{Payload: snapshot},
	}
	reconcileRedemptionsReply, err := s.meteredRedemptionSync.ReconcileRedemptions(ctx, reconcileRedemptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionSyncService/ReconcileRedemptions")
	}
	return &promotionsv1.TerminateCampaignResponse{
		Outcome: &promotionsv1.TerminateCampaignResponse_Outcome{
			Message:     reconcileRedemptionsReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result:   &commonv1.OperationResult{Status: reconcileRedemptionsReply.GetResult().GetStatus()},
		Campaign: &promotionsv1.Campaign{CampaignId: req.GetCampaignId()},
	}, nil
}

// RenewCampaign renews a campaign.
func (s *CampaignLifecycleServer) RenewCampaign(ctx context.Context, req *promotionsv1.RenewCampaignRequest) (*promotionsv1.RenewCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewCampaign", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "renew_campaign"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew campaign", "tenant", req.GetContext().GetTenantId())
	countRedemptionTotalsRequest := &promotionsv1.CountRedemptionTotalsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.CountRedemptionTotalsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}
	countRedemptionTotalsReply := new(promotionsv1.CountRedemptionTotalsResponse)
	if err := grpc.Invoke(ctx, "/acme.promotions.v1.RedemptionReportingService/CountRedemptionTotals", countRedemptionTotalsRequest, countRedemptionTotalsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionReportingService/CountRedemptionTotals")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullRedemptionsRequest := &promotionsv1.PullRedemptionsRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			CorrelationId: countRedemptionTotalsReply.GetResult().GetCorrelationId(),
			Subject: &promotionsv1.CouponRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &promotionsv1.PullRedemptionsRequest_Details{Payload: snapshot},
	}
	pullRedemptionsReply := new(promotionsv1.PullRedemptionsResponse)
	if err := grpc.Invoke(ctx, "/acme.promotions.v1.RedemptionSyncService/PullRedemptions", pullRedemptionsRequest, pullRedemptionsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionSyncService/PullRedemptions")
	}
	validateRedemptionImportRequest := &promotionsv1.ValidateRedemptionImportRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: pullRedemptionsReply.GetRedemption().GetSubject().GetDisplayName(),
			},
		},
	}
	validateRedemptionImportReply, err := grpcx.Call(ctx, s.redemptionImportClient.ValidateRedemptionImport, validateRedemptionImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionImportService/ValidateRedemptionImport")
	}
	return &promotionsv1.RenewCampaignResponse{
		Result: &commonv1.OperationResult{
			Message:       countRedemptionTotalsReply.GetResult().GetMessage(),
			CorrelationId: countRedemptionTotalsReply.GetResult().GetCorrelationId(),
			Details:       validateRedemptionImportReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Outcome: &promotionsv1.RenewCampaignResponse_Outcome{
			NextSteps:   pullRedemptionsReply.GetOutcome().GetNextSteps(),
			Message:     pullRedemptionsReply.GetOutcome().GetMessage(),
			ReferenceId: validateRedemptionImportReply.GetOutcome().GetReferenceId(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Campaign: &promotionsv1.Campaign{CampaignId: req.GetCampaignId()},
	}, nil
}

// ExpireCampaign expires a campaign.
func (s *CampaignLifecycleServer) ExpireCampaign(ctx context.Context, req *promotionsv1.ExpireCampaignRequest) (*promotionsv1.ExpireCampaignResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "expire_campaign"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire campaign", "tenant", req.GetContext().GetTenantId())
	schedulePromotionExpiryReply, err := s.promotionLifecycleClient.SchedulePromotionExpiry(ctx, &promotionsv1.SchedulePromotionExpiryRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionLifecycleService/SchedulePromotionExpiry")
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotifyContact().GetPhone())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeSynonymUpdatesRequest := &searchv1.UnsubscribeSynonymUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone:            tenancy.Value(ctx, "phone"),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
			Payload: packed,
		},
	}
	unsubscribeSynonymUpdatesReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*searchv1.UnsubscribeSynonymUpdatesResponse, error) {
		return s.synonymNotificationClient.UnsubscribeSynonymUpdates(ctx, unsubscribeSynonymUpdatesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/UnsubscribeSynonymUpdates")
	}
	scoreCouponReply, err := s.couponValidationClient.ScoreCoupon(ctx, &promotionsv1.ScoreCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				CreatedAt: unsubscribeSynonymUpdatesReply.GetSynonym().GetAudit().GetCreatedAt(),
				Etag:      schedulePromotionExpiryReply.GetPromotion().GetAudit().GetEtag(),
			},
			Contact: &commonv1.ContactInfo{
				Email: unsubscribeSynonymUpdatesReply.GetSynonym().GetOwner().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ScoreCoupon")
	}
	return &promotionsv1.ExpireCampaignResponse{
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{
				DisplayName: schedulePromotionExpiryReply.GetPromotion().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: scoreCouponReply.GetCoupon().GetAudit().GetCreatedAt(),
			},
			CampaignId: req.GetCampaignId(),
		},
		Result: &commonv1.OperationResult{
			Details: unsubscribeSynonymUpdatesReply.GetResult().GetDetails(),
			Status:  scoreCouponReply.GetResult().GetStatus(),
		},
		Outcome: &promotionsv1.ExpireCampaignResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ScheduleCampaignExpiry schedules a campaign.
func (s *CampaignLifecycleServer) ScheduleCampaignExpiry(ctx context.Context, req *promotionsv1.ScheduleCampaignExpiryRequest) (*promotionsv1.ScheduleCampaignExpiryResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "schedule campaign expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	browsePromotionsRequest := &promotionsv1.BrowsePromotionsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.BrowsePromotionsRequest_Filter{
			Region:       req.GetCampaign().GetLocation().GetRegion(),
			CreatedAfter: req.GetCampaign().GetAudit().GetCreatedAt(),
		},
	}
	browsePromotionsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*promotionsv1.BrowsePromotionsResponse, error) {
		return s.promotionQueryClient.BrowsePromotions(ctx, browsePromotionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionQueryService/BrowsePromotions")
	}
	source := req.GetDetails().GetNotify().ProtoReflect()
	phone := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("phone"))).String()
	pauseSubscriptionWorkflowRequest := &subscriptionsv1.PauseSubscriptionWorkflowRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.PauseSubscriptionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Email:            req.GetCampaign().GetOrganizer().GetEmail(),
			},
		},
		Subscription: &subscriptionsv1.Subscription{Contact: &commonv1.ContactInfo{Phone: phone}},
	}
	pauseSubscriptionWorkflowReply := new(subscriptionsv1.PauseSubscriptionWorkflowResponse)
	if err := s.conn.Invoke(ctx, subscriptionsSubscriptionWorkflowServicePauseSubscriptionWorkflowMethod, pauseSubscriptionWorkflowRequest, pauseSubscriptionWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionWorkflowService/PauseSubscriptionWorkflow")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := campaignLifecycleServerGenerateCampaignReportSteps[mode]
	generateCampaignReportRequest := &promotionsv1.GenerateCampaignReportRequest{
		Context:     req.GetContext(),
		CampaignId:  req.GetCampaignId(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details: &promotionsv1.GenerateCampaignReportRequest_Details{
			Filters: pauseSubscriptionWorkflowReply.GetSubscription().GetLabels(),
		},
	}
	generateCampaignReportReply, err := run(s, ctx, generateCampaignReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignReportingService/GenerateCampaignReport")
	}
	return &promotionsv1.ScheduleCampaignExpiryResponse{
		Result: &commonv1.OperationResult{Status: browsePromotionsReply.GetResult().GetStatus()},
		Outcome: &promotionsv1.ScheduleCampaignExpiryResponse_Outcome{
			Message: pauseSubscriptionWorkflowReply.GetResult().GetMessage(),
			Status:  generateCampaignReportReply.GetResult().GetStatus(),
		},
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Status:     req.GetCampaign().GetStatus(),
		},
	}, nil
}

// campaignLifecycleServerActivateCampaignResolveExperimentsStep carries the request of one step of ActivateCampaign
// and, once it ran, its reply.
type campaignLifecycleServerActivateCampaignResolveExperimentsStep struct {
	request *experimentsv1.ResolveExperimentsRequest
	reply   *experimentsv1.ResolveExperimentsResponse
}

func (s *CampaignLifecycleServer) runActivateCampaignResolveExperiments(ctx context.Context, step *campaignLifecycleServerActivateCampaignResolveExperimentsStep) error {
	reply, err := s.experimentQueryClient.ResolveExperiments(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// campaignLifecycleServerGenerateCampaignReportSteps are the ways to run
// acme.promotions.v1.CampaignReportingService/GenerateCampaignReport, by name.
var campaignLifecycleServerGenerateCampaignReportSteps = map[string]func(*CampaignLifecycleServer, context.Context, *promotionsv1.GenerateCampaignReportRequest) (*promotionsv1.GenerateCampaignReportResponse, error){
	"default": (*CampaignLifecycleServer).generateCampaignReportStep,
	"audited": (*CampaignLifecycleServer).generateCampaignReportAuditedStep,
}

func (s *CampaignLifecycleServer) generateCampaignReportStep(ctx context.Context, request *promotionsv1.GenerateCampaignReportRequest) (*promotionsv1.GenerateCampaignReportResponse, error) {
	return s.campaignReportingClient.GenerateCampaignReport(ctx, request)
}

func (s *CampaignLifecycleServer) generateCampaignReportAuditedStep(ctx context.Context, request *promotionsv1.GenerateCampaignReportRequest) (*promotionsv1.GenerateCampaignReportResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.promotions.v1.CampaignReportingService/GenerateCampaignReport")
	return s.campaignReportingClient.GenerateCampaignReport(ctx, request)
}
