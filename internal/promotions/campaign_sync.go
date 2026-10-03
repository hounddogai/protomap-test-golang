package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"

	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// CampaignSyncServer implements `acme.promotions.v1.CampaignSyncService`.
// Keeps campaigns in step with the systems of record in other regions.
type CampaignSyncServer struct {
	promotionsv1.UnimplementedCampaignSyncServiceServer
	store                           *Store
	log                             *slog.Logger
	productReviewWorkflowClient     reviewsv1.ProductReviewWorkflowServiceClient
	accessPolicyClient              authorizationv1.AccessPolicyServiceClient
	couponValidationClient          promotionsv1.CouponValidationServiceClient
	meteredRiskAssessmentWorkflow   meteredRiskAssessmentWorkflow
	conn                            *grpc.ClientConn
	carePlanClient                  careplansv1.CarePlanServiceClient
	clients                         *registry.Registry
	couponQueryPort                 couponQueryPort
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
	featureFlagClient               featureflagsv1.FeatureFlagServiceClient
	promotionValidationClient       promotionsv1.PromotionValidationServiceClient
	pointsLedgerWorkflowClient      loyaltyv1.PointsLedgerWorkflowServiceClient
}

// NewCampaignSyncServer wires a CampaignSyncServer.
func NewCampaignSyncServer(deps platform.Deps) *CampaignSyncServer {
	return &CampaignSyncServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.promotions.v1.CampaignSyncService"),
		productReviewWorkflowClient:     reviewsv1.NewProductReviewWorkflowServiceClient(deps.Conn),
		accessPolicyClient:              authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		couponValidationClient:          promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		meteredRiskAssessmentWorkflow:   meteredRiskAssessmentWorkflow{RiskAssessmentWorkflowServiceClient: fraudv1.NewRiskAssessmentWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                            deps.Conn,
		carePlanClient:                  careplansv1.NewCarePlanServiceClient(deps.Conn),
		clients:                         deps.Clients,
		couponQueryPort:                 newCouponQueryPort(deps),
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		featureFlagClient:               featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		promotionValidationClient:       promotionsv1.NewPromotionValidationServiceClient(deps.Conn),
		pointsLedgerWorkflowClient:      loyaltyv1.NewPointsLedgerWorkflowServiceClient(deps.Conn),
	}
}

// SyncCampaigns syncs a campaign.
func (s *CampaignSyncServer) SyncCampaigns(ctx context.Context, req *promotionsv1.SyncCampaignsRequest) (*promotionsv1.SyncCampaignsResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "sync campaigns", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	signalProductReviewWorkflowRequest := buildSignalProductReviewWorkflowRequestForSyncCampaigns(req)
	signalProductReviewWorkflowReply, err := s.productReviewWorkflowClient.SignalProductReviewWorkflow(ctx, signalProductReviewWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewWorkflowService/SignalProductReviewWorkflow")
	}
	encoded, err := protojson.Marshal(req.GetCampaign())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.Campaign
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	watchAccessPoliciesStream, err := s.accessPolicyClient.WatchAccessPolicies(ctx, &authorizationv1.WatchAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.WatchAccessPoliciesRequest_Filter{
			Region:       (&decoded).GetLocation().GetRegion(),
			CreatedAfter: signalProductReviewWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		Subscriber: &commonv1.ContactInfo{
			Email: signalProductReviewWorkflowReply.GetProductReview().GetCounterparty().GetEmail(),
		},
		Since: signalProductReviewWorkflowReply.GetProductReview().GetIssuedAt(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
	}
	for {
		received, err := watchAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
		}
		_ = received
	}
	return &promotionsv1.SyncCampaignsResponse{
		Outcome: &promotionsv1.SyncCampaignsResponse_Outcome{
			Status: signalProductReviewWorkflowReply.GetOutcome().GetStatus(),
		},
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Location:   req.GetCampaign().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PushCampaign pushs a campaign.
func (s *CampaignSyncServer) PushCampaign(ctx context.Context, req *promotionsv1.PushCampaignRequest) (*promotionsv1.PushCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "push campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	normalizeCouponRequest := &promotionsv1.NormalizeCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetCampaign().GetAudit().GetEtag(),
				CreatedAt: req.GetCampaign().GetAudit().GetCreatedAt(),
			},
		},
	}
	normalizeCouponReply, err := promotionsv1.CouponValidationServiceClient.NormalizeCoupon(s.couponValidationClient, ctx, normalizeCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/NormalizeCoupon")
	}
	return &promotionsv1.PushCampaignResponse{
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizeCouponReply.GetCoupon().GetAudit().GetCreatedBy(),
			},
			CampaignId: req.GetCampaignId(),
			Organizer:  req.GetCampaign().GetOrganizer(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PullCampaigns pulls a campaign.
func (s *CampaignSyncServer) PullCampaigns(ctx context.Context, req *promotionsv1.PullCampaignsRequest) (*promotionsv1.PullCampaignsResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "pull campaigns", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	cancelRiskAssessmentWorkflowRequest := &fraudv1.CancelRiskAssessmentWorkflowRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin:   &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{Email: req.GetCampaign().GetOrganizer().GetEmail()}},
			Assignee: &identityv1.UserRef{UserId: req.GetCampaign().GetOrganizer().GetUserId()},
			Audit:    &commonv1.AuditInfo{UpdatedBy: req.GetCampaign().GetAudit().GetUpdatedBy()},
			Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{CreatedAt: req.GetCampaign().GetAudit().GetCreatedAt()}},
		},
	}
	cancelRiskAssessmentWorkflowReply, err := s.meteredRiskAssessmentWorkflow.CancelRiskAssessmentWorkflow(ctx, cancelRiskAssessmentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentWorkflowService/CancelRiskAssessmentWorkflow")
	}
	calculateCampaignRequest := &promotionsv1.CalculateCampaignRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Email: cancelRiskAssessmentWorkflowReply.GetRiskAssessment().GetAssignee().GetEmail(),
					Name:  cancelRiskAssessmentWorkflowReply.GetRiskAssessment().GetRequester().GetName(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cancelRiskAssessmentWorkflowReply.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
		},
	}
	calculateCampaignReply := new(promotionsv1.CalculateCampaignResponse)
	if err := grpcx.Invoke(ctx, s.conn, promotionsv1.CampaignCalculationService_CalculateCampaign_FullMethodName, calculateCampaignRequest, calculateCampaignReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignCalculationService/CalculateCampaign")
	}
	createCarePlanReply, err := s.carePlanClient.CreateCarePlan(ctx, &careplansv1.CreateCarePlanRequest{
		Context: req.GetContext(),
		CarePlan: &careplansv1.CarePlan{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cancelRiskAssessmentWorkflowReply.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &appointmentsv1.AppointmentRef{
				DisplayName: calculateCampaignReply.GetCampaign().GetOrganizer().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanService/CreateCarePlan")
	}
	return &promotionsv1.PullCampaignsResponse{
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{
				DisplayName: cancelRiskAssessmentWorkflowReply.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
			Metadata:   createCarePlanReply.GetCarePlan().GetMetadata(),
			CampaignId: req.GetCampaignId(),
			Title:      req.GetCampaign().GetTitle(),
		},
		Result: &commonv1.OperationResult{
			Details: calculateCampaignReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ReconcileCampaigns reconciles a campaign.
func (s *CampaignSyncServer) ReconcileCampaigns(ctx context.Context, req *promotionsv1.ReconcileCampaignsRequest) (*promotionsv1.ReconcileCampaignsResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "reconcile campaigns", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	fraudRuleLifecycle, ok := s.clients.Get("acme.fraud.v1.FraudRuleLifecycleService").(fraudv1.FraudRuleLifecycleServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.fraud.v1.FraudRuleLifecycleService")
	}
	scheduleFraudRuleExpiryRequest := &fraudv1.ScheduleFraudRuleExpiryRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{Etag: req.GetCampaign().GetAudit().GetEtag()},
			Owner: &identityv1.UserRef{UserId: req.GetCampaign().GetOrganizer().GetUserId()},
		},
	}
	scheduleFraudRuleExpiryReply, err := fraudRuleLifecycle.ScheduleFraudRuleExpiry(ctx, scheduleFraudRuleExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleLifecycleService/ScheduleFraudRuleExpiry")
	}
	resolveCouponsRequest := &promotionsv1.ResolveCouponsRequest{Context: req.GetContext()}
	resolveCouponsReply, err := s.couponQueryPort.ResolveCoupons(ctx, resolveCouponsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponQueryService/ResolveCoupons")
	}
	explainCampaignDecisionRequest := &promotionsv1.ExplainCampaignDecisionRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Recurrence: &promotionsv1.Campaign_Daily{Daily: &promotionsv1.CampaignDailyRule{Interval: req.GetCampaign().GetDaily().GetInterval()}},
		},
	}
	explainCampaignDecisionReply, err := grpcx.NewClient(s.conn, promotionsv1.NewCampaignValidationServiceClient).ExplainCampaignDecision(ctx, explainCampaignDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignValidationService/ExplainCampaignDecision")
	}
	return &promotionsv1.ReconcileCampaignsResponse{
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				CreatedAt: scheduleFraudRuleExpiryReply.GetFraudRule().GetAudit().GetCreatedAt(),
			},
			CampaignId: req.GetCampaignId(),
			Window:     req.GetCampaign().GetWindow(),
		},
		Result: &commonv1.OperationResult{
			Message:      scheduleFraudRuleExpiryReply.GetResult().GetMessage(),
			ReferenceIds: resolveCouponsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &promotionsv1.ReconcileCampaignsResponse_Outcome{
			Message: explainCampaignDecisionReply.GetResult().GetMessage(),
		},
	}, nil
}

// StreamCampaignChanges exchanges changes of campaigns with a peer.
func (s *CampaignSyncServer) StreamCampaignChanges(stream grpc.BidiStreamingServer[promotionsv1.StreamCampaignChangesRequest, promotionsv1.StreamCampaignChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *promotionsv1.StreamCampaignChangesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "stream campaign changes", "tenant", msg.GetContext().GetTenantId())
		explainCouponDecisionRequest := &promotionsv1.ExplainCouponDecisionRequest{}
		explainCouponDecisionRequest.Context = msg.GetContext()
		explainCouponDecisionRequest.Coupon = &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: msg.GetChange().GetOrganizer().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: msg.GetChange().GetAudit().GetUpdatedAt()},
		}
		explainCouponDecisionRequest.Details = &promotionsv1.ExplainCouponDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: msg.GetChange().GetOrganizer().GetEmail()},
		}
		if _, err := s.couponValidationClient.ExplainCouponDecision(ctx, explainCouponDecisionRequest); err != nil {
			return grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ExplainCouponDecision")
		}
		if err := stream.Send(&promotionsv1.StreamCampaignChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ResolveCampaignConflict resolves a campaign.
func (s *CampaignSyncServer) ResolveCampaignConflict(ctx context.Context, req *promotionsv1.ResolveCampaignConflictRequest) (*promotionsv1.ResolveCampaignConflictResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "resolve campaign conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	for _, participant := range req.GetCampaign().GetParticipants() {
		if _, err := s.apiCredentialNotificationClient.SendApiCredentialTestNotice(ctx, &identityv1.SendApiCredentialTestNoticeRequest{
			Context: req.GetContext(),
			Details: &identityv1.SendApiCredentialTestNoticeRequest_Details{
				Recipient: &commonv1.ContactInfo{
					Phone:            participant.GetContact().GetPhone(),
					Name:             participant.GetContact().GetName(),
					PreferredChannel: participant.GetContact().GetPreferredChannel(),
					Email:            participant.GetContact().GetEmail(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice")
		}
	}
	batchGetFeatureFlagsRequest := &featureflagsv1.BatchGetFeatureFlagsRequest{Context: req.GetContext()}
	batchGetFeatureFlagsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*featureflagsv1.BatchGetFeatureFlagsResponse, error) {
		return s.featureFlagClient.BatchGetFeatureFlags(ctx, batchGetFeatureFlagsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags")
	}
	packed, err := anypb.New(req.GetCampaign())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateRedemptionRequest := &promotionsv1.ValidateRedemptionRequest{
		Context:    req.GetContext(),
		Redemption: &promotionsv1.Redemption{Detail: packed},
	}
	validateRedemptionReply := new(promotionsv1.ValidateRedemptionResponse)
	if err := s.conn.Invoke(ctx, promotionsRedemptionValidationServiceValidateRedemptionMethod, validateRedemptionRequest, validateRedemptionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionValidationService/ValidateRedemption")
	}
	for _, participant2 := range req.GetCampaign().GetParticipants() {
		if _, err := s.promotionValidationClient.PreviewPromotion(ctx, &promotionsv1.PreviewPromotionRequest{
			Context: req.GetContext(),
			Details: &promotionsv1.PreviewPromotionRequest_Details{
				Contact: &commonv1.ContactInfo{Email: participant2.GetContact().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/PreviewPromotion")
		}
	}
	return &promotionsv1.ResolveCampaignConflictResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetFeatureFlagsReply.GetResult().GetCorrelationId(),
			Details:       batchGetFeatureFlagsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Outcome: &promotionsv1.ResolveCampaignConflictResponse_Outcome{Message: validateRedemptionReply.GetResult().GetMessage()},
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Location:   req.GetCampaign().GetLocation(),
		},
	}, nil
}

// GetCampaignSyncState returns a campaign.
func (s *CampaignSyncServer) GetCampaignSyncState(ctx context.Context, req *promotionsv1.GetCampaignSyncStateRequest) (*promotionsv1.GetCampaignSyncStateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "get_campaign_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get campaign sync state", "tenant", req.GetContext().GetTenantId())
	retryPointsLedgerWorkflowReply, err := s.pointsLedgerWorkflowClient.RetryPointsLedgerWorkflow(ctx, &loyaltyv1.RetryPointsLedgerWorkflowRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Funding: &loyaltyv1.PointsLedger_ExternalAccountId{ExternalAccountId: req.GetCampaignId()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerWorkflowService/RetryPointsLedgerWorkflow")
	}
	return &promotionsv1.GetCampaignSyncStateResponse{
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: retryPointsLedgerWorkflowReply.GetPointsLedger().GetContact().GetPreferredChannel(),
				},
			}},
			CampaignId: req.GetCampaignId(),
		},
		Result: &commonv1.OperationResult{
			Message: retryPointsLedgerWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}
