package promotions

import (
	"context"
	"log/slog"
	"sync"

	certificatesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/certificates/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// RedemptionSyncServer implements `acme.promotions.v1.RedemptionSyncService`.
// Keeps redemptions in step with the systems of record in other regions.
type RedemptionSyncServer struct {
	promotionsv1.UnimplementedRedemptionSyncServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	couponQueryOnce                sync.Once
	couponQueryCached              promotionsv1.CouponQueryServiceClient
	telemetryReadingClient         telematicsv1.TelemetryReadingServiceClient
	couponCalculationOnce          sync.Once
	couponCalculationCached        promotionsv1.CouponCalculationServiceClient
	certificateQueryClient         certificatesv1.CertificateQueryServiceClient
	promotionReportingClient       promotionsv1.PromotionReportingServiceClient
	campaignValidationClient       promotionsv1.CampaignValidationServiceClient
	redemptionCalculationClient    promotionsv1.RedemptionCalculationServiceClient
	clients                        *registry.Registry
	putawayTaskApprovalClient      warehousingv1.PutawayTaskApprovalServiceClient
	notificationValidationClient   notificationsv1.NotificationValidationServiceClient
	promotionValidationOnce        sync.Once
	promotionValidationCached      promotionsv1.PromotionValidationServiceClient
	listSearchIndexSubscriptionsFn func(context.Context, *searchv1.ListSearchIndexSubscriptionsRequest, ...grpc.CallOption) (*searchv1.ListSearchIndexSubscriptionsResponse, error)
}

// NewRedemptionSyncServer wires a RedemptionSyncServer.
func NewRedemptionSyncServer(deps platform.Deps) *RedemptionSyncServer {
	searchIndexNotification := searchv1.NewSearchIndexNotificationServiceClient(deps.Conn)
	return &RedemptionSyncServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.promotions.v1.RedemptionSyncService"),
		conn:                           deps.Conn,
		telemetryReadingClient:         telematicsv1.NewTelemetryReadingServiceClient(deps.Conn),
		certificateQueryClient:         certificatesv1.NewCertificateQueryServiceClient(deps.Conn),
		promotionReportingClient:       promotionsv1.NewPromotionReportingServiceClient(deps.Conn),
		campaignValidationClient:       promotionsv1.NewCampaignValidationServiceClient(deps.Conn),
		redemptionCalculationClient:    promotionsv1.NewRedemptionCalculationServiceClient(deps.Conn),
		clients:                        deps.Clients,
		putawayTaskApprovalClient:      warehousingv1.NewPutawayTaskApprovalServiceClient(deps.Conn),
		notificationValidationClient:   notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		listSearchIndexSubscriptionsFn: searchIndexNotification.ListSearchIndexSubscriptions,
	}
}

// SyncRedemptions syncs a redemption.
func (s *RedemptionSyncServer) SyncRedemptions(ctx context.Context, req *promotionsv1.SyncRedemptionsRequest) (*promotionsv1.SyncRedemptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "sync redemptions", "tenant", req.GetContext().GetTenantId())
	lookupCouponRequest := &promotionsv1.LookupCouponRequest{
		Context:  req.GetContext(),
		CouponId: req.GetRedemption().GetSubject().GetCouponId(),
	}
	lookupCouponReply, err := s.couponQueryLazy().LookupCoupon(ctx, lookupCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponQueryService/LookupCoupon")
	}
	redemptionCache := cache.New[string, *promotionsv1.Redemption](1)
	redemptionCache.Put(req.GetContext().GetTraceId(), req.GetRedemption())
	cachedRedemption, _ := redemptionCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetRedemption())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	updateTelemetryReadingReply, err := s.telemetryReadingClient.UpdateTelemetryReading(ctx, &telematicsv1.UpdateTelemetryReadingRequest{
		Context: req.GetContext(),
		TelemetryReading: &telematicsv1.TelemetryReading{
			Subject: &telematicsv1.GeoFenceRef{
				DisplayName: lookupCouponReply.GetCoupon().GetHolder().GetDisplayName(),
			},
			Source:             cachedRedemption.GetSource(),
			Attributes:         req.GetRedemption().GetAttributes(),
			CorrelationId:      req.GetRedemption().GetCorrelationId(),
			Detail:             packed,
			TelemetryReadingId: envelope.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telematics.v1.TelemetryReadingService/UpdateTelemetryReading")
	}
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainCouponCalculationRequest := &promotionsv1.ExplainCouponCalculationRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Funding: &promotionsv1.Coupon_Card{Card: &promotionsv1.CouponCardFunding{
				HolderName: lookupCouponReply.GetCoupon().GetCard().GetHolderName(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lookupCouponReply.GetCoupon().GetAudit().GetCreatedAt(),
				UpdatedBy: lookupCouponReply.GetCoupon().GetAudit().GetUpdatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Phone: lookupCouponReply.GetCoupon().GetContact().GetPhone(),
			},
		},
		CouponId: envelope2.GetDisplayName(),
	}
	explainCouponCalculationReply, err := s.couponCalculationLazy().ExplainCouponCalculation(ctx, explainCouponCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/ExplainCouponCalculation")
	}
	return &promotionsv1.SyncRedemptionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: lookupCouponReply.GetResult().GetCorrelationId(),
			Message:       explainCouponCalculationReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Redemption: &promotionsv1.Redemption{
			CorrelationId: updateTelemetryReadingReply.GetTelemetryReading().GetCorrelationId(),
			RedemptionId:  req.GetRedemptionId(),
			Attributes:    req.GetRedemption().GetAttributes(),
		},
	}, nil
}

// PushRedemption pushs a redemption.
func (s *RedemptionSyncServer) PushRedemption(ctx context.Context, req *promotionsv1.PushRedemptionRequest) (*promotionsv1.PushRedemptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "push redemption", "tenant", req.GetContext().GetTenantId())
	step := &redemptionSyncServerPushRedemptionCountCertificatesStep{request: &certificatesv1.CountCertificatesRequest{
		Context: req.GetContext(),
		Filter: &certificatesv1.CountCertificatesRequest_Filter{
			CreatedAfter: req.GetRedemption().GetOccurredAt(),
			OwnerId:      req.GetRedemption().GetSubject().GetCouponId(),
		},
	}}
	if err := s.runPushRedemptionCountCertificates(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.certificates.v1.CertificateQueryService/CountCertificates")
	}
	countCertificatesReply := step.reply
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countPromotionTotalsRequest := &promotionsv1.CountPromotionTotalsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.CountPromotionTotalsRequest_Filter{
			CreatedAfter: req.GetRedemption().GetOccurredAt(),
			Labels:       countCertificatesReply.GetResult().GetDetails(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	var countPromotionTotalsReply *promotionsv1.CountPromotionTotalsResponse
	if err := pipeline.Run(ctx, "acme.promotions.v1.PromotionReportingService/CountPromotionTotals", func(ctx context.Context) error {
		answer, err := s.promotionReportingClient.CountPromotionTotals(ctx, countPromotionTotalsRequest)
		countPromotionTotalsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/CountPromotionTotals")
	}
	return &promotionsv1.PushRedemptionResponse{
		Result: &commonv1.OperationResult{
			Details: countCertificatesReply.GetResult().GetDetails(),
			Status:  countPromotionTotalsReply.GetResult().GetStatus(),
		},
		Outcome: &promotionsv1.PushRedemptionResponse_Outcome{Status: countPromotionTotalsReply.GetResult().GetStatus()},
		Redemption: &promotionsv1.Redemption{
			RedemptionId:  req.GetRedemptionId(),
			CorrelationId: req.GetRedemption().GetCorrelationId(),
		},
	}, nil
}

// PullRedemptions pulls a redemption.
func (s *RedemptionSyncServer) PullRedemptions(ctx context.Context, req *promotionsv1.PullRedemptionsRequest) (*promotionsv1.PullRedemptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "pull redemptions", "tenant", req.GetContext().GetTenantId())
	for measurement := range iterx.Each(req.GetRedemption().GetMeasurements(), func(candidate *promotionsv1.RedemptionMeasurement) bool { return candidate != nil }) {
		if _, err := s.campaignValidationClient.CheckCampaignEligibility(ctx, &promotionsv1.CheckCampaignEligibilityRequest{
			Context: req.GetContext(),
			Campaign: &promotionsv1.Campaign{
				Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignValidationService/CheckCampaignEligibility")
		}
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*promotionsv1.SimulateRedemptionResponse, len(req.GetRedemption().GetMeasurements()))
	for i, measurement2 := range req.GetRedemption().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.redemptionCalculationClient.SimulateRedemption(groupCtx, &promotionsv1.SimulateRedemptionRequest{
				Context: req.GetContext(),
				Redemption: &promotionsv1.Redemption{
					Measurements: []*promotionsv1.RedemptionMeasurement{&promotionsv1.RedemptionMeasurement{Unit: measurement2.GetUnit()}},
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
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionCalculationService/SimulateRedemption")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	softwareLicenseQuery, ok := s.clients.Get("acme.licensing.v1.SoftwareLicenseQueryService").(licensingv1.SoftwareLicenseQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.licensing.v1.SoftwareLicenseQueryService")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	browseSoftwareLicensesRequest := &licensingv1.BrowseSoftwareLicensesRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	browseSoftwareLicensesReply, err := softwareLicenseQuery.BrowseSoftwareLicenses(ctx, browseSoftwareLicensesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.licensing.v1.SoftwareLicenseQueryService/BrowseSoftwareLicenses")
	}
	for _, measurement3 := range req.GetRedemption().GetMeasurements() {
		if _, err := s.putawayTaskApprovalClient.ListPendingPutawayTasks(ctx, &warehousingv1.ListPendingPutawayTasksRequest{
			Context: req.GetContext(),
			Filter:  &warehousingv1.ListPendingPutawayTasksRequest_Filter{CreatedAfter: measurement3.GetObservedAt()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskApprovalService/ListPendingPutawayTasks")
		}
	}
	return &promotionsv1.PullRedemptionsResponse{
		Outcome: &promotionsv1.PullRedemptionsResponse_Outcome{Status: browseSoftwareLicensesReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Details: browseSoftwareLicensesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Sequence:     req.GetRedemption().GetSequence(),
		},
	}, nil
}

// ReconcileRedemptions reconciles a redemption.
func (s *RedemptionSyncServer) ReconcileRedemptions(ctx context.Context, req *promotionsv1.ReconcileRedemptionsRequest) (*promotionsv1.ReconcileRedemptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReconcileRedemptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "reconcile redemptions", "tenant", req.GetContext().GetTenantId())
	scoreNotificationRequest := buildScoreNotificationRequestForReconcileRedemptions(req)
	scoreNotificationReply, err := s.notificationValidationClient.ScoreNotification(ctx, scoreNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ScoreNotification")
	}
	return &promotionsv1.ReconcileRedemptionsResponse{
		Result: &commonv1.OperationResult{Status: scoreNotificationReply.GetResult().GetStatus()},
		Redemption: &promotionsv1.Redemption{
			RedemptionId: req.GetRedemptionId(),
			Sequence:     req.GetRedemption().GetSequence(),
		},
	}, nil
}

// ResolveRedemptionConflict resolves a redemption.
func (s *RedemptionSyncServer) ResolveRedemptionConflict(ctx context.Context, req *promotionsv1.ResolveRedemptionConflictRequest) (*promotionsv1.ResolveRedemptionConflictResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveRedemptionConflict", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "resolve redemption conflict", "tenant", req.GetContext().GetTenantId())
	redemptionCalculation, err := registry.Lookup[promotionsv1.RedemptionCalculationServiceClient](s.clients, "acme.promotions.v1.RedemptionCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetRedemption())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	simulateRedemptionRequest := &promotionsv1.SimulateRedemptionRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject:      req.GetRedemption().GetSubject(),
			RedemptionId: req.GetRedemption().GetRedemptionId(),
			Detail:       packed,
		},
		Details: &promotionsv1.SimulateRedemptionRequest_Details{
			CurrencyCode: req.GetRedemption().GetSubject().GetCurrencyCode(),
		},
		RedemptionId: envelope.GetDisplayName(),
	}
	simulateRedemptionReply, err := redemptionCalculation.SimulateRedemption(ctx, simulateRedemptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionCalculationService/SimulateRedemption")
	}
	return &promotionsv1.ResolveRedemptionConflictResponse{
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				CurrencyCode: simulateRedemptionReply.GetRedemption().GetSubject().GetCurrencyCode(),
			},
			RedemptionId: simulateRedemptionReply.GetRedemption().GetRedemptionId(),
			Sequence:     req.GetRedemption().GetSequence(),
			OccurredAt:   req.GetRedemption().GetOccurredAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetRedemptionSyncState returns a redemption.
func (s *RedemptionSyncServer) GetRedemptionSyncState(ctx context.Context, req *promotionsv1.GetRedemptionSyncStateRequest) (*promotionsv1.GetRedemptionSyncStateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "get_redemption_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get redemption sync state", "tenant", req.GetContext().GetTenantId())
	scorePromotionRequest := &promotionsv1.ScorePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	scorePromotionReply, err := s.promotionValidationLazy().ScorePromotion(ctx, scorePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/ScorePromotion")
	}
	return &promotionsv1.GetRedemptionSyncStateResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scorePromotionReply.GetResult().GetReferenceIds(),
			Details:      scorePromotionReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Redemption: &promotionsv1.Redemption{RedemptionId: req.GetRedemptionId()},
	}, nil
}

// ListRedemptionConflicts lists redemptions a page at a time.
func (s *RedemptionSyncServer) ListRedemptionConflicts(ctx context.Context, req *promotionsv1.ListRedemptionConflictsRequest) (*promotionsv1.ListRedemptionConflictsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list redemption conflicts", "tenant", req.GetContext().GetTenantId())
	listSearchIndexSubscriptionsRequest := &searchv1.ListSearchIndexSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexSubscriptionsRequest_Filter{
			Query:   req.GetFilter().GetQuery(),
			Region:  req.GetFilter().GetRegion(),
			OwnerId: req.GetFilter().GetOwnerId(),
		},
		Page: &commonv1.PageRequest{PageSize: req.GetPage().GetPageSize()},
	}
	listSearchIndexSubscriptionsReply, err := s.listSearchIndexSubscriptionsFn(ctx, listSearchIndexSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions")
	}
	return &promotionsv1.ListRedemptionConflictsResponse{
		Redemptions: []*promotionsv1.Redemption{&promotionsv1.Redemption{
			CorrelationId: listSearchIndexSubscriptionsReply.GetResult().GetCorrelationId(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// couponQueryLazy dials acme.promotions.v1.CouponQueryService the first time a handler needs it.
func (s *RedemptionSyncServer) couponQueryLazy() promotionsv1.CouponQueryServiceClient {
	s.couponQueryOnce.Do(func() { s.couponQueryCached = promotionsv1.NewCouponQueryServiceClient(s.conn) })
	return s.couponQueryCached
}

// couponCalculationLazy dials acme.promotions.v1.CouponCalculationService the first time a handler needs it.
func (s *RedemptionSyncServer) couponCalculationLazy() promotionsv1.CouponCalculationServiceClient {
	s.couponCalculationOnce.Do(func() { s.couponCalculationCached = promotionsv1.NewCouponCalculationServiceClient(s.conn) })
	return s.couponCalculationCached
}

// redemptionSyncServerPushRedemptionCountCertificatesStep carries the request of one step of PushRedemption and, once
// it ran, its reply.
type redemptionSyncServerPushRedemptionCountCertificatesStep struct {
	request *certificatesv1.CountCertificatesRequest
	reply   *certificatesv1.CountCertificatesResponse
}

func (s *RedemptionSyncServer) runPushRedemptionCountCertificates(ctx context.Context, step *redemptionSyncServerPushRedemptionCountCertificatesStep) error {
	reply, err := s.certificateQueryClient.CountCertificates(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// promotionValidationLazy dials acme.promotions.v1.PromotionValidationService the first time a handler needs it.
func (s *RedemptionSyncServer) promotionValidationLazy() promotionsv1.PromotionValidationServiceClient {
	s.promotionValidationOnce.Do(func() { s.promotionValidationCached = promotionsv1.NewPromotionValidationServiceClient(s.conn) })
	return s.promotionValidationCached
}
