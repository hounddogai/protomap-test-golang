package promotions

import (
	"context"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	expensesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/expenses/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	routingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/routing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PromotionCalculationServer implements `acme.promotions.v1.PromotionCalculationService`.
// Computes the figures of a promotion, such as totals, taxes, and estimates.
type PromotionCalculationServer struct {
	promotionsv1.UnimplementedPromotionCalculationServiceServer
	store                                *Store
	log                                  *slog.Logger
	searchIndexExportPort                searchIndexExportPort
	userValidationClient                 identityv1.UserValidationServiceClient
	tenantNotificationClient             identityv1.TenantNotificationServiceClient
	giftCardClient                       giftcardsv1.GiftCardServiceClient
	sellerPayoutScheduleValidationClient sellersv1.SellerPayoutScheduleValidationServiceClient
	parcelQueryClient                    shippingv1.ParcelQueryServiceClient
	conn                                 *grpc.ClientConn
	synonymNotificationClient            searchv1.SynonymNotificationServiceClient
	routeQueryClient                     routingv1.RouteQueryServiceClient
	featureFlagClient                    featureflagsv1.FeatureFlagServiceClient
	metricSeriesQueryClient              metricsv1.MetricSeriesQueryServiceClient
	meteredCartQuery                     meteredCartQuery
}

// NewPromotionCalculationServer wires a PromotionCalculationServer.
func NewPromotionCalculationServer(deps platform.Deps) *PromotionCalculationServer {
	return &PromotionCalculationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.promotions.v1.PromotionCalculationService"),
		searchIndexExportPort:                newSearchIndexExportPort(deps),
		userValidationClient:                 identityv1.NewUserValidationServiceClient(deps.Conn),
		tenantNotificationClient:             identityv1.NewTenantNotificationServiceClient(deps.Conn),
		giftCardClient:                       giftcardsv1.NewGiftCardServiceClient(deps.Conn),
		sellerPayoutScheduleValidationClient: sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn),
		parcelQueryClient:                    shippingv1.NewParcelQueryServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		synonymNotificationClient:            searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		routeQueryClient:                     routingv1.NewRouteQueryServiceClient(deps.Conn),
		featureFlagClient:                    featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		metricSeriesQueryClient:              metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		meteredCartQuery:                     meteredCartQuery{CartQueryServiceClient: cartv1.NewCartQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// CalculatePromotion calculates a promotion.
func (s *PromotionCalculationServer) CalculatePromotion(ctx context.Context, req *promotionsv1.CalculatePromotionRequest) (*promotionsv1.CalculatePromotionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "calculate promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	scheduleSearchIndexExportRequest := &searchv1.ScheduleSearchIndexExportRequest{
		Context: req.GetContext(),
		Reason:  req.GetPromotion().GetBlock().GetReason(),
	}
	scheduleSearchIndexExportReply, err := s.searchIndexExportPort.ScheduleSearchIndexExport(ctx, scheduleSearchIndexExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ScheduleSearchIndexExport")
	}
	validateUserRequest := buildValidateUserRequestForCalculatePromotion(req)
	validateUserReply, err := s.userValidationClient.ValidateUser(ctx, validateUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ValidateUser")
	}
	_ = validateUserReply
	return &promotionsv1.CalculatePromotionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scheduleSearchIndexExportReply.GetResult().GetCorrelationId(),
			Details:       scheduleSearchIndexExportReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Promotion: &promotionsv1.Promotion{
			PromotionId: req.GetPromotionId(),
			Version:     req.GetPromotion().GetVersion(),
		},
	}, nil
}

// EstimatePromotion estimates a promotion.
func (s *PromotionCalculationServer) EstimatePromotion(ctx context.Context, req *promotionsv1.EstimatePromotionRequest) (*promotionsv1.EstimatePromotionResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "estimate promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	unsubscribeTenantUpdatesRequest := buildUnsubscribeTenantUpdatesRequestForEstimatePromotion(req)
	unsubscribeTenantUpdatesReply, err := s.tenantNotificationClient.UnsubscribeTenantUpdates(ctx, unsubscribeTenantUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/UnsubscribeTenantUpdates")
	}
	ctx = tenancy.With(ctx, "created_by", req.GetPromotion().GetAudit().GetCreatedBy())
	createGiftCardRequest := &giftcardsv1.CreateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{DisplayName: req.GetPromotion().GetOwner().GetDisplayName()},
			Audit:  &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
		},
	}
	createGiftCardReply, err := grpcx.Call(ctx, s.giftCardClient.CreateGiftCard, createGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/CreateGiftCard")
	}
	return &promotionsv1.EstimatePromotionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: unsubscribeTenantUpdatesReply.GetResult().GetCorrelationId(),
			Details:       createGiftCardReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedAt: createGiftCardReply.GetGiftCard().GetAudit().GetCreatedAt(),
			},
			PromotionId: req.GetPromotionId(),
			Priority:    req.GetPromotion().GetPriority(),
		},
	}, nil
}

// QuotePromotion quotes a promotion.
func (s *PromotionCalculationServer) QuotePromotion(ctx context.Context, req *promotionsv1.QuotePromotionRequest) (*promotionsv1.QuotePromotionResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "quote promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	validateSellerPayoutScheduleRequest := &sellersv1.ValidateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetPromotion().GetOwner().GetEmail()},
			}},
			Audit: &commonv1.AuditInfo{Etag: req.GetPromotion().GetAudit().GetEtag()},
		},
	}
	type validateSellerPayoutScheduleResult struct {
		reply *sellersv1.ValidateSellerPayoutScheduleResponse
		err   error
	}
	validateSellerPayoutScheduleResults := make(chan validateSellerPayoutScheduleResult, 1)
	go func() {
		reply, err := s.sellerPayoutScheduleValidationClient.ValidateSellerPayoutSchedule(ctx, validateSellerPayoutScheduleRequest)
		validateSellerPayoutScheduleResults <- validateSellerPayoutScheduleResult{reply, err}
	}()
	var validateSellerPayoutScheduleReply *sellersv1.ValidateSellerPayoutScheduleResponse
	select {
	case result := <-validateSellerPayoutScheduleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerPayoutScheduleValidationService/ValidateSellerPayoutSchedule")
		}
		validateSellerPayoutScheduleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	browseParcelsRequest := &shippingv1.BrowseParcelsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.BrowseParcelsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: validateSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
		},
	}
	browseParcelsReply, err := shippingv1.ParcelQueryServiceClient.BrowseParcels(s.parcelQueryClient, ctx, browseParcelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/BrowseParcels")
	}
	return &promotionsv1.QuotePromotionResponse{
		Result: &commonv1.OperationResult{
			Status:        validateSellerPayoutScheduleReply.GetResult().GetStatus(),
			Message:       validateSellerPayoutScheduleReply.GetResult().GetMessage(),
			CorrelationId: browseParcelsReply.GetResult().GetCorrelationId(),
		},
		Promotion: &promotionsv1.Promotion{
			PromotionId: req.GetPromotionId(),
			Name:        req.GetPromotion().GetName(),
		},
	}, nil
}

// RecalculatePromotions recalculates a promotion.
func (s *PromotionCalculationServer) RecalculatePromotions(ctx context.Context, req *promotionsv1.RecalculatePromotionsRequest) (*promotionsv1.RecalculatePromotionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "recalculate promotions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	createExpenseReportRequest := &expensesv1.CreateExpenseReportRequest{
		Context: req.GetContext(),
		ExpenseReport: &expensesv1.ExpenseReport{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetPromotion().GetAudit().GetCreatedBy()},
		},
	}
	createExpenseReportReply := new(expensesv1.CreateExpenseReportResponse)
	if err := grpcx.Invoke(ctx, s.conn, expensesv1.ExpenseReportService_CreateExpenseReport_FullMethodName, createExpenseReportRequest, createExpenseReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.expenses.v1.ExpenseReportService/CreateExpenseReport")
	}
	var reason string
	switch choice := req.GetPromotion().GetAction().(type) {
	case *promotionsv1.Promotion_Route:
		reason = choice.Route.GetTarget()
	case *promotionsv1.Promotion_Block:
		reason = choice.Block.GetReason()
	}
	unsubscribeSynonymUpdatesRequest := &searchv1.UnsubscribeSynonymUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: createExpenseReportReply.GetExpenseReport().GetCounterparty().GetEmail(),
			},
		},
		Reason:      reason,
		Annotations: req.GetAnnotations(),
	}
	unsubscribeSynonymUpdatesReply, err := searchv1.SynonymNotificationServiceClient.UnsubscribeSynonymUpdates(s.synonymNotificationClient, ctx, unsubscribeSynonymUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/UnsubscribeSynonymUpdates")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := promotionCalculationServerCountRoutesSteps[mode]
	ctx = tenancy.With(ctx, "user_id", req.GetPromotion().GetOwner().GetUserId())
	countRoutesRequest := &routingv1.CountRoutesRequest{
		Context: req.GetContext(),
		Filter: &routingv1.CountRoutesRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: unsubscribeSynonymUpdatesReply.GetSynonym().GetAudit().GetCreatedAt(),
			OwnerId:      tenancy.Value(ctx, "user_id"),
			Labels:       createExpenseReportReply.GetExpenseReport().GetMetadata(),
		},
	}
	countRoutesReply, err := run(s, ctx, countRoutesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.routing.v1.RouteQueryService/CountRoutes")
	}
	summarizeSearchIndexesRequest := &searchv1.SummarizeSearchIndexesRequest{
		Context: req.GetContext(),
		Reason:  req.GetPromotion().GetBlock().GetReason(),
	}
	summarizeSearchIndexesReply := new(searchv1.SummarizeSearchIndexesResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SearchIndexQueryService_SummarizeSearchIndexes_FullMethodName, summarizeSearchIndexesRequest, summarizeSearchIndexesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/SummarizeSearchIndexes")
	}
	return &promotionsv1.RecalculatePromotionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  createExpenseReportReply.GetResult().GetReferenceIds(),
			Message:       countRoutesReply.GetResult().GetMessage(),
			CorrelationId: summarizeSearchIndexesReply.GetResult().GetCorrelationId(),
			Details:       summarizeSearchIndexesReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				Email: unsubscribeSynonymUpdatesReply.GetSynonym().GetOwner().GetEmail(),
			},
			PromotionId: req.GetPromotionId(),
			Priority:    req.GetPromotion().GetPriority(),
		},
	}, nil
}

// SimulatePromotion simulates a promotion.
func (s *PromotionCalculationServer) SimulatePromotion(ctx context.Context, req *promotionsv1.SimulatePromotionRequest) (*promotionsv1.SimulatePromotionResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "simulate promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	createFeatureFlagRequest := &featureflagsv1.CreateFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Action: &featureflagsv1.FeatureFlag_Route{Route: &featureflagsv1.FeatureFlagRouteAction{Weight: req.GetPromotion().GetRoute().GetWeight()}},
			Owner:  &identityv1.UserRef{DisplayName: req.GetPromotion().GetOwner().GetDisplayName()},
		},
	}
	createFeatureFlagReply, err := featureflagsv1.FeatureFlagServiceClient.CreateFeatureFlag(s.featureFlagClient, ctx, createFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/CreateFeatureFlag")
	}
	return &promotionsv1.SimulatePromotionResponse{
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: createFeatureFlagReply.GetFeatureFlag().GetAudit().GetUpdatedAt(),
			},
			PromotionId: req.GetPromotionId(),
			Priority:    req.GetPromotion().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			Message: createFeatureFlagReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetPromotionRates returns a promotion.
func (s *PromotionCalculationServer) GetPromotionRates(ctx context.Context, req *promotionsv1.GetPromotionRatesRequest) (*promotionsv1.GetPromotionRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "get_promotion_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get promotion rates", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	browseMetricSeriesesRequest := &metricsv1.BrowseMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.BrowseMetricSeriesesRequest_Filter{
			OwnerId:      req.GetPromotionId(),
			CreatedAfter: cloned.GetDeadline(),
			Labels:       baggage,
		},
	}
	browseMetricSeriesesReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*metricsv1.BrowseMetricSeriesesResponse, error) {
		return s.metricSeriesQueryClient.BrowseMetricSerieses(ctx, browseMetricSeriesesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses")
	}
	suggestCartsRequest := &cartv1.SuggestCartsRequest{
		Context: req.GetContext(),
		Filter:  &cartv1.SuggestCartsRequest_Filter{OwnerId: req.GetPromotionId()},
	}
	suggestCartsReply, err := s.meteredCartQuery.SuggestCarts(ctx, suggestCartsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartQueryService/SuggestCarts")
	}
	_ = suggestCartsReply
	return &promotionsv1.GetPromotionRatesResponse{
		Result: &commonv1.OperationResult{
			Status:        browseMetricSeriesesReply.GetResult().GetStatus(),
			CorrelationId: browseMetricSeriesesReply.GetResult().GetCorrelationId(),
		},
		Promotion: &promotionsv1.Promotion{PromotionId: req.GetPromotionId()},
	}, nil
}

// promotionCalculationServerCountRoutesSteps are the ways to run acme.routing.v1.RouteQueryService/CountRoutes, by
// name.
var promotionCalculationServerCountRoutesSteps = map[string]func(*PromotionCalculationServer, context.Context, *routingv1.CountRoutesRequest) (*routingv1.CountRoutesResponse, error){
	"default": (*PromotionCalculationServer).countRoutesStep,
	"audited": (*PromotionCalculationServer).countRoutesAuditedStep,
}

func (s *PromotionCalculationServer) countRoutesStep(ctx context.Context, request *routingv1.CountRoutesRequest) (*routingv1.CountRoutesResponse, error) {
	return s.routeQueryClient.CountRoutes(ctx, request)
}

func (s *PromotionCalculationServer) countRoutesAuditedStep(ctx context.Context, request *routingv1.CountRoutesRequest) (*routingv1.CountRoutesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.routing.v1.RouteQueryService/CountRoutes")
	return s.routeQueryClient.CountRoutes(ctx, request)
}
