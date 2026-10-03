package promotions

import (
	"context"
	"log/slog"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the promotions domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.admissions.v1.AdmissionService", admissionsv1.NewAdmissionServiceClient(deps.Conn))
	deps.Clients.Put("acme.documents.v1.DocumentValidationService", documentsv1.NewDocumentValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.fraud.v1.FraudRuleLifecycleService", fraudv1.NewFraudRuleLifecycleServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.UserHistoryService", identityv1.NewUserHistoryServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.UserService", identityv1.NewUserServiceClient(deps.Conn))
	deps.Clients.Put("acme.licensing.v1.SoftwareLicenseQueryService", licensingv1.NewSoftwareLicenseQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.BackorderSyncService", ordersv1.NewBackorderSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.payments.v1.PaymentValidationService", paymentsv1.NewPaymentValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.promotions.v1.CouponLifecycleService", promotionsv1.NewCouponLifecycleServiceClient(deps.Conn))
	deps.Clients.Put("acme.promotions.v1.PromotionLifecycleService", promotionsv1.NewPromotionLifecycleServiceClient(deps.Conn))
	deps.Clients.Put("acme.promotions.v1.RedemptionCalculationService", promotionsv1.NewRedemptionCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.promotions.v1.RedemptionImportService", promotionsv1.NewRedemptionImportServiceClient(deps.Conn))
	deps.Clients.Put("acme.warehousing.v1.PickListValidationService", warehousingv1.NewPickListValidationServiceClient(deps.Conn))
	promotionsv1.RegisterPromotionServiceServer(server, NewPromotionServer(deps))
	promotionsv1.RegisterCouponQueryServiceServer(server, NewCouponQueryServer(deps))
	campaignWorkflowServer := NewCampaignWorkflowServer(deps)
	server.RegisterService(&promotionsv1.CampaignWorkflowService_ServiceDesc, campaignWorkflowServer)
	promotionsv1.RegisterRedemptionApprovalServiceServer(server, NewRedemptionApprovalServer(deps))
	promotionsv1.RegisterPromotionQueryServiceServer(server, NewPromotionQueryServer(deps))
	promotionsv1.RegisterCouponWorkflowServiceServer(server, NewCouponWorkflowServer(deps))
	promotionsv1.RegisterCampaignApprovalServiceServer(server, NewCampaignApprovalServer(deps))
	redemptionValidationServer := NewRedemptionValidationServer(deps)
	server.RegisterService(&promotionsv1.RedemptionValidationService_ServiceDesc, redemptionValidationServer)
	promotionsv1.RegisterPromotionWorkflowServiceServer(server, NewPromotionWorkflowServer(deps))
	promotionsv1.RegisterCouponApprovalServiceServer(server, NewCouponApprovalServer(deps))
	promotionsv1.RegisterCampaignValidationServiceServer(server, NewCampaignValidationServer(deps))
	redemptionCalculationServer := NewRedemptionCalculationServer(deps)
	server.RegisterService(&promotionsv1.RedemptionCalculationService_ServiceDesc, redemptionCalculationServer)
	promotionsv1.RegisterPromotionApprovalServiceServer(server, NewPromotionApprovalServer(deps))
	promotionsv1.RegisterCouponValidationServiceServer(server, NewCouponValidationServer(deps))
	promotionsv1.RegisterCampaignCalculationServiceServer(server, NewCampaignCalculationServer(deps))
	redemptionLifecycleServer := NewRedemptionLifecycleServer(deps)
	server.RegisterService(&promotionsv1.RedemptionLifecycleService_ServiceDesc, redemptionLifecycleServer)
	promotionsv1.RegisterPromotionValidationServiceServer(server, NewPromotionValidationServer(deps))
	couponCalculationServer := NewCouponCalculationServer(deps)
	server.RegisterService(&promotionsv1.CouponCalculationService_ServiceDesc, couponCalculationServer)
	promotionsv1.RegisterCampaignLifecycleServiceServer(server, NewCampaignLifecycleServer(deps))
	promotionsv1.RegisterRedemptionReportingServiceServer(server, NewRedemptionReportingServer(deps))
	promotionsv1.RegisterPromotionCalculationServiceServer(server, NewPromotionCalculationServer(deps))
	promotionsv1.RegisterCouponLifecycleServiceServer(server, NewCouponLifecycleServer(deps))
	promotionsv1.RegisterCampaignReportingServiceServer(server, NewCampaignReportingServer(deps))
	promotionsv1.RegisterRedemptionSyncServiceServer(server, NewRedemptionSyncServer(deps))
	promotionsv1.RegisterPromotionLifecycleServiceServer(server, NewPromotionLifecycleServer(deps))
	promotionsv1.RegisterCouponReportingServiceServer(server, NewCouponReportingServer(deps))
	promotionsv1.RegisterCampaignSyncServiceServer(server, NewCampaignSyncServer(deps))
	promotionsv1.RegisterRedemptionImportServiceServer(server, NewRedemptionImportServer(deps))
	promotionsv1.RegisterPromotionReportingServiceServer(server, NewPromotionReportingServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	compareCouponOptionsCall             func(context.Context, *promotionsv1.CompareCouponOptionsRequest) (*promotionsv1.CompareCouponOptionsResponse, error)
	pauseReturnLabelWorkflowCall         func(context.Context, *returnsv1.PauseReturnLabelWorkflowRequest) (*returnsv1.PauseReturnLabelWorkflowResponse, error)
	submitGiftCardTransactionCall        func(context.Context, *giftcardsv1.SubmitGiftCardTransactionRequest, ...grpc.CallOption) (*giftcardsv1.SubmitGiftCardTransactionResponse, error)
	explainReplenishmentPlanDecisionCall func(context.Context, *inventoryv1.ExplainReplenishmentPlanDecisionRequest, ...grpc.CallOption) (*inventoryv1.ExplainReplenishmentPlanDecisionResponse, error)
	browseAccessPoliciesCall             func(context.Context, *authorizationv1.BrowseAccessPoliciesRequest) (*authorizationv1.BrowseAccessPoliciesResponse, error)
	listRedemptionConflictsCall          func(context.Context, *promotionsv1.ListRedemptionConflictsRequest) (*promotionsv1.ListRedemptionConflictsResponse, error)
	scorePromotionCall                   func(context.Context, *promotionsv1.ScorePromotionRequest, ...grpc.CallOption) (*promotionsv1.ScorePromotionResponse, error)
	batchGetRiskAssessmentsCall          func(context.Context, *fraudv1.BatchGetRiskAssessmentsRequest, ...grpc.CallOption) (*fraudv1.BatchGetRiskAssessmentsResponse, error)
	generateRedemptionReportCall         func(context.Context, *promotionsv1.GenerateRedemptionReportRequest) (*promotionsv1.GenerateRedemptionReportResponse, error)
	countRedemptionTotalsCall            func(context.Context, *promotionsv1.CountRedemptionTotalsRequest, ...grpc.CallOption) (*promotionsv1.CountRedemptionTotalsResponse, error)
	calculateCouponCall                  func(context.Context, *promotionsv1.CalculateCouponRequest) (*promotionsv1.CalculateCouponResponse, error)
	quoteCouponCall                      func(context.Context, *promotionsv1.QuoteCouponRequest) (*promotionsv1.QuoteCouponResponse, error)
	explainCampaignCalculationCall       func(context.Context, *promotionsv1.ExplainCampaignCalculationRequest, ...grpc.CallOption) (*promotionsv1.ExplainCampaignCalculationResponse, error)
	searchMetricSeriesesCall             func(context.Context, *metricsv1.SearchMetricSeriesesRequest, ...grpc.CallOption) (*metricsv1.SearchMetricSeriesesResponse, error)
	approveRefundCall                    func(context.Context, *paymentsv1.ApproveRefundRequest, ...grpc.CallOption) (*paymentsv1.ApproveRefundResponse, error)
	countCouponsCall                     func(context.Context, *promotionsv1.CountCouponsRequest) (*promotionsv1.CountCouponsResponse, error)
	resolveCampaignConflictCall          func(context.Context, *promotionsv1.ResolveCampaignConflictRequest) (*promotionsv1.ResolveCampaignConflictResponse, error)
	previewCouponCall                    func(context.Context, *promotionsv1.PreviewCouponRequest) (*promotionsv1.PreviewCouponResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	couponCalculationForCompareCouponOptions := promotionsv1.NewCouponCalculationServiceClient(deps.Conn)
	compareCouponOptionsCall = func(ctx context.Context, request *promotionsv1.CompareCouponOptionsRequest) (*promotionsv1.CompareCouponOptionsResponse, error) {
		return couponCalculationForCompareCouponOptions.CompareCouponOptions(ctx, request)
	}
	returnLabelWorkflowForPauseReturnLabelWorkflow := returnsv1.NewReturnLabelWorkflowServiceClient(deps.Conn)
	pauseReturnLabelWorkflowCall = func(ctx context.Context, request *returnsv1.PauseReturnLabelWorkflowRequest) (*returnsv1.PauseReturnLabelWorkflowResponse, error) {
		return returnLabelWorkflowForPauseReturnLabelWorkflow.PauseReturnLabelWorkflow(ctx, request)
	}
	giftCardTransactionApprovalForSubmitGiftCardTransaction := giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn)
	submitGiftCardTransactionCall = giftCardTransactionApprovalForSubmitGiftCardTransaction.SubmitGiftCardTransaction
	replenishmentPlanValidationForExplainReplenishmentPlanDecision := inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn)
	explainReplenishmentPlanDecisionCall = replenishmentPlanValidationForExplainReplenishmentPlanDecision.ExplainReplenishmentPlanDecision
	accessPolicyQueryForBrowseAccessPolicies := authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn)
	browseAccessPoliciesCall = func(ctx context.Context, request *authorizationv1.BrowseAccessPoliciesRequest) (*authorizationv1.BrowseAccessPoliciesResponse, error) {
		return accessPolicyQueryForBrowseAccessPolicies.BrowseAccessPolicies(ctx, request)
	}
	redemptionSyncForListRedemptionConflicts := promotionsv1.NewRedemptionSyncServiceClient(deps.Conn)
	listRedemptionConflictsCall = func(ctx context.Context, request *promotionsv1.ListRedemptionConflictsRequest) (*promotionsv1.ListRedemptionConflictsResponse, error) {
		return redemptionSyncForListRedemptionConflicts.ListRedemptionConflicts(ctx, request)
	}
	promotionValidationForScorePromotion := promotionsv1.NewPromotionValidationServiceClient(deps.Conn)
	scorePromotionCall = promotionValidationForScorePromotion.ScorePromotion
	riskAssessmentForBatchGetRiskAssessments := fraudv1.NewRiskAssessmentServiceClient(deps.Conn)
	batchGetRiskAssessmentsCall = riskAssessmentForBatchGetRiskAssessments.BatchGetRiskAssessments
	redemptionReportingForGenerateRedemptionReport := promotionsv1.NewRedemptionReportingServiceClient(deps.Conn)
	generateRedemptionReportCall = func(ctx context.Context, request *promotionsv1.GenerateRedemptionReportRequest) (*promotionsv1.GenerateRedemptionReportResponse, error) {
		return redemptionReportingForGenerateRedemptionReport.GenerateRedemptionReport(ctx, request)
	}
	redemptionReportingForCountRedemptionTotals := promotionsv1.NewRedemptionReportingServiceClient(deps.Conn)
	countRedemptionTotalsCall = redemptionReportingForCountRedemptionTotals.CountRedemptionTotals
	couponCalculationForCalculateCoupon := promotionsv1.NewCouponCalculationServiceClient(deps.Conn)
	calculateCouponCall = func(ctx context.Context, request *promotionsv1.CalculateCouponRequest) (*promotionsv1.CalculateCouponResponse, error) {
		return couponCalculationForCalculateCoupon.CalculateCoupon(ctx, request)
	}
	couponCalculationForQuoteCoupon := promotionsv1.NewCouponCalculationServiceClient(deps.Conn)
	quoteCouponCall = func(ctx context.Context, request *promotionsv1.QuoteCouponRequest) (*promotionsv1.QuoteCouponResponse, error) {
		return couponCalculationForQuoteCoupon.QuoteCoupon(ctx, request)
	}
	campaignCalculationForExplainCampaignCalculation := promotionsv1.NewCampaignCalculationServiceClient(deps.Conn)
	explainCampaignCalculationCall = campaignCalculationForExplainCampaignCalculation.ExplainCampaignCalculation
	metricSeriesForSearchMetricSerieses := metricsv1.NewMetricSeriesServiceClient(deps.Conn)
	searchMetricSeriesesCall = metricSeriesForSearchMetricSerieses.SearchMetricSerieses
	refundApprovalForApproveRefund := paymentsv1.NewRefundApprovalServiceClient(deps.Conn)
	approveRefundCall = refundApprovalForApproveRefund.ApproveRefund
	couponQueryForCountCoupons := promotionsv1.NewCouponQueryServiceClient(deps.Conn)
	countCouponsCall = func(ctx context.Context, request *promotionsv1.CountCouponsRequest) (*promotionsv1.CountCouponsResponse, error) {
		return couponQueryForCountCoupons.CountCoupons(ctx, request)
	}
	campaignSyncForResolveCampaignConflict := promotionsv1.NewCampaignSyncServiceClient(deps.Conn)
	resolveCampaignConflictCall = func(ctx context.Context, request *promotionsv1.ResolveCampaignConflictRequest) (*promotionsv1.ResolveCampaignConflictResponse, error) {
		return campaignSyncForResolveCampaignConflict.ResolveCampaignConflict(ctx, request)
	}
	couponValidationForPreviewCoupon := promotionsv1.NewCouponValidationServiceClient(deps.Conn)
	previewCouponCall = func(ctx context.Context, request *promotionsv1.PreviewCouponRequest) (*promotionsv1.PreviewCouponResponse, error) {
		return couponValidationForPreviewCoupon.PreviewCoupon(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                        *slog.Logger
	orderWorkflowClient        ordersv1.OrderWorkflowServiceClient
	promotionWorkflowClient    promotionsv1.PromotionWorkflowServiceClient
	promotionCalculationClient promotionsv1.PromotionCalculationServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                        deps.Logger,
		orderWorkflowClient:        ordersv1.NewOrderWorkflowServiceClient(deps.Conn),
		promotionWorkflowClient:    promotionsv1.NewPromotionWorkflowServiceClient(deps.Conn),
		promotionCalculationClient: promotionsv1.NewPromotionCalculationServiceClient(deps.Conn),
	}
	go w.consume(ctx, "promotions.reject_redemption.list_order_workflows", deps.Bus.Subscribe("promotions.reject_redemption.list_order_workflows"), w.handlePromotionsRejectRedemptionListOrderWorkflows)
	go w.consume(ctx, "promotions.approve_campaign.list_promotion_workflows", deps.Bus.Subscribe("promotions.approve_campaign.list_promotion_workflows"), w.handlePromotionsApproveCampaignListPromotionWorkflows)
	go w.consume(ctx, "promotions.get_coupon_rates.recalculate_promotions", deps.Bus.Subscribe("promotions.get_coupon_rates.recalculate_promotions"), w.handlePromotionsGetCouponRatesRecalculatePromotions)
}

func (w *workers) consume(ctx context.Context, topic string, events <-chan eventbus.Event, handle func(context.Context, eventbus.Event) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			if err := handle(ctx, event); err != nil {
				w.log.ErrorContext(ctx, "event failed", "topic", topic, "error", err)
			}
		}
	}
}

// handlePromotionsRejectRedemptionListOrderWorkflows makes the call the
// promotions.reject_redemption.list_order_workflows event asks for.
func (w *workers) handlePromotionsRejectRedemptionListOrderWorkflows(ctx context.Context, event eventbus.Event) error {
	_, err := w.orderWorkflowClient.ListOrderWorkflows(ctx, &ordersv1.ListOrderWorkflowsRequest{
		Filter: &ordersv1.ListOrderWorkflowsRequest_Filter{
			Region:  event.Attributes["filter.region"],
			OwnerId: event.Attributes["filter.owner_id"],
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handlePromotionsApproveCampaignListPromotionWorkflows makes the call the
// promotions.approve_campaign.list_promotion_workflows event asks for.
func (w *workers) handlePromotionsApproveCampaignListPromotionWorkflows(ctx context.Context, event eventbus.Event) error {
	_, err := w.promotionWorkflowClient.ListPromotionWorkflows(ctx, &promotionsv1.ListPromotionWorkflowsRequest{
		Filter: &promotionsv1.ListPromotionWorkflowsRequest_Filter{
			Region:  event.Attributes["filter.region"],
			OwnerId: event.Attributes["filter.owner_id"],
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handlePromotionsGetCouponRatesRecalculatePromotions makes the call the
// promotions.get_coupon_rates.recalculate_promotions event asks for.
func (w *workers) handlePromotionsGetCouponRatesRecalculatePromotions(ctx context.Context, event eventbus.Event) error {
	_, err := w.promotionCalculationClient.RecalculatePromotions(ctx, &promotionsv1.RecalculatePromotionsRequest{
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				Email:  event.Attributes["promotion.owner.email"],
				UserId: event.Attributes["promotion.owner.user_id"],
			},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}
