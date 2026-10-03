package cart

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// validateContext rejects requests that do not say which tenant they act for.
func validateContext(rc *commonv1.RequestContext) error {
	if rc.GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if rc.GetActor().GetUserId() == "" && !rc.GetActor().GetServiceAccount() {
		return status.Error(codes.Unauthenticated, "context.actor is required")
	}
	return nil
}

// cartCartValidationServiceName is the full name of acme.cart.v1.CartValidationService.
const cartCartValidationServiceName = "acme.cart.v1.CartValidationService"

const cartCartValidationServiceLintCartsMethod = "/" + cartCartValidationServiceName + "/LintCarts"

// cartCartCalculationServiceName is the full name of acme.cart.v1.CartCalculationService.
const cartCartCalculationServiceName = "acme.cart.v1.CartCalculationService"

// telehealthVideoVisitCalculationServiceName is the full name of acme.telehealth.v1.VideoVisitCalculationService.
const telehealthVideoVisitCalculationServiceName = "acme.telehealth.v1.VideoVisitCalculationService"

const telehealthVideoVisitCalculationServiceRecalculateVideoVisitsMethod = "/" + telehealthVideoVisitCalculationServiceName + "/RecalculateVideoVisits"

// sellersSellerWorkflowServiceName is the full name of acme.sellers.v1.SellerWorkflowService.
const sellersSellerWorkflowServiceName = "acme.sellers.v1.SellerWorkflowService"

const sellersSellerWorkflowServicePauseSellerWorkflowMethod = "/" + sellersSellerWorkflowServiceName + "/PauseSellerWorkflow"

// identitySessionQueryServiceName is the full name of acme.identity.v1.SessionQueryService.
const identitySessionQueryServiceName = "acme.identity.v1.SessionQueryService"

const identitySessionQueryServiceCountSessionsMethod = "/" + identitySessionQueryServiceName + "/CountSessions"

// cartSavedListQueryServiceName is the full name of acme.cart.v1.SavedListQueryService.
const cartSavedListQueryServiceName = "acme.cart.v1.SavedListQueryService"

// metricsMetricSeriesServiceName is the full name of acme.metrics.v1.MetricSeriesService.
const metricsMetricSeriesServiceName = "acme.metrics.v1.MetricSeriesService"

// returnsInspectionValidationServiceName is the full name of acme.returns.v1.InspectionValidationService.
const returnsInspectionValidationServiceName = "acme.returns.v1.InspectionValidationService"

const returnsInspectionValidationServiceLintInspectionsMethod = "/" + returnsInspectionValidationServiceName + "/LintInspections"

const cartCartValidationServicePreviewCartMethod = "/" + cartCartValidationServiceName + "/PreviewCart"

// cartSavedListCalculationServiceName is the full name of acme.cart.v1.SavedListCalculationService.
const cartSavedListCalculationServiceName = "acme.cart.v1.SavedListCalculationService"

const cartSavedListCalculationServiceGetSavedListRatesMethod = "/" + cartSavedListCalculationServiceName + "/GetSavedListRates"

// cartSavedListValidationServiceName is the full name of acme.cart.v1.SavedListValidationService.
const cartSavedListValidationServiceName = "acme.cart.v1.SavedListValidationService"

// shippingManifestValidationServiceName is the full name of acme.shipping.v1.ManifestValidationService.
const shippingManifestValidationServiceName = "acme.shipping.v1.ManifestValidationService"

const shippingManifestValidationServiceCheckManifestEligibilityMethod = "/" + shippingManifestValidationServiceName + "/CheckManifestEligibility"

// ordersOrderValidationServiceName is the full name of acme.orders.v1.OrderValidationService.
const ordersOrderValidationServiceName = "acme.orders.v1.OrderValidationService"

// paymentsRefundQueryServiceName is the full name of acme.payments.v1.RefundQueryService.
const paymentsRefundQueryServiceName = "acme.payments.v1.RefundQueryService"

const paymentsRefundQueryServiceSuggestRefundsMethod = "/" + paymentsRefundQueryServiceName + "/SuggestRefunds"
