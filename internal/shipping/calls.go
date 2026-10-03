package shipping

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

// identityApiCredentialValidationServiceName is the full name of acme.identity.v1.ApiCredentialValidationService.
const identityApiCredentialValidationServiceName = "acme.identity.v1.ApiCredentialValidationService"

const identityApiCredentialValidationServicePreviewApiCredentialMethod = "/" + identityApiCredentialValidationServiceName + "/PreviewApiCredential"

// shippingShipmentCalculationServiceName is the full name of acme.shipping.v1.ShipmentCalculationService.
const shippingShipmentCalculationServiceName = "acme.shipping.v1.ShipmentCalculationService"

// shippingManifestCalculationServiceName is the full name of acme.shipping.v1.ManifestCalculationService.
const shippingManifestCalculationServiceName = "acme.shipping.v1.ManifestCalculationService"

// searchSearchIndexNotificationServiceName is the full name of acme.search.v1.SearchIndexNotificationService.
const searchSearchIndexNotificationServiceName = "acme.search.v1.SearchIndexNotificationService"

// shippingCarrierRateValidationServiceName is the full name of acme.shipping.v1.CarrierRateValidationService.
const shippingCarrierRateValidationServiceName = "acme.shipping.v1.CarrierRateValidationService"

// shippingShipmentValidationServiceName is the full name of acme.shipping.v1.ShipmentValidationService.
const shippingShipmentValidationServiceName = "acme.shipping.v1.ShipmentValidationService"

const shippingShipmentValidationServicePreviewShipmentMethod = "/" + shippingShipmentValidationServiceName + "/PreviewShipment"

// searchSynonymExportServiceName is the full name of acme.search.v1.SynonymExportService.
const searchSynonymExportServiceName = "acme.search.v1.SynonymExportService"

const searchSynonymExportServiceShareSynonymExportMethod = "/" + searchSynonymExportServiceName + "/ShareSynonymExport"

// shippingParcelLifecycleServiceName is the full name of acme.shipping.v1.ParcelLifecycleService.
const shippingParcelLifecycleServiceName = "acme.shipping.v1.ParcelLifecycleService"

// subscriptionsSubscriptionPlanApprovalServiceName is the full name of
// acme.subscriptions.v1.SubscriptionPlanApprovalService.
const subscriptionsSubscriptionPlanApprovalServiceName = "acme.subscriptions.v1.SubscriptionPlanApprovalService"

// identityUserServiceName is the full name of acme.identity.v1.UserService.
const identityUserServiceName = "acme.identity.v1.UserService"

const identityUserServiceGetUserMethod = "/" + identityUserServiceName + "/GetUser"

// merchandisingMerchandisingRuleValidationServiceName is the full name of
// acme.merchandising.v1.MerchandisingRuleValidationService.
const merchandisingMerchandisingRuleValidationServiceName = "acme.merchandising.v1.MerchandisingRuleValidationService"

// shippingParcelCalculationServiceName is the full name of acme.shipping.v1.ParcelCalculationService.
const shippingParcelCalculationServiceName = "acme.shipping.v1.ParcelCalculationService"

// returnsInspectionCalculationServiceName is the full name of acme.returns.v1.InspectionCalculationService.
const returnsInspectionCalculationServiceName = "acme.returns.v1.InspectionCalculationService"

// customersCustomerServiceName is the full name of acme.customers.v1.CustomerService.
const customersCustomerServiceName = "acme.customers.v1.CustomerService"

// merchandisingCollectionWorkflowServiceName is the full name of acme.merchandising.v1.CollectionWorkflowService.
const merchandisingCollectionWorkflowServiceName = "acme.merchandising.v1.CollectionWorkflowService"

const merchandisingCollectionWorkflowServiceAdvanceCollectionWorkflowMethod = "/" + merchandisingCollectionWorkflowServiceName + "/AdvanceCollectionWorkflow"

const merchandisingCollectionWorkflowServiceResumeCollectionWorkflowMethod = "/" + merchandisingCollectionWorkflowServiceName + "/ResumeCollectionWorkflow"

const shippingShipmentCalculationServiceQuoteShipmentMethod = "/" + shippingShipmentCalculationServiceName + "/QuoteShipment"

// searchSearchIndexServiceName is the full name of acme.search.v1.SearchIndexService.
const searchSearchIndexServiceName = "acme.search.v1.SearchIndexService"

// shippingTrackingEventValidationServiceName is the full name of acme.shipping.v1.TrackingEventValidationService.
const shippingTrackingEventValidationServiceName = "acme.shipping.v1.TrackingEventValidationService"

const shippingTrackingEventValidationServiceExplainTrackingEventDecisionMethod = "/" + shippingTrackingEventValidationServiceName + "/ExplainTrackingEventDecision"

// deliveriesDeliveryQueryServiceName is the full name of acme.deliveries.v1.DeliveryQueryService.
const deliveriesDeliveryQueryServiceName = "acme.deliveries.v1.DeliveryQueryService"

const deliveriesDeliveryQueryServiceCountDeliveriesMethod = "/" + deliveriesDeliveryQueryServiceName + "/CountDeliveries"

// subscriptionsSubscriptionPlanQueryServiceName is the full name of acme.subscriptions.v1.SubscriptionPlanQueryService.
const subscriptionsSubscriptionPlanQueryServiceName = "acme.subscriptions.v1.SubscriptionPlanQueryService"

const subscriptionsSubscriptionPlanQueryServiceSuggestSubscriptionPlansMethod = "/" + subscriptionsSubscriptionPlanQueryServiceName + "/SuggestSubscriptionPlans"

// billingBillingAccountServiceName is the full name of acme.billing.v1.BillingAccountService.
const billingBillingAccountServiceName = "acme.billing.v1.BillingAccountService"

const billingBillingAccountServiceCreateBillingAccountMethod = "/" + billingBillingAccountServiceName + "/CreateBillingAccount"
