package storefront

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

// customersHouseholdWorkflowServiceName is the full name of acme.customers.v1.HouseholdWorkflowService.
const customersHouseholdWorkflowServiceName = "acme.customers.v1.HouseholdWorkflowService"

// storefrontShopperViewNotificationServiceName is the full name of acme.storefront.v1.ShopperViewNotificationService.
const storefrontShopperViewNotificationServiceName = "acme.storefront.v1.ShopperViewNotificationService"

// storefrontLandingPageNotificationServiceName is the full name of acme.storefront.v1.LandingPageNotificationService.
const storefrontLandingPageNotificationServiceName = "acme.storefront.v1.LandingPageNotificationService"

// procurementPurchaseRequisitionServiceName is the full name of acme.procurement.v1.PurchaseRequisitionService.
const procurementPurchaseRequisitionServiceName = "acme.procurement.v1.PurchaseRequisitionService"

// storefrontStorefrontSessionIntegrationServiceName is the full name of
// acme.storefront.v1.StorefrontSessionIntegrationService.
const storefrontStorefrontSessionIntegrationServiceName = "acme.storefront.v1.StorefrontSessionIntegrationService"

const storefrontStorefrontSessionIntegrationServiceConnectStorefrontSessionSourceMethod = "/" + storefrontStorefrontSessionIntegrationServiceName + "/ConnectStorefrontSessionSource"

// partnerapiPartnerIntegrationWorkflowServiceName is the full name of
// acme.partnerapi.v1.PartnerIntegrationWorkflowService.
const partnerapiPartnerIntegrationWorkflowServiceName = "acme.partnerapi.v1.PartnerIntegrationWorkflowService"

// storefrontLandingPageWorkflowServiceName is the full name of acme.storefront.v1.LandingPageWorkflowService.
const storefrontLandingPageWorkflowServiceName = "acme.storefront.v1.LandingPageWorkflowService"

// storefrontStorefrontSessionWorkflowServiceName is the full name of
// acme.storefront.v1.StorefrontSessionWorkflowService.
const storefrontStorefrontSessionWorkflowServiceName = "acme.storefront.v1.StorefrontSessionWorkflowService"

const storefrontStorefrontSessionWorkflowServiceStartStorefrontSessionWorkflowMethod = "/" + storefrontStorefrontSessionWorkflowServiceName + "/StartStorefrontSessionWorkflow"

// edgegatewayRouteBindingQueryServiceName is the full name of acme.edgegateway.v1.RouteBindingQueryService.
const edgegatewayRouteBindingQueryServiceName = "acme.edgegateway.v1.RouteBindingQueryService"
