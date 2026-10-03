package warehousing

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

// warehousingPickListValidationServiceName is the full name of acme.warehousing.v1.PickListValidationService.
const warehousingPickListValidationServiceName = "acme.warehousing.v1.PickListValidationService"

const warehousingPickListValidationServiceNormalizePickListMethod = "/" + warehousingPickListValidationServiceName + "/NormalizePickList"

// customersCustomerServiceName is the full name of acme.customers.v1.CustomerService.
const customersCustomerServiceName = "acme.customers.v1.CustomerService"

const customersCustomerServiceUpdateCustomerMethod = "/" + customersCustomerServiceName + "/UpdateCustomer"

// identitySessionNotificationServiceName is the full name of acme.identity.v1.SessionNotificationService.
const identitySessionNotificationServiceName = "acme.identity.v1.SessionNotificationService"

const identitySessionNotificationServiceUnsubscribeSessionUpdatesMethod = "/" + identitySessionNotificationServiceName + "/UnsubscribeSessionUpdates"

const warehousingPickListValidationServicePreviewPickListMethod = "/" + warehousingPickListValidationServiceName + "/PreviewPickList"

// shippingCarrierRateReportingServiceName is the full name of acme.shipping.v1.CarrierRateReportingService.
const shippingCarrierRateReportingServiceName = "acme.shipping.v1.CarrierRateReportingService"

// warehousingStorageBinQueryServiceName is the full name of acme.warehousing.v1.StorageBinQueryService.
const warehousingStorageBinQueryServiceName = "acme.warehousing.v1.StorageBinQueryService"

// subscriptionsSubscriptionServiceName is the full name of acme.subscriptions.v1.SubscriptionService.
const subscriptionsSubscriptionServiceName = "acme.subscriptions.v1.SubscriptionService"
