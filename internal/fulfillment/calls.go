package fulfillment

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

// fulfillmentPackingSlipQueryServiceName is the full name of acme.fulfillment.v1.PackingSlipQueryService.
const fulfillmentPackingSlipQueryServiceName = "acme.fulfillment.v1.PackingSlipQueryService"

// customersConsentRecordValidationServiceName is the full name of acme.customers.v1.ConsentRecordValidationService.
const customersConsentRecordValidationServiceName = "acme.customers.v1.ConsentRecordValidationService"

const customersConsentRecordValidationServiceExplainConsentRecordDecisionMethod = "/" + customersConsentRecordValidationServiceName + "/ExplainConsentRecordDecision"

// identityUserServiceName is the full name of acme.identity.v1.UserService.
const identityUserServiceName = "acme.identity.v1.UserService"

// fulfillmentFulfillmentServiceName is the full name of acme.fulfillment.v1.FulfillmentService.
const fulfillmentFulfillmentServiceName = "acme.fulfillment.v1.FulfillmentService"

// fulfillmentFulfillmentQueryServiceName is the full name of acme.fulfillment.v1.FulfillmentQueryService.
const fulfillmentFulfillmentQueryServiceName = "acme.fulfillment.v1.FulfillmentQueryService"

// seatingSeatMapQueryServiceName is the full name of acme.seating.v1.SeatMapQueryService.
const seatingSeatMapQueryServiceName = "acme.seating.v1.SeatMapQueryService"

const fulfillmentFulfillmentServiceSearchFulfillmentsMethod = "/" + fulfillmentFulfillmentServiceName + "/SearchFulfillments"

// subscriptionsSubscriptionPlanValidationServiceName is the full name of
// acme.subscriptions.v1.SubscriptionPlanValidationService.
const subscriptionsSubscriptionPlanValidationServiceName = "acme.subscriptions.v1.SubscriptionPlanValidationService"

const subscriptionsSubscriptionPlanValidationServiceCheckSubscriptionPlanEligibilityMethod = "/" + subscriptionsSubscriptionPlanValidationServiceName + "/CheckSubscriptionPlanEligibility"

// returnsRestockDecisionValidationServiceName is the full name of acme.returns.v1.RestockDecisionValidationService.
const returnsRestockDecisionValidationServiceName = "acme.returns.v1.RestockDecisionValidationService"

// searchSearchQueryNotificationServiceName is the full name of acme.search.v1.SearchQueryNotificationService.
const searchSearchQueryNotificationServiceName = "acme.search.v1.SearchQueryNotificationService"

// documentsDocumentQueryServiceName is the full name of acme.documents.v1.DocumentQueryService.
const documentsDocumentQueryServiceName = "acme.documents.v1.DocumentQueryService"
