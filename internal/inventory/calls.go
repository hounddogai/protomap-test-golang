package inventory

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

// showsShowServiceName is the full name of acme.shows.v1.ShowService.
const showsShowServiceName = "acme.shows.v1.ShowService"

const showsShowServiceCloneShowMethod = "/" + showsShowServiceName + "/CloneShow"

// warehousingStorageBinApprovalServiceName is the full name of acme.warehousing.v1.StorageBinApprovalService.
const warehousingStorageBinApprovalServiceName = "acme.warehousing.v1.StorageBinApprovalService"

// inventoryReplenishmentPlanValidationServiceName is the full name of
// acme.inventory.v1.ReplenishmentPlanValidationService.
const inventoryReplenishmentPlanValidationServiceName = "acme.inventory.v1.ReplenishmentPlanValidationService"

// customersHouseholdCalculationServiceName is the full name of acme.customers.v1.HouseholdCalculationService.
const customersHouseholdCalculationServiceName = "acme.customers.v1.HouseholdCalculationService"

// ledgerLedgerAccountQueryServiceName is the full name of acme.ledger.v1.LedgerAccountQueryService.
const ledgerLedgerAccountQueryServiceName = "acme.ledger.v1.LedgerAccountQueryService"

const ledgerLedgerAccountQueryServiceSummarizeLedgerAccountsMethod = "/" + ledgerLedgerAccountQueryServiceName + "/SummarizeLedgerAccounts"

// inventoryReplenishmentPlanCalculationServiceName is the full name of
// acme.inventory.v1.ReplenishmentPlanCalculationService.
const inventoryReplenishmentPlanCalculationServiceName = "acme.inventory.v1.ReplenishmentPlanCalculationService"

const inventoryReplenishmentPlanCalculationServiceCompareReplenishmentPlanOptionsMethod = "/" + inventoryReplenishmentPlanCalculationServiceName + "/CompareReplenishmentPlanOptions"

// fulfillmentFulfillmentWorkflowServiceName is the full name of acme.fulfillment.v1.FulfillmentWorkflowService.
const fulfillmentFulfillmentWorkflowServiceName = "acme.fulfillment.v1.FulfillmentWorkflowService"

// shippingTrackingEventApprovalServiceName is the full name of acme.shipping.v1.TrackingEventApprovalService.
const shippingTrackingEventApprovalServiceName = "acme.shipping.v1.TrackingEventApprovalService"

const shippingTrackingEventApprovalServiceRecallTrackingEventMethod = "/" + shippingTrackingEventApprovalServiceName + "/RecallTrackingEvent"

// returnsReturnLabelCalculationServiceName is the full name of acme.returns.v1.ReturnLabelCalculationService.
const returnsReturnLabelCalculationServiceName = "acme.returns.v1.ReturnLabelCalculationService"

// identityUserServiceName is the full name of acme.identity.v1.UserService.
const identityUserServiceName = "acme.identity.v1.UserService"
