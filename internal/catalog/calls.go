package catalog

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

// searchSynonymHistoryServiceName is the full name of acme.search.v1.SynonymHistoryService.
const searchSynonymHistoryServiceName = "acme.search.v1.SynonymHistoryService"

// catalogProductVariantValidationServiceName is the full name of acme.catalog.v1.ProductVariantValidationService.
const catalogProductVariantValidationServiceName = "acme.catalog.v1.ProductVariantValidationService"

// catalogBrandCalculationServiceName is the full name of acme.catalog.v1.BrandCalculationService.
const catalogBrandCalculationServiceName = "acme.catalog.v1.BrandCalculationService"

const catalogBrandCalculationServiceExplainBrandCalculationMethod = "/" + catalogBrandCalculationServiceName + "/ExplainBrandCalculation"

// accountingChartOfAccountsServiceName is the full name of acme.accounting.v1.ChartOfAccountsService.
const accountingChartOfAccountsServiceName = "acme.accounting.v1.ChartOfAccountsService"

// identityRoleAdminServiceName is the full name of acme.identity.v1.RoleAdminService.
const identityRoleAdminServiceName = "acme.identity.v1.RoleAdminService"

const identityRoleAdminServiceRepairRoleMethod = "/" + identityRoleAdminServiceName + "/RepairRole"

// catalogCategoryValidationServiceName is the full name of acme.catalog.v1.CategoryValidationService.
const catalogCategoryValidationServiceName = "acme.catalog.v1.CategoryValidationService"

// catalogBrandReportingServiceName is the full name of acme.catalog.v1.BrandReportingService.
const catalogBrandReportingServiceName = "acme.catalog.v1.BrandReportingService"

// loyaltyRewardWorkflowServiceName is the full name of acme.loyalty.v1.RewardWorkflowService.
const loyaltyRewardWorkflowServiceName = "acme.loyalty.v1.RewardWorkflowService"

// subscriptionsSubscriptionPlanWorkflowServiceName is the full name of
// acme.subscriptions.v1.SubscriptionPlanWorkflowService.
const subscriptionsSubscriptionPlanWorkflowServiceName = "acme.subscriptions.v1.SubscriptionPlanWorkflowService"

// concessionsConcessionStandServiceName is the full name of acme.concessions.v1.ConcessionStandService.
const concessionsConcessionStandServiceName = "acme.concessions.v1.ConcessionStandService"

// fulfillmentFulfillmentExceptionValidationServiceName is the full name of
// acme.fulfillment.v1.FulfillmentExceptionValidationService.
const fulfillmentFulfillmentExceptionValidationServiceName = "acme.fulfillment.v1.FulfillmentExceptionValidationService"

const fulfillmentFulfillmentExceptionValidationServiceValidateFulfillmentExceptionMethod = "/" + fulfillmentFulfillmentExceptionValidationServiceName + "/ValidateFulfillmentException"

// inventoryStockReservationApprovalServiceName is the full name of acme.inventory.v1.StockReservationApprovalService.
const inventoryStockReservationApprovalServiceName = "acme.inventory.v1.StockReservationApprovalService"

// featureflagsFeatureFlagQueryServiceName is the full name of acme.featureflags.v1.FeatureFlagQueryService.
const featureflagsFeatureFlagQueryServiceName = "acme.featureflags.v1.FeatureFlagQueryService"

// identityUserQueryServiceName is the full name of acme.identity.v1.UserQueryService.
const identityUserQueryServiceName = "acme.identity.v1.UserQueryService"

const identityUserQueryServicePeekUserMethod = "/" + identityUserQueryServiceName + "/PeekUser"

// catalogProductServiceName is the full name of acme.catalog.v1.ProductService.
const catalogProductServiceName = "acme.catalog.v1.ProductService"

const catalogProductServiceBatchGetProductsMethod = "/" + catalogProductServiceName + "/BatchGetProducts"
