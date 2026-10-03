package customers

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

// identityTenantAdminServiceName is the full name of acme.identity.v1.TenantAdminService.
const identityTenantAdminServiceName = "acme.identity.v1.TenantAdminService"

const identityTenantAdminServiceUnlockTenantMethod = "/" + identityTenantAdminServiceName + "/UnlockTenant"

// customersHouseholdApprovalServiceName is the full name of acme.customers.v1.HouseholdApprovalService.
const customersHouseholdApprovalServiceName = "acme.customers.v1.HouseholdApprovalService"

const customersHouseholdApprovalServiceDelegateHouseholdApprovalMethod = "/" + customersHouseholdApprovalServiceName + "/DelegateHouseholdApproval"

// personalizationProfileCalculationServiceName is the full name of acme.personalization.v1.ProfileCalculationService.
const personalizationProfileCalculationServiceName = "acme.personalization.v1.ProfileCalculationService"

const personalizationProfileCalculationServiceSimulateProfileMethod = "/" + personalizationProfileCalculationServiceName + "/SimulateProfile"

// catalogBrandApprovalServiceName is the full name of acme.catalog.v1.BrandApprovalService.
const catalogBrandApprovalServiceName = "acme.catalog.v1.BrandApprovalService"

const catalogBrandApprovalServiceSubmitBrandMethod = "/" + catalogBrandApprovalServiceName + "/SubmitBrand"

// sellersSellerCalculationServiceName is the full name of acme.sellers.v1.SellerCalculationService.
const sellersSellerCalculationServiceName = "acme.sellers.v1.SellerCalculationService"

// catalogCategoryValidationServiceName is the full name of acme.catalog.v1.CategoryValidationService.
const catalogCategoryValidationServiceName = "acme.catalog.v1.CategoryValidationService"

const catalogCategoryValidationServiceValidateCategoryMethod = "/" + catalogCategoryValidationServiceName + "/ValidateCategory"

// customersConsentRecordValidationServiceName is the full name of acme.customers.v1.ConsentRecordValidationService.
const customersConsentRecordValidationServiceName = "acme.customers.v1.ConsentRecordValidationService"

const customersConsentRecordValidationServiceExplainConsentRecordDecisionMethod = "/" + customersConsentRecordValidationServiceName + "/ExplainConsentRecordDecision"

// documentsDocumentServiceName is the full name of acme.documents.v1.DocumentService.
const documentsDocumentServiceName = "acme.documents.v1.DocumentService"

// performersPerformerQueryServiceName is the full name of acme.performers.v1.PerformerQueryService.
const performersPerformerQueryServiceName = "acme.performers.v1.PerformerQueryService"

// customersPreferenceCalculationServiceName is the full name of acme.customers.v1.PreferenceCalculationService.
const customersPreferenceCalculationServiceName = "acme.customers.v1.PreferenceCalculationService"

const customersPreferenceCalculationServiceSimulatePreferenceMethod = "/" + customersPreferenceCalculationServiceName + "/SimulatePreference"

// authorizationAccessPolicyValidationServiceName is the full name of
// acme.authorization.v1.AccessPolicyValidationService.
const authorizationAccessPolicyValidationServiceName = "acme.authorization.v1.AccessPolicyValidationService"

const authorizationAccessPolicyValidationServiceLintAccessPoliciesMethod = "/" + authorizationAccessPolicyValidationServiceName + "/LintAccessPolicies"

// customersHouseholdCalculationServiceName is the full name of acme.customers.v1.HouseholdCalculationService.
const customersHouseholdCalculationServiceName = "acme.customers.v1.HouseholdCalculationService"

// customersCustomerCalculationServiceName is the full name of acme.customers.v1.CustomerCalculationService.
const customersCustomerCalculationServiceName = "acme.customers.v1.CustomerCalculationService"

// customersCustomerSegmentCalculationServiceName is the full name of
// acme.customers.v1.CustomerSegmentCalculationService.
const customersCustomerSegmentCalculationServiceName = "acme.customers.v1.CustomerSegmentCalculationService"

const customersCustomerSegmentCalculationServiceRecalculateCustomerSegmentsMethod = "/" + customersCustomerSegmentCalculationServiceName + "/RecalculateCustomerSegments"

// customersPreferenceReportingServiceName is the full name of acme.customers.v1.PreferenceReportingService.
const customersPreferenceReportingServiceName = "acme.customers.v1.PreferenceReportingService"

const customersPreferenceReportingServiceListPreferenceReportsMethod = "/" + customersPreferenceReportingServiceName + "/ListPreferenceReports"

const customersCustomerCalculationServiceSimulateCustomerMethod = "/" + customersCustomerCalculationServiceName + "/SimulateCustomer"

// notificationsNotificationQueryServiceName is the full name of acme.notifications.v1.NotificationQueryService.
const notificationsNotificationQueryServiceName = "acme.notifications.v1.NotificationQueryService"

// customersHouseholdReportingServiceName is the full name of acme.customers.v1.HouseholdReportingService.
const customersHouseholdReportingServiceName = "acme.customers.v1.HouseholdReportingService"

const customersHouseholdReportingServiceShareHouseholdReportMethod = "/" + customersHouseholdReportingServiceName + "/ShareHouseholdReport"

// ordersOrderAmendmentValidationServiceName is the full name of acme.orders.v1.OrderAmendmentValidationService.
const ordersOrderAmendmentValidationServiceName = "acme.orders.v1.OrderAmendmentValidationService"

const customersCustomerCalculationServiceExplainCustomerCalculationMethod = "/" + customersCustomerCalculationServiceName + "/ExplainCustomerCalculation"

// subscriptionsRenewalScheduleWorkflowServiceName is the full name of
// acme.subscriptions.v1.RenewalScheduleWorkflowService.
const subscriptionsRenewalScheduleWorkflowServiceName = "acme.subscriptions.v1.RenewalScheduleWorkflowService"

const subscriptionsRenewalScheduleWorkflowServiceSignalRenewalScheduleWorkflowMethod = "/" + subscriptionsRenewalScheduleWorkflowServiceName + "/SignalRenewalScheduleWorkflow"

// paymentsPaymentInstrumentSyncServiceName is the full name of acme.payments.v1.PaymentInstrumentSyncService.
const paymentsPaymentInstrumentSyncServiceName = "acme.payments.v1.PaymentInstrumentSyncService"

// cartSavedListApprovalServiceName is the full name of acme.cart.v1.SavedListApprovalService.
const cartSavedListApprovalServiceName = "acme.cart.v1.SavedListApprovalService"

const cartSavedListApprovalServiceCommentSavedListApprovalMethod = "/" + cartSavedListApprovalServiceName + "/CommentSavedListApproval"

// sellersSellerPayoutScheduleReportingServiceName is the full name of
// acme.sellers.v1.SellerPayoutScheduleReportingService.
const sellersSellerPayoutScheduleReportingServiceName = "acme.sellers.v1.SellerPayoutScheduleReportingService"

// customersConsentRecordReportingServiceName is the full name of acme.customers.v1.ConsentRecordReportingService.
const customersConsentRecordReportingServiceName = "acme.customers.v1.ConsentRecordReportingService"

const customersConsentRecordReportingServiceCountConsentRecordTotalsMethod = "/" + customersConsentRecordReportingServiceName + "/CountConsentRecordTotals"

// catalogCategoryWorkflowServiceName is the full name of acme.catalog.v1.CategoryWorkflowService.
const catalogCategoryWorkflowServiceName = "acme.catalog.v1.CategoryWorkflowService"

// customersPreferenceImportServiceName is the full name of acme.customers.v1.PreferenceImportService.
const customersPreferenceImportServiceName = "acme.customers.v1.PreferenceImportService"

const customersPreferenceImportServiceRetryPreferenceImportMethod = "/" + customersPreferenceImportServiceName + "/RetryPreferenceImport"

// identityUserServiceName is the full name of acme.identity.v1.UserService.
const identityUserServiceName = "acme.identity.v1.UserService"

// customersCustomerSegmentImportServiceName is the full name of acme.customers.v1.CustomerSegmentImportService.
const customersCustomerSegmentImportServiceName = "acme.customers.v1.CustomerSegmentImportService"

const customersCustomerSegmentImportServiceRetryCustomerSegmentImportMethod = "/" + customersCustomerSegmentImportServiceName + "/RetryCustomerSegmentImport"

const customersConsentRecordValidationServicePreviewConsentRecordMethod = "/" + customersConsentRecordValidationServiceName + "/PreviewConsentRecord"
