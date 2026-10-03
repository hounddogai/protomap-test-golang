package orders

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

// customersPreferenceCalculationServiceName is the full name of acme.customers.v1.PreferenceCalculationService.
const customersPreferenceCalculationServiceName = "acme.customers.v1.PreferenceCalculationService"

// ordersBackorderApprovalServiceName is the full name of acme.orders.v1.BackorderApprovalService.
const ordersBackorderApprovalServiceName = "acme.orders.v1.BackorderApprovalService"

const ordersBackorderApprovalServiceListPendingBackordersMethod = "/" + ordersBackorderApprovalServiceName + "/ListPendingBackorders"

// customersHouseholdQueryServiceName is the full name of acme.customers.v1.HouseholdQueryService.
const customersHouseholdQueryServiceName = "acme.customers.v1.HouseholdQueryService"

// reviewsProductReviewWorkflowServiceName is the full name of acme.reviews.v1.ProductReviewWorkflowService.
const reviewsProductReviewWorkflowServiceName = "acme.reviews.v1.ProductReviewWorkflowService"

// fraudWatchlistCalculationServiceName is the full name of acme.fraud.v1.WatchlistCalculationService.
const fraudWatchlistCalculationServiceName = "acme.fraud.v1.WatchlistCalculationService"

// sponsorshipsSponsorServiceName is the full name of acme.sponsorships.v1.SponsorService.
const sponsorshipsSponsorServiceName = "acme.sponsorships.v1.SponsorService"

const sponsorshipsSponsorServiceUpdateSponsorMethod = "/" + sponsorshipsSponsorServiceName + "/UpdateSponsor"

// identityTenantAdminServiceName is the full name of acme.identity.v1.TenantAdminService.
const identityTenantAdminServiceName = "acme.identity.v1.TenantAdminService"

// ordersOrderAmendmentApprovalServiceName is the full name of acme.orders.v1.OrderAmendmentApprovalService.
const ordersOrderAmendmentApprovalServiceName = "acme.orders.v1.OrderAmendmentApprovalService"

const ordersOrderAmendmentApprovalServiceListPendingOrderAmendmentsMethod = "/" + ordersOrderAmendmentApprovalServiceName + "/ListPendingOrderAmendments"

// paymentsPayoutCalculationServiceName is the full name of acme.payments.v1.PayoutCalculationService.
const paymentsPayoutCalculationServiceName = "acme.payments.v1.PayoutCalculationService"

const paymentsPayoutCalculationServiceExplainPayoutCalculationMethod = "/" + paymentsPayoutCalculationServiceName + "/ExplainPayoutCalculation"

// identityApiCredentialValidationServiceName is the full name of acme.identity.v1.ApiCredentialValidationService.
const identityApiCredentialValidationServiceName = "acme.identity.v1.ApiCredentialValidationService"

const identityApiCredentialValidationServiceNormalizeApiCredentialMethod = "/" + identityApiCredentialValidationServiceName + "/NormalizeApiCredential"

// prescriptionsPrescriptionServiceName is the full name of acme.prescriptions.v1.PrescriptionService.
const prescriptionsPrescriptionServiceName = "acme.prescriptions.v1.PrescriptionService"

// ordersOrderNoteSyncServiceName is the full name of acme.orders.v1.OrderNoteSyncService.
const ordersOrderNoteSyncServiceName = "acme.orders.v1.OrderNoteSyncService"

// customersPreferenceValidationServiceName is the full name of acme.customers.v1.PreferenceValidationService.
const customersPreferenceValidationServiceName = "acme.customers.v1.PreferenceValidationService"

const customersPreferenceValidationServiceNormalizePreferenceMethod = "/" + customersPreferenceValidationServiceName + "/NormalizePreference"

// sellersSellerScorecardReportingServiceName is the full name of acme.sellers.v1.SellerScorecardReportingService.
const sellersSellerScorecardReportingServiceName = "acme.sellers.v1.SellerScorecardReportingService"

const sellersSellerScorecardReportingServiceShareSellerScorecardReportMethod = "/" + sellersSellerScorecardReportingServiceName + "/ShareSellerScorecardReport"

// ordersOrderAmendmentQueryServiceName is the full name of acme.orders.v1.OrderAmendmentQueryService.
const ordersOrderAmendmentQueryServiceName = "acme.orders.v1.OrderAmendmentQueryService"

// paymentsPaymentWorkflowServiceName is the full name of acme.payments.v1.PaymentWorkflowService.
const paymentsPaymentWorkflowServiceName = "acme.payments.v1.PaymentWorkflowService"
