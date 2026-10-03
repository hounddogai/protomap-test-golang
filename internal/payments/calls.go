package payments

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

// budgetingBudgetQueryServiceName is the full name of acme.budgeting.v1.BudgetQueryService.
const budgetingBudgetQueryServiceName = "acme.budgeting.v1.BudgetQueryService"

const budgetingBudgetQueryServiceSuggestBudgetsMethod = "/" + budgetingBudgetQueryServiceName + "/SuggestBudgets"

// paymentsDisputeReportingServiceName is the full name of acme.payments.v1.DisputeReportingService.
const paymentsDisputeReportingServiceName = "acme.payments.v1.DisputeReportingService"

// reviewsReviewVoteCalculationServiceName is the full name of acme.reviews.v1.ReviewVoteCalculationService.
const reviewsReviewVoteCalculationServiceName = "acme.reviews.v1.ReviewVoteCalculationService"

const reviewsReviewVoteCalculationServiceExplainReviewVoteCalculationMethod = "/" + reviewsReviewVoteCalculationServiceName + "/ExplainReviewVoteCalculation"

// paymentsPaymentInstrumentSyncServiceName is the full name of acme.payments.v1.PaymentInstrumentSyncService.
const paymentsPaymentInstrumentSyncServiceName = "acme.payments.v1.PaymentInstrumentSyncService"

// paymentsRefundCalculationServiceName is the full name of acme.payments.v1.RefundCalculationService.
const paymentsRefundCalculationServiceName = "acme.payments.v1.RefundCalculationService"

// paymentsDisputeSyncServiceName is the full name of acme.payments.v1.DisputeSyncService.
const paymentsDisputeSyncServiceName = "acme.payments.v1.DisputeSyncService"

const paymentsDisputeSyncServiceReconcileDisputesMethod = "/" + paymentsDisputeSyncServiceName + "/ReconcileDisputes"

// reviewsReviewVoteWorkflowServiceName is the full name of acme.reviews.v1.ReviewVoteWorkflowService.
const reviewsReviewVoteWorkflowServiceName = "acme.reviews.v1.ReviewVoteWorkflowService"

const reviewsReviewVoteWorkflowServiceResumeReviewVoteWorkflowMethod = "/" + reviewsReviewVoteWorkflowServiceName + "/ResumeReviewVoteWorkflow"

// customersConsentRecordValidationServiceName is the full name of acme.customers.v1.ConsentRecordValidationService.
const customersConsentRecordValidationServiceName = "acme.customers.v1.ConsentRecordValidationService"

// reviewsReviewModerationApprovalServiceName is the full name of acme.reviews.v1.ReviewModerationApprovalService.
const reviewsReviewModerationApprovalServiceName = "acme.reviews.v1.ReviewModerationApprovalService"

const reviewsReviewModerationApprovalServiceRejectReviewModerationMethod = "/" + reviewsReviewModerationApprovalServiceName + "/RejectReviewModeration"

// customersCustomerSegmentValidationServiceName is the full name of acme.customers.v1.CustomerSegmentValidationService.
const customersCustomerSegmentValidationServiceName = "acme.customers.v1.CustomerSegmentValidationService"

const customersCustomerSegmentValidationServiceLintCustomerSegmentsMethod = "/" + customersCustomerSegmentValidationServiceName + "/LintCustomerSegments"

// paymentsPaymentValidationServiceName is the full name of acme.payments.v1.PaymentValidationService.
const paymentsPaymentValidationServiceName = "acme.payments.v1.PaymentValidationService"

// customersConsentRecordCalculationServiceName is the full name of acme.customers.v1.ConsentRecordCalculationService.
const customersConsentRecordCalculationServiceName = "acme.customers.v1.ConsentRecordCalculationService"

// loyaltyTierApprovalServiceName is the full name of acme.loyalty.v1.TierApprovalService.
const loyaltyTierApprovalServiceName = "acme.loyalty.v1.TierApprovalService"

// paymentsPayoutSyncServiceName is the full name of acme.payments.v1.PayoutSyncService.
const paymentsPayoutSyncServiceName = "acme.payments.v1.PayoutSyncService"

// searchSearchIndexExportServiceName is the full name of acme.search.v1.SearchIndexExportService.
const searchSearchIndexExportServiceName = "acme.search.v1.SearchIndexExportService"

const searchSearchIndexExportServiceScheduleSearchIndexExportMethod = "/" + searchSearchIndexExportServiceName + "/ScheduleSearchIndexExport"

// paymentsPaymentCalculationServiceName is the full name of acme.payments.v1.PaymentCalculationService.
const paymentsPaymentCalculationServiceName = "acme.payments.v1.PaymentCalculationService"

// shippingShipmentCalculationServiceName is the full name of acme.shipping.v1.ShipmentCalculationService.
const shippingShipmentCalculationServiceName = "acme.shipping.v1.ShipmentCalculationService"

const shippingShipmentCalculationServiceQuoteShipmentMethod = "/" + shippingShipmentCalculationServiceName + "/QuoteShipment"

// returnsRestockDecisionValidationServiceName is the full name of acme.returns.v1.RestockDecisionValidationService.
const returnsRestockDecisionValidationServiceName = "acme.returns.v1.RestockDecisionValidationService"

// fulfillmentFulfillmentWorkflowServiceName is the full name of acme.fulfillment.v1.FulfillmentWorkflowService.
const fulfillmentFulfillmentWorkflowServiceName = "acme.fulfillment.v1.FulfillmentWorkflowService"

const fulfillmentFulfillmentWorkflowServiceListFulfillmentWorkflowsMethod = "/" + fulfillmentFulfillmentWorkflowServiceName + "/ListFulfillmentWorkflows"

const paymentsPayoutSyncServiceReconcilePayoutsMethod = "/" + paymentsPayoutSyncServiceName + "/ReconcilePayouts"

// paymentsRefundReportingServiceName is the full name of acme.payments.v1.RefundReportingService.
const paymentsRefundReportingServiceName = "acme.payments.v1.RefundReportingService"

const paymentsRefundReportingServiceShareRefundReportMethod = "/" + paymentsRefundReportingServiceName + "/ShareRefundReport"

const paymentsPaymentInstrumentSyncServiceListPaymentInstrumentConflictsMethod = "/" + paymentsPaymentInstrumentSyncServiceName + "/ListPaymentInstrumentConflicts"

// searchSynonymAdminServiceName is the full name of acme.search.v1.SynonymAdminService.
const searchSynonymAdminServiceName = "acme.search.v1.SynonymAdminService"

const searchSynonymAdminServiceRepairSynonymMethod = "/" + searchSynonymAdminServiceName + "/RepairSynonym"

// paymentsDisputeCalculationServiceName is the full name of acme.payments.v1.DisputeCalculationService.
const paymentsDisputeCalculationServiceName = "acme.payments.v1.DisputeCalculationService"

const paymentsDisputeCalculationServiceCalculateDisputeMethod = "/" + paymentsDisputeCalculationServiceName + "/CalculateDispute"

// personalizationProfileCalculationServiceName is the full name of acme.personalization.v1.ProfileCalculationService.
const personalizationProfileCalculationServiceName = "acme.personalization.v1.ProfileCalculationService"
