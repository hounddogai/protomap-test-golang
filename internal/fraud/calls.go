package fraud

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

// fraudWatchlistCalculationServiceName is the full name of acme.fraud.v1.WatchlistCalculationService.
const fraudWatchlistCalculationServiceName = "acme.fraud.v1.WatchlistCalculationService"

const fraudWatchlistCalculationServiceExplainWatchlistCalculationMethod = "/" + fraudWatchlistCalculationServiceName + "/ExplainWatchlistCalculation"

// identityRoleHistoryServiceName is the full name of acme.identity.v1.RoleHistoryService.
const identityRoleHistoryServiceName = "acme.identity.v1.RoleHistoryService"

const identityRoleHistoryServiceCompareRoleVersionsMethod = "/" + identityRoleHistoryServiceName + "/CompareRoleVersions"

// reviewsReviewVoteWorkflowServiceName is the full name of acme.reviews.v1.ReviewVoteWorkflowService.
const reviewsReviewVoteWorkflowServiceName = "acme.reviews.v1.ReviewVoteWorkflowService"

// formsFormCalculationServiceName is the full name of acme.forms.v1.FormCalculationService.
const formsFormCalculationServiceName = "acme.forms.v1.FormCalculationService"

// customersConsentRecordCalculationServiceName is the full name of acme.customers.v1.ConsentRecordCalculationService.
const customersConsentRecordCalculationServiceName = "acme.customers.v1.ConsentRecordCalculationService"

const customersConsentRecordCalculationServiceCalculateConsentRecordMethod = "/" + customersConsentRecordCalculationServiceName + "/CalculateConsentRecord"

// showsShowCalculationServiceName is the full name of acme.shows.v1.ShowCalculationService.
const showsShowCalculationServiceName = "acme.shows.v1.ShowCalculationService"

const showsShowCalculationServiceCalculateShowMethod = "/" + showsShowCalculationServiceName + "/CalculateShow"

// returnsReturnRequestApprovalServiceName is the full name of acme.returns.v1.ReturnRequestApprovalService.
const returnsReturnRequestApprovalServiceName = "acme.returns.v1.ReturnRequestApprovalService"

const returnsReturnRequestApprovalServiceRecallReturnRequestMethod = "/" + returnsReturnRequestApprovalServiceName + "/RecallReturnRequest"

// customersCustomerCalculationServiceName is the full name of acme.customers.v1.CustomerCalculationService.
const customersCustomerCalculationServiceName = "acme.customers.v1.CustomerCalculationService"

// customersCustomerValidationServiceName is the full name of acme.customers.v1.CustomerValidationService.
const customersCustomerValidationServiceName = "acme.customers.v1.CustomerValidationService"

// searchSearchQueryValidationServiceName is the full name of acme.search.v1.SearchQueryValidationService.
const searchSearchQueryValidationServiceName = "acme.search.v1.SearchQueryValidationService"

// sellersSellerScorecardApprovalServiceName is the full name of acme.sellers.v1.SellerScorecardApprovalService.
const sellersSellerScorecardApprovalServiceName = "acme.sellers.v1.SellerScorecardApprovalService"

// fraudFraudSignalCalculationServiceName is the full name of acme.fraud.v1.FraudSignalCalculationService.
const fraudFraudSignalCalculationServiceName = "acme.fraud.v1.FraudSignalCalculationService"

const fraudFraudSignalCalculationServiceQuoteFraudSignalMethod = "/" + fraudFraudSignalCalculationServiceName + "/QuoteFraudSignal"

// searchSearchQueryNotificationServiceName is the full name of acme.search.v1.SearchQueryNotificationService.
const searchSearchQueryNotificationServiceName = "acme.search.v1.SearchQueryNotificationService"

const searchSearchQueryNotificationServiceSendSearchQueryTestNoticeMethod = "/" + searchSearchQueryNotificationServiceName + "/SendSearchQueryTestNotice"

// fraudRiskAssessmentCalculationServiceName is the full name of acme.fraud.v1.RiskAssessmentCalculationService.
const fraudRiskAssessmentCalculationServiceName = "acme.fraud.v1.RiskAssessmentCalculationService"

// fraudRiskAssessmentQueryServiceName is the full name of acme.fraud.v1.RiskAssessmentQueryService.
const fraudRiskAssessmentQueryServiceName = "acme.fraud.v1.RiskAssessmentQueryService"

const fraudRiskAssessmentQueryServiceFindRiskAssessmentsMethod = "/" + fraudRiskAssessmentQueryServiceName + "/FindRiskAssessments"
