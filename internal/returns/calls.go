package returns

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

// returnsReturnLabelApprovalServiceName is the full name of acme.returns.v1.ReturnLabelApprovalService.
const returnsReturnLabelApprovalServiceName = "acme.returns.v1.ReturnLabelApprovalService"

// returnsRestockDecisionCalculationServiceName is the full name of acme.returns.v1.RestockDecisionCalculationService.
const returnsRestockDecisionCalculationServiceName = "acme.returns.v1.RestockDecisionCalculationService"

const returnsRestockDecisionCalculationServiceExplainRestockDecisionCalculationMethod = "/" + returnsRestockDecisionCalculationServiceName + "/ExplainRestockDecisionCalculation"

// sellersSellerApprovalServiceName is the full name of acme.sellers.v1.SellerApprovalService.
const sellersSellerApprovalServiceName = "acme.sellers.v1.SellerApprovalService"

// reviewsReviewVoteApprovalServiceName is the full name of acme.reviews.v1.ReviewVoteApprovalService.
const reviewsReviewVoteApprovalServiceName = "acme.reviews.v1.ReviewVoteApprovalService"

// returnsReturnRequestValidationServiceName is the full name of acme.returns.v1.ReturnRequestValidationService.
const returnsReturnRequestValidationServiceName = "acme.returns.v1.ReturnRequestValidationService"

const returnsReturnRequestValidationServicePreviewReturnRequestMethod = "/" + returnsReturnRequestValidationServiceName + "/PreviewReturnRequest"

// sellersSellerScorecardCalculationServiceName is the full name of acme.sellers.v1.SellerScorecardCalculationService.
const sellersSellerScorecardCalculationServiceName = "acme.sellers.v1.SellerScorecardCalculationService"

const sellersSellerScorecardCalculationServiceCalculateSellerScorecardMethod = "/" + sellersSellerScorecardCalculationServiceName + "/CalculateSellerScorecard"

// searchSearchIndexServiceName is the full name of acme.search.v1.SearchIndexService.
const searchSearchIndexServiceName = "acme.search.v1.SearchIndexService"

// identitySessionQueryServiceName is the full name of acme.identity.v1.SessionQueryService.
const identitySessionQueryServiceName = "acme.identity.v1.SessionQueryService"

const identitySessionQueryServiceSuggestSessionsMethod = "/" + identitySessionQueryServiceName + "/SuggestSessions"

// customersCustomerCalculationServiceName is the full name of acme.customers.v1.CustomerCalculationService.
const customersCustomerCalculationServiceName = "acme.customers.v1.CustomerCalculationService"

const customersCustomerCalculationServiceRecalculateCustomersMethod = "/" + customersCustomerCalculationServiceName + "/RecalculateCustomers"

// customersCustomerSegmentValidationServiceName is the full name of acme.customers.v1.CustomerSegmentValidationService.
const customersCustomerSegmentValidationServiceName = "acme.customers.v1.CustomerSegmentValidationService"

// documentsDocumentValidationServiceName is the full name of acme.documents.v1.DocumentValidationService.
const documentsDocumentValidationServiceName = "acme.documents.v1.DocumentValidationService"
