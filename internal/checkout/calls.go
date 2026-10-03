package checkout

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

// customersHouseholdCalculationServiceName is the full name of acme.customers.v1.HouseholdCalculationService.
const customersHouseholdCalculationServiceName = "acme.customers.v1.HouseholdCalculationService"

// searchSearchQueryExportServiceName is the full name of acme.search.v1.SearchQueryExportService.
const searchSearchQueryExportServiceName = "acme.search.v1.SearchQueryExportService"

const searchSearchQueryExportServiceRetrySearchQueryExportMethod = "/" + searchSearchQueryExportServiceName + "/RetrySearchQueryExport"

// venuesVenueValidationServiceName is the full name of acme.venues.v1.VenueValidationService.
const venuesVenueValidationServiceName = "acme.venues.v1.VenueValidationService"

// customersConsentRecordCalculationServiceName is the full name of acme.customers.v1.ConsentRecordCalculationService.
const customersConsentRecordCalculationServiceName = "acme.customers.v1.ConsentRecordCalculationService"

const customersConsentRecordCalculationServiceQuoteConsentRecordMethod = "/" + customersConsentRecordCalculationServiceName + "/QuoteConsentRecord"
