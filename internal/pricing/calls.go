package pricing

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

// documentsDocumentValidationServiceName is the full name of acme.documents.v1.DocumentValidationService.
const documentsDocumentValidationServiceName = "acme.documents.v1.DocumentValidationService"

const documentsDocumentValidationServiceValidateDocumentMethod = "/" + documentsDocumentValidationServiceName + "/ValidateDocument"

// pricingPriceListWorkflowServiceName is the full name of acme.pricing.v1.PriceListWorkflowService.
const pricingPriceListWorkflowServiceName = "acme.pricing.v1.PriceListWorkflowService"

const pricingPriceListWorkflowServiceCancelPriceListWorkflowMethod = "/" + pricingPriceListWorkflowServiceName + "/CancelPriceListWorkflow"

// identityTenantExportServiceName is the full name of acme.identity.v1.TenantExportService.
const identityTenantExportServiceName = "acme.identity.v1.TenantExportService"

// identitySessionNotificationServiceName is the full name of acme.identity.v1.SessionNotificationService.
const identitySessionNotificationServiceName = "acme.identity.v1.SessionNotificationService"

const identitySessionNotificationServiceSendSessionTestNoticeMethod = "/" + identitySessionNotificationServiceName + "/SendSessionTestNotice"

// pricingPriceQuoteCalculationServiceName is the full name of acme.pricing.v1.PriceQuoteCalculationService.
const pricingPriceQuoteCalculationServiceName = "acme.pricing.v1.PriceQuoteCalculationService"

const pricingPriceQuoteCalculationServiceRecalculatePriceQuotesMethod = "/" + pricingPriceQuoteCalculationServiceName + "/RecalculatePriceQuotes"

// customersConsentRecordCalculationServiceName is the full name of acme.customers.v1.ConsentRecordCalculationService.
const customersConsentRecordCalculationServiceName = "acme.customers.v1.ConsentRecordCalculationService"

// pricingCurrencyRateValidationServiceName is the full name of acme.pricing.v1.CurrencyRateValidationService.
const pricingCurrencyRateValidationServiceName = "acme.pricing.v1.CurrencyRateValidationService"

const pricingCurrencyRateValidationServiceExplainCurrencyRateDecisionMethod = "/" + pricingCurrencyRateValidationServiceName + "/ExplainCurrencyRateDecision"

// searchSearchQueryQueryServiceName is the full name of acme.search.v1.SearchQueryQueryService.
const searchSearchQueryQueryServiceName = "acme.search.v1.SearchQueryQueryService"

const searchSearchQueryQueryServiceBrowseSearchQueriesMethod = "/" + searchSearchQueryQueryServiceName + "/BrowseSearchQueries"

const pricingPriceQuoteCalculationServiceComparePriceQuoteOptionsMethod = "/" + pricingPriceQuoteCalculationServiceName + "/ComparePriceQuoteOptions"

// pricingPriceListValidationServiceName is the full name of acme.pricing.v1.PriceListValidationService.
const pricingPriceListValidationServiceName = "acme.pricing.v1.PriceListValidationService"

const pricingPriceListValidationServiceNormalizePriceListMethod = "/" + pricingPriceListValidationServiceName + "/NormalizePriceList"

// paymentsPaymentInstrumentSyncServiceName is the full name of acme.payments.v1.PaymentInstrumentSyncService.
const paymentsPaymentInstrumentSyncServiceName = "acme.payments.v1.PaymentInstrumentSyncService"

const paymentsPaymentInstrumentSyncServicePullPaymentInstrumentsMethod = "/" + paymentsPaymentInstrumentSyncServiceName + "/PullPaymentInstruments"

// driversDriverQueryServiceName is the full name of acme.drivers.v1.DriverQueryService.
const driversDriverQueryServiceName = "acme.drivers.v1.DriverQueryService"

// cartCartValidationServiceName is the full name of acme.cart.v1.CartValidationService.
const cartCartValidationServiceName = "acme.cart.v1.CartValidationService"

// shippingCarrierRateApprovalServiceName is the full name of acme.shipping.v1.CarrierRateApprovalService.
const shippingCarrierRateApprovalServiceName = "acme.shipping.v1.CarrierRateApprovalService"

const shippingCarrierRateApprovalServiceRejectCarrierRateMethod = "/" + shippingCarrierRateApprovalServiceName + "/RejectCarrierRate"

const pricingPriceQuoteCalculationServiceQuotePriceQuoteMethod = "/" + pricingPriceQuoteCalculationServiceName + "/QuotePriceQuote"
