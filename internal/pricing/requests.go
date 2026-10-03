package pricing

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	experimentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
)

// scorePriceQuoteOption sets part of a request of acme.pricing.v1.PriceQuoteValidationService/ScorePriceQuote.
type scorePriceQuoteOption func(*pricingv1.ScorePriceQuoteRequest)

// newScorePriceQuoteRequest builds a request of acme.pricing.v1.PriceQuoteValidationService/ScorePriceQuote from
// options.
func newScorePriceQuoteRequest(options ...scorePriceQuoteOption) *pricingv1.ScorePriceQuoteRequest {
	request := &pricingv1.ScorePriceQuoteRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withScorePriceQuoteContext(value *commonv1.RequestContext) scorePriceQuoteOption {
	return func(request *pricingv1.ScorePriceQuoteRequest) { request.Context = value }
}

func withScorePriceQuotePriceQuote(value *pricingv1.PriceQuote) scorePriceQuoteOption {
	return func(request *pricingv1.ScorePriceQuoteRequest) { request.PriceQuote = value }
}

// scheduleRestockDecisionExpiryOption sets part of a request of
// acme.returns.v1.RestockDecisionLifecycleService/ScheduleRestockDecisionExpiry.
type scheduleRestockDecisionExpiryOption func(*returnsv1.ScheduleRestockDecisionExpiryRequest)

// newScheduleRestockDecisionExpiryRequest builds a request of
// acme.returns.v1.RestockDecisionLifecycleService/ScheduleRestockDecisionExpiry from options.
func newScheduleRestockDecisionExpiryRequest(options ...scheduleRestockDecisionExpiryOption) *returnsv1.ScheduleRestockDecisionExpiryRequest {
	request := &returnsv1.ScheduleRestockDecisionExpiryRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withScheduleRestockDecisionExpiryContext(value *commonv1.RequestContext) scheduleRestockDecisionExpiryOption {
	return func(request *returnsv1.ScheduleRestockDecisionExpiryRequest) { request.Context = value }
}

func withScheduleRestockDecisionExpiryDetails(value *returnsv1.ScheduleRestockDecisionExpiryRequest_Details) scheduleRestockDecisionExpiryOption {
	return func(request *returnsv1.ScheduleRestockDecisionExpiryRequest) { request.Details = value }
}

func withScheduleRestockDecisionExpiryRestockDecision(value *returnsv1.RestockDecision) scheduleRestockDecisionExpiryOption {
	return func(request *returnsv1.ScheduleRestockDecisionExpiryRequest) { request.RestockDecision = value }
}

// cancelPriceListWorkflowOption sets part of a request of
// acme.pricing.v1.PriceListWorkflowService/CancelPriceListWorkflow.
type cancelPriceListWorkflowOption func(*pricingv1.CancelPriceListWorkflowRequest)

// newCancelPriceListWorkflowRequest builds a request of
// acme.pricing.v1.PriceListWorkflowService/CancelPriceListWorkflow from options.
func newCancelPriceListWorkflowRequest(options ...cancelPriceListWorkflowOption) *pricingv1.CancelPriceListWorkflowRequest {
	request := &pricingv1.CancelPriceListWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCancelPriceListWorkflowContext(value *commonv1.RequestContext) cancelPriceListWorkflowOption {
	return func(request *pricingv1.CancelPriceListWorkflowRequest) { request.Context = value }
}

func withCancelPriceListWorkflowPriceList(value *pricingv1.PriceList) cancelPriceListWorkflowOption {
	return func(request *pricingv1.CancelPriceListWorkflowRequest) { request.PriceList = value }
}

// normalizeRenewalScheduleOption sets part of a request of
// acme.subscriptions.v1.RenewalScheduleValidationService/NormalizeRenewalSchedule.
type normalizeRenewalScheduleOption func(*subscriptionsv1.NormalizeRenewalScheduleRequest)

// newNormalizeRenewalScheduleRequest builds a request of
// acme.subscriptions.v1.RenewalScheduleValidationService/NormalizeRenewalSchedule from options.
func newNormalizeRenewalScheduleRequest(options ...normalizeRenewalScheduleOption) *subscriptionsv1.NormalizeRenewalScheduleRequest {
	request := &subscriptionsv1.NormalizeRenewalScheduleRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withNormalizeRenewalScheduleContext(value *commonv1.RequestContext) normalizeRenewalScheduleOption {
	return func(request *subscriptionsv1.NormalizeRenewalScheduleRequest) { request.Context = value }
}

func withNormalizeRenewalScheduleRenewalSchedule(value *subscriptionsv1.RenewalSchedule) normalizeRenewalScheduleOption {
	return func(request *subscriptionsv1.NormalizeRenewalScheduleRequest) { request.RenewalSchedule = value }
}

// approvePriceListOption sets part of a request of acme.pricing.v1.PriceListApprovalService/ApprovePriceList.
type approvePriceListOption func(*pricingv1.ApprovePriceListRequest)

// newApprovePriceListRequest builds a request of acme.pricing.v1.PriceListApprovalService/ApprovePriceList from
// options.
func newApprovePriceListRequest(options ...approvePriceListOption) *pricingv1.ApprovePriceListRequest {
	request := &pricingv1.ApprovePriceListRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withApprovePriceListContext(value *commonv1.RequestContext) approvePriceListOption {
	return func(request *pricingv1.ApprovePriceListRequest) { request.Context = value }
}

func withApprovePriceListPriceList(value *pricingv1.PriceList) approvePriceListOption {
	return func(request *pricingv1.ApprovePriceListRequest) { request.PriceList = value }
}

// approvePriceOverrideOption sets part of a request of
// acme.pricing.v1.PriceOverrideApprovalService/ApprovePriceOverride.
type approvePriceOverrideOption func(*pricingv1.ApprovePriceOverrideRequest)

// newApprovePriceOverrideRequest builds a request of acme.pricing.v1.PriceOverrideApprovalService/ApprovePriceOverride
// from options.
func newApprovePriceOverrideRequest(options ...approvePriceOverrideOption) *pricingv1.ApprovePriceOverrideRequest {
	request := &pricingv1.ApprovePriceOverrideRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withApprovePriceOverrideContext(value *commonv1.RequestContext) approvePriceOverrideOption {
	return func(request *pricingv1.ApprovePriceOverrideRequest) { request.Context = value }
}

func withApprovePriceOverridePriceOverride(value *pricingv1.PriceOverride) approvePriceOverrideOption {
	return func(request *pricingv1.ApprovePriceOverrideRequest) { request.PriceOverride = value }
}

// validateHouseholdOption sets part of a request of acme.customers.v1.HouseholdValidationService/ValidateHousehold.
type validateHouseholdOption func(*customersv1.ValidateHouseholdRequest)

// newValidateHouseholdRequest builds a request of acme.customers.v1.HouseholdValidationService/ValidateHousehold from
// options.
func newValidateHouseholdRequest(options ...validateHouseholdOption) *customersv1.ValidateHouseholdRequest {
	request := &customersv1.ValidateHouseholdRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withValidateHouseholdContext(value *commonv1.RequestContext) validateHouseholdOption {
	return func(request *customersv1.ValidateHouseholdRequest) { request.Context = value }
}

func withValidateHouseholdHousehold(value *customersv1.Household) validateHouseholdOption {
	return func(request *customersv1.ValidateHouseholdRequest) { request.Household = value }
}

// searchExperimentsOption sets part of a request of acme.experiments.v1.ExperimentService/SearchExperiments.
type searchExperimentsOption func(*experimentsv1.SearchExperimentsRequest)

// newSearchExperimentsRequest builds a request of acme.experiments.v1.ExperimentService/SearchExperiments from options.
func newSearchExperimentsRequest(options ...searchExperimentsOption) *experimentsv1.SearchExperimentsRequest {
	request := &experimentsv1.SearchExperimentsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSearchExperimentsContext(value *commonv1.RequestContext) searchExperimentsOption {
	return func(request *experimentsv1.SearchExperimentsRequest) { request.Context = value }
}

func withSearchExperimentsFilter(value *experimentsv1.SearchExperimentsRequest_Filter) searchExperimentsOption {
	return func(request *experimentsv1.SearchExperimentsRequest) { request.Filter = value }
}

// explainPriceQuoteCalculationOption sets part of a request of
// acme.pricing.v1.PriceQuoteCalculationService/ExplainPriceQuoteCalculation.
type explainPriceQuoteCalculationOption func(*pricingv1.ExplainPriceQuoteCalculationRequest)

// newExplainPriceQuoteCalculationRequest builds a request of
// acme.pricing.v1.PriceQuoteCalculationService/ExplainPriceQuoteCalculation from options.
func newExplainPriceQuoteCalculationRequest(options ...explainPriceQuoteCalculationOption) *pricingv1.ExplainPriceQuoteCalculationRequest {
	request := &pricingv1.ExplainPriceQuoteCalculationRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainPriceQuoteCalculationContext(value *commonv1.RequestContext) explainPriceQuoteCalculationOption {
	return func(request *pricingv1.ExplainPriceQuoteCalculationRequest) { request.Context = value }
}

func withExplainPriceQuoteCalculationPriceQuote(value *pricingv1.PriceQuote) explainPriceQuoteCalculationOption {
	return func(request *pricingv1.ExplainPriceQuoteCalculationRequest) { request.PriceQuote = value }
}

// normalizeFraudSignalOption sets part of a request of acme.fraud.v1.FraudSignalValidationService/NormalizeFraudSignal.
type normalizeFraudSignalOption func(*fraudv1.NormalizeFraudSignalRequest)

// newNormalizeFraudSignalRequest builds a request of acme.fraud.v1.FraudSignalValidationService/NormalizeFraudSignal
// from options.
func newNormalizeFraudSignalRequest(options ...normalizeFraudSignalOption) *fraudv1.NormalizeFraudSignalRequest {
	request := &fraudv1.NormalizeFraudSignalRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withNormalizeFraudSignalContext(value *commonv1.RequestContext) normalizeFraudSignalOption {
	return func(request *fraudv1.NormalizeFraudSignalRequest) { request.Context = value }
}

func withNormalizeFraudSignalDetails(value *fraudv1.NormalizeFraudSignalRequest_Details) normalizeFraudSignalOption {
	return func(request *fraudv1.NormalizeFraudSignalRequest) { request.Details = value }
}

// normalizeDisputeOption sets part of a request of acme.payments.v1.DisputeValidationService/NormalizeDispute.
type normalizeDisputeOption func(*paymentsv1.NormalizeDisputeRequest)

// newNormalizeDisputeRequest builds a request of acme.payments.v1.DisputeValidationService/NormalizeDispute from
// options.
func newNormalizeDisputeRequest(options ...normalizeDisputeOption) *paymentsv1.NormalizeDisputeRequest {
	request := &paymentsv1.NormalizeDisputeRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withNormalizeDisputeContext(value *commonv1.RequestContext) normalizeDisputeOption {
	return func(request *paymentsv1.NormalizeDisputeRequest) { request.Context = value }
}

func withNormalizeDisputeDispute(value *paymentsv1.Dispute) normalizeDisputeOption {
	return func(request *paymentsv1.NormalizeDisputeRequest) { request.Dispute = value }
}
