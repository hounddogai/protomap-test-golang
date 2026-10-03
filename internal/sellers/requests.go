package sellers

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
)

// calculateSellerPayoutScheduleOption sets part of a request of
// acme.sellers.v1.SellerPayoutScheduleCalculationService/CalculateSellerPayoutSchedule.
type calculateSellerPayoutScheduleOption func(*sellersv1.CalculateSellerPayoutScheduleRequest)

// newCalculateSellerPayoutScheduleRequest builds a request of
// acme.sellers.v1.SellerPayoutScheduleCalculationService/CalculateSellerPayoutSchedule from options.
func newCalculateSellerPayoutScheduleRequest(options ...calculateSellerPayoutScheduleOption) *sellersv1.CalculateSellerPayoutScheduleRequest {
	request := &sellersv1.CalculateSellerPayoutScheduleRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCalculateSellerPayoutScheduleContext(value *commonv1.RequestContext) calculateSellerPayoutScheduleOption {
	return func(request *sellersv1.CalculateSellerPayoutScheduleRequest) { request.Context = value }
}

func withCalculateSellerPayoutScheduleSellerPayoutSchedule(value *sellersv1.SellerPayoutSchedule) calculateSellerPayoutScheduleOption {
	return func(request *sellersv1.CalculateSellerPayoutScheduleRequest) { request.SellerPayoutSchedule = value }
}

// explainSellerPayoutScheduleCalculationOption sets part of a request of
// acme.sellers.v1.SellerPayoutScheduleCalculationService/ExplainSellerPayoutScheduleCalculation.
type explainSellerPayoutScheduleCalculationOption func(*sellersv1.ExplainSellerPayoutScheduleCalculationRequest)

// newExplainSellerPayoutScheduleCalculationRequest builds a request of
// acme.sellers.v1.SellerPayoutScheduleCalculationService/ExplainSellerPayoutScheduleCalculation from options.
func newExplainSellerPayoutScheduleCalculationRequest(options ...explainSellerPayoutScheduleCalculationOption) *sellersv1.ExplainSellerPayoutScheduleCalculationRequest {
	request := &sellersv1.ExplainSellerPayoutScheduleCalculationRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainSellerPayoutScheduleCalculationContext(value *commonv1.RequestContext) explainSellerPayoutScheduleCalculationOption {
	return func(request *sellersv1.ExplainSellerPayoutScheduleCalculationRequest) { request.Context = value }
}

func withExplainSellerPayoutScheduleCalculationSellerPayoutSchedule(value *sellersv1.SellerPayoutSchedule) explainSellerPayoutScheduleCalculationOption {
	return func(request *sellersv1.ExplainSellerPayoutScheduleCalculationRequest) {
		request.SellerPayoutSchedule = value
	}
}

// checkSellerEligibilityOption sets part of a request of
// acme.sellers.v1.SellerValidationService/CheckSellerEligibility.
type checkSellerEligibilityOption func(*sellersv1.CheckSellerEligibilityRequest)

// newCheckSellerEligibilityRequest builds a request of acme.sellers.v1.SellerValidationService/CheckSellerEligibility
// from options.
func newCheckSellerEligibilityRequest(options ...checkSellerEligibilityOption) *sellersv1.CheckSellerEligibilityRequest {
	request := &sellersv1.CheckSellerEligibilityRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCheckSellerEligibilityContext(value *commonv1.RequestContext) checkSellerEligibilityOption {
	return func(request *sellersv1.CheckSellerEligibilityRequest) { request.Context = value }
}

func withCheckSellerEligibilitySeller(value *sellersv1.Seller) checkSellerEligibilityOption {
	return func(request *sellersv1.CheckSellerEligibilityRequest) { request.Seller = value }
}

// signalGiftCardWorkflowOption sets part of a request of
// acme.giftcards.v1.GiftCardWorkflowService/SignalGiftCardWorkflow.
type signalGiftCardWorkflowOption func(*giftcardsv1.SignalGiftCardWorkflowRequest)

// newSignalGiftCardWorkflowRequest builds a request of acme.giftcards.v1.GiftCardWorkflowService/SignalGiftCardWorkflow
// from options.
func newSignalGiftCardWorkflowRequest(options ...signalGiftCardWorkflowOption) *giftcardsv1.SignalGiftCardWorkflowRequest {
	request := &giftcardsv1.SignalGiftCardWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSignalGiftCardWorkflowContext(value *commonv1.RequestContext) signalGiftCardWorkflowOption {
	return func(request *giftcardsv1.SignalGiftCardWorkflowRequest) { request.Context = value }
}

func withSignalGiftCardWorkflowGiftCard(value *giftcardsv1.GiftCard) signalGiftCardWorkflowOption {
	return func(request *giftcardsv1.SignalGiftCardWorkflowRequest) { request.GiftCard = value }
}

// lintGiftCardTransactionsOption sets part of a request of
// acme.giftcards.v1.GiftCardTransactionValidationService/LintGiftCardTransactions.
type lintGiftCardTransactionsOption func(*giftcardsv1.LintGiftCardTransactionsRequest)

// newLintGiftCardTransactionsRequest builds a request of
// acme.giftcards.v1.GiftCardTransactionValidationService/LintGiftCardTransactions from options.
func newLintGiftCardTransactionsRequest(options ...lintGiftCardTransactionsOption) *giftcardsv1.LintGiftCardTransactionsRequest {
	request := &giftcardsv1.LintGiftCardTransactionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withLintGiftCardTransactionsContext(value *commonv1.RequestContext) lintGiftCardTransactionsOption {
	return func(request *giftcardsv1.LintGiftCardTransactionsRequest) { request.Context = value }
}

func withLintGiftCardTransactionsDetails(value *giftcardsv1.LintGiftCardTransactionsRequest_Details) lintGiftCardTransactionsOption {
	return func(request *giftcardsv1.LintGiftCardTransactionsRequest) { request.Details = value }
}

func withLintGiftCardTransactionsGiftCardTransaction(value *giftcardsv1.GiftCardTransaction) lintGiftCardTransactionsOption {
	return func(request *giftcardsv1.LintGiftCardTransactionsRequest) { request.GiftCardTransaction = value }
}

// quoteSellerOption sets part of a request of acme.sellers.v1.SellerCalculationService/QuoteSeller.
type quoteSellerOption func(*sellersv1.QuoteSellerRequest)

// newQuoteSellerRequest builds a request of acme.sellers.v1.SellerCalculationService/QuoteSeller from options.
func newQuoteSellerRequest(options ...quoteSellerOption) *sellersv1.QuoteSellerRequest {
	request := &sellersv1.QuoteSellerRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withQuoteSellerContext(value *commonv1.RequestContext) quoteSellerOption {
	return func(request *sellersv1.QuoteSellerRequest) { request.Context = value }
}

func withQuoteSellerDetails(value *sellersv1.QuoteSellerRequest_Details) quoteSellerOption {
	return func(request *sellersv1.QuoteSellerRequest) { request.Details = value }
}

// createSoftwareLicenseOption sets part of a request of acme.licensing.v1.SoftwareLicenseService/CreateSoftwareLicense.
type createSoftwareLicenseOption func(*licensingv1.CreateSoftwareLicenseRequest)

// newCreateSoftwareLicenseRequest builds a request of acme.licensing.v1.SoftwareLicenseService/CreateSoftwareLicense
// from options.
func newCreateSoftwareLicenseRequest(options ...createSoftwareLicenseOption) *licensingv1.CreateSoftwareLicenseRequest {
	request := &licensingv1.CreateSoftwareLicenseRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCreateSoftwareLicenseContext(value *commonv1.RequestContext) createSoftwareLicenseOption {
	return func(request *licensingv1.CreateSoftwareLicenseRequest) { request.Context = value }
}

func withCreateSoftwareLicenseSoftwareLicense(value *licensingv1.SoftwareLicense) createSoftwareLicenseOption {
	return func(request *licensingv1.CreateSoftwareLicenseRequest) { request.SoftwareLicense = value }
}

// browseMetricSeriesesOption sets part of a request of acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses.
type browseMetricSeriesesOption func(*metricsv1.BrowseMetricSeriesesRequest)

// newBrowseMetricSeriesesRequest builds a request of acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses from
// options.
func newBrowseMetricSeriesesRequest(options ...browseMetricSeriesesOption) *metricsv1.BrowseMetricSeriesesRequest {
	request := &metricsv1.BrowseMetricSeriesesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseMetricSeriesesContext(value *commonv1.RequestContext) browseMetricSeriesesOption {
	return func(request *metricsv1.BrowseMetricSeriesesRequest) { request.Context = value }
}

func withBrowseMetricSeriesesFilter(value *metricsv1.BrowseMetricSeriesesRequest_Filter) browseMetricSeriesesOption {
	return func(request *metricsv1.BrowseMetricSeriesesRequest) { request.Filter = value }
}

// signalGiftCardTransactionWorkflowOption sets part of a request of
// acme.giftcards.v1.GiftCardTransactionWorkflowService/SignalGiftCardTransactionWorkflow.
type signalGiftCardTransactionWorkflowOption func(*giftcardsv1.SignalGiftCardTransactionWorkflowRequest)

// newSignalGiftCardTransactionWorkflowRequest builds a request of
// acme.giftcards.v1.GiftCardTransactionWorkflowService/SignalGiftCardTransactionWorkflow from options.
func newSignalGiftCardTransactionWorkflowRequest(options ...signalGiftCardTransactionWorkflowOption) *giftcardsv1.SignalGiftCardTransactionWorkflowRequest {
	request := &giftcardsv1.SignalGiftCardTransactionWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSignalGiftCardTransactionWorkflowContext(value *commonv1.RequestContext) signalGiftCardTransactionWorkflowOption {
	return func(request *giftcardsv1.SignalGiftCardTransactionWorkflowRequest) { request.Context = value }
}

func withSignalGiftCardTransactionWorkflowDetails(value *giftcardsv1.SignalGiftCardTransactionWorkflowRequest_Details) signalGiftCardTransactionWorkflowOption {
	return func(request *giftcardsv1.SignalGiftCardTransactionWorkflowRequest) { request.Details = value }
}

func withSignalGiftCardTransactionWorkflowGiftCardTransaction(value *giftcardsv1.GiftCardTransaction) signalGiftCardTransactionWorkflowOption {
	return func(request *giftcardsv1.SignalGiftCardTransactionWorkflowRequest) {
		request.GiftCardTransaction = value
	}
}
