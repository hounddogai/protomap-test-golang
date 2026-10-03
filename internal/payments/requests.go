package payments

import (
	bankingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/banking/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
)

// searchVenuesOption sets part of a request of acme.venues.v1.VenueService/SearchVenues.
type searchVenuesOption func(*venuesv1.SearchVenuesRequest)

// newSearchVenuesRequest builds a request of acme.venues.v1.VenueService/SearchVenues from options.
func newSearchVenuesRequest(options ...searchVenuesOption) *venuesv1.SearchVenuesRequest {
	request := &venuesv1.SearchVenuesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSearchVenuesContext(value *commonv1.RequestContext) searchVenuesOption {
	return func(request *venuesv1.SearchVenuesRequest) { request.Context = value }
}

func withSearchVenuesFilter(value *venuesv1.SearchVenuesRequest_Filter) searchVenuesOption {
	return func(request *venuesv1.SearchVenuesRequest) { request.Filter = value }
}

// browseShipmentsOption sets part of a request of acme.shipping.v1.ShipmentQueryService/BrowseShipments.
type browseShipmentsOption func(*shippingv1.BrowseShipmentsRequest)

// newBrowseShipmentsRequest builds a request of acme.shipping.v1.ShipmentQueryService/BrowseShipments from options.
func newBrowseShipmentsRequest(options ...browseShipmentsOption) *shippingv1.BrowseShipmentsRequest {
	request := &shippingv1.BrowseShipmentsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseShipmentsContext(value *commonv1.RequestContext) browseShipmentsOption {
	return func(request *shippingv1.BrowseShipmentsRequest) { request.Context = value }
}

func withBrowseShipmentsFilter(value *shippingv1.BrowseShipmentsRequest_Filter) browseShipmentsOption {
	return func(request *shippingv1.BrowseShipmentsRequest) { request.Filter = value }
}

// quoteRefundOption sets part of a request of acme.payments.v1.RefundCalculationService/QuoteRefund.
type quoteRefundOption func(*paymentsv1.QuoteRefundRequest)

// newQuoteRefundRequest builds a request of acme.payments.v1.RefundCalculationService/QuoteRefund from options.
func newQuoteRefundRequest(options ...quoteRefundOption) *paymentsv1.QuoteRefundRequest {
	request := &paymentsv1.QuoteRefundRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withQuoteRefundContext(value *commonv1.RequestContext) quoteRefundOption {
	return func(request *paymentsv1.QuoteRefundRequest) { request.Context = value }
}

func withQuoteRefundDetails(value *paymentsv1.QuoteRefundRequest_Details) quoteRefundOption {
	return func(request *paymentsv1.QuoteRefundRequest) { request.Details = value }
}

func withQuoteRefundRefund(value *paymentsv1.Refund) quoteRefundOption {
	return func(request *paymentsv1.QuoteRefundRequest) { request.Refund = value }
}

// subscribeSearchQueryUpdatesOption sets part of a request of
// acme.search.v1.SearchQueryNotificationService/SubscribeSearchQueryUpdates.
type subscribeSearchQueryUpdatesOption func(*searchv1.SubscribeSearchQueryUpdatesRequest)

// newSubscribeSearchQueryUpdatesRequest builds a request of
// acme.search.v1.SearchQueryNotificationService/SubscribeSearchQueryUpdates from options.
func newSubscribeSearchQueryUpdatesRequest(options ...subscribeSearchQueryUpdatesOption) *searchv1.SubscribeSearchQueryUpdatesRequest {
	request := &searchv1.SubscribeSearchQueryUpdatesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSubscribeSearchQueryUpdatesContext(value *commonv1.RequestContext) subscribeSearchQueryUpdatesOption {
	return func(request *searchv1.SubscribeSearchQueryUpdatesRequest) { request.Context = value }
}

func withSubscribeSearchQueryUpdatesDetails(value *searchv1.SubscribeSearchQueryUpdatesRequest_Details) subscribeSearchQueryUpdatesOption {
	return func(request *searchv1.SubscribeSearchQueryUpdatesRequest) { request.Details = value }
}

// suggestBankAccountsOption sets part of a request of acme.banking.v1.BankAccountQueryService/SuggestBankAccounts.
type suggestBankAccountsOption func(*bankingv1.SuggestBankAccountsRequest)

// newSuggestBankAccountsRequest builds a request of acme.banking.v1.BankAccountQueryService/SuggestBankAccounts from
// options.
func newSuggestBankAccountsRequest(options ...suggestBankAccountsOption) *bankingv1.SuggestBankAccountsRequest {
	request := &bankingv1.SuggestBankAccountsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSuggestBankAccountsContext(value *commonv1.RequestContext) suggestBankAccountsOption {
	return func(request *bankingv1.SuggestBankAccountsRequest) { request.Context = value }
}

func withSuggestBankAccountsFilter(value *bankingv1.SuggestBankAccountsRequest_Filter) suggestBankAccountsOption {
	return func(request *bankingv1.SuggestBankAccountsRequest) { request.Filter = value }
}

func withSuggestBankAccountsPage(value *commonv1.PageRequest) suggestBankAccountsOption {
	return func(request *bankingv1.SuggestBankAccountsRequest) { request.Page = value }
}

// recalculateCustomerSegmentsOption sets part of a request of
// acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments.
type recalculateCustomerSegmentsOption func(*customersv1.RecalculateCustomerSegmentsRequest)

// newRecalculateCustomerSegmentsRequest builds a request of
// acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments from options.
func newRecalculateCustomerSegmentsRequest(options ...recalculateCustomerSegmentsOption) *customersv1.RecalculateCustomerSegmentsRequest {
	request := &customersv1.RecalculateCustomerSegmentsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRecalculateCustomerSegmentsContext(value *commonv1.RequestContext) recalculateCustomerSegmentsOption {
	return func(request *customersv1.RecalculateCustomerSegmentsRequest) { request.Context = value }
}

func withRecalculateCustomerSegmentsCustomerSegment(value *customersv1.CustomerSegment) recalculateCustomerSegmentsOption {
	return func(request *customersv1.RecalculateCustomerSegmentsRequest) { request.CustomerSegment = value }
}

// getPaymentRatesOption sets part of a request of acme.payments.v1.PaymentCalculationService/GetPaymentRates.
type getPaymentRatesOption func(*paymentsv1.GetPaymentRatesRequest)

// newGetPaymentRatesRequest builds a request of acme.payments.v1.PaymentCalculationService/GetPaymentRates from
// options.
func newGetPaymentRatesRequest(options ...getPaymentRatesOption) *paymentsv1.GetPaymentRatesRequest {
	request := &paymentsv1.GetPaymentRatesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withGetPaymentRatesContext(value *commonv1.RequestContext) getPaymentRatesOption {
	return func(request *paymentsv1.GetPaymentRatesRequest) { request.Context = value }
}

func withGetPaymentRatesPaymentId(value string) getPaymentRatesOption {
	return func(request *paymentsv1.GetPaymentRatesRequest) { request.PaymentId = value }
}
