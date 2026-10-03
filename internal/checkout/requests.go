package checkout

import (
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
)

// lookupShippingQuoteOption sets part of a request of acme.checkout.v1.ShippingQuoteQueryService/LookupShippingQuote.
type lookupShippingQuoteOption func(*checkoutv1.LookupShippingQuoteRequest)

// newLookupShippingQuoteRequest builds a request of acme.checkout.v1.ShippingQuoteQueryService/LookupShippingQuote from
// options.
func newLookupShippingQuoteRequest(options ...lookupShippingQuoteOption) *checkoutv1.LookupShippingQuoteRequest {
	request := &checkoutv1.LookupShippingQuoteRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withLookupShippingQuoteContext(value *commonv1.RequestContext) lookupShippingQuoteOption {
	return func(request *checkoutv1.LookupShippingQuoteRequest) { request.Context = value }
}

func withLookupShippingQuoteShippingQuoteId(value string) lookupShippingQuoteOption {
	return func(request *checkoutv1.LookupShippingQuoteRequest) { request.ShippingQuoteId = value }
}
