package catalog

import (
	billingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/billing/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
)

// browseBillingAccountsOption sets part of a request of
// acme.billing.v1.BillingAccountQueryService/BrowseBillingAccounts.
type browseBillingAccountsOption func(*billingv1.BrowseBillingAccountsRequest)

// newBrowseBillingAccountsRequest builds a request of acme.billing.v1.BillingAccountQueryService/BrowseBillingAccounts
// from options.
func newBrowseBillingAccountsRequest(options ...browseBillingAccountsOption) *billingv1.BrowseBillingAccountsRequest {
	request := &billingv1.BrowseBillingAccountsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseBillingAccountsContext(value *commonv1.RequestContext) browseBillingAccountsOption {
	return func(request *billingv1.BrowseBillingAccountsRequest) { request.Context = value }
}

func withBrowseBillingAccountsFilter(value *billingv1.BrowseBillingAccountsRequest_Filter) browseBillingAccountsOption {
	return func(request *billingv1.BrowseBillingAccountsRequest) { request.Filter = value }
}

// compareOrderNoteOptionsOption sets part of a request of
// acme.orders.v1.OrderNoteCalculationService/CompareOrderNoteOptions.
type compareOrderNoteOptionsOption func(*ordersv1.CompareOrderNoteOptionsRequest)

// newCompareOrderNoteOptionsRequest builds a request of
// acme.orders.v1.OrderNoteCalculationService/CompareOrderNoteOptions from options.
func newCompareOrderNoteOptionsRequest(options ...compareOrderNoteOptionsOption) *ordersv1.CompareOrderNoteOptionsRequest {
	request := &ordersv1.CompareOrderNoteOptionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCompareOrderNoteOptionsContext(value *commonv1.RequestContext) compareOrderNoteOptionsOption {
	return func(request *ordersv1.CompareOrderNoteOptionsRequest) { request.Context = value }
}

func withCompareOrderNoteOptionsDetails(value *ordersv1.CompareOrderNoteOptionsRequest_Details) compareOrderNoteOptionsOption {
	return func(request *ordersv1.CompareOrderNoteOptionsRequest) { request.Details = value }
}

func withCompareOrderNoteOptionsOrderNote(value *ordersv1.OrderNote) compareOrderNoteOptionsOption {
	return func(request *ordersv1.CompareOrderNoteOptionsRequest) { request.OrderNote = value }
}

// shareProductVariantReportOption sets part of a request of
// acme.catalog.v1.ProductVariantReportingService/ShareProductVariantReport.
type shareProductVariantReportOption func(*catalogv1.ShareProductVariantReportRequest)

// newShareProductVariantReportRequest builds a request of
// acme.catalog.v1.ProductVariantReportingService/ShareProductVariantReport from options.
func newShareProductVariantReportRequest(options ...shareProductVariantReportOption) *catalogv1.ShareProductVariantReportRequest {
	request := &catalogv1.ShareProductVariantReportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withShareProductVariantReportContext(value *commonv1.RequestContext) shareProductVariantReportOption {
	return func(request *catalogv1.ShareProductVariantReportRequest) { request.Context = value }
}

func withShareProductVariantReportDetails(value *catalogv1.ShareProductVariantReportRequest_Details) shareProductVariantReportOption {
	return func(request *catalogv1.ShareProductVariantReportRequest) { request.Details = value }
}

// generateBrandReportOption sets part of a request of acme.catalog.v1.BrandReportingService/GenerateBrandReport.
type generateBrandReportOption func(*catalogv1.GenerateBrandReportRequest)

// newGenerateBrandReportRequest builds a request of acme.catalog.v1.BrandReportingService/GenerateBrandReport from
// options.
func newGenerateBrandReportRequest(options ...generateBrandReportOption) *catalogv1.GenerateBrandReportRequest {
	request := &catalogv1.GenerateBrandReportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withGenerateBrandReportContext(value *commonv1.RequestContext) generateBrandReportOption {
	return func(request *catalogv1.GenerateBrandReportRequest) { request.Context = value }
}

func withGenerateBrandReportBrandId(value string) generateBrandReportOption {
	return func(request *catalogv1.GenerateBrandReportRequest) { request.BrandId = value }
}

func withGenerateBrandReportReason(value string) generateBrandReportOption {
	return func(request *catalogv1.GenerateBrandReportRequest) { request.Reason = value }
}

func withGenerateBrandReportAnnotations(value map[string]string) generateBrandReportOption {
	return func(request *catalogv1.GenerateBrandReportRequest) { request.Annotations = value }
}

// explainSearchIndexDecisionOption sets part of a request of
// acme.search.v1.SearchIndexValidationService/ExplainSearchIndexDecision.
type explainSearchIndexDecisionOption func(*searchv1.ExplainSearchIndexDecisionRequest)

// newExplainSearchIndexDecisionRequest builds a request of
// acme.search.v1.SearchIndexValidationService/ExplainSearchIndexDecision from options.
func newExplainSearchIndexDecisionRequest(options ...explainSearchIndexDecisionOption) *searchv1.ExplainSearchIndexDecisionRequest {
	request := &searchv1.ExplainSearchIndexDecisionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainSearchIndexDecisionContext(value *commonv1.RequestContext) explainSearchIndexDecisionOption {
	return func(request *searchv1.ExplainSearchIndexDecisionRequest) { request.Context = value }
}

func withExplainSearchIndexDecisionSearchIndex(value *searchv1.SearchIndex) explainSearchIndexDecisionOption {
	return func(request *searchv1.ExplainSearchIndexDecisionRequest) { request.SearchIndex = value }
}

// simulateProductVariantOption sets part of a request of
// acme.catalog.v1.ProductVariantCalculationService/SimulateProductVariant.
type simulateProductVariantOption func(*catalogv1.SimulateProductVariantRequest)

// newSimulateProductVariantRequest builds a request of
// acme.catalog.v1.ProductVariantCalculationService/SimulateProductVariant from options.
func newSimulateProductVariantRequest(options ...simulateProductVariantOption) *catalogv1.SimulateProductVariantRequest {
	request := &catalogv1.SimulateProductVariantRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSimulateProductVariantContext(value *commonv1.RequestContext) simulateProductVariantOption {
	return func(request *catalogv1.SimulateProductVariantRequest) { request.Context = value }
}

func withSimulateProductVariantProductVariant(value *catalogv1.ProductVariant) simulateProductVariantOption {
	return func(request *catalogv1.SimulateProductVariantRequest) { request.ProductVariant = value }
}

// previewBrandOption sets part of a request of acme.catalog.v1.BrandValidationService/PreviewBrand.
type previewBrandOption func(*catalogv1.PreviewBrandRequest)

// newPreviewBrandRequest builds a request of acme.catalog.v1.BrandValidationService/PreviewBrand from options.
func newPreviewBrandRequest(options ...previewBrandOption) *catalogv1.PreviewBrandRequest {
	request := &catalogv1.PreviewBrandRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withPreviewBrandContext(value *commonv1.RequestContext) previewBrandOption {
	return func(request *catalogv1.PreviewBrandRequest) { request.Context = value }
}

func withPreviewBrandBrand(value *catalogv1.Brand) previewBrandOption {
	return func(request *catalogv1.PreviewBrandRequest) { request.Brand = value }
}
