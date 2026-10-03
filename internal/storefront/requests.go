package storefront

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	patientappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patientapp/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
)

// sendLandingPageTestNoticeOption sets part of a request of
// acme.storefront.v1.LandingPageNotificationService/SendLandingPageTestNotice.
type sendLandingPageTestNoticeOption func(*storefrontv1.SendLandingPageTestNoticeRequest)

// newSendLandingPageTestNoticeRequest builds a request of
// acme.storefront.v1.LandingPageNotificationService/SendLandingPageTestNotice from options.
func newSendLandingPageTestNoticeRequest(options ...sendLandingPageTestNoticeOption) *storefrontv1.SendLandingPageTestNoticeRequest {
	request := &storefrontv1.SendLandingPageTestNoticeRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSendLandingPageTestNoticeContext(value *commonv1.RequestContext) sendLandingPageTestNoticeOption {
	return func(request *storefrontv1.SendLandingPageTestNoticeRequest) { request.Context = value }
}

func withSendLandingPageTestNoticeDetails(value *storefrontv1.SendLandingPageTestNoticeRequest_Details) sendLandingPageTestNoticeOption {
	return func(request *storefrontv1.SendLandingPageTestNoticeRequest) { request.Details = value }
}

// cancelPartnerIntegrationWorkflowOption sets part of a request of
// acme.partnerapi.v1.PartnerIntegrationWorkflowService/CancelPartnerIntegrationWorkflow.
type cancelPartnerIntegrationWorkflowOption func(*partnerapiv1.CancelPartnerIntegrationWorkflowRequest)

// newCancelPartnerIntegrationWorkflowRequest builds a request of
// acme.partnerapi.v1.PartnerIntegrationWorkflowService/CancelPartnerIntegrationWorkflow from options.
func newCancelPartnerIntegrationWorkflowRequest(options ...cancelPartnerIntegrationWorkflowOption) *partnerapiv1.CancelPartnerIntegrationWorkflowRequest {
	request := &partnerapiv1.CancelPartnerIntegrationWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCancelPartnerIntegrationWorkflowContext(value *commonv1.RequestContext) cancelPartnerIntegrationWorkflowOption {
	return func(request *partnerapiv1.CancelPartnerIntegrationWorkflowRequest) { request.Context = value }
}

func withCancelPartnerIntegrationWorkflowPartnerIntegration(value *partnerapiv1.PartnerIntegration) cancelPartnerIntegrationWorkflowOption {
	return func(request *partnerapiv1.CancelPartnerIntegrationWorkflowRequest) {
		request.PartnerIntegration = value
	}
}

func withCancelPartnerIntegrationWorkflowDetails(value *partnerapiv1.CancelPartnerIntegrationWorkflowRequest_Details) cancelPartnerIntegrationWorkflowOption {
	return func(request *partnerapiv1.CancelPartnerIntegrationWorkflowRequest) { request.Details = value }
}

// listPatientHomesOption sets part of a request of acme.patientapp.v1.PatientHomeService/ListPatientHomes.
type listPatientHomesOption func(*patientappv1.ListPatientHomesRequest)

// newListPatientHomesRequest builds a request of acme.patientapp.v1.PatientHomeService/ListPatientHomes from options.
func newListPatientHomesRequest(options ...listPatientHomesOption) *patientappv1.ListPatientHomesRequest {
	request := &patientappv1.ListPatientHomesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withListPatientHomesContext(value *commonv1.RequestContext) listPatientHomesOption {
	return func(request *patientappv1.ListPatientHomesRequest) { request.Context = value }
}

func withListPatientHomesFilter(value *patientappv1.ListPatientHomesRequest_Filter) listPatientHomesOption {
	return func(request *patientappv1.ListPatientHomesRequest) { request.Filter = value }
}

func withListPatientHomesPage(value *commonv1.PageRequest) listPatientHomesOption {
	return func(request *patientappv1.ListPatientHomesRequest) { request.Page = value }
}

// inspectShopperViewWorkflowOption sets part of a request of
// acme.storefront.v1.ShopperViewWorkflowService/InspectShopperViewWorkflow.
type inspectShopperViewWorkflowOption func(*storefrontv1.InspectShopperViewWorkflowRequest)

// newInspectShopperViewWorkflowRequest builds a request of
// acme.storefront.v1.ShopperViewWorkflowService/InspectShopperViewWorkflow from options.
func newInspectShopperViewWorkflowRequest(options ...inspectShopperViewWorkflowOption) *storefrontv1.InspectShopperViewWorkflowRequest {
	request := &storefrontv1.InspectShopperViewWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withInspectShopperViewWorkflowContext(value *commonv1.RequestContext) inspectShopperViewWorkflowOption {
	return func(request *storefrontv1.InspectShopperViewWorkflowRequest) { request.Context = value }
}

func withInspectShopperViewWorkflowShopperViewId(value string) inspectShopperViewWorkflowOption {
	return func(request *storefrontv1.InspectShopperViewWorkflowRequest) { request.ShopperViewId = value }
}

// lintSearchQueriesOption sets part of a request of acme.search.v1.SearchQueryValidationService/LintSearchQueries.
type lintSearchQueriesOption func(*searchv1.LintSearchQueriesRequest)

// newLintSearchQueriesRequest builds a request of acme.search.v1.SearchQueryValidationService/LintSearchQueries from
// options.
func newLintSearchQueriesRequest(options ...lintSearchQueriesOption) *searchv1.LintSearchQueriesRequest {
	request := &searchv1.LintSearchQueriesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withLintSearchQueriesContext(value *commonv1.RequestContext) lintSearchQueriesOption {
	return func(request *searchv1.LintSearchQueriesRequest) { request.Context = value }
}

func withLintSearchQueriesDetails(value *searchv1.LintSearchQueriesRequest_Details) lintSearchQueriesOption {
	return func(request *searchv1.LintSearchQueriesRequest) { request.Details = value }
}

func withLintSearchQueriesSearchQuery(value *searchv1.SearchQuery) lintSearchQueriesOption {
	return func(request *searchv1.LintSearchQueriesRequest) { request.SearchQuery = value }
}
