package returns

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
)

// estimateReturnLabelOption sets part of a request of
// acme.returns.v1.ReturnLabelCalculationService/EstimateReturnLabel.
type estimateReturnLabelOption func(*returnsv1.EstimateReturnLabelRequest)

// newEstimateReturnLabelRequest builds a request of acme.returns.v1.ReturnLabelCalculationService/EstimateReturnLabel
// from options.
func newEstimateReturnLabelRequest(options ...estimateReturnLabelOption) *returnsv1.EstimateReturnLabelRequest {
	request := &returnsv1.EstimateReturnLabelRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withEstimateReturnLabelContext(value *commonv1.RequestContext) estimateReturnLabelOption {
	return func(request *returnsv1.EstimateReturnLabelRequest) { request.Context = value }
}

func withEstimateReturnLabelReturnLabel(value *returnsv1.ReturnLabel) estimateReturnLabelOption {
	return func(request *returnsv1.EstimateReturnLabelRequest) { request.ReturnLabel = value }
}

// countHouseholdsOption sets part of a request of acme.customers.v1.HouseholdQueryService/CountHouseholds.
type countHouseholdsOption func(*customersv1.CountHouseholdsRequest)

// newCountHouseholdsRequest builds a request of acme.customers.v1.HouseholdQueryService/CountHouseholds from options.
func newCountHouseholdsRequest(options ...countHouseholdsOption) *customersv1.CountHouseholdsRequest {
	request := &customersv1.CountHouseholdsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCountHouseholdsContext(value *commonv1.RequestContext) countHouseholdsOption {
	return func(request *customersv1.CountHouseholdsRequest) { request.Context = value }
}

func withCountHouseholdsFilter(value *customersv1.CountHouseholdsRequest_Filter) countHouseholdsOption {
	return func(request *customersv1.CountHouseholdsRequest) { request.Filter = value }
}

// calculateCustomerSegmentOption sets part of a request of
// acme.customers.v1.CustomerSegmentCalculationService/CalculateCustomerSegment.
type calculateCustomerSegmentOption func(*customersv1.CalculateCustomerSegmentRequest)

// newCalculateCustomerSegmentRequest builds a request of
// acme.customers.v1.CustomerSegmentCalculationService/CalculateCustomerSegment from options.
func newCalculateCustomerSegmentRequest(options ...calculateCustomerSegmentOption) *customersv1.CalculateCustomerSegmentRequest {
	request := &customersv1.CalculateCustomerSegmentRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCalculateCustomerSegmentContext(value *commonv1.RequestContext) calculateCustomerSegmentOption {
	return func(request *customersv1.CalculateCustomerSegmentRequest) { request.Context = value }
}

func withCalculateCustomerSegmentCustomerSegment(value *customersv1.CustomerSegment) calculateCustomerSegmentOption {
	return func(request *customersv1.CalculateCustomerSegmentRequest) { request.CustomerSegment = value }
}

// compareSellerPayoutScheduleOptionsOption sets part of a request of
// acme.sellers.v1.SellerPayoutScheduleCalculationService/CompareSellerPayoutScheduleOptions.
type compareSellerPayoutScheduleOptionsOption func(*sellersv1.CompareSellerPayoutScheduleOptionsRequest)

// newCompareSellerPayoutScheduleOptionsRequest builds a request of
// acme.sellers.v1.SellerPayoutScheduleCalculationService/CompareSellerPayoutScheduleOptions from options.
func newCompareSellerPayoutScheduleOptionsRequest(options ...compareSellerPayoutScheduleOptionsOption) *sellersv1.CompareSellerPayoutScheduleOptionsRequest {
	request := &sellersv1.CompareSellerPayoutScheduleOptionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCompareSellerPayoutScheduleOptionsContext(value *commonv1.RequestContext) compareSellerPayoutScheduleOptionsOption {
	return func(request *sellersv1.CompareSellerPayoutScheduleOptionsRequest) { request.Context = value }
}

func withCompareSellerPayoutScheduleOptionsSellerPayoutSchedule(value *sellersv1.SellerPayoutSchedule) compareSellerPayoutScheduleOptionsOption {
	return func(request *sellersv1.CompareSellerPayoutScheduleOptionsRequest) {
		request.SellerPayoutSchedule = value
	}
}

// recalculateReturnLabelsOption sets part of a request of
// acme.returns.v1.ReturnLabelCalculationService/RecalculateReturnLabels.
type recalculateReturnLabelsOption func(*returnsv1.RecalculateReturnLabelsRequest)

// newRecalculateReturnLabelsRequest builds a request of
// acme.returns.v1.ReturnLabelCalculationService/RecalculateReturnLabels from options.
func newRecalculateReturnLabelsRequest(options ...recalculateReturnLabelsOption) *returnsv1.RecalculateReturnLabelsRequest {
	request := &returnsv1.RecalculateReturnLabelsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRecalculateReturnLabelsContext(value *commonv1.RequestContext) recalculateReturnLabelsOption {
	return func(request *returnsv1.RecalculateReturnLabelsRequest) { request.Context = value }
}

func withRecalculateReturnLabelsReturnLabel(value *returnsv1.ReturnLabel) recalculateReturnLabelsOption {
	return func(request *returnsv1.RecalculateReturnLabelsRequest) { request.ReturnLabel = value }
}

// browseCustomersOption sets part of a request of acme.customers.v1.CustomerQueryService/BrowseCustomers.
type browseCustomersOption func(*customersv1.BrowseCustomersRequest)

// newBrowseCustomersRequest builds a request of acme.customers.v1.CustomerQueryService/BrowseCustomers from options.
func newBrowseCustomersRequest(options ...browseCustomersOption) *customersv1.BrowseCustomersRequest {
	request := &customersv1.BrowseCustomersRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseCustomersContext(value *commonv1.RequestContext) browseCustomersOption {
	return func(request *customersv1.BrowseCustomersRequest) { request.Context = value }
}

func withBrowseCustomersFilter(value *customersv1.BrowseCustomersRequest_Filter) browseCustomersOption {
	return func(request *customersv1.BrowseCustomersRequest) { request.Filter = value }
}

// simulateCustomerOption sets part of a request of acme.customers.v1.CustomerCalculationService/SimulateCustomer.
type simulateCustomerOption func(*customersv1.SimulateCustomerRequest)

// newSimulateCustomerRequest builds a request of acme.customers.v1.CustomerCalculationService/SimulateCustomer from
// options.
func newSimulateCustomerRequest(options ...simulateCustomerOption) *customersv1.SimulateCustomerRequest {
	request := &customersv1.SimulateCustomerRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSimulateCustomerContext(value *commonv1.RequestContext) simulateCustomerOption {
	return func(request *customersv1.SimulateCustomerRequest) { request.Context = value }
}

func withSimulateCustomerCustomer(value *customersv1.Customer) simulateCustomerOption {
	return func(request *customersv1.SimulateCustomerRequest) { request.Customer = value }
}

// estimateCustomerOption sets part of a request of acme.customers.v1.CustomerCalculationService/EstimateCustomer.
type estimateCustomerOption func(*customersv1.EstimateCustomerRequest)

// newEstimateCustomerRequest builds a request of acme.customers.v1.CustomerCalculationService/EstimateCustomer from
// options.
func newEstimateCustomerRequest(options ...estimateCustomerOption) *customersv1.EstimateCustomerRequest {
	request := &customersv1.EstimateCustomerRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withEstimateCustomerContext(value *commonv1.RequestContext) estimateCustomerOption {
	return func(request *customersv1.EstimateCustomerRequest) { request.Context = value }
}

func withEstimateCustomerCustomer(value *customersv1.Customer) estimateCustomerOption {
	return func(request *customersv1.EstimateCustomerRequest) { request.Customer = value }
}

func withEstimateCustomerDetails(value *customersv1.EstimateCustomerRequest_Details) estimateCustomerOption {
	return func(request *customersv1.EstimateCustomerRequest) { request.Details = value }
}

// calculateReturnLabelOption sets part of a request of
// acme.returns.v1.ReturnLabelCalculationService/CalculateReturnLabel.
type calculateReturnLabelOption func(*returnsv1.CalculateReturnLabelRequest)

// newCalculateReturnLabelRequest builds a request of acme.returns.v1.ReturnLabelCalculationService/CalculateReturnLabel
// from options.
func newCalculateReturnLabelRequest(options ...calculateReturnLabelOption) *returnsv1.CalculateReturnLabelRequest {
	request := &returnsv1.CalculateReturnLabelRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCalculateReturnLabelContext(value *commonv1.RequestContext) calculateReturnLabelOption {
	return func(request *returnsv1.CalculateReturnLabelRequest) { request.Context = value }
}

func withCalculateReturnLabelReturnLabel(value *returnsv1.ReturnLabel) calculateReturnLabelOption {
	return func(request *returnsv1.CalculateReturnLabelRequest) { request.ReturnLabel = value }
}

func withCalculateReturnLabelDetails(value *returnsv1.CalculateReturnLabelRequest_Details) calculateReturnLabelOption {
	return func(request *returnsv1.CalculateReturnLabelRequest) { request.Details = value }
}

// recalculateReturnRequestsOption sets part of a request of
// acme.returns.v1.ReturnRequestCalculationService/RecalculateReturnRequests.
type recalculateReturnRequestsOption func(*returnsv1.RecalculateReturnRequestsRequest)

// newRecalculateReturnRequestsRequest builds a request of
// acme.returns.v1.ReturnRequestCalculationService/RecalculateReturnRequests from options.
func newRecalculateReturnRequestsRequest(options ...recalculateReturnRequestsOption) *returnsv1.RecalculateReturnRequestsRequest {
	request := &returnsv1.RecalculateReturnRequestsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRecalculateReturnRequestsContext(value *commonv1.RequestContext) recalculateReturnRequestsOption {
	return func(request *returnsv1.RecalculateReturnRequestsRequest) { request.Context = value }
}

func withRecalculateReturnRequestsReturnRequest(value *returnsv1.ReturnRequest) recalculateReturnRequestsOption {
	return func(request *returnsv1.RecalculateReturnRequestsRequest) { request.ReturnRequest = value }
}

func withRecalculateReturnRequestsDetails(value *returnsv1.RecalculateReturnRequestsRequest_Details) recalculateReturnRequestsOption {
	return func(request *returnsv1.RecalculateReturnRequestsRequest) { request.Details = value }
}
