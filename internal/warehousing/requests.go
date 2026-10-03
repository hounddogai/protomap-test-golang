package warehousing

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
)

// explainFulfillmentExceptionDecisionOption sets part of a request of
// acme.fulfillment.v1.FulfillmentExceptionValidationService/ExplainFulfillmentExceptionDecision.
type explainFulfillmentExceptionDecisionOption func(*fulfillmentv1.ExplainFulfillmentExceptionDecisionRequest)

// newExplainFulfillmentExceptionDecisionRequest builds a request of
// acme.fulfillment.v1.FulfillmentExceptionValidationService/ExplainFulfillmentExceptionDecision from options.
func newExplainFulfillmentExceptionDecisionRequest(options ...explainFulfillmentExceptionDecisionOption) *fulfillmentv1.ExplainFulfillmentExceptionDecisionRequest {
	request := &fulfillmentv1.ExplainFulfillmentExceptionDecisionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainFulfillmentExceptionDecisionContext(value *commonv1.RequestContext) explainFulfillmentExceptionDecisionOption {
	return func(request *fulfillmentv1.ExplainFulfillmentExceptionDecisionRequest) { request.Context = value }
}

func withExplainFulfillmentExceptionDecisionFulfillmentException(value *fulfillmentv1.FulfillmentException) explainFulfillmentExceptionDecisionOption {
	return func(request *fulfillmentv1.ExplainFulfillmentExceptionDecisionRequest) {
		request.FulfillmentException = value
	}
}

// peekWarehouseOption sets part of a request of acme.warehousing.v1.WarehouseQueryService/PeekWarehouse.
type peekWarehouseOption func(*warehousingv1.PeekWarehouseRequest)

// newPeekWarehouseRequest builds a request of acme.warehousing.v1.WarehouseQueryService/PeekWarehouse from options.
func newPeekWarehouseRequest(options ...peekWarehouseOption) *warehousingv1.PeekWarehouseRequest {
	request := &warehousingv1.PeekWarehouseRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withPeekWarehouseContext(value *commonv1.RequestContext) peekWarehouseOption {
	return func(request *warehousingv1.PeekWarehouseRequest) { request.Context = value }
}

func withPeekWarehouseWarehouseId(value string) peekWarehouseOption {
	return func(request *warehousingv1.PeekWarehouseRequest) { request.WarehouseId = value }
}

// summarizeWarehousesOption sets part of a request of acme.warehousing.v1.WarehouseQueryService/SummarizeWarehouses.
type summarizeWarehousesOption func(*warehousingv1.SummarizeWarehousesRequest)

// newSummarizeWarehousesRequest builds a request of acme.warehousing.v1.WarehouseQueryService/SummarizeWarehouses from
// options.
func newSummarizeWarehousesRequest(options ...summarizeWarehousesOption) *warehousingv1.SummarizeWarehousesRequest {
	request := &warehousingv1.SummarizeWarehousesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSummarizeWarehousesContext(value *commonv1.RequestContext) summarizeWarehousesOption {
	return func(request *warehousingv1.SummarizeWarehousesRequest) { request.Context = value }
}

func withSummarizeWarehousesDetails(value *warehousingv1.SummarizeWarehousesRequest_Details) summarizeWarehousesOption {
	return func(request *warehousingv1.SummarizeWarehousesRequest) { request.Details = value }
}

// checkHouseholdEligibilityOption sets part of a request of
// acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility.
type checkHouseholdEligibilityOption func(*customersv1.CheckHouseholdEligibilityRequest)

// newCheckHouseholdEligibilityRequest builds a request of
// acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility from options.
func newCheckHouseholdEligibilityRequest(options ...checkHouseholdEligibilityOption) *customersv1.CheckHouseholdEligibilityRequest {
	request := &customersv1.CheckHouseholdEligibilityRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCheckHouseholdEligibilityContext(value *commonv1.RequestContext) checkHouseholdEligibilityOption {
	return func(request *customersv1.CheckHouseholdEligibilityRequest) { request.Context = value }
}

func withCheckHouseholdEligibilityHousehold(value *customersv1.Household) checkHouseholdEligibilityOption {
	return func(request *customersv1.CheckHouseholdEligibilityRequest) { request.Household = value }
}

// batchGetWarehousesOption sets part of a request of acme.warehousing.v1.WarehouseService/BatchGetWarehouses.
type batchGetWarehousesOption func(*warehousingv1.BatchGetWarehousesRequest)

// newBatchGetWarehousesRequest builds a request of acme.warehousing.v1.WarehouseService/BatchGetWarehouses from
// options.
func newBatchGetWarehousesRequest(options ...batchGetWarehousesOption) *warehousingv1.BatchGetWarehousesRequest {
	request := &warehousingv1.BatchGetWarehousesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBatchGetWarehousesContext(value *commonv1.RequestContext) batchGetWarehousesOption {
	return func(request *warehousingv1.BatchGetWarehousesRequest) { request.Context = value }
}

// rejectTrackingEventOption sets part of a request of
// acme.shipping.v1.TrackingEventApprovalService/RejectTrackingEvent.
type rejectTrackingEventOption func(*shippingv1.RejectTrackingEventRequest)

// newRejectTrackingEventRequest builds a request of acme.shipping.v1.TrackingEventApprovalService/RejectTrackingEvent
// from options.
func newRejectTrackingEventRequest(options ...rejectTrackingEventOption) *shippingv1.RejectTrackingEventRequest {
	request := &shippingv1.RejectTrackingEventRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRejectTrackingEventContext(value *commonv1.RequestContext) rejectTrackingEventOption {
	return func(request *shippingv1.RejectTrackingEventRequest) { request.Context = value }
}

func withRejectTrackingEventDetails(value *shippingv1.RejectTrackingEventRequest_Details) rejectTrackingEventOption {
	return func(request *shippingv1.RejectTrackingEventRequest) { request.Details = value }
}
