package fulfillment

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
)

// previewPreferenceOption sets part of a request of acme.customers.v1.PreferenceValidationService/PreviewPreference.
type previewPreferenceOption func(*customersv1.PreviewPreferenceRequest)

// newPreviewPreferenceRequest builds a request of acme.customers.v1.PreferenceValidationService/PreviewPreference from
// options.
func newPreviewPreferenceRequest(options ...previewPreferenceOption) *customersv1.PreviewPreferenceRequest {
	request := &customersv1.PreviewPreferenceRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withPreviewPreferenceContext(value *commonv1.RequestContext) previewPreferenceOption {
	return func(request *customersv1.PreviewPreferenceRequest) { request.Context = value }
}

func withPreviewPreferencePreference(value *customersv1.Preference) previewPreferenceOption {
	return func(request *customersv1.PreviewPreferenceRequest) { request.Preference = value }
}

func withPreviewPreferenceDetails(value *customersv1.PreviewPreferenceRequest_Details) previewPreferenceOption {
	return func(request *customersv1.PreviewPreferenceRequest) { request.Details = value }
}

// approvePackingSlipOption sets part of a request of acme.fulfillment.v1.PackingSlipApprovalService/ApprovePackingSlip.
type approvePackingSlipOption func(*fulfillmentv1.ApprovePackingSlipRequest)

// newApprovePackingSlipRequest builds a request of acme.fulfillment.v1.PackingSlipApprovalService/ApprovePackingSlip
// from options.
func newApprovePackingSlipRequest(options ...approvePackingSlipOption) *fulfillmentv1.ApprovePackingSlipRequest {
	request := &fulfillmentv1.ApprovePackingSlipRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withApprovePackingSlipContext(value *commonv1.RequestContext) approvePackingSlipOption {
	return func(request *fulfillmentv1.ApprovePackingSlipRequest) { request.Context = value }
}

func withApprovePackingSlipPackingSlip(value *fulfillmentv1.PackingSlip) approvePackingSlipOption {
	return func(request *fulfillmentv1.ApprovePackingSlipRequest) { request.PackingSlip = value }
}

// calculateParcelOption sets part of a request of acme.shipping.v1.ParcelCalculationService/CalculateParcel.
type calculateParcelOption func(*shippingv1.CalculateParcelRequest)

// newCalculateParcelRequest builds a request of acme.shipping.v1.ParcelCalculationService/CalculateParcel from options.
func newCalculateParcelRequest(options ...calculateParcelOption) *shippingv1.CalculateParcelRequest {
	request := &shippingv1.CalculateParcelRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCalculateParcelContext(value *commonv1.RequestContext) calculateParcelOption {
	return func(request *shippingv1.CalculateParcelRequest) { request.Context = value }
}

func withCalculateParcelParcel(value *shippingv1.Parcel) calculateParcelOption {
	return func(request *shippingv1.CalculateParcelRequest) { request.Parcel = value }
}
