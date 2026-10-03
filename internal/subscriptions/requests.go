package subscriptions

import (
	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
)

// cloneVenueOption sets part of a request of acme.venues.v1.VenueService/CloneVenue.
type cloneVenueOption func(*venuesv1.CloneVenueRequest)

// newCloneVenueRequest builds a request of acme.venues.v1.VenueService/CloneVenue from options.
func newCloneVenueRequest(options ...cloneVenueOption) *venuesv1.CloneVenueRequest {
	request := &venuesv1.CloneVenueRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCloneVenueContext(value *commonv1.RequestContext) cloneVenueOption {
	return func(request *venuesv1.CloneVenueRequest) { request.Context = value }
}

func withCloneVenueVenue(value *venuesv1.Venue) cloneVenueOption {
	return func(request *venuesv1.CloneVenueRequest) { request.Venue = value }
}

// resumeSubscriptionPlanWorkflowOption sets part of a request of
// acme.subscriptions.v1.SubscriptionPlanWorkflowService/ResumeSubscriptionPlanWorkflow.
type resumeSubscriptionPlanWorkflowOption func(*subscriptionsv1.ResumeSubscriptionPlanWorkflowRequest)

// newResumeSubscriptionPlanWorkflowRequest builds a request of
// acme.subscriptions.v1.SubscriptionPlanWorkflowService/ResumeSubscriptionPlanWorkflow from options.
func newResumeSubscriptionPlanWorkflowRequest(options ...resumeSubscriptionPlanWorkflowOption) *subscriptionsv1.ResumeSubscriptionPlanWorkflowRequest {
	request := &subscriptionsv1.ResumeSubscriptionPlanWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withResumeSubscriptionPlanWorkflowContext(value *commonv1.RequestContext) resumeSubscriptionPlanWorkflowOption {
	return func(request *subscriptionsv1.ResumeSubscriptionPlanWorkflowRequest) { request.Context = value }
}

func withResumeSubscriptionPlanWorkflowSubscriptionPlan(value *subscriptionsv1.SubscriptionPlan) resumeSubscriptionPlanWorkflowOption {
	return func(request *subscriptionsv1.ResumeSubscriptionPlanWorkflowRequest) { request.SubscriptionPlan = value }
}

func withResumeSubscriptionPlanWorkflowDetails(value *subscriptionsv1.ResumeSubscriptionPlanWorkflowRequest_Details) resumeSubscriptionPlanWorkflowOption {
	return func(request *subscriptionsv1.ResumeSubscriptionPlanWorkflowRequest) { request.Details = value }
}

// suggestSubscriptionsOption sets part of a request of
// acme.subscriptions.v1.SubscriptionQueryService/SuggestSubscriptions.
type suggestSubscriptionsOption func(*subscriptionsv1.SuggestSubscriptionsRequest)

// newSuggestSubscriptionsRequest builds a request of
// acme.subscriptions.v1.SubscriptionQueryService/SuggestSubscriptions from options.
func newSuggestSubscriptionsRequest(options ...suggestSubscriptionsOption) *subscriptionsv1.SuggestSubscriptionsRequest {
	request := &subscriptionsv1.SuggestSubscriptionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSuggestSubscriptionsContext(value *commonv1.RequestContext) suggestSubscriptionsOption {
	return func(request *subscriptionsv1.SuggestSubscriptionsRequest) { request.Context = value }
}

func withSuggestSubscriptionsFilter(value *subscriptionsv1.SuggestSubscriptionsRequest_Filter) suggestSubscriptionsOption {
	return func(request *subscriptionsv1.SuggestSubscriptionsRequest) { request.Filter = value }
}

// batchGetAppointmentsOption sets part of a request of acme.appointments.v1.AppointmentService/BatchGetAppointments.
type batchGetAppointmentsOption func(*appointmentsv1.BatchGetAppointmentsRequest)

// newBatchGetAppointmentsRequest builds a request of acme.appointments.v1.AppointmentService/BatchGetAppointments from
// options.
func newBatchGetAppointmentsRequest(options ...batchGetAppointmentsOption) *appointmentsv1.BatchGetAppointmentsRequest {
	request := &appointmentsv1.BatchGetAppointmentsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBatchGetAppointmentsContext(value *commonv1.RequestContext) batchGetAppointmentsOption {
	return func(request *appointmentsv1.BatchGetAppointmentsRequest) { request.Context = value }
}

// updateSubscriptionOption sets part of a request of acme.subscriptions.v1.SubscriptionService/UpdateSubscription.
type updateSubscriptionOption func(*subscriptionsv1.UpdateSubscriptionRequest)

// newUpdateSubscriptionRequest builds a request of acme.subscriptions.v1.SubscriptionService/UpdateSubscription from
// options.
func newUpdateSubscriptionRequest(options ...updateSubscriptionOption) *subscriptionsv1.UpdateSubscriptionRequest {
	request := &subscriptionsv1.UpdateSubscriptionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withUpdateSubscriptionContext(value *commonv1.RequestContext) updateSubscriptionOption {
	return func(request *subscriptionsv1.UpdateSubscriptionRequest) { request.Context = value }
}

func withUpdateSubscriptionSubscription(value *subscriptionsv1.Subscription) updateSubscriptionOption {
	return func(request *subscriptionsv1.UpdateSubscriptionRequest) { request.Subscription = value }
}
