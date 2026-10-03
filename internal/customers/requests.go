package customers

import (
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	forecastingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forecasting/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
)

// createDemandForecastOption sets part of a request of acme.forecasting.v1.DemandForecastService/CreateDemandForecast.
type createDemandForecastOption func(*forecastingv1.CreateDemandForecastRequest)

// newCreateDemandForecastRequest builds a request of acme.forecasting.v1.DemandForecastService/CreateDemandForecast
// from options.
func newCreateDemandForecastRequest(options ...createDemandForecastOption) *forecastingv1.CreateDemandForecastRequest {
	request := &forecastingv1.CreateDemandForecastRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCreateDemandForecastContext(value *commonv1.RequestContext) createDemandForecastOption {
	return func(request *forecastingv1.CreateDemandForecastRequest) { request.Context = value }
}

func withCreateDemandForecastDemandForecast(value *forecastingv1.DemandForecast) createDemandForecastOption {
	return func(request *forecastingv1.CreateDemandForecastRequest) { request.DemandForecast = value }
}

// searchNotificationsOption sets part of a request of acme.notifications.v1.NotificationService/SearchNotifications.
type searchNotificationsOption func(*notificationsv1.SearchNotificationsRequest)

// newSearchNotificationsRequest builds a request of acme.notifications.v1.NotificationService/SearchNotifications from
// options.
func newSearchNotificationsRequest(options ...searchNotificationsOption) *notificationsv1.SearchNotificationsRequest {
	request := &notificationsv1.SearchNotificationsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSearchNotificationsContext(value *commonv1.RequestContext) searchNotificationsOption {
	return func(request *notificationsv1.SearchNotificationsRequest) { request.Context = value }
}

func withSearchNotificationsFilter(value *notificationsv1.SearchNotificationsRequest_Filter) searchNotificationsOption {
	return func(request *notificationsv1.SearchNotificationsRequest) { request.Filter = value }
}

// validateCustomerOption sets part of a request of acme.customers.v1.CustomerValidationService/ValidateCustomer.
type validateCustomerOption func(*customersv1.ValidateCustomerRequest)

// newValidateCustomerRequest builds a request of acme.customers.v1.CustomerValidationService/ValidateCustomer from
// options.
func newValidateCustomerRequest(options ...validateCustomerOption) *customersv1.ValidateCustomerRequest {
	request := &customersv1.ValidateCustomerRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withValidateCustomerContext(value *commonv1.RequestContext) validateCustomerOption {
	return func(request *customersv1.ValidateCustomerRequest) { request.Context = value }
}

func withValidateCustomerCustomer(value *customersv1.Customer) validateCustomerOption {
	return func(request *customersv1.ValidateCustomerRequest) { request.Customer = value }
}

// generateTrackingEventReportOption sets part of a request of
// acme.shipping.v1.TrackingEventReportingService/GenerateTrackingEventReport.
type generateTrackingEventReportOption func(*shippingv1.GenerateTrackingEventReportRequest)

// newGenerateTrackingEventReportRequest builds a request of
// acme.shipping.v1.TrackingEventReportingService/GenerateTrackingEventReport from options.
func newGenerateTrackingEventReportRequest(options ...generateTrackingEventReportOption) *shippingv1.GenerateTrackingEventReportRequest {
	request := &shippingv1.GenerateTrackingEventReportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withGenerateTrackingEventReportContext(value *commonv1.RequestContext) generateTrackingEventReportOption {
	return func(request *shippingv1.GenerateTrackingEventReportRequest) { request.Context = value }
}

func withGenerateTrackingEventReportAnnotations(value map[string]string) generateTrackingEventReportOption {
	return func(request *shippingv1.GenerateTrackingEventReportRequest) { request.Annotations = value }
}

func withGenerateTrackingEventReportReason(value string) generateTrackingEventReportOption {
	return func(request *shippingv1.GenerateTrackingEventReportRequest) { request.Reason = value }
}

func withGenerateTrackingEventReportDetails(value *shippingv1.GenerateTrackingEventReportRequest_Details) generateTrackingEventReportOption {
	return func(request *shippingv1.GenerateTrackingEventReportRequest) { request.Details = value }
}

// subscribeRoleUpdatesOption sets part of a request of acme.identity.v1.RoleNotificationService/SubscribeRoleUpdates.
type subscribeRoleUpdatesOption func(*identityv1.SubscribeRoleUpdatesRequest)

// newSubscribeRoleUpdatesRequest builds a request of acme.identity.v1.RoleNotificationService/SubscribeRoleUpdates from
// options.
func newSubscribeRoleUpdatesRequest(options ...subscribeRoleUpdatesOption) *identityv1.SubscribeRoleUpdatesRequest {
	request := &identityv1.SubscribeRoleUpdatesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSubscribeRoleUpdatesContext(value *commonv1.RequestContext) subscribeRoleUpdatesOption {
	return func(request *identityv1.SubscribeRoleUpdatesRequest) { request.Context = value }
}

func withSubscribeRoleUpdatesDetails(value *identityv1.SubscribeRoleUpdatesRequest_Details) subscribeRoleUpdatesOption {
	return func(request *identityv1.SubscribeRoleUpdatesRequest) { request.Details = value }
}

// quoteConsentRecordOption sets part of a request of
// acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord.
type quoteConsentRecordOption func(*customersv1.QuoteConsentRecordRequest)

// newQuoteConsentRecordRequest builds a request of acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord
// from options.
func newQuoteConsentRecordRequest(options ...quoteConsentRecordOption) *customersv1.QuoteConsentRecordRequest {
	request := &customersv1.QuoteConsentRecordRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withQuoteConsentRecordContext(value *commonv1.RequestContext) quoteConsentRecordOption {
	return func(request *customersv1.QuoteConsentRecordRequest) { request.Context = value }
}

func withQuoteConsentRecordDetails(value *customersv1.QuoteConsentRecordRequest_Details) quoteConsentRecordOption {
	return func(request *customersv1.QuoteConsentRecordRequest) { request.Details = value }
}

func withQuoteConsentRecordConsentRecord(value *customersv1.ConsentRecord) quoteConsentRecordOption {
	return func(request *customersv1.QuoteConsentRecordRequest) { request.ConsentRecord = value }
}

// browseCartsOption sets part of a request of acme.cart.v1.CartQueryService/BrowseCarts.
type browseCartsOption func(*cartv1.BrowseCartsRequest)

// newBrowseCartsRequest builds a request of acme.cart.v1.CartQueryService/BrowseCarts from options.
func newBrowseCartsRequest(options ...browseCartsOption) *cartv1.BrowseCartsRequest {
	request := &cartv1.BrowseCartsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseCartsContext(value *commonv1.RequestContext) browseCartsOption {
	return func(request *cartv1.BrowseCartsRequest) { request.Context = value }
}

func withBrowseCartsFilter(value *cartv1.BrowseCartsRequest_Filter) browseCartsOption {
	return func(request *cartv1.BrowseCartsRequest) { request.Filter = value }
}

// unsubscribeSearchQueryUpdatesOption sets part of a request of
// acme.search.v1.SearchQueryNotificationService/UnsubscribeSearchQueryUpdates.
type unsubscribeSearchQueryUpdatesOption func(*searchv1.UnsubscribeSearchQueryUpdatesRequest)

// newUnsubscribeSearchQueryUpdatesRequest builds a request of
// acme.search.v1.SearchQueryNotificationService/UnsubscribeSearchQueryUpdates from options.
func newUnsubscribeSearchQueryUpdatesRequest(options ...unsubscribeSearchQueryUpdatesOption) *searchv1.UnsubscribeSearchQueryUpdatesRequest {
	request := &searchv1.UnsubscribeSearchQueryUpdatesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withUnsubscribeSearchQueryUpdatesContext(value *commonv1.RequestContext) unsubscribeSearchQueryUpdatesOption {
	return func(request *searchv1.UnsubscribeSearchQueryUpdatesRequest) { request.Context = value }
}

func withUnsubscribeSearchQueryUpdatesDetails(value *searchv1.UnsubscribeSearchQueryUpdatesRequest_Details) unsubscribeSearchQueryUpdatesOption {
	return func(request *searchv1.UnsubscribeSearchQueryUpdatesRequest) { request.Details = value }
}

// revertSessionOption sets part of a request of acme.identity.v1.SessionHistoryService/RevertSession.
type revertSessionOption func(*identityv1.RevertSessionRequest)

// newRevertSessionRequest builds a request of acme.identity.v1.SessionHistoryService/RevertSession from options.
func newRevertSessionRequest(options ...revertSessionOption) *identityv1.RevertSessionRequest {
	request := &identityv1.RevertSessionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRevertSessionContext(value *commonv1.RequestContext) revertSessionOption {
	return func(request *identityv1.RevertSessionRequest) { request.Context = value }
}

func withRevertSessionSession(value *identityv1.Session) revertSessionOption {
	return func(request *identityv1.RevertSessionRequest) { request.Session = value }
}

func withRevertSessionReason(value string) revertSessionOption {
	return func(request *identityv1.RevertSessionRequest) { request.Reason = value }
}

func withRevertSessionDetails(value *identityv1.RevertSessionRequest_Details) revertSessionOption {
	return func(request *identityv1.RevertSessionRequest) { request.Details = value }
}

// getCustomerSegmentSyncStateOption sets part of a request of
// acme.customers.v1.CustomerSegmentSyncService/GetCustomerSegmentSyncState.
type getCustomerSegmentSyncStateOption func(*customersv1.GetCustomerSegmentSyncStateRequest)

// newGetCustomerSegmentSyncStateRequest builds a request of
// acme.customers.v1.CustomerSegmentSyncService/GetCustomerSegmentSyncState from options.
func newGetCustomerSegmentSyncStateRequest(options ...getCustomerSegmentSyncStateOption) *customersv1.GetCustomerSegmentSyncStateRequest {
	request := &customersv1.GetCustomerSegmentSyncStateRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withGetCustomerSegmentSyncStateContext(value *commonv1.RequestContext) getCustomerSegmentSyncStateOption {
	return func(request *customersv1.GetCustomerSegmentSyncStateRequest) { request.Context = value }
}

func withGetCustomerSegmentSyncStateCustomerSegmentId(value string) getCustomerSegmentSyncStateOption {
	return func(request *customersv1.GetCustomerSegmentSyncStateRequest) { request.CustomerSegmentId = value }
}

// listCustomerSegmentConflictsOption sets part of a request of
// acme.customers.v1.CustomerSegmentSyncService/ListCustomerSegmentConflicts.
type listCustomerSegmentConflictsOption func(*customersv1.ListCustomerSegmentConflictsRequest)

// newListCustomerSegmentConflictsRequest builds a request of
// acme.customers.v1.CustomerSegmentSyncService/ListCustomerSegmentConflicts from options.
func newListCustomerSegmentConflictsRequest(options ...listCustomerSegmentConflictsOption) *customersv1.ListCustomerSegmentConflictsRequest {
	request := &customersv1.ListCustomerSegmentConflictsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withListCustomerSegmentConflictsContext(value *commonv1.RequestContext) listCustomerSegmentConflictsOption {
	return func(request *customersv1.ListCustomerSegmentConflictsRequest) { request.Context = value }
}

func withListCustomerSegmentConflictsFilter(value *customersv1.ListCustomerSegmentConflictsRequest_Filter) listCustomerSegmentConflictsOption {
	return func(request *customersv1.ListCustomerSegmentConflictsRequest) { request.Filter = value }
}

// findVenuesOption sets part of a request of acme.venues.v1.VenueQueryService/FindVenues.
type findVenuesOption func(*venuesv1.FindVenuesRequest)

// newFindVenuesRequest builds a request of acme.venues.v1.VenueQueryService/FindVenues from options.
func newFindVenuesRequest(options ...findVenuesOption) *venuesv1.FindVenuesRequest {
	request := &venuesv1.FindVenuesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withFindVenuesContext(value *commonv1.RequestContext) findVenuesOption {
	return func(request *venuesv1.FindVenuesRequest) { request.Context = value }
}

func withFindVenuesFilter(value *venuesv1.FindVenuesRequest_Filter) findVenuesOption {
	return func(request *venuesv1.FindVenuesRequest) { request.Filter = value }
}

func withFindVenuesPage(value *commonv1.PageRequest) findVenuesOption {
	return func(request *venuesv1.FindVenuesRequest) { request.Page = value }
}

// peekUserOption sets part of a request of acme.identity.v1.UserQueryService/PeekUser.
type peekUserOption func(*identityv1.PeekUserRequest)

// newPeekUserRequest builds a request of acme.identity.v1.UserQueryService/PeekUser from options.
func newPeekUserRequest(options ...peekUserOption) *identityv1.PeekUserRequest {
	request := &identityv1.PeekUserRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withPeekUserContext(value *commonv1.RequestContext) peekUserOption {
	return func(request *identityv1.PeekUserRequest) { request.Context = value }
}

func withPeekUserUserId(value string) peekUserOption {
	return func(request *identityv1.PeekUserRequest) { request.UserId = value }
}

// normalizeConsentRecordOption sets part of a request of
// acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord.
type normalizeConsentRecordOption func(*customersv1.NormalizeConsentRecordRequest)

// newNormalizeConsentRecordRequest builds a request of
// acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord from options.
func newNormalizeConsentRecordRequest(options ...normalizeConsentRecordOption) *customersv1.NormalizeConsentRecordRequest {
	request := &customersv1.NormalizeConsentRecordRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withNormalizeConsentRecordContext(value *commonv1.RequestContext) normalizeConsentRecordOption {
	return func(request *customersv1.NormalizeConsentRecordRequest) { request.Context = value }
}

func withNormalizeConsentRecordConsentRecord(value *customersv1.ConsentRecord) normalizeConsentRecordOption {
	return func(request *customersv1.NormalizeConsentRecordRequest) { request.ConsentRecord = value }
}

func withNormalizeConsentRecordDetails(value *customersv1.NormalizeConsentRecordRequest_Details) normalizeConsentRecordOption {
	return func(request *customersv1.NormalizeConsentRecordRequest) { request.Details = value }
}
