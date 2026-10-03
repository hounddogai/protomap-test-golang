package search

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
)

// unsubscribeSearchIndexUpdatesOption sets part of a request of
// acme.search.v1.SearchIndexNotificationService/UnsubscribeSearchIndexUpdates.
type unsubscribeSearchIndexUpdatesOption func(*searchv1.UnsubscribeSearchIndexUpdatesRequest)

// newUnsubscribeSearchIndexUpdatesRequest builds a request of
// acme.search.v1.SearchIndexNotificationService/UnsubscribeSearchIndexUpdates from options.
func newUnsubscribeSearchIndexUpdatesRequest(options ...unsubscribeSearchIndexUpdatesOption) *searchv1.UnsubscribeSearchIndexUpdatesRequest {
	request := &searchv1.UnsubscribeSearchIndexUpdatesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withUnsubscribeSearchIndexUpdatesContext(value *commonv1.RequestContext) unsubscribeSearchIndexUpdatesOption {
	return func(request *searchv1.UnsubscribeSearchIndexUpdatesRequest) { request.Context = value }
}

func withUnsubscribeSearchIndexUpdatesDetails(value *searchv1.UnsubscribeSearchIndexUpdatesRequest_Details) unsubscribeSearchIndexUpdatesOption {
	return func(request *searchv1.UnsubscribeSearchIndexUpdatesRequest) { request.Details = value }
}

func withUnsubscribeSearchIndexUpdatesSearchIndexId(value string) unsubscribeSearchIndexUpdatesOption {
	return func(request *searchv1.UnsubscribeSearchIndexUpdatesRequest) { request.SearchIndexId = value }
}

// scheduleSearchIndexExportOption sets part of a request of
// acme.search.v1.SearchIndexExportService/ScheduleSearchIndexExport.
type scheduleSearchIndexExportOption func(*searchv1.ScheduleSearchIndexExportRequest)

// newScheduleSearchIndexExportRequest builds a request of
// acme.search.v1.SearchIndexExportService/ScheduleSearchIndexExport from options.
func newScheduleSearchIndexExportRequest(options ...scheduleSearchIndexExportOption) *searchv1.ScheduleSearchIndexExportRequest {
	request := &searchv1.ScheduleSearchIndexExportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withScheduleSearchIndexExportContext(value *commonv1.RequestContext) scheduleSearchIndexExportOption {
	return func(request *searchv1.ScheduleSearchIndexExportRequest) { request.Context = value }
}

func withScheduleSearchIndexExportAnnotations(value map[string]string) scheduleSearchIndexExportOption {
	return func(request *searchv1.ScheduleSearchIndexExportRequest) { request.Annotations = value }
}
