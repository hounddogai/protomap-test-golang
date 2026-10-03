package identity

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
)

// compareSearchIndexVersionsOption sets part of a request of
// acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions.
type compareSearchIndexVersionsOption func(*searchv1.CompareSearchIndexVersionsRequest)

// newCompareSearchIndexVersionsRequest builds a request of
// acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions from options.
func newCompareSearchIndexVersionsRequest(options ...compareSearchIndexVersionsOption) *searchv1.CompareSearchIndexVersionsRequest {
	request := &searchv1.CompareSearchIndexVersionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCompareSearchIndexVersionsContext(value *commonv1.RequestContext) compareSearchIndexVersionsOption {
	return func(request *searchv1.CompareSearchIndexVersionsRequest) { request.Context = value }
}

func withCompareSearchIndexVersionsSearchIndex(value *searchv1.SearchIndex) compareSearchIndexVersionsOption {
	return func(request *searchv1.CompareSearchIndexVersionsRequest) { request.SearchIndex = value }
}

func withCompareSearchIndexVersionsDetails(value *searchv1.CompareSearchIndexVersionsRequest_Details) compareSearchIndexVersionsOption {
	return func(request *searchv1.CompareSearchIndexVersionsRequest) { request.Details = value }
}

// browseFeatureFlagsOption sets part of a request of acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags.
type browseFeatureFlagsOption func(*featureflagsv1.BrowseFeatureFlagsRequest)

// newBrowseFeatureFlagsRequest builds a request of acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags from
// options.
func newBrowseFeatureFlagsRequest(options ...browseFeatureFlagsOption) *featureflagsv1.BrowseFeatureFlagsRequest {
	request := &featureflagsv1.BrowseFeatureFlagsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseFeatureFlagsContext(value *commonv1.RequestContext) browseFeatureFlagsOption {
	return func(request *featureflagsv1.BrowseFeatureFlagsRequest) { request.Context = value }
}

func withBrowseFeatureFlagsFilter(value *featureflagsv1.BrowseFeatureFlagsRequest_Filter) browseFeatureFlagsOption {
	return func(request *featureflagsv1.BrowseFeatureFlagsRequest) { request.Filter = value }
}
