package search

import (
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
)

// meteredSearchIndexNotification records how long each call to acme.search.v1.SearchIndexNotificationService takes.
type meteredSearchIndexNotification struct {
	searchv1.SearchIndexNotificationServiceClient
	recorder *metrics.Recorder
}
