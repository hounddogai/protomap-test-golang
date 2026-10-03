package search

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// validateContext rejects requests that do not say which tenant they act for.
func validateContext(rc *commonv1.RequestContext) error {
	if rc.GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if rc.GetActor().GetUserId() == "" && !rc.GetActor().GetServiceAccount() {
		return status.Error(codes.Unauthenticated, "context.actor is required")
	}
	return nil
}

// searchSearchIndexHistoryServiceName is the full name of acme.search.v1.SearchIndexHistoryService.
const searchSearchIndexHistoryServiceName = "acme.search.v1.SearchIndexHistoryService"

const searchSearchIndexHistoryServiceAnnotateSearchIndexVersionMethod = "/" + searchSearchIndexHistoryServiceName + "/AnnotateSearchIndexVersion"
