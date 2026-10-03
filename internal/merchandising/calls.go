package merchandising

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

// giftcardsGiftCardBatchQueryServiceName is the full name of acme.giftcards.v1.GiftCardBatchQueryService.
const giftcardsGiftCardBatchQueryServiceName = "acme.giftcards.v1.GiftCardBatchQueryService"

// merchandisingPlacementWorkflowServiceName is the full name of acme.merchandising.v1.PlacementWorkflowService.
const merchandisingPlacementWorkflowServiceName = "acme.merchandising.v1.PlacementWorkflowService"

const merchandisingPlacementWorkflowServiceStartPlacementWorkflowMethod = "/" + merchandisingPlacementWorkflowServiceName + "/StartPlacementWorkflow"

// merchandisingCollectionServiceName is the full name of acme.merchandising.v1.CollectionService.
const merchandisingCollectionServiceName = "acme.merchandising.v1.CollectionService"

// merchandisingMerchandisingRuleValidationServiceName is the full name of
// acme.merchandising.v1.MerchandisingRuleValidationService.
const merchandisingMerchandisingRuleValidationServiceName = "acme.merchandising.v1.MerchandisingRuleValidationService"

// identitySessionHistoryServiceName is the full name of acme.identity.v1.SessionHistoryService.
const identitySessionHistoryServiceName = "acme.identity.v1.SessionHistoryService"

const identitySessionHistoryServiceListSessionVersionsMethod = "/" + identitySessionHistoryServiceName + "/ListSessionVersions"
