package partnerapi

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

// partnerapiPartnerWebhookReportingServiceName is the full name of acme.partnerapi.v1.PartnerWebhookReportingService.
const partnerapiPartnerWebhookReportingServiceName = "acme.partnerapi.v1.PartnerWebhookReportingService"

// searchSynonymValidationServiceName is the full name of acme.search.v1.SynonymValidationService.
const searchSynonymValidationServiceName = "acme.search.v1.SynonymValidationService"

// giftcardsGiftCardBatchApprovalServiceName is the full name of acme.giftcards.v1.GiftCardBatchApprovalService.
const giftcardsGiftCardBatchApprovalServiceName = "acme.giftcards.v1.GiftCardBatchApprovalService"

// identitySessionValidationServiceName is the full name of acme.identity.v1.SessionValidationService.
const identitySessionValidationServiceName = "acme.identity.v1.SessionValidationService"

const identitySessionValidationServiceExplainSessionDecisionMethod = "/" + identitySessionValidationServiceName + "/ExplainSessionDecision"

// identityApiCredentialValidationServiceName is the full name of acme.identity.v1.ApiCredentialValidationService.
const identityApiCredentialValidationServiceName = "acme.identity.v1.ApiCredentialValidationService"

const identityApiCredentialValidationServiceCheckApiCredentialEligibilityMethod = "/" + identityApiCredentialValidationServiceName + "/CheckApiCredentialEligibility"

// cartCartRuleWorkflowServiceName is the full name of acme.cart.v1.CartRuleWorkflowService.
const cartCartRuleWorkflowServiceName = "acme.cart.v1.CartRuleWorkflowService"

const cartCartRuleWorkflowServiceSignalCartRuleWorkflowMethod = "/" + cartCartRuleWorkflowServiceName + "/SignalCartRuleWorkflow"

// sellersSellerListingWorkflowServiceName is the full name of acme.sellers.v1.SellerListingWorkflowService.
const sellersSellerListingWorkflowServiceName = "acme.sellers.v1.SellerListingWorkflowService"

const sellersSellerListingWorkflowServicePauseSellerListingWorkflowMethod = "/" + sellersSellerListingWorkflowServiceName + "/PauseSellerListingWorkflow"

// partnerapiPartnerOrderApprovalServiceName is the full name of acme.partnerapi.v1.PartnerOrderApprovalService.
const partnerapiPartnerOrderApprovalServiceName = "acme.partnerapi.v1.PartnerOrderApprovalService"

const partnerapiPartnerOrderApprovalServiceCommentPartnerOrderApprovalMethod = "/" + partnerapiPartnerOrderApprovalServiceName + "/CommentPartnerOrderApproval"

// contractsContractServiceName is the full name of acme.contracts.v1.ContractService.
const contractsContractServiceName = "acme.contracts.v1.ContractService"

const contractsContractServiceUpdateContractMethod = "/" + contractsContractServiceName + "/UpdateContract"

// sellersSellerPayoutScheduleApprovalServiceName is the full name of
// acme.sellers.v1.SellerPayoutScheduleApprovalService.
const sellersSellerPayoutScheduleApprovalServiceName = "acme.sellers.v1.SellerPayoutScheduleApprovalService"

// searchSynonymSyncServiceName is the full name of acme.search.v1.SynonymSyncService.
const searchSynonymSyncServiceName = "acme.search.v1.SynonymSyncService"

const searchSynonymSyncServiceResolveSynonymConflictMethod = "/" + searchSynonymSyncServiceName + "/ResolveSynonymConflict"

// partnerapiPartnerIntegrationWorkflowServiceName is the full name of
// acme.partnerapi.v1.PartnerIntegrationWorkflowService.
const partnerapiPartnerIntegrationWorkflowServiceName = "acme.partnerapi.v1.PartnerIntegrationWorkflowService"

// partnerapiPartnerWebhookValidationServiceName is the full name of acme.partnerapi.v1.PartnerWebhookValidationService.
const partnerapiPartnerWebhookValidationServiceName = "acme.partnerapi.v1.PartnerWebhookValidationService"

const partnerapiPartnerWebhookValidationServicePreviewPartnerWebhookMethod = "/" + partnerapiPartnerWebhookValidationServiceName + "/PreviewPartnerWebhook"

const searchSynonymValidationServiceScoreSynonymMethod = "/" + searchSynonymValidationServiceName + "/ScoreSynonym"

// authorizationAccessPolicyServiceName is the full name of acme.authorization.v1.AccessPolicyService.
const authorizationAccessPolicyServiceName = "acme.authorization.v1.AccessPolicyService"

const authorizationAccessPolicyServiceBatchGetAccessPoliciesMethod = "/" + authorizationAccessPolicyServiceName + "/BatchGetAccessPolicies"
