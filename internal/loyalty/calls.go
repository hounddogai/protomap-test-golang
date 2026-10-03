package loyalty

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

// loyaltyLoyaltyMemberQueryServiceName is the full name of acme.loyalty.v1.LoyaltyMemberQueryService.
const loyaltyLoyaltyMemberQueryServiceName = "acme.loyalty.v1.LoyaltyMemberQueryService"

// sellersSellerPayoutScheduleValidationServiceName is the full name of
// acme.sellers.v1.SellerPayoutScheduleValidationService.
const sellersSellerPayoutScheduleValidationServiceName = "acme.sellers.v1.SellerPayoutScheduleValidationService"

const sellersSellerPayoutScheduleValidationServicePreviewSellerPayoutScheduleMethod = "/" + sellersSellerPayoutScheduleValidationServiceName + "/PreviewSellerPayoutSchedule"

// sellersSellerListingQueryServiceName is the full name of acme.sellers.v1.SellerListingQueryService.
const sellersSellerListingQueryServiceName = "acme.sellers.v1.SellerListingQueryService"

// localizationTranslationKeyQueryServiceName is the full name of acme.localization.v1.TranslationKeyQueryService.
const localizationTranslationKeyQueryServiceName = "acme.localization.v1.TranslationKeyQueryService"

const localizationTranslationKeyQueryServiceBrowseTranslationKeysMethod = "/" + localizationTranslationKeyQueryServiceName + "/BrowseTranslationKeys"

// giftcardsGiftCardBatchWorkflowServiceName is the full name of acme.giftcards.v1.GiftCardBatchWorkflowService.
const giftcardsGiftCardBatchWorkflowServiceName = "acme.giftcards.v1.GiftCardBatchWorkflowService"

// loyaltyLoyaltyMemberApprovalServiceName is the full name of acme.loyalty.v1.LoyaltyMemberApprovalService.
const loyaltyLoyaltyMemberApprovalServiceName = "acme.loyalty.v1.LoyaltyMemberApprovalService"

const loyaltyLoyaltyMemberApprovalServiceRejectLoyaltyMemberMethod = "/" + loyaltyLoyaltyMemberApprovalServiceName + "/RejectLoyaltyMember"

// loyaltyRewardApprovalServiceName is the full name of acme.loyalty.v1.RewardApprovalService.
const loyaltyRewardApprovalServiceName = "acme.loyalty.v1.RewardApprovalService"

const loyaltyRewardApprovalServiceRecallRewardMethod = "/" + loyaltyRewardApprovalServiceName + "/RecallReward"

// notificationsNotificationValidationServiceName is the full name of
// acme.notifications.v1.NotificationValidationService.
const notificationsNotificationValidationServiceName = "acme.notifications.v1.NotificationValidationService"

// loyaltyRewardCalculationServiceName is the full name of acme.loyalty.v1.RewardCalculationService.
const loyaltyRewardCalculationServiceName = "acme.loyalty.v1.RewardCalculationService"

const loyaltyRewardCalculationServiceEstimateRewardMethod = "/" + loyaltyRewardCalculationServiceName + "/EstimateReward"

// loyaltyPointsLedgerQueryServiceName is the full name of acme.loyalty.v1.PointsLedgerQueryService.
const loyaltyPointsLedgerQueryServiceName = "acme.loyalty.v1.PointsLedgerQueryService"

// ledgerLedgerAccountValidationServiceName is the full name of acme.ledger.v1.LedgerAccountValidationService.
const ledgerLedgerAccountValidationServiceName = "acme.ledger.v1.LedgerAccountValidationService"

// loyaltyLoyaltyMemberCalculationServiceName is the full name of acme.loyalty.v1.LoyaltyMemberCalculationService.
const loyaltyLoyaltyMemberCalculationServiceName = "acme.loyalty.v1.LoyaltyMemberCalculationService"

// giftcardsGiftCardQueryServiceName is the full name of acme.giftcards.v1.GiftCardQueryService.
const giftcardsGiftCardQueryServiceName = "acme.giftcards.v1.GiftCardQueryService"

// authorizationAccessPolicyServiceName is the full name of acme.authorization.v1.AccessPolicyService.
const authorizationAccessPolicyServiceName = "acme.authorization.v1.AccessPolicyService"

const authorizationAccessPolicyServiceBatchGetAccessPoliciesMethod = "/" + authorizationAccessPolicyServiceName + "/BatchGetAccessPolicies"

const loyaltyLoyaltyMemberCalculationServiceCompareLoyaltyMemberOptionsMethod = "/" + loyaltyLoyaltyMemberCalculationServiceName + "/CompareLoyaltyMemberOptions"

// venuesVenueServiceName is the full name of acme.venues.v1.VenueService.
const venuesVenueServiceName = "acme.venues.v1.VenueService"

const venuesVenueServiceCreateVenueMethod = "/" + venuesVenueServiceName + "/CreateVenue"

// searchSearchIndexExportServiceName is the full name of acme.search.v1.SearchIndexExportService.
const searchSearchIndexExportServiceName = "acme.search.v1.SearchIndexExportService"
