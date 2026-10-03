package giftcards

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

// giftcardsGiftCardTransactionValidationServiceName is the full name of
// acme.giftcards.v1.GiftCardTransactionValidationService.
const giftcardsGiftCardTransactionValidationServiceName = "acme.giftcards.v1.GiftCardTransactionValidationService"

// giftcardsGiftCardTransactionApprovalServiceName is the full name of
// acme.giftcards.v1.GiftCardTransactionApprovalService.
const giftcardsGiftCardTransactionApprovalServiceName = "acme.giftcards.v1.GiftCardTransactionApprovalService"

const giftcardsGiftCardTransactionApprovalServiceDelegateGiftCardTransactionApprovalMethod = "/" + giftcardsGiftCardTransactionApprovalServiceName + "/DelegateGiftCardTransactionApproval"

// identityTenantNotificationServiceName is the full name of acme.identity.v1.TenantNotificationService.
const identityTenantNotificationServiceName = "acme.identity.v1.TenantNotificationService"

const identityTenantNotificationServiceSubscribeTenantUpdatesMethod = "/" + identityTenantNotificationServiceName + "/SubscribeTenantUpdates"
