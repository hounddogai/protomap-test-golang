package sellers

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

// giftcardsGiftCardBatchApprovalServiceName is the full name of acme.giftcards.v1.GiftCardBatchApprovalService.
const giftcardsGiftCardBatchApprovalServiceName = "acme.giftcards.v1.GiftCardBatchApprovalService"

// documentsDocumentQueryServiceName is the full name of acme.documents.v1.DocumentQueryService.
const documentsDocumentQueryServiceName = "acme.documents.v1.DocumentQueryService"

// giftcardsGiftCardTransactionWorkflowServiceName is the full name of
// acme.giftcards.v1.GiftCardTransactionWorkflowService.
const giftcardsGiftCardTransactionWorkflowServiceName = "acme.giftcards.v1.GiftCardTransactionWorkflowService"

const giftcardsGiftCardTransactionWorkflowServiceSignalGiftCardTransactionWorkflowMethod = "/" + giftcardsGiftCardTransactionWorkflowServiceName + "/SignalGiftCardTransactionWorkflow"

// sellersSellerScorecardLifecycleServiceName is the full name of acme.sellers.v1.SellerScorecardLifecycleService.
const sellersSellerScorecardLifecycleServiceName = "acme.sellers.v1.SellerScorecardLifecycleService"

// identitySessionQueryServiceName is the full name of acme.identity.v1.SessionQueryService.
const identitySessionQueryServiceName = "acme.identity.v1.SessionQueryService"

// sellersSellerScorecardCalculationServiceName is the full name of acme.sellers.v1.SellerScorecardCalculationService.
const sellersSellerScorecardCalculationServiceName = "acme.sellers.v1.SellerScorecardCalculationService"

const sellersSellerScorecardCalculationServiceCompareSellerScorecardOptionsMethod = "/" + sellersSellerScorecardCalculationServiceName + "/CompareSellerScorecardOptions"

// cartCartRuleCalculationServiceName is the full name of acme.cart.v1.CartRuleCalculationService.
const cartCartRuleCalculationServiceName = "acme.cart.v1.CartRuleCalculationService"

const cartCartRuleCalculationServiceCalculateCartRuleMethod = "/" + cartCartRuleCalculationServiceName + "/CalculateCartRule"

// sellersSellerApprovalServiceName is the full name of acme.sellers.v1.SellerApprovalService.
const sellersSellerApprovalServiceName = "acme.sellers.v1.SellerApprovalService"

const giftcardsGiftCardBatchApprovalServiceEscalateGiftCardBatchMethod = "/" + giftcardsGiftCardBatchApprovalServiceName + "/EscalateGiftCardBatch"

// datasetsDatasetServiceName is the full name of acme.datasets.v1.DatasetService.
const datasetsDatasetServiceName = "acme.datasets.v1.DatasetService"

// identityRoleHistoryServiceName is the full name of acme.identity.v1.RoleHistoryService.
const identityRoleHistoryServiceName = "acme.identity.v1.RoleHistoryService"

const identityRoleHistoryServiceAnnotateRoleVersionMethod = "/" + identityRoleHistoryServiceName + "/AnnotateRoleVersion"

// sellersSellerScorecardReportingServiceName is the full name of acme.sellers.v1.SellerScorecardReportingService.
const sellersSellerScorecardReportingServiceName = "acme.sellers.v1.SellerScorecardReportingService"

const sellersSellerScorecardReportingServiceShareSellerScorecardReportMethod = "/" + sellersSellerScorecardReportingServiceName + "/ShareSellerScorecardReport"

// venuesVenueServiceName is the full name of acme.venues.v1.VenueService.
const venuesVenueServiceName = "acme.venues.v1.VenueService"

// sellersSellerListingCalculationServiceName is the full name of acme.sellers.v1.SellerListingCalculationService.
const sellersSellerListingCalculationServiceName = "acme.sellers.v1.SellerListingCalculationService"

// auditAuditEventValidationServiceName is the full name of acme.audit.v1.AuditEventValidationService.
const auditAuditEventValidationServiceName = "acme.audit.v1.AuditEventValidationService"

const auditAuditEventValidationServiceValidateAuditEventMethod = "/" + auditAuditEventValidationServiceName + "/ValidateAuditEvent"

const documentsDocumentQueryServiceResolveDocumentsMethod = "/" + documentsDocumentQueryServiceName + "/ResolveDocuments"

// giftcardsGiftCardQueryServiceName is the full name of acme.giftcards.v1.GiftCardQueryService.
const giftcardsGiftCardQueryServiceName = "acme.giftcards.v1.GiftCardQueryService"

// sellersSellerListingReportingServiceName is the full name of acme.sellers.v1.SellerListingReportingService.
const sellersSellerListingReportingServiceName = "acme.sellers.v1.SellerListingReportingService"

const giftcardsGiftCardTransactionWorkflowServiceAdvanceGiftCardTransactionWorkflowMethod = "/" + giftcardsGiftCardTransactionWorkflowServiceName + "/AdvanceGiftCardTransactionWorkflow"
