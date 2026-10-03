package promotions

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

// promotionsCouponCalculationServiceName is the full name of acme.promotions.v1.CouponCalculationService.
const promotionsCouponCalculationServiceName = "acme.promotions.v1.CouponCalculationService"

const promotionsCouponCalculationServiceCalculateCouponMethod = "/" + promotionsCouponCalculationServiceName + "/CalculateCoupon"

// fraudFraudSignalWorkflowServiceName is the full name of acme.fraud.v1.FraudSignalWorkflowService.
const fraudFraudSignalWorkflowServiceName = "acme.fraud.v1.FraudSignalWorkflowService"

// cartCartApprovalServiceName is the full name of acme.cart.v1.CartApprovalService.
const cartCartApprovalServiceName = "acme.cart.v1.CartApprovalService"

// fulfillmentPackingSlipWorkflowServiceName is the full name of acme.fulfillment.v1.PackingSlipWorkflowService.
const fulfillmentPackingSlipWorkflowServiceName = "acme.fulfillment.v1.PackingSlipWorkflowService"

const fulfillmentPackingSlipWorkflowServiceStartPackingSlipWorkflowMethod = "/" + fulfillmentPackingSlipWorkflowServiceName + "/StartPackingSlipWorkflow"

// promotionsPromotionValidationServiceName is the full name of acme.promotions.v1.PromotionValidationService.
const promotionsPromotionValidationServiceName = "acme.promotions.v1.PromotionValidationService"

const promotionsPromotionValidationServiceNormalizePromotionMethod = "/" + promotionsPromotionValidationServiceName + "/NormalizePromotion"

// promotionsCouponValidationServiceName is the full name of acme.promotions.v1.CouponValidationService.
const promotionsCouponValidationServiceName = "acme.promotions.v1.CouponValidationService"

// identityUserServiceName is the full name of acme.identity.v1.UserService.
const identityUserServiceName = "acme.identity.v1.UserService"

const identityUserServiceUpdateUserMethod = "/" + identityUserServiceName + "/UpdateUser"

// returnsReturnLabelCalculationServiceName is the full name of acme.returns.v1.ReturnLabelCalculationService.
const returnsReturnLabelCalculationServiceName = "acme.returns.v1.ReturnLabelCalculationService"

const returnsReturnLabelCalculationServiceCompareReturnLabelOptionsMethod = "/" + returnsReturnLabelCalculationServiceName + "/CompareReturnLabelOptions"

const promotionsCouponValidationServiceValidateCouponMethod = "/" + promotionsCouponValidationServiceName + "/ValidateCoupon"

// fraudRiskAssessmentApprovalServiceName is the full name of acme.fraud.v1.RiskAssessmentApprovalService.
const fraudRiskAssessmentApprovalServiceName = "acme.fraud.v1.RiskAssessmentApprovalService"

const fraudRiskAssessmentApprovalServiceDelegateRiskAssessmentApprovalMethod = "/" + fraudRiskAssessmentApprovalServiceName + "/DelegateRiskAssessmentApproval"

// promotionsPromotionCalculationServiceName is the full name of acme.promotions.v1.PromotionCalculationService.
const promotionsPromotionCalculationServiceName = "acme.promotions.v1.PromotionCalculationService"

// promotionsRedemptionValidationServiceName is the full name of acme.promotions.v1.RedemptionValidationService.
const promotionsRedemptionValidationServiceName = "acme.promotions.v1.RedemptionValidationService"

const promotionsRedemptionValidationServiceExplainRedemptionDecisionMethod = "/" + promotionsRedemptionValidationServiceName + "/ExplainRedemptionDecision"

// subscriptionsSubscriptionWorkflowServiceName is the full name of acme.subscriptions.v1.SubscriptionWorkflowService.
const subscriptionsSubscriptionWorkflowServiceName = "acme.subscriptions.v1.SubscriptionWorkflowService"

const subscriptionsSubscriptionWorkflowServicePauseSubscriptionWorkflowMethod = "/" + subscriptionsSubscriptionWorkflowServiceName + "/PauseSubscriptionWorkflow"

const promotionsRedemptionValidationServiceValidateRedemptionMethod = "/" + promotionsRedemptionValidationServiceName + "/ValidateRedemption"

// warehousingWarehouseWorkflowServiceName is the full name of acme.warehousing.v1.WarehouseWorkflowService.
const warehousingWarehouseWorkflowServiceName = "acme.warehousing.v1.WarehouseWorkflowService"

// paymentsRefundWorkflowServiceName is the full name of acme.payments.v1.RefundWorkflowService.
const paymentsRefundWorkflowServiceName = "acme.payments.v1.RefundWorkflowService"

const paymentsRefundWorkflowServiceCancelRefundWorkflowMethod = "/" + paymentsRefundWorkflowServiceName + "/CancelRefundWorkflow"

// promotionsCampaignCalculationServiceName is the full name of acme.promotions.v1.CampaignCalculationService.
const promotionsCampaignCalculationServiceName = "acme.promotions.v1.CampaignCalculationService"

// documentsDocumentQueryServiceName is the full name of acme.documents.v1.DocumentQueryService.
const documentsDocumentQueryServiceName = "acme.documents.v1.DocumentQueryService"
