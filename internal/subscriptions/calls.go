package subscriptions

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

// subscriptionsUsageRecordApprovalServiceName is the full name of acme.subscriptions.v1.UsageRecordApprovalService.
const subscriptionsUsageRecordApprovalServiceName = "acme.subscriptions.v1.UsageRecordApprovalService"

const subscriptionsUsageRecordApprovalServiceCommentUsageRecordApprovalMethod = "/" + subscriptionsUsageRecordApprovalServiceName + "/CommentUsageRecordApproval"

// subscriptionsRenewalScheduleValidationServiceName is the full name of
// acme.subscriptions.v1.RenewalScheduleValidationService.
const subscriptionsRenewalScheduleValidationServiceName = "acme.subscriptions.v1.RenewalScheduleValidationService"

// sellersSellerServiceName is the full name of acme.sellers.v1.SellerService.
const sellersSellerServiceName = "acme.sellers.v1.SellerService"

// sellersSellerScorecardApprovalServiceName is the full name of acme.sellers.v1.SellerScorecardApprovalService.
const sellersSellerScorecardApprovalServiceName = "acme.sellers.v1.SellerScorecardApprovalService"

const sellersSellerScorecardApprovalServiceEscalateSellerScorecardMethod = "/" + sellersSellerScorecardApprovalServiceName + "/EscalateSellerScorecard"

// subscriptionsUsageRecordCalculationServiceName is the full name of
// acme.subscriptions.v1.UsageRecordCalculationService.
const subscriptionsUsageRecordCalculationServiceName = "acme.subscriptions.v1.UsageRecordCalculationService"

const subscriptionsUsageRecordCalculationServiceExplainUsageRecordCalculationMethod = "/" + subscriptionsUsageRecordCalculationServiceName + "/ExplainUsageRecordCalculation"

// subscriptionsSubscriptionPlanValidationServiceName is the full name of
// acme.subscriptions.v1.SubscriptionPlanValidationService.
const subscriptionsSubscriptionPlanValidationServiceName = "acme.subscriptions.v1.SubscriptionPlanValidationService"

// subscriptionsSubscriptionPlanQueryServiceName is the full name of acme.subscriptions.v1.SubscriptionPlanQueryService.
const subscriptionsSubscriptionPlanQueryServiceName = "acme.subscriptions.v1.SubscriptionPlanQueryService"

const subscriptionsSubscriptionPlanQueryServiceSummarizeSubscriptionPlansMethod = "/" + subscriptionsSubscriptionPlanQueryServiceName + "/SummarizeSubscriptionPlans"

const subscriptionsSubscriptionPlanValidationServiceValidateSubscriptionPlanMethod = "/" + subscriptionsSubscriptionPlanValidationServiceName + "/ValidateSubscriptionPlan"

// auditAuditEventQueryServiceName is the full name of acme.audit.v1.AuditEventQueryService.
const auditAuditEventQueryServiceName = "acme.audit.v1.AuditEventQueryService"

const auditAuditEventQueryServiceCountAuditEventsMethod = "/" + auditAuditEventQueryServiceName + "/CountAuditEvents"
