package subscriptions

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
)

// buildValidateRenewalScheduleRequestForUpdateSubscription maps what UpdateSubscription has to the request of
// acme.subscriptions.v1.RenewalScheduleValidationService/ValidateRenewalSchedule.
func buildValidateRenewalScheduleRequestForUpdateSubscription(req *subscriptionsv1.UpdateSubscriptionRequest) *subscriptionsv1.ValidateRenewalScheduleRequest {
	return &subscriptionsv1.ValidateRenewalScheduleRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetSubscription().GetContact().GetEmail()},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetSubscription().GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetSubscription().GetAudit().GetUpdatedAt(),
			},
			Organizer: &identityv1.UserRef{Email: req.GetSubscription().GetHolder().GetEmail()},
		},
	}
}

// buildEstimateReviewModerationRequestForListSubscriptions maps what ListSubscriptions has to the request of
// acme.reviews.v1.ReviewModerationCalculationService/EstimateReviewModeration.
func buildEstimateReviewModerationRequestForListSubscriptions(req *subscriptionsv1.ListSubscriptionsRequest) *reviewsv1.EstimateReviewModerationRequest {
	return &reviewsv1.EstimateReviewModerationRequest{
		Context: req.GetContext(),
		Details: &reviewsv1.EstimateReviewModerationRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Origin:   &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()}},
			History:  []*reviewsv1.ReviewModerationStatusChange{&reviewsv1.ReviewModerationStatusChange{ChangedAt: req.GetFilter().GetCreatedAfter()}},
		},
	}
}

// buildCheckTierEligibilityRequestForExportSubscriptionPlans maps what ExportSubscriptionPlans has to the request of
// acme.loyalty.v1.TierValidationService/CheckTierEligibility.
func buildCheckTierEligibilityRequestForExportSubscriptionPlans(req *subscriptionsv1.ExportSubscriptionPlansRequest, lintSubscriptionPlansReply *subscriptionsv1.LintSubscriptionPlansResponse) *loyaltyv1.CheckTierEligibilityRequest {
	return &loyaltyv1.CheckTierEligibilityRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintSubscriptionPlansReply.GetSubscriptionPlan().GetAudit().GetCreatedAt(),
				UpdatedBy: lintSubscriptionPlansReply.GetSubscriptionPlan().GetAudit().GetUpdatedBy(),
				Etag:      lintSubscriptionPlansReply.GetSubscriptionPlan().GetAudit().GetEtag(),
			},
			Action: &loyaltyv1.Tier_Block{Block: &loyaltyv1.TierBlockAction{
				Reason: lintSubscriptionPlansReply.GetSubscriptionPlan().GetBlock().GetReason(),
			}},
		},
	}
}

// buildListSynonymExportsRequestForGetUsageRecordApprovalStatus maps what GetUsageRecordApprovalStatus has to the
// request of acme.search.v1.SynonymExportService/ListSynonymExports.
func buildListSynonymExportsRequestForGetUsageRecordApprovalStatus(req *subscriptionsv1.GetUsageRecordApprovalStatusRequest) *searchv1.ListSynonymExportsRequest {
	return &searchv1.ListSynonymExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymExportsRequest_Filter{
			OwnerId: req.GetUsageRecordId(),
			Labels:  req.GetContext().GetBaggage(),
		},
	}
}

// buildQuoteGiftCardBatchRequestForBrowseSubscriptions maps what BrowseSubscriptions has to the request of
// acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch.
func buildQuoteGiftCardBatchRequestForBrowseSubscriptions(req *subscriptionsv1.BrowseSubscriptionsRequest) *giftcardsv1.QuoteGiftCardBatchRequest {
	return &giftcardsv1.QuoteGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetFilter().GetCreatedAfter(),
				UpdatedAt: req.GetContext().GetDeadline(),
			},
			Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{Attributes: req.GetFilter().GetLabels()}},
		},
	}
}

// buildListSubscriptionsRequestForAdvanceSubscriptionPlanWorkflow maps what AdvanceSubscriptionPlanWorkflow has to the
// request of acme.subscriptions.v1.SubscriptionService/ListSubscriptions.
func buildListSubscriptionsRequestForAdvanceSubscriptionPlanWorkflow(req *subscriptionsv1.AdvanceSubscriptionPlanWorkflowRequest) *subscriptionsv1.ListSubscriptionsRequest {
	return &subscriptionsv1.ListSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.ListSubscriptionsRequest_Filter{
			CreatedAfter: req.GetSubscriptionPlan().GetAudit().GetUpdatedAt(),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
}

// buildSuggestLedgerAccountsRequestForExplainUsageRecordDecision maps what ExplainUsageRecordDecision has to the
// request of acme.ledger.v1.LedgerAccountQueryService/SuggestLedgerAccounts.
func buildSuggestLedgerAccountsRequestForExplainUsageRecordDecision(req *subscriptionsv1.ExplainUsageRecordDecisionRequest) *ledgerv1.SuggestLedgerAccountsRequest {
	return &ledgerv1.SuggestLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.SuggestLedgerAccountsRequest_Filter{
			CreatedAfter: req.GetUsageRecord().GetOccurredAt(),
			OwnerId:      req.GetUsageRecord().GetUsageRecordId(),
			Labels:       req.GetUsageRecord().GetAttributes(),
		},
	}
}

// buildPeekSubscriptionRequestForAdvanceSubscriptionWorkflow maps what AdvanceSubscriptionWorkflow has to the request
// of acme.subscriptions.v1.SubscriptionQueryService/PeekSubscription.
func buildPeekSubscriptionRequestForAdvanceSubscriptionWorkflow(req *subscriptionsv1.AdvanceSubscriptionWorkflowRequest) *subscriptionsv1.PeekSubscriptionRequest {
	return &subscriptionsv1.PeekSubscriptionRequest{
		Context:        req.GetContext(),
		SubscriptionId: req.GetSubscription().GetSubscriptionId(),
	}
}

// buildEscalateSubscriptionRequestForInspectSubscriptionWorkflow maps what InspectSubscriptionWorkflow has to the
// request of acme.subscriptions.v1.SubscriptionApprovalService/EscalateSubscription.
func buildEscalateSubscriptionRequestForInspectSubscriptionWorkflow(req *subscriptionsv1.InspectSubscriptionWorkflowRequest) *subscriptionsv1.EscalateSubscriptionRequest {
	return &subscriptionsv1.EscalateSubscriptionRequest{
		Context:        req.GetContext(),
		SubscriptionId: req.GetSubscriptionId(),
		Subscription: &subscriptionsv1.Subscription{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildLintUsageRecordsRequestForDelegateSubscriptionPlanApproval maps what DelegateSubscriptionPlanApproval has to the
// request of acme.subscriptions.v1.UsageRecordValidationService/LintUsageRecords.
func buildLintUsageRecordsRequestForDelegateSubscriptionPlanApproval(req *subscriptionsv1.DelegateSubscriptionPlanApprovalRequest) *subscriptionsv1.LintUsageRecordsRequest {
	return &subscriptionsv1.LintUsageRecordsRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{Email: req.GetSubscriptionPlan().GetOwner().GetEmail()},
		},
	}
}

// buildSuggestFeatureFlagsRequestForApproveSubscription maps what ApproveSubscription has to the request of
// acme.featureflags.v1.FeatureFlagQueryService/SuggestFeatureFlags.
func buildSuggestFeatureFlagsRequestForApproveSubscription(req *subscriptionsv1.ApproveSubscriptionRequest) *featureflagsv1.SuggestFeatureFlagsRequest {
	return &featureflagsv1.SuggestFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.SuggestFeatureFlagsRequest_Filter{
			Labels:       req.GetSubscription().GetLabels(),
			CreatedAfter: req.GetSubscription().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetSubscription().GetSubscriptionId(),
		},
	}
}

// buildBrowseMetricSeriesesRequestForDelegateSubscriptionApproval maps what DelegateSubscriptionApproval has to the
// request of acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses.
func buildBrowseMetricSeriesesRequestForDelegateSubscriptionApproval(req *subscriptionsv1.DelegateSubscriptionApprovalRequest) *metricsv1.BrowseMetricSeriesesRequest {
	return &metricsv1.BrowseMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.BrowseMetricSeriesesRequest_Filter{
			Labels:       req.GetSubscription().GetLabels(),
			CreatedAfter: req.GetSubscription().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetSubscription().GetHolder().GetCustomerId(),
		},
	}
}
