package loyalty

import (
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
)

// buildRetryTenantExportRequestForBatchGetLoyaltyMembers maps what BatchGetLoyaltyMembers has to the request of
// acme.identity.v1.TenantExportService/RetryTenantExport.
func buildRetryTenantExportRequestForBatchGetLoyaltyMembers(req *loyaltyv1.BatchGetLoyaltyMembersRequest) *identityv1.RetryTenantExportRequest {
	return &identityv1.RetryTenantExportRequest{
		Context: req.GetContext(),
		Tenant: &identityv1.Tenant{
			Children: []*identityv1.Tenant{&identityv1.Tenant{
				Children: []*identityv1.Tenant{&identityv1.Tenant{
					Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
				}},
			}},
		},
	}
}

// buildExplainAccessPolicyDecisionRequestForStartPointsLedgerWorkflow maps what StartPointsLedgerWorkflow has to the
// request of acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision.
func buildExplainAccessPolicyDecisionRequestForStartPointsLedgerWorkflow(req *loyaltyv1.StartPointsLedgerWorkflowRequest) *authorizationv1.ExplainAccessPolicyDecisionRequest {
	return &authorizationv1.ExplainAccessPolicyDecisionRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetPointsLedger().GetAudit().GetUpdatedAt(),
				CreatedBy: req.GetPointsLedger().GetAudit().GetCreatedBy(),
			},
		},
		Details: &authorizationv1.ExplainAccessPolicyDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotify().GetPhone(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
	}
}

// buildPreviewGiftCardBatchRequestForCheckTierEligibility maps what CheckTierEligibility has to the request of
// acme.giftcards.v1.GiftCardBatchValidationService/PreviewGiftCardBatch.
func buildPreviewGiftCardBatchRequestForCheckTierEligibility(req *loyaltyv1.CheckTierEligibilityRequest) *giftcardsv1.PreviewGiftCardBatchRequest {
	return &giftcardsv1.PreviewGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{
				Tax: &giftcardsv1.GiftCardBatchLineTax{Amount: req.GetTier().GetDiscount().GetAmount()},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetTier().GetAudit().GetCreatedBy(),
				UpdatedBy: req.GetTier().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &giftcardsv1.PreviewGiftCardBatchRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
}

// buildFindReviewModerationsRequestForCheckRewardEligibility maps what CheckRewardEligibility has to the request of
// acme.reviews.v1.ReviewModerationQueryService/FindReviewModerations.
func buildFindReviewModerationsRequestForCheckRewardEligibility(req *loyaltyv1.CheckRewardEligibilityRequest, scorePointsLedgerReply *loyaltyv1.ScorePointsLedgerResponse) *reviewsv1.FindReviewModerationsRequest {
	return &reviewsv1.FindReviewModerationsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.FindReviewModerationsRequest_Filter{
			Labels:       scorePointsLedgerReply.GetPointsLedger().GetLabels(),
			CreatedAfter: req.GetReward().GetAudit().GetCreatedAt(),
		},
	}
}

// buildScoreSellerScorecardRequestForExplainRewardDecision maps what ExplainRewardDecision has to the request of
// acme.sellers.v1.SellerScorecardValidationService/ScoreSellerScorecard.
func buildScoreSellerScorecardRequestForExplainRewardDecision(req *loyaltyv1.ExplainRewardDecisionRequest) *sellersv1.ScoreSellerScorecardRequest {
	return &sellersv1.ScoreSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Amount:      req.GetReward().GetDiscount().GetAmount(),
				Description: req.GetReward().GetDescription(),
			}},
			Counterparty: &customersv1.CustomerRef{Email: req.GetReward().GetOwner().GetEmail()},
			Audit:        &commonv1.AuditInfo{UpdatedAt: req.GetReward().GetAudit().GetUpdatedAt()},
		},
	}
}

// buildSearchLoyaltyMembersRequestForTerminateReward maps what TerminateReward has to the request of
// acme.loyalty.v1.LoyaltyMemberService/SearchLoyaltyMembers.
func buildSearchLoyaltyMembersRequestForTerminateReward(req *loyaltyv1.TerminateRewardRequest) *loyaltyv1.SearchLoyaltyMembersRequest {
	return &loyaltyv1.SearchLoyaltyMembersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.SearchLoyaltyMembersRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}
}

// buildListSearchIndexSubscriptionsRequestForShareTierReport maps what ShareTierReport has to the request of
// acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions.
func buildListSearchIndexSubscriptionsRequestForShareTierReport(req *loyaltyv1.ShareTierReportRequest) *searchv1.ListSearchIndexSubscriptionsRequest {
	return &searchv1.ListSearchIndexSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexSubscriptionsRequest_Filter{
			OwnerId:      req.GetTierId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetDetails().GetVariables(),
		},
	}
}

// buildScoreNotificationRequestForEstimateLoyaltyMember maps what EstimateLoyaltyMember has to the request of
// acme.notifications.v1.NotificationValidationService/ScoreNotification.
func buildScoreNotificationRequestForEstimateLoyaltyMember(req *loyaltyv1.EstimateLoyaltyMemberRequest) *notificationsv1.ScoreNotificationRequest {
	return &notificationsv1.ScoreNotificationRequest{
		Context: req.GetContext(),
		Details: &notificationsv1.ScoreNotificationRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
		Notification: &notificationsv1.Notification{
			Requester: &commonv1.ContactInfo{Name: req.GetLoyaltyMember().GetContact().GetName()},
		},
	}
}
