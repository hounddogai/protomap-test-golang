package merchandising

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
)

// collectCollectionNames lists the names of a collection and of everything under it.
func collectCollectionNames(node *merchandisingv1.Collection, names []string) []string {
	if node == nil {
		return names
	}
	names = append(names, node.GetName())
	for _, child := range node.GetChildren() {
		names = collectCollectionNames(child, names)
	}
	return names
}

// buildCalculateGiftCardRequestForPeekPlacement maps what PeekPlacement has to the request of
// acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard.
func buildCalculateGiftCardRequestForPeekPlacement(req *merchandisingv1.PeekPlacementRequest, explainMerchandisingRuleDecisionReply *merchandisingv1.ExplainMerchandisingRuleDecisionResponse) *giftcardsv1.CalculateGiftCardRequest {
	return &giftcardsv1.CalculateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainMerchandisingRuleDecisionReply.GetMerchandisingRule().GetAudit().GetCreatedBy(),
				UpdatedBy: explainMerchandisingRuleDecisionReply.GetMerchandisingRule().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildPauseGiftCardBatchWorkflowRequestForCancelCollectionWorkflow maps what CancelCollectionWorkflow has to the
// request of acme.giftcards.v1.GiftCardBatchWorkflowService/PauseGiftCardBatchWorkflow.
func buildPauseGiftCardBatchWorkflowRequestForCancelCollectionWorkflow(req *merchandisingv1.CancelCollectionWorkflowRequest) *giftcardsv1.PauseGiftCardBatchWorkflowRequest {
	return &giftcardsv1.PauseGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				Recipient:   req.GetCollection().GetLocation().GetRecipient(),
				Lines:       req.GetCollection().GetLocation().GetLines(),
				CountryCode: req.GetCollection().GetLocation().GetCountryCode(),
			},
		},
		Details: &giftcardsv1.PauseGiftCardBatchWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
	}
}

// buildSummarizeCollectionsRequestForResumeCollectionWorkflow maps what ResumeCollectionWorkflow has to the request of
// acme.merchandising.v1.CollectionQueryService/SummarizeCollections.
func buildSummarizeCollectionsRequestForResumeCollectionWorkflow(req *merchandisingv1.ResumeCollectionWorkflowRequest) *merchandisingv1.SummarizeCollectionsRequest {
	return &merchandisingv1.SummarizeCollectionsRequest{
		Context:      req.GetContext(),
		CollectionId: req.GetCollectionId(),
	}
}

// buildExplainMerchandisingRuleDecisionRequestForResumeCollectionWorkflow maps what ResumeCollectionWorkflow has to the
// request of acme.merchandising.v1.MerchandisingRuleValidationService/ExplainMerchandisingRuleDecision.
func buildExplainMerchandisingRuleDecisionRequestForResumeCollectionWorkflow(req *merchandisingv1.ResumeCollectionWorkflowRequest) *merchandisingv1.ExplainMerchandisingRuleDecisionRequest {
	return &merchandisingv1.ExplainMerchandisingRuleDecisionRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{UserId: req.GetCollection().GetManager().GetUserId()},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetCollection().GetAudit().GetUpdatedBy()},
		},
		Details: &merchandisingv1.ExplainMerchandisingRuleDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
	}
}

// buildSendSearchIndexTestNoticeRequestForDelegatePlacementApproval maps what DelegatePlacementApproval has to the
// request of acme.search.v1.SearchIndexNotificationService/SendSearchIndexTestNotice.
func buildSendSearchIndexTestNoticeRequestForDelegatePlacementApproval(req *merchandisingv1.DelegatePlacementApprovalRequest) *searchv1.SendSearchIndexTestNoticeRequest {
	return &searchv1.SendSearchIndexTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchIndexTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			Variables: req.GetPlacement().GetMetadata(),
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
}

// buildCountCollectionsRequestForCommentCollectionApproval maps what CommentCollectionApproval has to the request of
// acme.merchandising.v1.CollectionQueryService/CountCollections.
func buildCountCollectionsRequestForCommentCollectionApproval(req *merchandisingv1.CommentCollectionApprovalRequest) *merchandisingv1.CountCollectionsRequest {
	return &merchandisingv1.CountCollectionsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.CountCollectionsRequest_Filter{
			Region:       req.GetCollection().GetLocation().GetRegion(),
			CreatedAfter: req.GetCollection().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetCollection().GetParentId(),
		},
	}
}
