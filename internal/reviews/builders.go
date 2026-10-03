package reviews

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
)

// buildGetReviewModerationRatesRequestForLookupReviewModeration maps what LookupReviewModeration has to the request of
// acme.reviews.v1.ReviewModerationCalculationService/GetReviewModerationRates.
func buildGetReviewModerationRatesRequestForLookupReviewModeration(req *reviewsv1.LookupReviewModerationRequest) *reviewsv1.GetReviewModerationRatesRequest {
	return &reviewsv1.GetReviewModerationRatesRequest{
		Context:            req.GetContext(),
		ReviewModerationId: req.GetReviewModerationId(),
	}
}

// buildListReviewModerationWorkflowsRequestForResumeReviewVoteWorkflow maps what ResumeReviewVoteWorkflow has to the
// request of acme.reviews.v1.ReviewModerationWorkflowService/ListReviewModerationWorkflows.
func buildListReviewModerationWorkflowsRequestForResumeReviewVoteWorkflow(req *reviewsv1.ResumeReviewVoteWorkflowRequest) *reviewsv1.ListReviewModerationWorkflowsRequest {
	return &reviewsv1.ListReviewModerationWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.ListReviewModerationWorkflowsRequest_Filter{
			OwnerId:      req.GetReviewVote().GetSubject().GetCustomerId(),
			CreatedAfter: req.GetReviewVote().GetOccurredAt(),
		},
	}
}

// buildPausePlacementWorkflowRequestForRetryReviewModerationWorkflow maps what RetryReviewModerationWorkflow has to the
// request of acme.merchandising.v1.PlacementWorkflowService/PausePlacementWorkflow.
func buildPausePlacementWorkflowRequestForRetryReviewModerationWorkflow(req *reviewsv1.RetryReviewModerationWorkflowRequest) *merchandisingv1.PausePlacementWorkflowRequest {
	return &merchandisingv1.PausePlacementWorkflowRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: req.GetReviewModeration().GetRequester().GetPreferredChannel(),
					Name:             req.GetReviewModeration().GetRequester().GetName(),
					Phone:            req.GetReviewModeration().GetRequester().GetPhone(),
				},
			}},
			Organizer: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
}

// buildEscalateGiftCardTransactionRequestForRejectReviewVote maps what RejectReviewVote has to the request of
// acme.giftcards.v1.GiftCardTransactionApprovalService/EscalateGiftCardTransaction.
func buildEscalateGiftCardTransactionRequestForRejectReviewVote(req *reviewsv1.RejectReviewVoteRequest) *giftcardsv1.EscalateGiftCardTransactionRequest {
	return &giftcardsv1.EscalateGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.EscalateGiftCardTransactionRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
}

// buildListSellerWorkflowsRequestForRejectReviewVote maps what RejectReviewVote has to the request of
// acme.sellers.v1.SellerWorkflowService/ListSellerWorkflows.
func buildListSellerWorkflowsRequestForRejectReviewVote(req *reviewsv1.RejectReviewVoteRequest, escalateGiftCardTransactionReply *giftcardsv1.EscalateGiftCardTransactionResponse) *sellersv1.ListSellerWorkflowsRequest {
	return &sellersv1.ListSellerWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerWorkflowsRequest_Filter{
			CreatedAfter: req.GetReviewVote().GetOccurredAt(),
			OwnerId:      escalateGiftCardTransactionReply.GetGiftCardTransaction().GetGiftCardTransactionId(),
			Labels:       req.GetReviewVote().GetAttributes(),
		},
	}
}

// buildNormalizeHouseholdRequestForPauseProductReviewWorkflow maps what PauseProductReviewWorkflow has to the request
// of acme.customers.v1.HouseholdValidationService/NormalizeHousehold.
func buildNormalizeHouseholdRequestForPauseProductReviewWorkflow(req *reviewsv1.PauseProductReviewWorkflowRequest) *customersv1.NormalizeHouseholdRequest {
	return &customersv1.NormalizeHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Lines: req.GetProductReview().GetBillingAddress().GetLines(),
					},
				}},
			}},
		},
	}
}

// buildValidateProductReviewRequestForApproveReviewModeration maps what ApproveReviewModeration has to the request of
// acme.reviews.v1.ProductReviewValidationService/ValidateProductReview.
func buildValidateProductReviewRequestForApproveReviewModeration(req *reviewsv1.ApproveReviewModerationRequest) *reviewsv1.ValidateProductReviewRequest {
	return &reviewsv1.ValidateProductReviewRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetReviewModeration().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &reviewsv1.ValidateProductReviewRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetReviewModeration().GetAssignee().GetEmail()},
		},
	}
}

// buildPeekReviewModerationRequestForCommentReviewModerationApproval maps what CommentReviewModerationApproval has to
// the request of acme.reviews.v1.ReviewModerationQueryService/PeekReviewModeration.
func buildPeekReviewModerationRequestForCommentReviewModerationApproval(req *reviewsv1.CommentReviewModerationApprovalRequest) *reviewsv1.PeekReviewModerationRequest {
	return &reviewsv1.PeekReviewModerationRequest{
		Context:            req.GetContext(),
		ReviewModerationId: req.GetReviewModeration().GetReviewModerationId(),
	}
}

// buildCompareReviewVoteOptionsRequestForNormalizeReviewVote maps what NormalizeReviewVote has to the request of
// acme.reviews.v1.ReviewVoteCalculationService/CompareReviewVoteOptions.
func buildCompareReviewVoteOptionsRequestForNormalizeReviewVote(req *reviewsv1.NormalizeReviewVoteRequest) *reviewsv1.CompareReviewVoteOptionsRequest {
	return &reviewsv1.CompareReviewVoteOptionsRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				CustomerId:  req.GetReviewVote().GetSubject().GetCustomerId(),
				DisplayName: req.GetReviewVote().GetSubject().GetDisplayName(),
				Email:       req.GetDetails().GetContact().GetEmail(),
			},
			Kind: req.GetReviewVote().GetKind(),
		},
	}
}

// buildExplainHouseholdDecisionRequestForApproveProductReview maps what ApproveProductReview has to the request of
// acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision.
func buildExplainHouseholdDecisionRequestForApproveProductReview(req *reviewsv1.ApproveProductReviewRequest, getProductReviewReply *reviewsv1.GetProductReviewResponse) *customersv1.ExplainHouseholdDecisionRequest {
	return &customersv1.ExplainHouseholdDecisionRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: req.GetProductReview().GetCounterparty().GetEmail()},
					Location: &commonv1.PostalAddress{
						CountryCode: getProductReviewReply.GetProductReview().GetBillingAddress().GetCountryCode(),
					},
				}},
			}},
		},
	}
}
