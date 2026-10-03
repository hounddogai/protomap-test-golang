package cart

import (
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	maintenancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
)

// buildQuoteCartRuleRequestForWatchCarts maps what WatchCarts has to the request of
// acme.cart.v1.CartRuleCalculationService/QuoteCartRule.
func buildQuoteCartRuleRequestForWatchCarts(req *cartv1.WatchCartsRequest) *cartv1.QuoteCartRuleRequest {
	return &cartv1.QuoteCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
		Details: &cartv1.QuoteCartRuleRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
}

// buildPreviewCartRequestForSuggestCarts maps what SuggestCarts has to the request of
// acme.cart.v1.CartValidationService/PreviewCart.
func buildPreviewCartRequestForSuggestCarts(req *cartv1.SuggestCartsRequest) *cartv1.PreviewCartRequest {
	return &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Details: &cartv1.PreviewCartRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildSubmitCartRuleRequestForStartSavedListWorkflow maps what StartSavedListWorkflow has to the request of
// acme.cart.v1.CartRuleApprovalService/SubmitCartRule.
func buildSubmitCartRuleRequestForStartSavedListWorkflow(req *cartv1.StartSavedListWorkflowRequest, recallCartRuleReply *cartv1.RecallCartRuleResponse) *cartv1.SubmitCartRuleRequest {
	return &cartv1.SubmitCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetSavedList().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetSavedList().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &cartv1.SubmitCartRuleRequest_Details{
			Approver: &identityv1.UserRef{
				Email: recallCartRuleReply.GetCartRule().GetOwner().GetEmail(),
			},
		},
	}
}

// buildApproveCartRuleRequestForResumeSavedListWorkflow maps what ResumeSavedListWorkflow has to the request of
// acme.cart.v1.CartRuleApprovalService/ApproveCartRule.
func buildApproveCartRuleRequestForResumeSavedListWorkflow(req *cartv1.ResumeSavedListWorkflowRequest) *cartv1.ApproveCartRuleRequest {
	return &cartv1.ApproveCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetSavedList().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetSavedList().GetAudit().GetUpdatedAt(),
				Etag:      req.GetSavedList().GetAudit().GetEtag(),
			},
			Version: req.GetSavedList().GetCounterparty().GetVersion(),
		},
	}
}

// buildBrowseSessionsRequestForSubmitCartRule maps what SubmitCartRule has to the request of
// acme.identity.v1.SessionQueryService/BrowseSessions.
func buildBrowseSessionsRequestForSubmitCartRule(req *cartv1.SubmitCartRuleRequest) *identityv1.BrowseSessionsRequest {
	return &identityv1.BrowseSessionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.BrowseSessionsRequest_Filter{CreatedAfter: req.GetCartRule().GetAudit().GetCreatedAt()},
	}
}

// buildSubscribeSearchQueryUpdatesRequestForRejectSavedList maps what RejectSavedList has to the request of
// acme.search.v1.SearchQueryNotificationService/SubscribeSearchQueryUpdates.
func buildSubscribeSearchQueryUpdatesRequestForRejectSavedList(req *cartv1.RejectSavedListRequest) *searchv1.SubscribeSearchQueryUpdatesRequest {
	return &searchv1.SubscribeSearchQueryUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.SubscribeSearchQueryUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		Annotations: req.GetAnnotations(),
	}
}

// buildSummarizeSavedListsRequestForRecallSavedList maps what RecallSavedList has to the request of
// acme.cart.v1.SavedListQueryService/SummarizeSavedLists.
func buildSummarizeSavedListsRequestForRecallSavedList(req *cartv1.RecallSavedListRequest, scoreSavedListReply *cartv1.ScoreSavedListResponse) *cartv1.SummarizeSavedListsRequest {
	return &cartv1.SummarizeSavedListsRequest{
		Context:     req.GetContext(),
		SavedListId: scoreSavedListReply.GetSavedList().GetSavedListId(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details:     &cartv1.SummarizeSavedListsRequest_Details{Filters: scoreSavedListReply.GetResult().GetDetails()},
	}
}

// buildSimulateParcelRequestForPreviewCartRule maps what PreviewCartRule has to the request of
// acme.shipping.v1.ParcelCalculationService/SimulateParcel.
func buildSimulateParcelRequestForPreviewCartRule(req *cartv1.PreviewCartRuleRequest) *shippingv1.SimulateParcelRequest {
	return &shippingv1.SimulateParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetCartRule().GetAudit().GetUpdatedBy(),
				CreatedAt: req.GetCartRule().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				UserId:      req.GetCartRule().GetOwner().GetUserId(),
				DisplayName: req.GetCartRule().GetOwner().GetDisplayName(),
			},
		},
	}
}

// buildUpdateLoyaltyMemberRequestForNormalizeCartRule maps what NormalizeCartRule has to the request of
// acme.loyalty.v1.LoyaltyMemberService/UpdateLoyaltyMember.
func buildUpdateLoyaltyMemberRequestForNormalizeCartRule(req *cartv1.NormalizeCartRuleRequest) *loyaltyv1.UpdateLoyaltyMemberRequest {
	return &loyaltyv1.UpdateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCartRule().GetAudit().GetUpdatedAt(),
				UpdatedBy: req.GetCartRule().GetAudit().GetUpdatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetContact().GetEmail(),
				Name:  req.GetDetails().GetContact().GetName(),
			},
		},
	}
}

// buildEstimateWatchlistRequestForExplainCartRuleDecision maps what ExplainCartRuleDecision has to the request of
// acme.fraud.v1.WatchlistCalculationService/EstimateWatchlist.
func buildEstimateWatchlistRequestForExplainCartRuleDecision(req *cartv1.ExplainCartRuleDecisionRequest) *fraudv1.EstimateWatchlistRequest {
	return &fraudv1.EstimateWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{UpdatedBy: req.GetCartRule().GetAudit().GetUpdatedBy()},
				}},
			}},
		},
	}
}

// buildCloneCartRequestForDelegateCartApproval maps what DelegateCartApproval has to the request of
// acme.cart.v1.CartService/CloneCart.
func buildCloneCartRequestForDelegateCartApproval(req *cartv1.DelegateCartApprovalRequest, advanceSellerPayoutScheduleWorkflowReply *sellersv1.AdvanceSellerPayoutScheduleWorkflowResponse) *cartv1.CloneCartRequest {
	return &cartv1.CloneCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Region:     advanceSellerPayoutScheduleWorkflowReply.GetSellerPayoutSchedule().GetLocation().GetRegion(),
				PostalCode: advanceSellerPayoutScheduleWorkflowReply.GetSellerPayoutSchedule().GetLocation().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetCart().GetAudit().GetCreatedBy()},
		},
	}
}

// buildCheckCartEligibilityRequestForEstimateCartRule maps what EstimateCartRule has to the request of
// acme.cart.v1.CartValidationService/CheckCartEligibility.
func buildCheckCartEligibilityRequestForEstimateCartRule(req *cartv1.EstimateCartRuleRequest) *cartv1.CheckCartEligibilityRequest {
	return &cartv1.CheckCartEligibilityRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
		Details: &cartv1.CheckCartEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetCartRule().GetOwner().GetEmail()},
		},
	}
}

// buildPreviewNotificationRequestForNormalizeCart maps what NormalizeCart has to the request of
// acme.notifications.v1.NotificationValidationService/PreviewNotification.
func buildPreviewNotificationRequestForNormalizeCart(req *cartv1.NormalizeCartRequest) *notificationsv1.PreviewNotificationRequest {
	return &notificationsv1.PreviewNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCart().GetAudit().GetUpdatedAt(),
				Etag:      req.GetCart().GetAudit().GetEtag(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetCart().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &notificationsv1.PreviewNotificationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
}

// buildFindRefundsRequestForEstimateSavedList maps what EstimateSavedList has to the request of
// acme.payments.v1.RefundQueryService/FindRefunds.
func buildFindRefundsRequestForEstimateSavedList(req *cartv1.EstimateSavedListRequest) *paymentsv1.FindRefundsRequest {
	return &paymentsv1.FindRefundsRequest{
		Context: req.GetContext(),
		Filter:  &paymentsv1.FindRefundsRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	}
}

// buildEstimateWorkOrderRequestForEstimateCart maps what EstimateCart has to the request of
// acme.maintenance.v1.WorkOrderCalculationService/EstimateWorkOrder.
func buildEstimateWorkOrderRequestForEstimateCart(req *cartv1.EstimateCartRequest) *maintenancev1.EstimateWorkOrderRequest {
	return &maintenancev1.EstimateWorkOrderRequest{
		Context: req.GetContext(),
		Details: &maintenancev1.EstimateWorkOrderRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
		WorkOrder: &maintenancev1.WorkOrder{
			Audit: &commonv1.AuditInfo{Etag: req.GetCart().GetAudit().GetEtag()},
		},
	}
}
