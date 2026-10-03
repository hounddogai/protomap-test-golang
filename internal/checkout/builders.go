package checkout

import (
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
)

// buildRecordSearchQueryVersionRequestForWatchCheckoutSessions maps what WatchCheckoutSessions has to the request of
// acme.search.v1.SearchQueryHistoryService/RecordSearchQueryVersion.
func buildRecordSearchQueryVersionRequestForWatchCheckoutSessions(req *checkoutv1.WatchCheckoutSessionsRequest, suggestCheckoutSessionsReply *checkoutv1.SuggestCheckoutSessionsResponse) *searchv1.RecordSearchQueryVersionRequest {
	return &searchv1.RecordSearchQueryVersionRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			CorrelationId: suggestCheckoutSessionsReply.GetResult().GetCorrelationId(),
		},
	}
}

// buildSummarizeDocumentsRequestForExportShippingQuotes maps what ExportShippingQuotes has to the request of
// acme.documents.v1.DocumentQueryService/SummarizeDocuments.
func buildSummarizeDocumentsRequestForExportShippingQuotes(req *checkoutv1.ExportShippingQuotesRequest, explainTrackingEventCalculationReply *shippingv1.ExplainTrackingEventCalculationResponse) *documentsv1.SummarizeDocumentsRequest {
	return &documentsv1.SummarizeDocumentsRequest{
		Context: req.GetContext(),
		Details: &documentsv1.SummarizeDocumentsRequest_Details{
			Format:  req.GetFormat(),
			Filters: explainTrackingEventCalculationReply.GetResult().GetDetails(),
		},
		Annotations: req.GetFilter().GetLabels(),
	}
}

// buildResumeGiftCardTransactionWorkflowRequestForStartShippingQuoteWorkflow maps what StartShippingQuoteWorkflow has
// to the request of acme.giftcards.v1.GiftCardTransactionWorkflowService/ResumeGiftCardTransactionWorkflow.
func buildResumeGiftCardTransactionWorkflowRequestForStartShippingQuoteWorkflow(req *checkoutv1.StartShippingQuoteWorkflowRequest) *giftcardsv1.ResumeGiftCardTransactionWorkflowRequest {
	return &giftcardsv1.ResumeGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ResumeGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotify().GetPhone(),
				Name:             req.GetDetails().GetNotify().GetName(),
				Email:            req.GetShippingQuote().GetCounterparty().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
}

// buildSummarizeShippingQuotesRequestForStartCheckoutSessionWorkflow maps what StartCheckoutSessionWorkflow has to the
// request of acme.checkout.v1.ShippingQuoteQueryService/SummarizeShippingQuotes.
func buildSummarizeShippingQuotesRequestForStartCheckoutSessionWorkflow(req *checkoutv1.StartCheckoutSessionWorkflowRequest) *checkoutv1.SummarizeShippingQuotesRequest {
	return &checkoutv1.SummarizeShippingQuotesRequest{
		Context:         req.GetContext(),
		Annotations:     req.GetAnnotations(),
		ShippingQuoteId: req.GetCheckoutSession().GetCounterparty().GetShippingQuoteId(),
	}
}

// buildValidateSessionRequestForCancelCheckoutSessionWorkflow maps what CancelCheckoutSessionWorkflow has to the
// request of acme.identity.v1.SessionValidationService/ValidateSession.
func buildValidateSessionRequestForCancelCheckoutSessionWorkflow(req *checkoutv1.CancelCheckoutSessionWorkflowRequest) *identityv1.ValidateSessionRequest {
	return &identityv1.ValidateSessionRequest{
		Context: req.GetContext(),
		Details: &identityv1.ValidateSessionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
}

// buildSubmitRestockDecisionRequestForSignalCheckoutSessionWorkflow maps what SignalCheckoutSessionWorkflow has to the
// request of acme.returns.v1.RestockDecisionApprovalService/SubmitRestockDecision.
func buildSubmitRestockDecisionRequestForSignalCheckoutSessionWorkflow(req *checkoutv1.SignalCheckoutSessionWorkflowRequest, peekShippingQuoteReply *checkoutv1.PeekShippingQuoteResponse) *returnsv1.SubmitRestockDecisionRequest {
	return &returnsv1.SubmitRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetCheckoutSession().GetAudit().GetEtag(),
				UpdatedBy: peekShippingQuoteReply.GetShippingQuote().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email:       peekShippingQuoteReply.GetShippingQuote().GetCounterparty().GetEmail(),
				DisplayName: peekShippingQuoteReply.GetShippingQuote().GetCounterparty().GetDisplayName(),
			},
		},
	}
}

// buildListPendingCheckoutSessionsRequestForSubmitShippingQuote maps what SubmitShippingQuote has to the request of
// acme.checkout.v1.CheckoutSessionApprovalService/ListPendingCheckoutSessions.
func buildListPendingCheckoutSessionsRequestForSubmitShippingQuote(req *checkoutv1.SubmitShippingQuoteRequest, lockTenantReply *identityv1.LockTenantResponse) *checkoutv1.ListPendingCheckoutSessionsRequest {
	return &checkoutv1.ListPendingCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.ListPendingCheckoutSessionsRequest_Filter{
			Region: lockTenantReply.GetTenant().GetLocation().GetRegion(),
		},
	}
}

// buildCancelTenantExportRequestForLintPaymentIntents maps what LintPaymentIntents has to the request of
// acme.identity.v1.TenantExportService/CancelTenantExport.
func buildCancelTenantExportRequestForLintPaymentIntents(req *checkoutv1.LintPaymentIntentsRequest) *identityv1.CancelTenantExportRequest {
	return &identityv1.CancelTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.CancelTenantExportRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetPaymentIntent().GetCustomer().GetDisplayName(),
				Email:       req.GetPaymentIntent().GetRequester().GetEmail(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Name:  req.GetDetails().GetContact().GetName(),
			},
		},
	}
}

// buildCommentLoyaltyMemberApprovalRequestForRejectCheckoutSession maps what RejectCheckoutSession has to the request
// of acme.loyalty.v1.LoyaltyMemberApprovalService/CommentLoyaltyMemberApproval.
func buildCommentLoyaltyMemberApprovalRequestForRejectCheckoutSession(req *checkoutv1.RejectCheckoutSessionRequest) *loyaltyv1.CommentLoyaltyMemberApprovalRequest {
	return &loyaltyv1.CommentLoyaltyMemberApprovalRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Funding: &loyaltyv1.LoyaltyMember_Bank{Bank: &loyaltyv1.LoyaltyMemberBankFunding{Iban: req.GetCheckoutSession().GetBank().GetIban()}},
		},
	}
}
