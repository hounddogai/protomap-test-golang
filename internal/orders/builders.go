package orders

import (
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	messagingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/messaging/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
)

// buildCompareOrderNoteOptionsRequestForBrowseOrderAmendments maps what BrowseOrderAmendments has to the request of
// acme.orders.v1.OrderNoteCalculationService/CompareOrderNoteOptions.
func buildCompareOrderNoteOptionsRequestForBrowseOrderAmendments(req *ordersv1.BrowseOrderAmendmentsRequest) *ordersv1.CompareOrderNoteOptionsRequest {
	return &ordersv1.CompareOrderNoteOptionsRequest{
		Context: req.GetContext(),
		Details: &ordersv1.CompareOrderNoteOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		OrderNote: &ordersv1.OrderNote{
			Measurements: []*ordersv1.OrderNoteMeasurement{&ordersv1.OrderNoteMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Source:       req.GetFilter().GetOwnerId(),
			Attributes:   req.GetFilter().GetLabels(),
		},
	}
}

// buildListFraudSignalWorkflowsRequestForInspectBackorderWorkflow maps what InspectBackorderWorkflow has to the request
// of acme.fraud.v1.FraudSignalWorkflowService/ListFraudSignalWorkflows.
func buildListFraudSignalWorkflowsRequestForInspectBackorderWorkflow(req *ordersv1.InspectBackorderWorkflowRequest, delegateOrderNoteApprovalReply *ordersv1.DelegateOrderNoteApprovalResponse) *fraudv1.ListFraudSignalWorkflowsRequest {
	return &fraudv1.ListFraudSignalWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.ListFraudSignalWorkflowsRequest_Filter{
			CreatedAfter: delegateOrderNoteApprovalReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      delegateOrderNoteApprovalReply.GetOutcome().GetReferenceId(),
			Labels:       delegateOrderNoteApprovalReply.GetOrderNote().GetAttributes(),
		},
	}
}

// buildPreviewAuditEventRequestForLookupOrder maps what LookupOrder has to the request of
// acme.audit.v1.AuditEventValidationService/PreviewAuditEvent.
func buildPreviewAuditEventRequestForLookupOrder(req *ordersv1.LookupOrderRequest) *auditv1.PreviewAuditEventRequest {
	return &auditv1.PreviewAuditEventRequest{
		Context: req.GetContext(),
		Details: &auditv1.PreviewAuditEventRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildCheckUserEligibilityRequestForExportOrders maps what ExportOrders has to the request of
// acme.identity.v1.UserValidationService/CheckUserEligibility.
func buildCheckUserEligibilityRequestForExportOrders(req *ordersv1.ExportOrdersRequest, scoreOrderNoteReply *ordersv1.ScoreOrderNoteResponse) *identityv1.CheckUserEligibilityRequest {
	return &identityv1.CheckUserEligibilityRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			IdentityDocument: &identityv1.User_Passport{Passport: &identityv1.UserPassport{
				Number: scoreOrderNoteReply.GetOrderNote().GetSubject().GetNumber(),
			}},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: scoreOrderNoteReply.GetOrderNote().GetSubject().GetDisplayName(),
			},
			Attributes:     scoreOrderNoteReply.GetOrderNote().GetAttributes(),
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
}

// buildValidateVenueRequestForStartOrderAmendmentWorkflow maps what StartOrderAmendmentWorkflow has to the request of
// acme.venues.v1.VenueValidationService/ValidateVenue.
func buildValidateVenueRequestForStartOrderAmendmentWorkflow(req *ordersv1.StartOrderAmendmentWorkflowRequest) *venuesv1.ValidateVenueRequest {
	return &venuesv1.ValidateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{
				Region: req.GetOrderAmendment().GetBillingAddress().GetRegion(),
				Lines:  req.GetOrderAmendment().GetBillingAddress().GetLines(),
			}},
		},
	}
}

// buildValidateCustomerSegmentRequestForDelegateBackorderApproval maps what DelegateBackorderApproval has to the
// request of acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment.
func buildValidateCustomerSegmentRequestForDelegateBackorderApproval(req *ordersv1.DelegateBackorderApprovalRequest) *customersv1.ValidateCustomerSegmentRequest {
	return &customersv1.ValidateCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetBackorder().GetAudit().GetUpdatedBy(),
				CreatedAt: req.GetBackorder().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetBackorder().GetAssignee().GetDisplayName(),
			},
		},
	}
}

// buildEstimateOrderRequestForExplainOrderNoteDecision maps what ExplainOrderNoteDecision has to the request of
// acme.orders.v1.OrderCalculationService/EstimateOrder.
func buildEstimateOrderRequestForExplainOrderNoteDecision(req *ordersv1.ExplainOrderNoteDecisionRequest) *ordersv1.EstimateOrderRequest {
	return &ordersv1.EstimateOrderRequest{Context: req.GetContext()}
}

// buildCompareOrderAmendmentOptionsRequestForLintOrderNotes maps what LintOrderNotes has to the request of
// acme.orders.v1.OrderAmendmentCalculationService/CompareOrderAmendmentOptions.
func buildCompareOrderAmendmentOptionsRequestForLintOrderNotes(req *ordersv1.LintOrderNotesRequest, sendApiCredentialTestNoticeReply *identityv1.SendApiCredentialTestNoticeResponse) *ordersv1.CompareOrderAmendmentOptionsRequest {
	return &ordersv1.CompareOrderAmendmentOptionsRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: sendApiCredentialTestNoticeReply.GetApiCredential().GetAudit().GetCreatedAt(),
			},
			Settlement: &ordersv1.OrderAmendment_Card{Card: &ordersv1.OrderAmendmentCardSettlement{
				Token: sendApiCredentialTestNoticeReply.GetApiCredential().GetCard().GetToken(),
			}},
		},
	}
}

// buildLintOrdersRequestForEstimateOrderNote maps what EstimateOrderNote has to the request of
// acme.orders.v1.OrderValidationService/LintOrders.
func buildLintOrdersRequestForEstimateOrderNote(req *ordersv1.EstimateOrderNoteRequest) *ordersv1.LintOrdersRequest {
	return &ordersv1.LintOrdersRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Locality: req.GetDetails().GetDestination().GetLocality(),
				Region:   req.GetDetails().GetDestination().GetRegion(),
			},
		},
	}
}

// buildCompareBackorderOptionsRequestForRecalculateOrderNotes maps what RecalculateOrderNotes has to the request of
// acme.orders.v1.BackorderCalculationService/CompareBackorderOptions.
func buildCompareBackorderOptionsRequestForRecalculateOrderNotes(req *ordersv1.RecalculateOrderNotesRequest, lintOrdersReply *ordersv1.LintOrdersResponse) *ordersv1.CompareBackorderOptionsRequest {
	return &ordersv1.CompareBackorderOptionsRequest{
		Context: req.GetContext(),
		Details: &ordersv1.CompareBackorderOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: lintOrdersReply.GetOrder().GetBillingAddress().GetRegion(),
			},
		},
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				DisplayName: lintOrdersReply.GetOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
}

// buildRecalculateOrderAmendmentsRequestForPreviewOrderAmendment maps what PreviewOrderAmendment has to the request of
// acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments.
func buildRecalculateOrderAmendmentsRequestForPreviewOrderAmendment(req *ordersv1.PreviewOrderAmendmentRequest, getOrderAmendmentRatesReply *ordersv1.GetOrderAmendmentRatesResponse) *ordersv1.RecalculateOrderAmendmentsRequest {
	return &ordersv1.RecalculateOrderAmendmentsRequest{
		Context: req.GetContext(),
		Details: &ordersv1.RecalculateOrderAmendmentsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: req.GetOrderAmendment().GetBillingAddress().GetRegion(),
			},
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: getOrderAmendmentRatesReply.GetOrderAmendment().GetBillingAddress().GetPostalCode(),
			},
			Counterparty: &ordersv1.BackorderRef{
				Priority: req.GetOrderAmendment().GetCounterparty().GetPriority(),
			},
			Settlement: &ordersv1.OrderAmendment_Card{Card: &ordersv1.OrderAmendmentCardSettlement{Network: req.GetOrderAmendment().GetCard().GetNetwork()}},
		},
	}
}

// buildSuggestFeatureFlagsRequestForNormalizeOrderAmendment maps what NormalizeOrderAmendment has to the request of
// acme.featureflags.v1.FeatureFlagQueryService/SuggestFeatureFlags.
func buildSuggestFeatureFlagsRequestForNormalizeOrderAmendment(req *ordersv1.NormalizeOrderAmendmentRequest, explainPayoutCalculationReply *paymentsv1.ExplainPayoutCalculationResponse) *featureflagsv1.SuggestFeatureFlagsRequest {
	return &featureflagsv1.SuggestFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.SuggestFeatureFlagsRequest_Filter{
			Region:       req.GetOrderAmendment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetOrderAmendment().GetDueAt(),
			OwnerId:      explainPayoutCalculationReply.GetPayout().GetCounterparty().GetRefundId(),
		},
	}
}

// buildCalculateGiftCardRequestForCompareBackorderOptions maps what CompareBackorderOptions has to the request of
// acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard.
func buildCalculateGiftCardRequestForCompareBackorderOptions(req *ordersv1.CompareBackorderOptionsRequest) *giftcardsv1.CalculateGiftCardRequest {
	return &giftcardsv1.CalculateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetBackorder().GetAudit().GetUpdatedBy()},
		},
	}
}

// buildCountOrderTotalsRequestForRenewOrderNote maps what RenewOrderNote has to the request of
// acme.orders.v1.OrderReportingService/CountOrderTotals.
func buildCountOrderTotalsRequestForRenewOrderNote(req *ordersv1.RenewOrderNoteRequest) *ordersv1.CountOrderTotalsRequest {
	return &ordersv1.CountOrderTotalsRequest{
		Context: req.GetContext(),
		Filter:  &ordersv1.CountOrderTotalsRequest_Filter{OwnerId: req.GetDetails().GetNewOwner().GetUserId()},
	}
}

// buildEstimateInspectionRequestForNormalizeOrder maps what NormalizeOrder has to the request of
// acme.returns.v1.InspectionCalculationService/EstimateInspection.
func buildEstimateInspectionRequestForNormalizeOrder(req *ordersv1.NormalizeOrderRequest) *returnsv1.EstimateInspectionRequest {
	return &returnsv1.EstimateInspectionRequest{
		Context: req.GetContext(),
		Details: &returnsv1.EstimateInspectionRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetOrder().GetBillingAddress().GetLines()},
		},
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{Email: req.GetOrder().GetCounterparty().GetEmail()},
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
}

// buildQuoteOrderRequestForScoreOrder maps what ScoreOrder has to the request of
// acme.orders.v1.OrderCalculationService/QuoteOrder.
func buildQuoteOrderRequestForScoreOrder(req *ordersv1.ScoreOrderRequest, calculateOrderAmendmentReply *ordersv1.CalculateOrderAmendmentResponse) *ordersv1.QuoteOrderRequest {
	return &ordersv1.QuoteOrderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.QuoteOrderRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: calculateOrderAmendmentReply.GetOrderAmendment().GetBillingAddress().GetRecipient(),
				Lines:     req.GetOrder().GetBillingAddress().GetLines(),
			},
		},
	}
}

// buildExplainConsentRecordDecisionRequestForActivateBackorder maps what ActivateBackorder has to the request of
// acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision.
func buildExplainConsentRecordDecisionRequestForActivateBackorder(req *ordersv1.ActivateBackorderRequest) *customersv1.ExplainConsentRecordDecisionRequest {
	return &customersv1.ExplainConsentRecordDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainConsentRecordDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
	}
}

// buildSimulateConversationRequestForQuoteOrder maps what QuoteOrder has to the request of
// acme.messaging.v1.ConversationCalculationService/SimulateConversation.
func buildSimulateConversationRequestForQuoteOrder(req *ordersv1.QuoteOrderRequest, validateSessionReply *identityv1.ValidateSessionResponse) *messagingv1.SimulateConversationRequest {
	return &messagingv1.SimulateConversationRequest{
		Context: req.GetContext(),
		Conversation: &messagingv1.Conversation{
			Requester: &commonv1.ContactInfo{
				Email: validateSessionReply.GetSession().GetRequester().GetEmail(),
			},
		},
	}
}

// buildCountOrderAmendmentTotalsRequestForStreamBackorderMetrics maps what StreamBackorderMetrics has to the request of
// acme.orders.v1.OrderAmendmentReportingService/CountOrderAmendmentTotals.
func buildCountOrderAmendmentTotalsRequestForStreamBackorderMetrics(req *ordersv1.StreamBackorderMetricsRequest) *ordersv1.CountOrderAmendmentTotalsRequest {
	return &ordersv1.CountOrderAmendmentTotalsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.CountOrderAmendmentTotalsRequest_Filter{
			Query:   req.GetFilter().GetQuery(),
			OwnerId: req.GetFilter().GetOwnerId(),
			Region:  req.GetFilter().GetRegion(),
		},
	}
}

// buildCheckBackorderEligibilityRequestForListOrderAmendmentReports maps what ListOrderAmendmentReports has to the
// request of acme.orders.v1.BackorderValidationService/CheckBackorderEligibility.
func buildCheckBackorderEligibilityRequestForListOrderAmendmentReports(req *ordersv1.ListOrderAmendmentReportsRequest) *ordersv1.CheckBackorderEligibilityRequest {
	return &ordersv1.CheckBackorderEligibilityRequest{
		Context: req.GetContext(),
		Details: &ordersv1.CheckBackorderEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildCountOrderTotalsRequestForCountOrderAmendmentTotals maps what CountOrderAmendmentTotals has to the request of
// acme.orders.v1.OrderReportingService/CountOrderTotals.
func buildCountOrderTotalsRequestForCountOrderAmendmentTotals(req *ordersv1.CountOrderAmendmentTotalsRequest) *ordersv1.CountOrderTotalsRequest {
	return &ordersv1.CountOrderTotalsRequest{
		Context: req.GetContext(),
		Page: &commonv1.PageRequest{
			PageSize:  req.GetPage().GetPageSize(),
			PageToken: req.GetPage().GetPageToken(),
		},
		Filter: &ordersv1.CountOrderTotalsRequest_Filter{
			OwnerId: req.GetFilter().GetOwnerId(),
			Region:  req.GetFilter().GetRegion(),
		},
	}
}

// buildCalculateOrderAmendmentRequestForSyncBackorders maps what SyncBackorders has to the request of
// acme.orders.v1.OrderAmendmentCalculationService/CalculateOrderAmendment.
func buildCalculateOrderAmendmentRequestForSyncBackorders(req *ordersv1.SyncBackordersRequest) *ordersv1.CalculateOrderAmendmentRequest {
	return &ordersv1.CalculateOrderAmendmentRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: req.GetBackorder().GetCustomer().GetDisplayName(),
			},
		},
	}
}
