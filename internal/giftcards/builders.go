package giftcards

import (
	broadcastsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/broadcasts/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	employeesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employees/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
)

// buildExplainGiftCardCalculationRequestForCreateGiftCard maps what CreateGiftCard has to the request of
// acme.giftcards.v1.GiftCardCalculationService/ExplainGiftCardCalculation.
func buildExplainGiftCardCalculationRequestForCreateGiftCard(req *giftcardsv1.CreateGiftCardRequest) *giftcardsv1.ExplainGiftCardCalculationRequest {
	return &giftcardsv1.ExplainGiftCardCalculationRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetGiftCard().GetAudit().GetUpdatedAt()},
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				Last4:      req.GetGiftCard().GetCard().GetLast4(),
				HolderName: req.GetGiftCard().GetCard().GetHolderName(),
			}},
			Contact: &commonv1.ContactInfo{Name: req.GetGiftCard().GetContact().GetName()},
		},
	}
}

// buildCheckGiftCardTransactionEligibilityRequestForSuggestGiftCards maps what SuggestGiftCards has to the request of
// acme.giftcards.v1.GiftCardTransactionValidationService/CheckGiftCardTransactionEligibility.
func buildCheckGiftCardTransactionEligibilityRequestForSuggestGiftCards(req *giftcardsv1.SuggestGiftCardsRequest) *giftcardsv1.CheckGiftCardTransactionEligibilityRequest {
	return &giftcardsv1.CheckGiftCardTransactionEligibilityRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CheckGiftCardTransactionEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Measurements: []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
		},
	}
}

// buildNormalizeGiftCardTransactionRequestForApproveGiftCardTransaction maps what ApproveGiftCardTransaction has to the
// request of acme.giftcards.v1.GiftCardTransactionValidationService/NormalizeGiftCardTransaction.
func buildNormalizeGiftCardTransactionRequestForApproveGiftCardTransaction(req *giftcardsv1.ApproveGiftCardTransactionRequest) *giftcardsv1.NormalizeGiftCardTransactionRequest {
	return &giftcardsv1.NormalizeGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				Number: req.GetGiftCardTransaction().GetSubject().GetNumber(),
			},
		},
	}
}

// buildPauseGiftCardWorkflowRequestForDelegateGiftCardTransactionApproval maps what DelegateGiftCardTransactionApproval
// has to the request of acme.giftcards.v1.GiftCardWorkflowService/PauseGiftCardWorkflow.
func buildPauseGiftCardWorkflowRequestForDelegateGiftCardTransactionApproval(req *giftcardsv1.DelegateGiftCardTransactionApprovalRequest) *giftcardsv1.PauseGiftCardWorkflowRequest {
	return &giftcardsv1.PauseGiftCardWorkflowRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{Kind: req.GetGiftCardTransaction().GetKind()},
		},
	}
}

// buildResolveBroadcastsRequestForCancelGiftCardWorkflow maps what CancelGiftCardWorkflow has to the request of
// acme.broadcasts.v1.BroadcastQueryService/ResolveBroadcasts.
func buildResolveBroadcastsRequestForCancelGiftCardWorkflow(req *giftcardsv1.CancelGiftCardWorkflowRequest) *broadcastsv1.ResolveBroadcastsRequest {
	return &broadcastsv1.ResolveBroadcastsRequest{Context: req.GetContext()}
}

// buildSubscribeApiCredentialUpdatesRequestForInspectGiftCardWorkflow maps what InspectGiftCardWorkflow has to the
// request of acme.identity.v1.ApiCredentialNotificationService/SubscribeApiCredentialUpdates.
func buildSubscribeApiCredentialUpdatesRequestForInspectGiftCardWorkflow(req *giftcardsv1.InspectGiftCardWorkflowRequest) *identityv1.SubscribeApiCredentialUpdatesRequest {
	return &identityv1.SubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildBatchGetFeatureFlagsRequestForRecallGiftCardBatch maps what RecallGiftCardBatch has to the request of
// acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags.
func buildBatchGetFeatureFlagsRequestForRecallGiftCardBatch(req *giftcardsv1.RecallGiftCardBatchRequest) *featureflagsv1.BatchGetFeatureFlagsRequest {
	return &featureflagsv1.BatchGetFeatureFlagsRequest{Context: req.GetContext()}
}

// buildCompareSellerScorecardOptionsRequestForSubmitGiftCard maps what SubmitGiftCard has to the request of
// acme.sellers.v1.SellerScorecardCalculationService/CompareSellerScorecardOptions.
func buildCompareSellerScorecardOptionsRequestForSubmitGiftCard(req *giftcardsv1.SubmitGiftCardRequest) *sellersv1.CompareSellerScorecardOptionsRequest {
	return &sellersv1.CompareSellerScorecardOptionsRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Settlement: &sellersv1.SellerScorecard_Bank{Bank: &sellersv1.SellerScorecardBankSettlement{
				AccountHolder: req.GetGiftCard().GetBank().GetAccountHolder(),
			}},
		},
	}
}

// buildListStockItemsRequestForRejectGiftCard maps what RejectGiftCard has to the request of
// acme.inventory.v1.StockItemService/ListStockItems.
func buildListStockItemsRequestForRejectGiftCard(req *giftcardsv1.RejectGiftCardRequest) *inventoryv1.ListStockItemsRequest {
	return &inventoryv1.ListStockItemsRequest{
		Context: req.GetContext(),
		Filter:  &inventoryv1.ListStockItemsRequest_Filter{Labels: req.GetGiftCard().GetLabels()},
	}
}

// buildCalculateGiftCardRequestForCheckGiftCardEligibility maps what CheckGiftCardEligibility has to the request of
// acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard.
func buildCalculateGiftCardRequestForCheckGiftCardEligibility(req *giftcardsv1.CheckGiftCardEligibilityRequest) *giftcardsv1.CalculateGiftCardRequest {
	return &giftcardsv1.CalculateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetGiftCard().GetAudit().GetUpdatedBy()},
		},
	}
}

// buildUpdateTimesheetRequestForNormalizeGiftCard maps what NormalizeGiftCard has to the request of
// acme.timekeeping.v1.TimesheetService/UpdateTimesheet.
func buildUpdateTimesheetRequestForNormalizeGiftCard(req *giftcardsv1.NormalizeGiftCardRequest, estimateGiftCardReply *giftcardsv1.EstimateGiftCardResponse) *timekeepingv1.UpdateTimesheetRequest {
	return &timekeepingv1.UpdateTimesheetRequest{
		Context: req.GetContext(),
		Timesheet: &timekeepingv1.Timesheet{
			Counterparty: &employeesv1.EmployeeRef{
				DisplayName: estimateGiftCardReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
		},
	}
}
