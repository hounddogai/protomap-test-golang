package sellers

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	experimentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	labelingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/labeling/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	seatingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/seating/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
)

// buildGetUserRequestForUpdateSeller maps what UpdateSeller has to the request of acme.identity.v1.UserService/GetUser.
func buildGetUserRequestForUpdateSeller(req *sellersv1.UpdateSellerRequest) *identityv1.GetUserRequest {
	return &identityv1.GetUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetSeller().GetRelationshipManager().GetUserId(),
	}
}

// buildValidateSellerRequestForDeleteSeller maps what DeleteSeller has to the request of
// acme.sellers.v1.SellerValidationService/ValidateSeller.
func buildValidateSellerRequestForDeleteSeller(req *sellersv1.DeleteSellerRequest, previewSellerScorecardReply *sellersv1.PreviewSellerScorecardResponse) *sellersv1.ValidateSellerRequest {
	return &sellersv1.ValidateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				Email: previewSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
		},
	}
}

// buildExplainSellerCalculationRequestForListSellers maps what ListSellers has to the request of
// acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation.
func buildExplainSellerCalculationRequestForListSellers(req *sellersv1.ListSellersRequest) *sellersv1.ExplainSellerCalculationRequest {
	return &sellersv1.ExplainSellerCalculationRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
}

// buildSuggestSearchQueriesRequestForSignalSellerPayoutScheduleWorkflow maps what SignalSellerPayoutScheduleWorkflow
// has to the request of acme.search.v1.SearchQueryQueryService/SuggestSearchQueries.
func buildSuggestSearchQueriesRequestForSignalSellerPayoutScheduleWorkflow(req *sellersv1.SignalSellerPayoutScheduleWorkflowRequest) *searchv1.SuggestSearchQueriesRequest {
	return &searchv1.SuggestSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.SuggestSearchQueriesRequest_Filter{
			Region:       req.GetSellerPayoutSchedule().GetLocation().GetRegion(),
			CreatedAfter: req.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetSellerPayoutSchedule().GetSellerPayoutScheduleId(),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
}

// buildPushSellerScorecardRequestForCommentSellerScorecardApproval maps what CommentSellerScorecardApproval has to the
// request of acme.sellers.v1.SellerScorecardSyncService/PushSellerScorecard.
func buildPushSellerScorecardRequestForCommentSellerScorecardApproval(req *sellersv1.CommentSellerScorecardApprovalRequest) *sellersv1.PushSellerScorecardRequest {
	return &sellersv1.PushSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetSellerScorecard().GetAudit().GetCreatedBy(),
			},
		},
	}
}

// buildQuoteInvoiceBatchRequestForSuggestSellers maps what SuggestSellers has to the request of
// acme.invoicing.v1.InvoiceBatchCalculationService/QuoteInvoiceBatch.
func buildQuoteInvoiceBatchRequestForSuggestSellers(req *sellersv1.SuggestSellersRequest, recalculateSellerPayoutSchedulesReply *sellersv1.RecalculateSellerPayoutSchedulesResponse) *invoicingv1.QuoteInvoiceBatchRequest {
	return &invoicingv1.QuoteInvoiceBatchRequest{
		Context: req.GetContext(),
		Details: &invoicingv1.QuoteInvoiceBatchRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: recalculateSellerPayoutSchedulesReply.GetSellerPayoutSchedule().GetLocation().GetRecipient(),
			},
		},
	}
}

// buildFetchSellerListingReportRequestForRetrySellerListingWorkflow maps what RetrySellerListingWorkflow has to the
// request of acme.sellers.v1.SellerListingReportingService/FetchSellerListingReport.
func buildFetchSellerListingReportRequestForRetrySellerListingWorkflow(req *sellersv1.RetrySellerListingWorkflowRequest) *sellersv1.FetchSellerListingReportRequest {
	return &sellersv1.FetchSellerListingReportRequest{
		Context:         req.GetContext(),
		SellerListingId: req.GetSellerListing().GetSellerListingId(),
	}
}

// buildResolveExperimentsRequestForEscalateSellerPayoutSchedule maps what EscalateSellerPayoutSchedule has to the
// request of acme.experiments.v1.ExperimentQueryService/ResolveExperiments.
func buildResolveExperimentsRequestForEscalateSellerPayoutSchedule(req *sellersv1.EscalateSellerPayoutScheduleRequest) *experimentsv1.ResolveExperimentsRequest {
	return &experimentsv1.ResolveExperimentsRequest{Context: req.GetContext()}
}

// buildValidatePreferenceRequestForListPendingSellerPayoutSchedules maps what ListPendingSellerPayoutSchedules has to
// the request of acme.customers.v1.PreferenceValidationService/ValidatePreference.
func buildValidatePreferenceRequestForListPendingSellerPayoutSchedules(req *sellersv1.ListPendingSellerPayoutSchedulesRequest) *customersv1.ValidatePreferenceRequest {
	return &customersv1.ValidatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
}

// buildPeekUserRequestForAdvanceSellerWorkflow maps what AdvanceSellerWorkflow has to the request of
// acme.identity.v1.UserQueryService/PeekUser.
func buildPeekUserRequestForAdvanceSellerWorkflow(req *sellersv1.AdvanceSellerWorkflowRequest) *identityv1.PeekUserRequest {
	return &identityv1.PeekUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetSeller().GetRelationshipManager().GetUserId(),
	}
}

// buildQuoteSellerRequestForPreviewSellerPayoutSchedule maps what PreviewSellerPayoutSchedule has to the request of
// acme.sellers.v1.SellerCalculationService/QuoteSeller.
func buildQuoteSellerRequestForPreviewSellerPayoutSchedule(req *sellersv1.PreviewSellerPayoutScheduleRequest, compareSellerPayoutScheduleOptionsReply *sellersv1.CompareSellerPayoutScheduleOptionsResponse) *sellersv1.QuoteSellerRequest {
	return &sellersv1.QuoteSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				Email: compareSellerPayoutScheduleOptionsReply.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
			},
		},
		Details: &sellersv1.QuoteSellerRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:     compareSellerPayoutScheduleOptionsReply.GetSellerPayoutSchedule().GetLocation().GetLines(),
				Recipient: req.GetSellerPayoutSchedule().GetLocation().GetRecipient(),
			},
		},
	}
}

// buildBrowseVenuesRequestForQuoteSellerScorecard maps what QuoteSellerScorecard has to the request of
// acme.venues.v1.VenueQueryService/BrowseVenues.
func buildBrowseVenuesRequestForQuoteSellerScorecard(req *sellersv1.QuoteSellerScorecardRequest) *venuesv1.BrowseVenuesRequest {
	return &venuesv1.BrowseVenuesRequest{
		Context: req.GetContext(),
		Filter: &venuesv1.BrowseVenuesRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetSellerScorecard().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetSellerScorecard().GetCounterparty().GetCustomerId(),
			Labels:       req.GetAnnotations(),
		},
	}
}

// buildCloneVenueRequestForApproveSeller maps what ApproveSeller has to the request of
// acme.venues.v1.VenueService/CloneVenue.
func buildCloneVenueRequestForApproveSeller(req *sellersv1.ApproveSellerRequest, advanceGiftCardBatchWorkflowReply *giftcardsv1.AdvanceGiftCardBatchWorkflowResponse) *venuesv1.CloneVenueRequest {
	return &venuesv1.CloneVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Audit: &commonv1.AuditInfo{
				CreatedBy: advanceGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetCreatedBy(),
			},
		},
	}
}

// buildCompareSeatMapOptionsRequestForRecallSeller maps what RecallSeller has to the request of
// acme.seating.v1.SeatMapCalculationService/CompareSeatMapOptions.
func buildCompareSeatMapOptionsRequestForRecallSeller(req *sellersv1.RecallSellerRequest) *seatingv1.CompareSeatMapOptionsRequest {
	return &seatingv1.CompareSeatMapOptionsRequest{
		Context: req.GetContext(),
		SeatMap: &seatingv1.SeatMap{
			Children: []*seatingv1.SeatMap{&seatingv1.SeatMap{
				Children: []*seatingv1.SeatMap{&seatingv1.SeatMap{
					Manager: &identityv1.UserRef{
						Email:  req.GetSeller().GetContact().GetEmail(),
						UserId: req.GetDetails().GetApprover().GetUserId(),
					},
					Location: &commonv1.PostalAddress{
						CountryCode: req.GetSeller().GetPrimaryAddress().GetCountryCode(),
					},
				}},
			}},
		},
	}
}

// buildCompareGiftCardTransactionOptionsRequestForValidateSellerListing maps what ValidateSellerListing has to the
// request of acme.giftcards.v1.GiftCardTransactionCalculationService/CompareGiftCardTransactionOptions.
func buildCompareGiftCardTransactionOptionsRequestForValidateSellerListing(req *sellersv1.ValidateSellerListingRequest) *giftcardsv1.CompareGiftCardTransactionOptionsRequest {
	return &giftcardsv1.CompareGiftCardTransactionOptionsRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetSellerListing().GetOwner().GetDisplayName(),
			},
		},
		Details: &giftcardsv1.CompareGiftCardTransactionOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetSellerListing().GetAddress().GetLines()},
		},
	}
}

// buildPreviewPreferenceRequestForActivateSellerScorecard maps what ActivateSellerScorecard has to the request of
// acme.customers.v1.PreferenceValidationService/PreviewPreference.
func buildPreviewPreferenceRequestForActivateSellerScorecard(req *sellersv1.ActivateSellerScorecardRequest) *customersv1.PreviewPreferenceRequest {
	return &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
}

// buildFindGiftCardsRequestForCheckSellerEligibility maps what CheckSellerEligibility has to the request of
// acme.giftcards.v1.GiftCardQueryService/FindGiftCards.
func buildFindGiftCardsRequestForCheckSellerEligibility(req *sellersv1.CheckSellerEligibilityRequest) *giftcardsv1.FindGiftCardsRequest {
	return &giftcardsv1.FindGiftCardsRequest{
		Context: req.GetContext(),
		Filter:  &giftcardsv1.FindGiftCardsRequest_Filter{Region: req.GetSeller().GetPrimaryAddress().GetRegion()},
	}
}

// buildExplainLabelingTaskCalculationRequestForCompareSellerListingOptions maps what CompareSellerListingOptions has to
// the request of acme.labeling.v1.LabelingTaskCalculationService/ExplainLabelingTaskCalculation.
func buildExplainLabelingTaskCalculationRequestForCompareSellerListingOptions(req *sellersv1.CompareSellerListingOptionsRequest) *labelingv1.ExplainLabelingTaskCalculationRequest {
	return &labelingv1.ExplainLabelingTaskCalculationRequest{
		Context: req.GetContext(),
		LabelingTask: &labelingv1.LabelingTask{
			Audit:     &commonv1.AuditInfo{UpdatedAt: req.GetSellerListing().GetAudit().GetUpdatedAt()},
			Requester: &commonv1.ContactInfo{Email: req.GetSellerListing().GetOwner().GetEmail()},
			Comments:  []*labelingv1.LabelingTaskComment{&labelingv1.LabelingTaskComment{CreatedAt: req.GetSellerListing().GetAudit().GetCreatedAt()}},
			Origin: &labelingv1.LabelingTask_Customer{Customer: &labelingv1.LabelSetRef{
				DisplayName: req.GetSellerListing().GetOwner().GetDisplayName(),
			}},
		},
	}
}

// buildCalculateRedemptionRequestForRenewSellerPayoutSchedule maps what RenewSellerPayoutSchedule has to the request of
// acme.promotions.v1.RedemptionCalculationService/CalculateRedemption.
func buildCalculateRedemptionRequestForRenewSellerPayoutSchedule(req *sellersv1.RenewSellerPayoutScheduleRequest) *promotionsv1.CalculateRedemptionRequest {
	return &promotionsv1.CalculateRedemptionRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Annotations: req.GetAnnotations(),
	}
}

// buildGenerateSellerListingReportRequestForScheduleSellerPayoutScheduleExpiry maps what
// ScheduleSellerPayoutScheduleExpiry has to the request of
// acme.sellers.v1.SellerListingReportingService/GenerateSellerListingReport.
func buildGenerateSellerListingReportRequestForScheduleSellerPayoutScheduleExpiry(req *sellersv1.ScheduleSellerPayoutScheduleExpiryRequest) *sellersv1.GenerateSellerListingReportRequest {
	return &sellersv1.GenerateSellerListingReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details: &sellersv1.GenerateSellerListingReportRequest_Details{
			Period:  req.GetSellerPayoutSchedule().GetWindow(),
			Filters: req.GetSellerPayoutSchedule().GetMetadata(),
		},
	}
}

// buildExplainGiftCardTransactionCalculationRequestForEstimateSeller maps what EstimateSeller has to the request of
// acme.giftcards.v1.GiftCardTransactionCalculationService/ExplainGiftCardTransactionCalculation.
func buildExplainGiftCardTransactionCalculationRequestForEstimateSeller(req *sellersv1.EstimateSellerRequest) *giftcardsv1.ExplainGiftCardTransactionCalculationRequest {
	return &giftcardsv1.ExplainGiftCardTransactionCalculationRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ExplainGiftCardTransactionCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetSeller().GetRelationshipManager().GetEmail(),
				Phone: req.GetSeller().GetContact().GetPhone(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				Number:      req.GetSeller().GetNationalId().GetNumber(),
				DisplayName: req.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
		},
	}
}

// buildResolveGiftCardsRequestForCompareSellerOptions maps what CompareSellerOptions has to the request of
// acme.giftcards.v1.GiftCardQueryService/ResolveGiftCards.
func buildResolveGiftCardsRequestForCompareSellerOptions(req *sellersv1.CompareSellerOptionsRequest) *giftcardsv1.ResolveGiftCardsRequest {
	return &giftcardsv1.ResolveGiftCardsRequest{Context: req.GetContext()}
}

// buildNormalizePreferenceRequestForReinstateSellerListing maps what ReinstateSellerListing has to the request of
// acme.customers.v1.PreferenceValidationService/NormalizePreference.
func buildNormalizePreferenceRequestForReinstateSellerListing(req *sellersv1.ReinstateSellerListingRequest) *customersv1.NormalizePreferenceRequest {
	return &customersv1.NormalizePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNewOwner().GetEmail(),
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
		Details: &customersv1.NormalizePreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
}

// buildAnnotateRoleVersionRequestForFetchSellerPayoutScheduleReport maps what FetchSellerPayoutScheduleReport has to
// the request of acme.identity.v1.RoleHistoryService/AnnotateRoleVersion.
func buildAnnotateRoleVersionRequestForFetchSellerPayoutScheduleReport(req *sellersv1.FetchSellerPayoutScheduleReportRequest) *identityv1.AnnotateRoleVersionRequest {
	return &identityv1.AnnotateRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
	}
}

// buildValidateHouseholdRequestForDeleteSellerPayoutScheduleReport maps what DeleteSellerPayoutScheduleReport has to
// the request of acme.customers.v1.HouseholdValidationService/ValidateHousehold.
func buildValidateHouseholdRequestForDeleteSellerPayoutScheduleReport(req *sellersv1.DeleteSellerPayoutScheduleReportRequest, explainConsentRecordDecisionReply *customersv1.ExplainConsentRecordDecisionResponse) *customersv1.ValidateHouseholdRequest {
	return &customersv1.ValidateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Children: []*customersv1.Household{&customersv1.Household{
						HouseholdId: explainConsentRecordDecisionReply.GetConsentRecord().GetSubject().GetHouseholdId(),
						Attributes:  explainConsentRecordDecisionReply.GetConsentRecord().GetAttributes(),
					}},
					Manager: &identityv1.UserRef{
						DisplayName: explainConsentRecordDecisionReply.GetConsentRecord().GetSubject().GetDisplayName(),
					},
				}},
				Audit: &commonv1.AuditInfo{Etag: req.GetEtag()},
			}},
		},
	}
}

// buildListSellersRequestForScheduleSellerExpiry maps what ScheduleSellerExpiry has to the request of
// acme.sellers.v1.SellerService/ListSellers.
func buildListSellersRequestForScheduleSellerExpiry(req *sellersv1.ScheduleSellerExpiryRequest, previewConsentRecordReply *customersv1.PreviewConsentRecordResponse, validateSellerScorecardReply *sellersv1.ValidateSellerScorecardResponse) *sellersv1.ListSellersRequest {
	return &sellersv1.ListSellersRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellersRequest_Filter{
			Region:       req.GetSeller().GetPrimaryAddress().GetRegion(),
			CreatedAfter: validateSellerScorecardReply.GetSellerScorecard().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetSeller().GetRelationshipManager().GetUserId(),
			Labels:       previewConsentRecordReply.GetResult().GetDetails(),
		},
	}
}
