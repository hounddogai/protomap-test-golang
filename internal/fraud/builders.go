package fraud

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
)

// buildCompareFraudRuleOptionsRequestForCancelFraudRuleWorkflow maps what CancelFraudRuleWorkflow has to the request of
// acme.fraud.v1.FraudRuleCalculationService/CompareFraudRuleOptions.
func buildCompareFraudRuleOptionsRequestForCancelFraudRuleWorkflow(req *fraudv1.CancelFraudRuleWorkflowRequest) *fraudv1.CompareFraudRuleOptionsRequest {
	return &fraudv1.CompareFraudRuleOptionsRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetFraudRule().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetFraudRule().GetAudit().GetUpdatedAt(),
				Etag:      req.GetFraudRule().GetAudit().GetEtag(),
				UpdatedBy: req.GetFraudRule().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildResumeLoyaltyMemberWorkflowRequestForCancelFraudRuleWorkflow maps what CancelFraudRuleWorkflow has to the
// request of acme.loyalty.v1.LoyaltyMemberWorkflowService/ResumeLoyaltyMemberWorkflow.
func buildResumeLoyaltyMemberWorkflowRequestForCancelFraudRuleWorkflow(req *fraudv1.CancelFraudRuleWorkflowRequest, compareFraudRuleOptionsReply *fraudv1.CompareFraudRuleOptionsResponse) *loyaltyv1.ResumeLoyaltyMemberWorkflowRequest {
	return &loyaltyv1.ResumeLoyaltyMemberWorkflowRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: req.GetFraudRule().GetOwner().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedBy: compareFraudRuleOptionsReply.GetFraudRule().GetAudit().GetCreatedBy(),
				CreatedAt: compareFraudRuleOptionsReply.GetFraudRule().GetAudit().GetCreatedAt(),
				UpdatedBy: compareFraudRuleOptionsReply.GetFraudRule().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildListPendingShipmentsRequestForPauseFraudRuleWorkflow maps what PauseFraudRuleWorkflow has to the request of
// acme.shipping.v1.ShipmentApprovalService/ListPendingShipments.
func buildListPendingShipmentsRequestForPauseFraudRuleWorkflow(req *fraudv1.PauseFraudRuleWorkflowRequest, pauseRiskAssessmentWorkflowReply *fraudv1.PauseRiskAssessmentWorkflowResponse) *shippingv1.ListPendingShipmentsRequest {
	return &shippingv1.ListPendingShipmentsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListPendingShipmentsRequest_Filter{
			CreatedAfter: pauseRiskAssessmentWorkflowReply.GetRiskAssessment().GetSla().GetDueAt(),
		},
	}
}

// collectWatchlistNames lists the names of a watchlist and of everything under it.
func collectWatchlistNames(node *fraudv1.Watchlist, names []string) []string {
	if node == nil {
		return names
	}
	names = append(names, node.GetName())
	for _, child := range node.GetChildren() {
		names = collectWatchlistNames(child, names)
	}
	return names
}

// buildValidateGiftCardRequestForScoreWatchlist maps what ScoreWatchlist has to the request of
// acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard.
func buildValidateGiftCardRequestForScoreWatchlist(req *fraudv1.ScoreWatchlistRequest, previewRiskAssessmentReply *fraudv1.PreviewRiskAssessmentResponse, scoreSessionReply *identityv1.ScoreSessionResponse) *giftcardsv1.ValidateGiftCardRequest {
	return &giftcardsv1.ValidateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewRiskAssessmentReply.GetRiskAssessment().GetAudit().GetCreatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Phone: scoreSessionReply.GetSession().GetRequester().GetPhone(),
			},
		},
	}
}

// buildCalculateHouseholdRequestForStartRiskAssessmentWorkflow maps what StartRiskAssessmentWorkflow has to the request
// of acme.customers.v1.HouseholdCalculationService/CalculateHousehold.
func buildCalculateHouseholdRequestForStartRiskAssessmentWorkflow(req *fraudv1.StartRiskAssessmentWorkflowRequest) *customersv1.CalculateHouseholdRequest {
	return &customersv1.CalculateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						CreatedAt: req.GetRiskAssessment().GetAudit().GetCreatedAt(),
						Etag:      req.GetRiskAssessment().GetAudit().GetEtag(),
					},
					Manager: &identityv1.UserRef{Email: req.GetRiskAssessment().GetCustomer().GetEmail()},
				}},
			}},
		},
	}
}

// buildCompareConsentRecordOptionsRequestForPauseRiskAssessmentWorkflow maps what PauseRiskAssessmentWorkflow has to
// the request of acme.customers.v1.ConsentRecordCalculationService/CompareConsentRecordOptions.
func buildCompareConsentRecordOptionsRequestForPauseRiskAssessmentWorkflow(req *fraudv1.PauseRiskAssessmentWorkflowRequest, quoteRiskAssessmentReply *fraudv1.QuoteRiskAssessmentResponse) *customersv1.CompareConsentRecordOptionsRequest {
	return &customersv1.CompareConsentRecordOptionsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: quoteRiskAssessmentReply.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
		},
		Annotations: req.GetAnnotations(),
	}
}

// buildGetFraudSignalRulesRequestForRejectFraudSignal maps what RejectFraudSignal has to the request of
// acme.fraud.v1.FraudSignalValidationService/GetFraudSignalRules.
func buildGetFraudSignalRulesRequestForRejectFraudSignal(req *fraudv1.RejectFraudSignalRequest) *fraudv1.GetFraudSignalRulesRequest {
	return &fraudv1.GetFraudSignalRulesRequest{
		Context:       req.GetContext(),
		FraudSignalId: req.GetFraudSignalId(),
	}
}

// buildCountFeatureFlagsRequestForCalculateWatchlist maps what CalculateWatchlist has to the request of
// acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags.
func buildCountFeatureFlagsRequestForCalculateWatchlist(req *fraudv1.CalculateWatchlistRequest, lintFraudSignalsReply *fraudv1.LintFraudSignalsResponse) *featureflagsv1.CountFeatureFlagsRequest {
	return &featureflagsv1.CountFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.CountFeatureFlagsRequest_Filter{
			Region:       req.GetWatchlist().GetLocation().GetRegion(),
			CreatedAfter: req.GetWatchlist().GetAudit().GetCreatedAt(),
			OwnerId:      lintFraudSignalsReply.GetResult().GetCorrelationId(),
		},
	}
}

// buildPreviewGiftCardTransactionRequestForNormalizeFraudSignal maps what NormalizeFraudSignal has to the request of
// acme.giftcards.v1.GiftCardTransactionValidationService/PreviewGiftCardTransaction.
func buildPreviewGiftCardTransactionRequestForNormalizeFraudSignal(req *fraudv1.NormalizeFraudSignalRequest) *giftcardsv1.PreviewGiftCardTransactionRequest {
	return &giftcardsv1.PreviewGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.PreviewGiftCardTransactionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetFraudSignal().GetSubject().GetDisplayName(),
			},
		},
	}
}

// buildCalculateRiskAssessmentRequestForCalculateFraudRule maps what CalculateFraudRule has to the request of
// acme.fraud.v1.RiskAssessmentCalculationService/CalculateRiskAssessment.
func buildCalculateRiskAssessmentRequestForCalculateFraudRule(req *fraudv1.CalculateFraudRuleRequest) *fraudv1.CalculateRiskAssessmentRequest {
	return &fraudv1.CalculateRiskAssessmentRequest{
		Context: req.GetContext(),
		Details: &fraudv1.CalculateRiskAssessmentRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
	}
}

// buildListSearchIndexSubscriptionsRequestForActivateWatchlist maps what ActivateWatchlist has to the request of
// acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions.
func buildListSearchIndexSubscriptionsRequestForActivateWatchlist(req *fraudv1.ActivateWatchlistRequest, submitSubscriptionReply *subscriptionsv1.SubmitSubscriptionResponse) *searchv1.ListSearchIndexSubscriptionsRequest {
	return &searchv1.ListSearchIndexSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexSubscriptionsRequest_Filter{
			Labels:       submitSubscriptionReply.GetSubscription().GetLabels(),
			CreatedAfter: submitSubscriptionReply.GetSubscription().GetAudit().GetCreatedAt(),
			OwnerId:      submitSubscriptionReply.GetSubscription().GetHolder().GetCustomerId(),
		},
	}
}

// buildCommentReturnLabelApprovalRequestForReinstateWatchlist maps what ReinstateWatchlist has to the request of
// acme.returns.v1.ReturnLabelApprovalService/CommentReturnLabelApproval.
func buildCommentReturnLabelApprovalRequestForReinstateWatchlist(req *fraudv1.ReinstateWatchlistRequest) *returnsv1.CommentReturnLabelApprovalRequest {
	return &returnsv1.CommentReturnLabelApprovalRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &returnsv1.CommentReturnLabelApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
				Email:  req.GetDetails().GetNewOwner().GetEmail(),
			},
		},
	}
}

// buildCompareRiskAssessmentOptionsRequestForPreviewRiskAssessment maps what PreviewRiskAssessment has to the request
// of acme.fraud.v1.RiskAssessmentCalculationService/CompareRiskAssessmentOptions.
func buildCompareRiskAssessmentOptionsRequestForPreviewRiskAssessment(req *fraudv1.PreviewRiskAssessmentRequest, getRiskAssessmentRatesReply *fraudv1.GetRiskAssessmentRatesResponse) *fraudv1.CompareRiskAssessmentOptionsRequest {
	return &fraudv1.CompareRiskAssessmentOptionsRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetRiskAssessment().GetAssignee().GetDisplayName(),
			},
			Requester: &commonv1.ContactInfo{
				Email: getRiskAssessmentRatesReply.GetRiskAssessment().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetRiskAssessment().GetAudit().GetUpdatedBy(),
			},
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				Email: getRiskAssessmentRatesReply.GetRiskAssessment().GetAssignee().GetEmail(),
			}},
		},
	}
}

// buildRecalculateRiskAssessmentsRequestForLintRiskAssessments maps what LintRiskAssessments has to the request of
// acme.fraud.v1.RiskAssessmentCalculationService/RecalculateRiskAssessments.
func buildRecalculateRiskAssessmentsRequestForLintRiskAssessments(req *fraudv1.LintRiskAssessmentsRequest) *fraudv1.RecalculateRiskAssessmentsRequest {
	return &fraudv1.RecalculateRiskAssessmentsRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
			Origin: &fraudv1.RiskAssessment_Channel{Channel: &fraudv1.RiskAssessmentChannelOrigin{Address: req.GetRiskAssessment().GetChannel().GetAddress()}},
		},
	}
}

// buildRecalculateCustomerSegmentsRequestForGenerateWatchlistReport maps what GenerateWatchlistReport has to the
// request of acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments.
func buildRecalculateCustomerSegmentsRequestForGenerateWatchlistReport(req *fraudv1.GenerateWatchlistReportRequest) *customersv1.RecalculateCustomerSegmentsRequest {
	return &customersv1.RecalculateCustomerSegmentsRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations: req.GetAnnotations(),
	}
}

// buildCancelReviewModerationWorkflowRequestForGenerateWatchlistReport maps what GenerateWatchlistReport has to the
// request of acme.reviews.v1.ReviewModerationWorkflowService/CancelReviewModerationWorkflow.
func buildCancelReviewModerationWorkflowRequestForGenerateWatchlistReport(req *fraudv1.GenerateWatchlistReportRequest, recalculateCustomerSegmentsReply *customersv1.RecalculateCustomerSegmentsResponse) *reviewsv1.CancelReviewModerationWorkflowRequest {
	return &reviewsv1.CancelReviewModerationWorkflowRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recalculateCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
				UpdatedAt: recalculateCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: recalculateCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
		},
	}
}

// buildCalculateFraudRuleRequestForScheduleWatchlistReport maps what ScheduleWatchlistReport has to the request of
// acme.fraud.v1.FraudRuleCalculationService/CalculateFraudRule.
func buildCalculateFraudRuleRequestForScheduleWatchlistReport(req *fraudv1.ScheduleWatchlistReportRequest) *fraudv1.CalculateFraudRuleRequest {
	return &fraudv1.CalculateFraudRuleRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildValidateCustomerSegmentRequestForStreamWatchlistMetrics maps what StreamWatchlistMetrics has to the request of
// acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment.
func buildValidateCustomerSegmentRequestForStreamWatchlistMetrics(req *fraudv1.StreamWatchlistMetricsRequest) *customersv1.ValidateCustomerSegmentRequest {
	return &customersv1.ValidateCustomerSegmentRequest{
		Context: req.GetContext(),
		Details: &customersv1.ValidateCustomerSegmentRequest_Details{
			Contact: &commonv1.ContactInfo{PreferredChannel: req.GetSubscriber().GetPreferredChannel()},
		},
	}
}

// buildNormalizeUserRequestForSimulateRiskAssessment maps what SimulateRiskAssessment has to the request of
// acme.identity.v1.UserValidationService/NormalizeUser.
func buildNormalizeUserRequestForSimulateRiskAssessment(req *fraudv1.SimulateRiskAssessmentRequest) *identityv1.NormalizeUserRequest {
	return &identityv1.NormalizeUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetRiskAssessment().GetAudit().GetCreatedBy(),
			},
		},
	}
}

// buildCalculateWatchlistRequestForExpireFraudSignal maps what ExpireFraudSignal has to the request of
// acme.fraud.v1.WatchlistCalculationService/CalculateWatchlist.
func buildCalculateWatchlistRequestForExpireFraudSignal(req *fraudv1.ExpireFraudSignalRequest) *fraudv1.CalculateWatchlistRequest {
	return &fraudv1.CalculateWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
				}},
				Manager: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
			}},
			Manager: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
}
