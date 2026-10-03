package promotions

import (
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	maintenancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	toursv1 "github.com/hounddogai/protomap-test-golang/gen/acme/tours/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
)

// buildPullCampaignsRequestForCancelCampaignWorkflow maps what CancelCampaignWorkflow has to the request of
// acme.promotions.v1.CampaignSyncService/PullCampaigns.
func buildPullCampaignsRequestForCancelCampaignWorkflow(req *promotionsv1.CancelCampaignWorkflowRequest, escalateCampaignReply *promotionsv1.EscalateCampaignResponse) *promotionsv1.PullCampaignsRequest {
	return &promotionsv1.PullCampaignsRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
					Email:            req.GetCampaign().GetOrganizer().GetEmail(),
					Phone:            req.GetDetails().GetNotify().GetPhone(),
				},
			}},
			Location: &commonv1.PostalAddress{
				CountryCode: escalateCampaignReply.GetCampaign().GetLocation().GetCountryCode(),
			},
		},
	}
}

// buildValidateSellerPayoutScheduleRequestForPeekPromotion maps what PeekPromotion has to the request of
// acme.sellers.v1.SellerPayoutScheduleValidationService/ValidateSellerPayoutSchedule.
func buildValidateSellerPayoutScheduleRequestForPeekPromotion(req *promotionsv1.PeekPromotionRequest, explainFraudSignalCalculationReply *fraudv1.ExplainFraudSignalCalculationResponse) *sellersv1.ValidateSellerPayoutScheduleRequest {
	return &sellersv1.ValidateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Organizer: &identityv1.UserRef{
				DisplayName: explainFraudSignalCalculationReply.GetFraudSignal().GetSubject().GetDisplayName(),
				Email:       req.GetContext().GetActor().GetEmail(),
				UserId:      req.GetContext().GetActor().GetUserId(),
			},
		},
		Details: &sellersv1.ValidateSellerPayoutScheduleRequest_Details{Locale: req.GetContext().GetLocale()},
	}
}

// buildSummarizeVenuesRequestForStartCouponWorkflow maps what StartCouponWorkflow has to the request of
// acme.venues.v1.VenueQueryService/SummarizeVenues.
func buildSummarizeVenuesRequestForStartCouponWorkflow(req *promotionsv1.StartCouponWorkflowRequest, delegatePromotionApprovalReply *promotionsv1.DelegatePromotionApprovalResponse) *venuesv1.SummarizeVenuesRequest {
	return &venuesv1.SummarizeVenuesRequest{
		Context:     req.GetContext(),
		Reason:      delegatePromotionApprovalReply.GetPromotion().GetBlock().GetReason(),
		Annotations: req.GetAnnotations(),
	}
}

// buildSubmitFulfillmentExceptionRequestForAdvanceCouponWorkflow maps what AdvanceCouponWorkflow has to the request of
// acme.fulfillment.v1.FulfillmentExceptionApprovalService/SubmitFulfillmentException.
func buildSubmitFulfillmentExceptionRequestForAdvanceCouponWorkflow(req *promotionsv1.AdvanceCouponWorkflowRequest, rejectPromotionReply *promotionsv1.RejectPromotionResponse) *fulfillmentv1.SubmitFulfillmentExceptionRequest {
	return &fulfillmentv1.SubmitFulfillmentExceptionRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{
				UserId: rejectPromotionReply.GetPromotion().GetOwner().GetUserId(),
			},
		},
	}
}

// buildShareCampaignReportRequestForRetryPromotionWorkflow maps what RetryPromotionWorkflow has to the request of
// acme.promotions.v1.CampaignReportingService/ShareCampaignReport.
func buildShareCampaignReportRequestForRetryPromotionWorkflow(req *promotionsv1.RetryPromotionWorkflowRequest) *promotionsv1.ShareCampaignReportRequest {
	return &promotionsv1.ShareCampaignReportRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ShareCampaignReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            req.GetPromotion().GetOwner().GetEmail(),
				Name:             req.GetDetails().GetNotify().GetName(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
}

// buildResumePackingSlipWorkflowRequestForDelegateCouponApproval maps what DelegateCouponApproval has to the request of
// acme.fulfillment.v1.PackingSlipWorkflowService/ResumePackingSlipWorkflow.
func buildResumePackingSlipWorkflowRequestForDelegateCouponApproval(req *promotionsv1.DelegateCouponApprovalRequest, getPromotionApprovalStatusReply *promotionsv1.GetPromotionApprovalStatusResponse) *fulfillmentv1.ResumePackingSlipWorkflowRequest {
	return &fulfillmentv1.ResumePackingSlipWorkflowRequest{
		Context: req.GetContext(),
		PackingSlip: &fulfillmentv1.PackingSlip{
			Settlement: &fulfillmentv1.PackingSlip_Bank{Bank: &fulfillmentv1.PackingSlipBankSettlement{Iban: req.GetCoupon().GetBank().GetIban()}},
			Counterparty: &fulfillmentv1.FulfillmentRef{
				DisplayName: getPromotionApprovalStatusReply.GetPromotion().GetOwner().GetDisplayName(),
			},
		},
		Details: &fulfillmentv1.ResumePackingSlipWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: getPromotionApprovalStatusReply.GetPromotion().GetOwner().GetEmail(),
			},
		},
	}
}

// buildPreviewPromotionRequestForPreviewCampaign maps what PreviewCampaign has to the request of
// acme.promotions.v1.PromotionValidationService/PreviewPromotion.
func buildPreviewPromotionRequestForPreviewCampaign(req *promotionsv1.PreviewCampaignRequest, normalizePromotionReply *promotionsv1.NormalizePromotionResponse) *promotionsv1.PreviewPromotionRequest {
	return &promotionsv1.PreviewPromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Action: &promotionsv1.Promotion_Route{Route: &promotionsv1.PromotionRouteAction{
				Weight: normalizePromotionReply.GetPromotion().GetRoute().GetWeight(),
			}},
			Owner: &identityv1.UserRef{Email: req.GetCampaign().GetOrganizer().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizePromotionReply.GetPromotion().GetAudit().GetCreatedBy(),
			},
		},
	}
}

// buildScoreCouponRequestForExplainRedemptionCalculation maps what ExplainRedemptionCalculation has to the request of
// acme.promotions.v1.CouponValidationService/ScoreCoupon.
func buildScoreCouponRequestForExplainRedemptionCalculation(req *promotionsv1.ExplainRedemptionCalculationRequest, validateCouponReply *promotionsv1.ValidateCouponResponse) *promotionsv1.ScoreCouponRequest {
	return &promotionsv1.ScoreCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateCouponReply.GetCoupon().GetAudit().GetCreatedBy(),
			},
			Holder: &promotionsv1.PromotionRef{
				DisplayName: validateCouponReply.GetCoupon().GetHolder().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{
				Phone: validateCouponReply.GetCoupon().GetContact().GetPhone(),
			},
		},
	}
}

// buildNormalizePromotionRequestForCheckCouponEligibility maps what CheckCouponEligibility has to the request of
// acme.promotions.v1.PromotionValidationService/NormalizePromotion.
func buildNormalizePromotionRequestForCheckCouponEligibility(req *promotionsv1.CheckCouponEligibilityRequest, lintPromotionsReply *promotionsv1.LintPromotionsResponse) *promotionsv1.NormalizePromotionRequest {
	return &promotionsv1.NormalizePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintPromotionsReply.GetPromotion().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildSummarizeMetricSeriesesRequestForExplainCouponDecision maps what ExplainCouponDecision has to the request of
// acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses.
func buildSummarizeMetricSeriesesRequestForExplainCouponDecision(req *promotionsv1.ExplainCouponDecisionRequest) *metricsv1.SummarizeMetricSeriesesRequest {
	return &metricsv1.SummarizeMetricSeriesesRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
}

// buildExplainReviewModerationDecisionRequestForPreviewPromotion maps what PreviewPromotion has to the request of
// acme.reviews.v1.ReviewModerationValidationService/ExplainReviewModerationDecision.
func buildExplainReviewModerationDecisionRequestForPreviewPromotion(req *promotionsv1.PreviewPromotionRequest) *reviewsv1.ExplainReviewModerationDecisionRequest {
	return &reviewsv1.ExplainReviewModerationDecisionRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
			Assignee:  &identityv1.UserRef{Email: req.GetPromotion().GetOwner().GetEmail()},
		},
	}
}

// buildEstimatePromotionRequestForNormalizePromotion maps what NormalizePromotion has to the request of
// acme.promotions.v1.PromotionCalculationService/EstimatePromotion.
func buildEstimatePromotionRequestForNormalizePromotion(req *promotionsv1.NormalizePromotionRequest) *promotionsv1.EstimatePromotionRequest {
	return &promotionsv1.EstimatePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetPromotion().GetAudit().GetCreatedAt()},
			Owner: &identityv1.UserRef{Email: req.GetPromotion().GetOwner().GetEmail()},
		},
	}
}

// buildCalculateCouponRequestForLintPromotions maps what LintPromotions has to the request of
// acme.promotions.v1.CouponCalculationService/CalculateCoupon.
func buildCalculateCouponRequestForLintPromotions(req *promotionsv1.LintPromotionsRequest, quotePromotionReply *promotionsv1.QuotePromotionResponse, comparePaymentInstrumentOptionsReply *paymentsv1.ComparePaymentInstrumentOptionsResponse) *promotionsv1.CalculateCouponRequest {
	return &promotionsv1.CalculateCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				CreatedAt: quotePromotionReply.GetPromotion().GetAudit().GetCreatedAt(),
				UpdatedAt: comparePaymentInstrumentOptionsReply.GetPaymentInstrument().GetAudit().GetUpdatedAt(),
				CreatedBy: comparePaymentInstrumentOptionsReply.GetPaymentInstrument().GetAudit().GetCreatedBy(),
			},
			Funding: &promotionsv1.Coupon_Bank{Bank: &promotionsv1.CouponBankFunding{
				AccountHolder: comparePaymentInstrumentOptionsReply.GetPaymentInstrument().GetBank().GetAccountHolder(),
			}},
		},
	}
}

// buildSummarizeToursRequestForReinstateCampaign maps what ReinstateCampaign has to the request of
// acme.tours.v1.TourQueryService/SummarizeTours.
func buildSummarizeToursRequestForReinstateCampaign(req *promotionsv1.ReinstateCampaignRequest) *toursv1.SummarizeToursRequest {
	return &toursv1.SummarizeToursRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
	}
}

// buildGeneratePromotionReportRequestForDeleteRedemptionReport maps what DeleteRedemptionReport has to the request of
// acme.promotions.v1.PromotionReportingService/GeneratePromotionReport.
func buildGeneratePromotionReportRequestForDeleteRedemptionReport(req *promotionsv1.DeleteRedemptionReportRequest, scheduleCouponReportReply *promotionsv1.ScheduleCouponReportResponse) *promotionsv1.GeneratePromotionReportRequest {
	return &promotionsv1.GeneratePromotionReportRequest{
		Context:     req.GetContext(),
		Details:     &promotionsv1.GeneratePromotionReportRequest_Details{Period: scheduleCouponReportReply.GetPeriod()},
		Reason:      req.GetReason(),
		Annotations: scheduleCouponReportReply.GetResult().GetDetails(),
	}
}

// buildValidateUserRequestForCalculatePromotion maps what CalculatePromotion has to the request of
// acme.identity.v1.UserValidationService/ValidateUser.
func buildValidateUserRequestForCalculatePromotion(req *promotionsv1.CalculatePromotionRequest) *identityv1.ValidateUserRequest {
	return &identityv1.ValidateUserRequest{
		Context: req.GetContext(),
		Details: &identityv1.ValidateUserRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetPromotion().GetOwner().GetEmail()},
		},
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPromotion().GetAudit().GetEtag(),
				UpdatedBy: req.GetPromotion().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildUnsubscribeTenantUpdatesRequestForEstimatePromotion maps what EstimatePromotion has to the request of
// acme.identity.v1.TenantNotificationService/UnsubscribeTenantUpdates.
func buildUnsubscribeTenantUpdatesRequestForEstimatePromotion(req *promotionsv1.EstimatePromotionRequest) *identityv1.UnsubscribeTenantUpdatesRequest {
	return &identityv1.UnsubscribeTenantUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeTenantUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetPromotion().GetOwner().GetEmail()},
			Variables: req.GetPromotion().GetParameters(),
		},
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
	}
}

// buildRejectOrderAmendmentRequestForRenewCoupon maps what RenewCoupon has to the request of
// acme.orders.v1.OrderAmendmentApprovalService/RejectOrderAmendment.
func buildRejectOrderAmendmentRequestForRenewCoupon(req *promotionsv1.RenewCouponRequest) *ordersv1.RejectOrderAmendmentRequest {
	return &ordersv1.RejectOrderAmendmentRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &ordersv1.RejectOrderAmendmentRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
}

// buildResolveRedemptionConflictRequestForExpireCoupon maps what ExpireCoupon has to the request of
// acme.promotions.v1.RedemptionSyncService/ResolveRedemptionConflict.
func buildResolveRedemptionConflictRequestForExpireCoupon(req *promotionsv1.ExpireCouponRequest) *promotionsv1.ResolveRedemptionConflictRequest {
	return &promotionsv1.ResolveRedemptionConflictRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
	}
}

// buildResolveWorkOrdersRequestForExpireCoupon maps what ExpireCoupon has to the request of
// acme.maintenance.v1.WorkOrderQueryService/ResolveWorkOrders.
func buildResolveWorkOrdersRequestForExpireCoupon(req *promotionsv1.ExpireCouponRequest) *maintenancev1.ResolveWorkOrdersRequest {
	return &maintenancev1.ResolveWorkOrdersRequest{Context: req.GetContext()}
}

// buildRecallCheckoutSessionRequestForShareCampaignReport maps what ShareCampaignReport has to the request of
// acme.checkout.v1.CheckoutSessionApprovalService/RecallCheckoutSession.
func buildRecallCheckoutSessionRequestForShareCampaignReport(req *promotionsv1.ShareCampaignReportRequest) *checkoutv1.RecallCheckoutSessionRequest {
	return &checkoutv1.RecallCheckoutSessionRequest{
		Context: req.GetContext(),
		Details: &checkoutv1.RecallCheckoutSessionRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}
}

// buildScoreNotificationRequestForReconcileRedemptions maps what ReconcileRedemptions has to the request of
// acme.notifications.v1.NotificationValidationService/ScoreNotification.
func buildScoreNotificationRequestForReconcileRedemptions(req *promotionsv1.ReconcileRedemptionsRequest) *notificationsv1.ScoreNotificationRequest {
	return &notificationsv1.ScoreNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Origin: &notificationsv1.Notification_Customer{Customer: &notificationsv1.NotificationTemplateRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			}},
		},
	}
}

// buildPullCampaignsRequestForActivatePromotion maps what ActivatePromotion has to the request of
// acme.promotions.v1.CampaignSyncService/PullCampaigns.
func buildPullCampaignsRequestForActivatePromotion(req *promotionsv1.ActivatePromotionRequest, explainCouponCalculationReply *promotionsv1.ExplainCouponCalculationResponse) *promotionsv1.PullCampaignsRequest {
	return &promotionsv1.PullCampaignsRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Email: req.GetDetails().GetNewOwner().GetEmail(),
					Name:  explainCouponCalculationReply.GetCoupon().GetContact().GetName(),
				},
			}},
		},
	}
}

// buildRecallSellerScorecardRequestForGenerateCouponReport maps what GenerateCouponReport has to the request of
// acme.sellers.v1.SellerScorecardApprovalService/RecallSellerScorecard.
func buildRecallSellerScorecardRequestForGenerateCouponReport(req *promotionsv1.GenerateCouponReportRequest, scoreLedgerAccountReply *ledgerv1.ScoreLedgerAccountResponse) *sellersv1.RecallSellerScorecardRequest {
	return &sellersv1.RecallSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildExplainRedemptionDecisionRequestForDeleteCouponReport maps what DeleteCouponReport has to the request of
// acme.promotions.v1.RedemptionValidationService/ExplainRedemptionDecision.
func buildExplainRedemptionDecisionRequestForDeleteCouponReport(req *promotionsv1.DeleteCouponReportRequest) *promotionsv1.ExplainRedemptionDecisionRequest {
	return &promotionsv1.ExplainRedemptionDecisionRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &promotionsv1.ExplainRedemptionDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildSignalProductReviewWorkflowRequestForSyncCampaigns maps what SyncCampaigns has to the request of
// acme.reviews.v1.ProductReviewWorkflowService/SignalProductReviewWorkflow.
func buildSignalProductReviewWorkflowRequestForSyncCampaigns(req *promotionsv1.SyncCampaignsRequest) *reviewsv1.SignalProductReviewWorkflowRequest {
	return &reviewsv1.SignalProductReviewWorkflowRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			Audit:          &commonv1.AuditInfo{UpdatedBy: req.GetCampaign().GetAudit().GetUpdatedBy()},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetCampaign().GetLocation().GetRegion()},
		},
	}
}

// buildDelegateStockReservationApprovalRequestForFetchPromotionReport maps what FetchPromotionReport has to the request
// of acme.inventory.v1.StockReservationApprovalService/DelegateStockReservationApproval.
func buildDelegateStockReservationApprovalRequestForFetchPromotionReport(req *promotionsv1.FetchPromotionReportRequest) *inventoryv1.DelegateStockReservationApprovalRequest {
	return &inventoryv1.DelegateStockReservationApprovalRequest{
		Context: req.GetContext(),
		StockReservation: &inventoryv1.StockReservation{
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact:       &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
				ParticipantId: req.GetPromotionId(),
			}},
		},
		Details: &inventoryv1.DelegateStockReservationApprovalRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
}
