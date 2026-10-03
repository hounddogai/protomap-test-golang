package customers

import (
	assetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/assets/v1"
	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	expensesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/expenses/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	keymanagementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/keymanagement/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	lineagev1 "github.com/hounddogai/protomap-test-golang/gen/acme/lineage/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	messagingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/messaging/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
)

// buildCheckHouseholdEligibilityRequestForValidatePreference maps what ValidatePreference has to the request of
// acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility.
func buildCheckHouseholdEligibilityRequestForValidatePreference(req *customersv1.ValidatePreferenceRequest, validateCustomerReply *customersv1.ValidateCustomerResponse, lintCustomerSegmentsReply *customersv1.LintCustomerSegmentsResponse, normalizePaymentReply *paymentsv1.NormalizePaymentResponse) *customersv1.CheckHouseholdEligibilityRequest {
	return &customersv1.CheckHouseholdEligibilityRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						PostalCode: validateCustomerReply.GetCustomer().GetPrimaryAddress().GetPostalCode(),
						Recipient:  normalizePaymentReply.GetPayment().GetBillingAddress().GetRecipient(),
						Lines:      normalizePaymentReply.GetPayment().GetBillingAddress().GetLines(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedAt: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
					},
				}},
			}},
		},
	}
}

// buildValidateCustomerSegmentRequestForNormalizePreference maps what NormalizePreference has to the request of
// acme.customers.v1.CustomerSegmentValidationService/ValidateCustomerSegment.
func buildValidateCustomerSegmentRequestForNormalizePreference(req *customersv1.NormalizePreferenceRequest) *customersv1.ValidateCustomerSegmentRequest {
	return &customersv1.ValidateCustomerSegmentRequest{
		Context: req.GetContext(),
		Details: &customersv1.ValidateCustomerSegmentRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Email:            req.GetPreference().GetOwner().GetEmail(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
}

// buildBrowseLoanApplicationsRequestForNormalizePreference maps what NormalizePreference has to the request of
// acme.lending.v1.LoanApplicationQueryService/BrowseLoanApplications.
func buildBrowseLoanApplicationsRequestForNormalizePreference(req *customersv1.NormalizePreferenceRequest, validateCustomerSegmentReply *customersv1.ValidateCustomerSegmentResponse, checkUserEligibilityReply *identityv1.CheckUserEligibilityResponse) *lendingv1.BrowseLoanApplicationsRequest {
	return &lendingv1.BrowseLoanApplicationsRequest{
		Context: req.GetContext(),
		Filter: &lendingv1.BrowseLoanApplicationsRequest_Filter{
			Region:       checkUserEligibilityReply.GetUser().GetPrimaryAddress().GetRegion(),
			CreatedAfter: validateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
		},
	}
}

// collectHouseholdNames lists the names of a household and of everything under it.
func collectHouseholdNames(node *customersv1.Household, names []string) []string {
	if node == nil {
		return names
	}
	names = append(names, node.GetName())
	for _, child := range node.GetChildren() {
		names = collectHouseholdNames(child, names)
	}
	return names
}

// buildAdvanceStockMovementWorkflowRequestForCancelHouseholdWorkflow maps what CancelHouseholdWorkflow has to the
// request of acme.inventory.v1.StockMovementWorkflowService/AdvanceStockMovementWorkflow.
func buildAdvanceStockMovementWorkflowRequestForCancelHouseholdWorkflow(req *customersv1.CancelHouseholdWorkflowRequest) *inventoryv1.AdvanceStockMovementWorkflowRequest {
	return &inventoryv1.AdvanceStockMovementWorkflowRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.AdvanceStockMovementWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNotify().GetEmail(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetHousehold().GetManager().GetDisplayName(),
			},
		},
	}
}

// buildBatchGetCarePlansRequestForRetryHouseholdWorkflow maps what RetryHouseholdWorkflow has to the request of
// acme.careplans.v1.CarePlanService/BatchGetCarePlans.
func buildBatchGetCarePlansRequestForRetryHouseholdWorkflow(req *customersv1.RetryHouseholdWorkflowRequest) *careplansv1.BatchGetCarePlansRequest {
	return &careplansv1.BatchGetCarePlansRequest{Context: req.GetContext()}
}

// buildExplainPriceQuoteDecisionRequestForRecalculatePreferences maps what RecalculatePreferences has to the request of
// acme.pricing.v1.PriceQuoteValidationService/ExplainPriceQuoteDecision.
func buildExplainPriceQuoteDecisionRequestForRecalculatePreferences(req *customersv1.RecalculatePreferencesRequest) *pricingv1.ExplainPriceQuoteDecisionRequest {
	return &pricingv1.ExplainPriceQuoteDecisionRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPreference().GetAudit().GetEtag(),
				CreatedAt: req.GetPreference().GetAudit().GetCreatedAt(),
			},
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: req.GetPreference().GetOwner().GetDisplayName(),
			},
		},
		Details: &pricingv1.ExplainPriceQuoteDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetPreference().GetOwner().GetEmail()},
		},
	}
}

// buildUnsubscribeRoleUpdatesRequestForStartCustomerWorkflow maps what StartCustomerWorkflow has to the request of
// acme.identity.v1.RoleNotificationService/UnsubscribeRoleUpdates.
func buildUnsubscribeRoleUpdatesRequestForStartCustomerWorkflow(req *customersv1.StartCustomerWorkflowRequest) *identityv1.UnsubscribeRoleUpdatesRequest {
	return &identityv1.UnsubscribeRoleUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeRoleUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotify().GetPhone(),
				Email: req.GetDetails().GetNotify().GetEmail(),
			},
		},
	}
}

// buildExplainCustomerCalculationRequestForEstimateCustomerSegment maps what EstimateCustomerSegment has to the request
// of acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation.
func buildExplainCustomerCalculationRequestForEstimateCustomerSegment(req *customersv1.EstimateCustomerSegmentRequest, validateCustomerReply *customersv1.ValidateCustomerResponse) *customersv1.ExplainCustomerCalculationRequest {
	return &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{
				IssuingCountry: validateCustomerReply.GetCustomer().GetNationalId().GetIssuingCountry(),
				Number:         validateCustomerReply.GetCustomer().GetPassport().GetNumber(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: validateCustomerReply.GetCustomer().GetAudit().GetEtag(),
			},
			Preferences: &customersv1.CustomerPreferences{
				Language: validateCustomerReply.GetCustomer().GetPreferences().GetLanguage(),
			},
		},
	}
}

// buildCountExpenseReportsRequestForCompareCustomerSegmentOptions maps what CompareCustomerSegmentOptions has to the
// request of acme.expenses.v1.ExpenseReportQueryService/CountExpenseReports.
func buildCountExpenseReportsRequestForCompareCustomerSegmentOptions(req *customersv1.CompareCustomerSegmentOptionsRequest) *expensesv1.CountExpenseReportsRequest {
	return &expensesv1.CountExpenseReportsRequest{
		Context: req.GetContext(),
		Filter:  &expensesv1.CountExpenseReportsRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	}
}

// buildCheckCustomerEligibilityRequestForCompareCustomerSegmentOptions maps what CompareCustomerSegmentOptions has to
// the request of acme.customers.v1.CustomerValidationService/CheckCustomerEligibility.
func buildCheckCustomerEligibilityRequestForCompareCustomerSegmentOptions(req *customersv1.CompareCustomerSegmentOptionsRequest, cloneProductReviewReply *reviewsv1.CloneProductReviewResponse) *customersv1.CheckCustomerEligibilityRequest {
	return &customersv1.CheckCustomerEligibilityRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
				Email:       cloneProductReviewReply.GetProductReview().GetCounterparty().GetEmail(),
			},
		},
	}
}

// buildAdvanceOrderWorkflowRequestForRenewPreference maps what RenewPreference has to the request of
// acme.orders.v1.OrderWorkflowService/AdvanceOrderWorkflow.
func buildAdvanceOrderWorkflowRequestForRenewPreference(req *customersv1.RenewPreferenceRequest) *ordersv1.AdvanceOrderWorkflowRequest {
	return &ordersv1.AdvanceOrderWorkflowRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
}

// buildCompareHouseholdOptionsRequestForExpirePreference maps what ExpirePreference has to the request of
// acme.customers.v1.HouseholdCalculationService/CompareHouseholdOptions.
func buildCompareHouseholdOptionsRequestForExpirePreference(req *customersv1.ExpirePreferenceRequest, quoteFixedAssetReply *assetsv1.QuoteFixedAssetResponse) *customersv1.CompareHouseholdOptionsRequest {
	return &customersv1.CompareHouseholdOptionsRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: quoteFixedAssetReply.GetFixedAsset().GetAudit().GetUpdatedBy(),
					},
					Manager: &identityv1.UserRef{
						DisplayName: quoteFixedAssetReply.GetFixedAsset().GetOwner().GetDisplayName(),
					},
				}},
			}},
		},
	}
}

// buildResolveConsentRecordConflictRequestForGetCustomerApprovalStatus maps what GetCustomerApprovalStatus has to the
// request of acme.customers.v1.ConsentRecordSyncService/ResolveConsentRecordConflict.
func buildResolveConsentRecordConflictRequestForGetCustomerApprovalStatus(req *customersv1.GetCustomerApprovalStatusRequest) *customersv1.ResolveConsentRecordConflictRequest {
	return &customersv1.ResolveConsentRecordConflictRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId: req.GetCustomerId(),
			Measurements:  []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: req.GetContext().GetDeadline()}},
			Source:        req.GetContext().GetActor().GetUserId(),
		},
	}
}

// buildCommentRedemptionApprovalRequestForCommentCustomerApproval maps what CommentCustomerApproval has to the request
// of acme.promotions.v1.RedemptionApprovalService/CommentRedemptionApproval.
func buildCommentRedemptionApprovalRequestForCommentCustomerApproval(req *customersv1.CommentCustomerApprovalRequest) *promotionsv1.CommentRedemptionApprovalRequest {
	return &promotionsv1.CommentRedemptionApprovalRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.CommentRedemptionApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetCustomer().GetContact().GetEmail()},
		},
	}
}

// buildCloneSoftwareLicenseRequestForEstimateConsentRecord maps what EstimateConsentRecord has to the request of
// acme.licensing.v1.SoftwareLicenseService/CloneSoftwareLicense.
func buildCloneSoftwareLicenseRequestForEstimateConsentRecord(req *customersv1.EstimateConsentRecordRequest) *licensingv1.CloneSoftwareLicenseRequest {
	return &licensingv1.CloneSoftwareLicenseRequest{
		Context: req.GetContext(),
		SoftwareLicense: &licensingv1.SoftwareLicense{
			Holder: &licensingv1.LicenseAssignmentRef{
				DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			CurrencyCode: req.GetDetails().GetCurrencyCode(),
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetConsentRecord().GetOccurredAt()},
		},
	}
}

// buildCompareTrackingEventOptionsRequestForQuoteConsentRecord maps what QuoteConsentRecord has to the request of
// acme.shipping.v1.TrackingEventCalculationService/CompareTrackingEventOptions.
func buildCompareTrackingEventOptionsRequestForQuoteConsentRecord(req *customersv1.QuoteConsentRecordRequest) *shippingv1.CompareTrackingEventOptionsRequest {
	return &shippingv1.CompareTrackingEventOptionsRequest{
		Context:       req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{OccurredAt: req.GetConsentRecord().GetOccurredAt()},
	}
}

// buildResolveLineageNodesRequestForScheduleCustomerSegmentExpiry maps what ScheduleCustomerSegmentExpiry has to the
// request of acme.lineage.v1.LineageNodeQueryService/ResolveLineageNodes.
func buildResolveLineageNodesRequestForScheduleCustomerSegmentExpiry(req *customersv1.ScheduleCustomerSegmentExpiryRequest) *lineagev1.ResolveLineageNodesRequest {
	return &lineagev1.ResolveLineageNodesRequest{Context: req.GetContext()}
}

// buildSimulateCustomerRequestForCalculateHousehold maps what CalculateHousehold has to the request of
// acme.customers.v1.CustomerCalculationService/SimulateCustomer.
func buildSimulateCustomerRequestForCalculateHousehold(req *customersv1.CalculateHouseholdRequest, explainCustomerCalculationReply *customersv1.ExplainCustomerCalculationResponse) *customersv1.SimulateCustomerRequest {
	return &customersv1.SimulateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{Locality: req.GetHousehold().GetLocation().GetLocality()},
			IdentityDocument: &customersv1.Customer_NationalId{NationalId: &customersv1.CustomerNationalId{
				Number: explainCustomerCalculationReply.GetCustomer().GetPassport().GetNumber(),
			}},
		},
	}
}

// buildCalculateCustomerRequestForRecalculateHouseholds maps what RecalculateHouseholds has to the request of
// acme.customers.v1.CustomerCalculationService/CalculateCustomer.
func buildCalculateCustomerRequestForRecalculateHouseholds(req *customersv1.RecalculateHouseholdsRequest) *customersv1.CalculateCustomerRequest {
	return &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetHousehold().GetManager().GetDisplayName(),
			},
		},
	}
}

// buildSearchNotificationsRequestForSimulateHousehold maps what SimulateHousehold has to the request of
// acme.notifications.v1.NotificationService/SearchNotifications.
func buildSearchNotificationsRequestForSimulateHousehold(req *customersv1.SimulateHouseholdRequest, calculateCustomerReply *customersv1.CalculateCustomerResponse) *notificationsv1.SearchNotificationsRequest {
	return &notificationsv1.SearchNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.SearchNotificationsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: calculateCustomerReply.GetCustomer().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetHousehold().GetHouseholdId(),
			Labels:       req.GetAnnotations(),
		},
	}
}

// buildSimulatePayRunRequestForReinstateConsentRecord maps what ReinstateConsentRecord has to the request of
// acme.payroll.v1.PayRunCalculationService/SimulatePayRun.
func buildSimulatePayRunRequestForReinstateConsentRecord(req *customersv1.ReinstateConsentRecordRequest) *payrollv1.SimulatePayRunRequest {
	return &payrollv1.SimulatePayRunRequest{
		Context: req.GetContext(),
		PayRun: &payrollv1.PayRun{
			Counterparty: &payrollv1.DeductionRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetDetails().GetEffectiveAt()},
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
}

// buildCommentCampaignApprovalRequestForRenewConsentRecord maps what RenewConsentRecord has to the request of
// acme.promotions.v1.CampaignApprovalService/CommentCampaignApproval.
func buildCommentCampaignApprovalRequestForRenewConsentRecord(req *customersv1.RenewConsentRecordRequest, previewApiCredentialReply *identityv1.PreviewApiCredentialResponse) *promotionsv1.CommentCampaignApprovalRequest {
	return &promotionsv1.CommentCampaignApprovalRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			}},
		},
		Details: &promotionsv1.CommentCampaignApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: previewApiCredentialReply.GetApiCredential().GetHolder().GetDisplayName(),
			},
		},
	}
}

// buildResumePromotionWorkflowRequestForScheduleConsentRecordExpiry maps what ScheduleConsentRecordExpiry has to the
// request of acme.promotions.v1.PromotionWorkflowService/ResumePromotionWorkflow.
func buildResumePromotionWorkflowRequestForScheduleConsentRecordExpiry(req *customersv1.ScheduleConsentRecordExpiryRequest) *promotionsv1.ResumePromotionWorkflowRequest {
	return &promotionsv1.ResumePromotionWorkflowRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ResumePromotionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
}

// buildRejectReturnRequestRequestForFetchCustomerSegmentReport maps what FetchCustomerSegmentReport has to the request
// of acme.returns.v1.ReturnRequestApprovalService/RejectReturnRequest.
func buildRejectReturnRequestRequestForFetchCustomerSegmentReport(req *customersv1.FetchCustomerSegmentReportRequest, updatePolicyReply *insurancev1.UpdatePolicyResponse) *returnsv1.RejectReturnRequestRequest {
	return &returnsv1.RejectReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: updatePolicyReply.GetPolicy().GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: updatePolicyReply.GetPolicy().GetAudit().GetCreatedBy(),
			},
		},
	}
}

// buildScoreNotificationRequestForListCustomerSegmentReports maps what ListCustomerSegmentReports has to the request of
// acme.notifications.v1.NotificationValidationService/ScoreNotification.
func buildScoreNotificationRequestForListCustomerSegmentReports(req *customersv1.ListCustomerSegmentReportsRequest) *notificationsv1.ScoreNotificationRequest {
	return &notificationsv1.ScoreNotificationRequest{
		Context: req.GetContext(),
		Details: &notificationsv1.ScoreNotificationRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Notification: &notificationsv1.Notification{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildResolveLedgerAccountsRequestForReconcilePreferences maps what ReconcilePreferences has to the request of
// acme.ledger.v1.LedgerAccountQueryService/ResolveLedgerAccounts.
func buildResolveLedgerAccountsRequestForReconcilePreferences(req *customersv1.ReconcilePreferencesRequest) *ledgerv1.ResolveLedgerAccountsRequest {
	return &ledgerv1.ResolveLedgerAccountsRequest{Context: req.GetContext()}
}

// buildBrowsePromotionsRequestForCalculateCustomer maps what CalculateCustomer has to the request of
// acme.promotions.v1.PromotionQueryService/BrowsePromotions.
func buildBrowsePromotionsRequestForCalculateCustomer(req *customersv1.CalculateCustomerRequest) *promotionsv1.BrowsePromotionsRequest {
	return &promotionsv1.BrowsePromotionsRequest{
		Context: req.GetContext(),
		Filter:  &promotionsv1.BrowsePromotionsRequest_Filter{Region: req.GetCustomer().GetPrimaryAddress().GetRegion()},
	}
}

// buildFindCouponsRequestForSimulateCustomer maps what SimulateCustomer has to the request of
// acme.promotions.v1.CouponQueryService/FindCoupons.
func buildFindCouponsRequestForSimulateCustomer(req *customersv1.SimulateCustomerRequest, repairSearchIndexReply *searchv1.RepairSearchIndexResponse) *promotionsv1.FindCouponsRequest {
	return &promotionsv1.FindCouponsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.FindCouponsRequest_Filter{
			Region:       repairSearchIndexReply.GetSearchIndex().GetAddress().GetRegion(),
			CreatedAfter: req.GetCustomer().GetAudit().GetCreatedAt(),
			Labels:       repairSearchIndexReply.GetSearchIndex().GetSpecs(),
			OwnerId:      req.GetCustomer().GetCustomerId(),
		},
	}
}

// buildCloneConversationRequestForGetCustomerRates maps what GetCustomerRates has to the request of
// acme.messaging.v1.ConversationService/CloneConversation.
func buildCloneConversationRequestForGetCustomerRates(req *customersv1.GetCustomerRatesRequest, suggestCryptoKeysReply *keymanagementv1.SuggestCryptoKeysResponse) *messagingv1.CloneConversationRequest {
	return &messagingv1.CloneConversationRequest{
		Context: req.GetContext(),
		Conversation: &messagingv1.Conversation{
			Assignee:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Subject:   suggestCryptoKeysReply.GetResult().GetCorrelationId(),
		},
	}
}

// buildCheckNotificationEligibilityRequestForStreamConsentRecordMetrics maps what StreamConsentRecordMetrics has to the
// request of acme.notifications.v1.NotificationValidationService/CheckNotificationEligibility.
func buildCheckNotificationEligibilityRequestForStreamConsentRecordMetrics(req *customersv1.StreamConsentRecordMetricsRequest, recalculateConsentRecordsReply *customersv1.RecalculateConsentRecordsResponse, rejectRewardReply *loyaltyv1.RejectRewardResponse) *notificationsv1.CheckNotificationEligibilityRequest {
	return &notificationsv1.CheckNotificationEligibilityRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Assignee: &identityv1.UserRef{
				DisplayName: recalculateConsentRecordsReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: rejectRewardReply.GetReward().GetAudit().GetCreatedAt(),
			},
		},
	}
}

// buildPushSynonymRequestForListConsentRecordReports maps what ListConsentRecordReports has to the request of
// acme.search.v1.SynonymSyncService/PushSynonym.
func buildPushSynonymRequestForListConsentRecordReports(req *customersv1.ListConsentRecordReportsRequest, resolveConsentRecordConflictReply *customersv1.ResolveConsentRecordConflictResponse) *searchv1.PushSynonymRequest {
	return &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: resolveConsentRecordConflictReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
		},
	}
}

// buildQuotePreferenceRequestForExpireCustomer maps what ExpireCustomer has to the request of
// acme.customers.v1.PreferenceCalculationService/QuotePreference.
func buildQuotePreferenceRequestForExpireCustomer(req *customersv1.ExpireCustomerRequest) *customersv1.QuotePreferenceRequest {
	return &customersv1.QuotePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNewOwner().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
}

// buildArchiveTenantRequestForExpireCustomer maps what ExpireCustomer has to the request of
// acme.identity.v1.TenantAdminService/ArchiveTenant.
func buildArchiveTenantRequestForExpireCustomer(req *customersv1.ExpireCustomerRequest, quotePreferenceReply *customersv1.QuotePreferenceResponse) *identityv1.ArchiveTenantRequest {
	return &identityv1.ArchiveTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ArchiveTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:  quotePreferenceReply.GetPreference().GetOwner().GetEmail(),
				UserId: quotePreferenceReply.GetPreference().GetOwner().GetUserId(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotifyContact().GetName(),
				Email: req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
	}
}

// buildRetryRenewalScheduleWorkflowRequestForCancelCustomerSegmentImport maps what CancelCustomerSegmentImport has to
// the request of acme.subscriptions.v1.RenewalScheduleWorkflowService/RetryRenewalScheduleWorkflow.
func buildRetryRenewalScheduleWorkflowRequestForCancelCustomerSegmentImport(req *customersv1.CancelCustomerSegmentImportRequest, recordUserVersionReply *identityv1.RecordUserVersionResponse) *subscriptionsv1.RetryRenewalScheduleWorkflowRequest {
	return &subscriptionsv1.RetryRenewalScheduleWorkflowRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
					PreferredChannel: recordUserVersionReply.GetUser().GetContact().GetPreferredChannel(),
				},
			}},
		},
	}
}
