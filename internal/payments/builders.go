package payments

import (
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	lineagev1 "github.com/hounddogai/protomap-test-golang/gen/acme/lineage/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	performancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	prescriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/prescriptions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
)

// buildPreviewSearchQueryRequestForDeletePayment maps what DeletePayment has to the request of
// acme.search.v1.SearchQueryValidationService/PreviewSearchQuery.
func buildPreviewSearchQueryRequestForDeletePayment(req *paymentsv1.DeletePaymentRequest) *searchv1.PreviewSearchQueryRequest {
	return &searchv1.PreviewSearchQueryRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &searchv1.PreviewSearchQueryRequest_Details{
			Locale:  req.GetContext().GetLocale(),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		SearchQuery: &searchv1.SearchQuery{
			Measurements: []*searchv1.SearchQueryMeasurement{&searchv1.SearchQueryMeasurement{ObservedAt: req.GetContext().GetDeadline()}},
		},
	}
}

// buildCheckSellerEligibilityRequestForPeekRefund maps what PeekRefund has to the request of
// acme.sellers.v1.SellerValidationService/CheckSellerEligibility.
func buildCheckSellerEligibilityRequestForPeekRefund(req *paymentsv1.PeekRefundRequest) *sellersv1.CheckSellerEligibilityRequest {
	return &sellersv1.CheckSellerEligibilityRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact:             &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &sellersv1.CheckSellerEligibilityRequest_Details{Locale: req.GetContext().GetLocale()},
	}
}

// buildMapDisputeImportColumnsRequestForAdvancePaymentInstrumentWorkflow maps what AdvancePaymentInstrumentWorkflow has
// to the request of acme.payments.v1.DisputeImportService/MapDisputeImportColumns.
func buildMapDisputeImportColumnsRequestForAdvancePaymentInstrumentWorkflow(req *paymentsv1.AdvancePaymentInstrumentWorkflowRequest) *paymentsv1.MapDisputeImportColumnsRequest {
	return &paymentsv1.MapDisputeImportColumnsRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetPaymentInstrument().GetContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetNotify().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildCheckCustomerEligibilityRequestForCancelPaymentInstrumentWorkflow maps what CancelPaymentInstrumentWorkflow has
// to the request of acme.customers.v1.CustomerValidationService/CheckCustomerEligibility.
func buildCheckCustomerEligibilityRequestForCancelPaymentInstrumentWorkflow(req *paymentsv1.CancelPaymentInstrumentWorkflowRequest) *customersv1.CheckCustomerEligibilityRequest {
	return &customersv1.CheckCustomerEligibilityRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			Contact: &commonv1.ContactInfo{
				Phone: req.GetPaymentInstrument().GetContact().GetPhone(),
				Name:  req.GetDetails().GetNotify().GetName(),
			},
		},
	}
}

// buildSubmitPaymentInstrumentRequestForSubmitPayout maps what SubmitPayout has to the request of
// acme.payments.v1.PaymentInstrumentApprovalService/SubmitPaymentInstrument.
func buildSubmitPaymentInstrumentRequestForSubmitPayout(req *paymentsv1.SubmitPayoutRequest) *paymentsv1.SubmitPaymentInstrumentRequest {
	return &paymentsv1.SubmitPaymentInstrumentRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		Details: &paymentsv1.SubmitPaymentInstrumentRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetPayout().GetCounterparty().GetDisplayName(),
			},
		},
	}
}

// buildCheckPreferenceEligibilityRequestForSubmitPayout maps what SubmitPayout has to the request of
// acme.customers.v1.PreferenceValidationService/CheckPreferenceEligibility.
func buildCheckPreferenceEligibilityRequestForSubmitPayout(req *paymentsv1.SubmitPayoutRequest, submitPaymentInstrumentReply *paymentsv1.SubmitPaymentInstrumentResponse) *customersv1.CheckPreferenceEligibilityRequest {
	return &customersv1.CheckPreferenceEligibilityRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPayout().GetAudit().GetCreatedAt(),
				Etag:      submitPaymentInstrumentReply.GetPaymentInstrument().GetAudit().GetEtag(),
			},
		},
	}
}

// buildRecalculatePaymentsRequestForLintDisputes maps what LintDisputes has to the request of
// acme.payments.v1.PaymentCalculationService/RecalculatePayments.
func buildRecalculatePaymentsRequestForLintDisputes(req *paymentsv1.LintDisputesRequest) *paymentsv1.RecalculatePaymentsRequest {
	return &paymentsv1.RecalculatePaymentsRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{Number: req.GetDispute().GetCustomer().GetNumber()},
		},
	}
}

// buildCalculatePayoutRequestForLintDisputes maps what LintDisputes has to the request of
// acme.payments.v1.PayoutCalculationService/CalculatePayout.
func buildCalculatePayoutRequestForLintDisputes(req *paymentsv1.LintDisputesRequest) *paymentsv1.CalculatePayoutRequest {
	return &paymentsv1.CalculatePayoutRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{Etag: req.GetDispute().GetAudit().GetEtag()},
		},
	}
}

// buildFindStorageBinsRequestForSuggestPayments maps what SuggestPayments has to the request of
// acme.warehousing.v1.StorageBinQueryService/FindStorageBins.
func buildFindStorageBinsRequestForSuggestPayments(req *paymentsv1.SuggestPaymentsRequest) *warehousingv1.FindStorageBinsRequest {
	return &warehousingv1.FindStorageBinsRequest{
		Context: req.GetContext(),
		Filter:  &warehousingv1.FindStorageBinsRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()},
		Page:    &commonv1.PageRequest{PageSize: req.GetPage().GetPageSize()},
	}
}

// buildBrowseAccessPoliciesRequestForPauseRefundWorkflow maps what PauseRefundWorkflow has to the request of
// acme.authorization.v1.AccessPolicyQueryService/BrowseAccessPolicies.
func buildBrowseAccessPoliciesRequestForPauseRefundWorkflow(req *paymentsv1.PauseRefundWorkflowRequest, pullPaymentInstrumentsReply *paymentsv1.PullPaymentInstrumentsResponse) *authorizationv1.BrowseAccessPoliciesRequest {
	return &authorizationv1.BrowseAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.BrowseAccessPoliciesRequest_Filter{
			Region: req.GetRefund().GetBillingAddress().GetRegion(),
			Labels: pullPaymentInstrumentsReply.GetPaymentInstrument().GetLabels(),
		},
	}
}

// buildResolveNotificationsRequestForSubmitPaymentInstrument maps what SubmitPaymentInstrument has to the request of
// acme.notifications.v1.NotificationQueryService/ResolveNotifications.
func buildResolveNotificationsRequestForSubmitPaymentInstrument(req *paymentsv1.SubmitPaymentInstrumentRequest) *notificationsv1.ResolveNotificationsRequest {
	return &notificationsv1.ResolveNotificationsRequest{Context: req.GetContext()}
}

// buildListPrescriptionsRequestForApprovePaymentInstrument maps what ApprovePaymentInstrument has to the request of
// acme.prescriptions.v1.PrescriptionService/ListPrescriptions.
func buildListPrescriptionsRequestForApprovePaymentInstrument(req *paymentsv1.ApprovePaymentInstrumentRequest) *prescriptionsv1.ListPrescriptionsRequest {
	return &prescriptionsv1.ListPrescriptionsRequest{
		Context: req.GetContext(),
		Filter:  &prescriptionsv1.ListPrescriptionsRequest_Filter{Labels: req.GetPaymentInstrument().GetLabels()},
	}
}

// buildScoreConsentRecordRequestForRecallPaymentInstrument maps what RecallPaymentInstrument has to the request of
// acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord.
func buildScoreConsentRecordRequestForRecallPaymentInstrument(req *paymentsv1.RecallPaymentInstrumentRequest) *customersv1.ScoreConsentRecordRequest {
	return &customersv1.ScoreConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
}

// buildBrowseReviewsRequestForGetRefundApprovalStatus maps what GetRefundApprovalStatus has to the request of
// acme.performance.v1.ReviewQueryService/BrowseReviews.
func buildBrowseReviewsRequestForGetRefundApprovalStatus(req *paymentsv1.GetRefundApprovalStatusRequest) *performancev1.BrowseReviewsRequest {
	return &performancev1.BrowseReviewsRequest{
		Context: req.GetContext(),
		Filter: &performancev1.BrowseReviewsRequest_Filter{
			OwnerId:      req.GetRefundId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
}

// buildCalculatePaymentInstrumentRequestForCalculatePayout maps what CalculatePayout has to the request of
// acme.payments.v1.PaymentInstrumentCalculationService/CalculatePaymentInstrument.
func buildCalculatePaymentInstrumentRequestForCalculatePayout(req *paymentsv1.CalculatePayoutRequest) *paymentsv1.CalculatePaymentInstrumentRequest {
	return &paymentsv1.CalculatePaymentInstrumentRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Funding: &paymentsv1.PaymentInstrument_Card{Card: &paymentsv1.PaymentInstrumentCardFunding{HolderName: req.GetPayout().GetCard().GetHolderName()}},
		},
		Details: &paymentsv1.CalculatePaymentInstrumentRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	}
}

// buildValidatePreferenceRequestForExpireDispute maps what ExpireDispute has to the request of
// acme.customers.v1.PreferenceValidationService/ValidatePreference.
func buildValidatePreferenceRequestForExpireDispute(req *paymentsv1.ExpireDisputeRequest) *customersv1.ValidatePreferenceRequest {
	return &customersv1.ValidatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}
}

// buildNotifyTenantWatchersRequestForGetPaymentApprovalStatus maps what GetPaymentApprovalStatus has to the request of
// acme.identity.v1.TenantNotificationService/NotifyTenantWatchers.
func buildNotifyTenantWatchersRequestForGetPaymentApprovalStatus(req *paymentsv1.GetPaymentApprovalStatusRequest) *identityv1.NotifyTenantWatchersRequest {
	return &identityv1.NotifyTenantWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyTenantWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		TenantId: req.GetContext().GetTenantId(),
	}
}

// buildQuotePaymentRequestForCheckRefundEligibility maps what CheckRefundEligibility has to the request of
// acme.payments.v1.PaymentCalculationService/QuotePayment.
func buildQuotePaymentRequestForCheckRefundEligibility(req *paymentsv1.CheckRefundEligibilityRequest) *paymentsv1.QuotePaymentRequest {
	return &paymentsv1.QuotePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: req.GetRefund().GetBillingAddress().GetRecipient(),
			},
		},
	}
}

// buildListParcelWorkflowsRequestForScheduleDisputeReport maps what ScheduleDisputeReport has to the request of
// acme.shipping.v1.ParcelWorkflowService/ListParcelWorkflows.
func buildListParcelWorkflowsRequestForScheduleDisputeReport(req *paymentsv1.ScheduleDisputeReportRequest) *shippingv1.ListParcelWorkflowsRequest {
	return &shippingv1.ListParcelWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListParcelWorkflowsRequest_Filter{
			Labels:       req.GetDetails().GetFilters(),
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetDisputeId(),
		},
	}
}

// buildSimulateLineageNodeRequestForRecalculateRefunds maps what RecalculateRefunds has to the request of
// acme.lineage.v1.LineageNodeCalculationService/SimulateLineageNode.
func buildSimulateLineageNodeRequestForRecalculateRefunds(req *paymentsv1.RecalculateRefundsRequest, recalculatePaymentsReply *paymentsv1.RecalculatePaymentsResponse) *lineagev1.SimulateLineageNodeRequest {
	return &lineagev1.SimulateLineageNodeRequest{
		Context: req.GetContext(),
		LineageNode: &lineagev1.LineageNode{
			Children: []*lineagev1.LineageNode{&lineagev1.LineageNode{
				Children: []*lineagev1.LineageNode{&lineagev1.LineageNode{
					Audit: &commonv1.AuditInfo{
						CreatedBy: recalculatePaymentsReply.GetPayment().GetAudit().GetCreatedBy(),
						Etag:      req.GetRefund().GetAudit().GetEtag(),
					},
					Manager: &identityv1.UserRef{Email: req.GetRefund().GetCounterparty().GetEmail()},
					Location: &commonv1.PostalAddress{
						PostalCode: recalculatePaymentsReply.GetPayment().GetBillingAddress().GetPostalCode(),
					},
				}},
			}},
		},
	}
}

// buildCountSellerScorecardTotalsRequestForActivatePaymentInstrument maps what ActivatePaymentInstrument has to the
// request of acme.sellers.v1.SellerScorecardReportingService/CountSellerScorecardTotals.
func buildCountSellerScorecardTotalsRequestForActivatePaymentInstrument(req *paymentsv1.ActivatePaymentInstrumentRequest) *sellersv1.CountSellerScorecardTotalsRequest {
	return &sellersv1.CountSellerScorecardTotalsRequest{
		Context: req.GetContext(),
		Filter:  &sellersv1.CountSellerScorecardTotalsRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()},
	}
}

// buildStartFulfillmentWorkflowRequestForStreamPayoutMetrics maps what StreamPayoutMetrics has to the request of
// acme.fulfillment.v1.FulfillmentWorkflowService/StartFulfillmentWorkflow.
func buildStartFulfillmentWorkflowRequestForStreamPayoutMetrics(req *paymentsv1.StreamPayoutMetricsRequest) *fulfillmentv1.StartFulfillmentWorkflowRequest {
	return &fulfillmentv1.StartFulfillmentWorkflowRequest{
		Context: req.GetContext(),
		Details: &fulfillmentv1.StartFulfillmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetSubscriber().GetEmail(),
				Name:  req.GetSubscriber().GetName(),
			},
		},
	}
}

// buildCloneAccessPolicyRequestForExplainPaymentCalculation maps what ExplainPaymentCalculation has to the request of
// acme.authorization.v1.AccessPolicyService/CloneAccessPolicy.
func buildCloneAccessPolicyRequestForExplainPaymentCalculation(req *paymentsv1.ExplainPaymentCalculationRequest) *authorizationv1.CloneAccessPolicyRequest {
	return &authorizationv1.CloneAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPayment().GetAudit().GetEtag(),
				UpdatedBy: req.GetPayment().GetAudit().GetUpdatedBy(),
				CreatedAt: req.GetPayment().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetPayment().GetCounterparty().GetDisplayName(),
			},
		},
	}
}

// buildRecallPickListRequestForActivateRefund maps what ActivateRefund has to the request of
// acme.warehousing.v1.PickListApprovalService/RecallPickList.
func buildRecallPickListRequestForActivateRefund(req *paymentsv1.ActivateRefundRequest) *warehousingv1.RecallPickListRequest {
	return &warehousingv1.RecallPickListRequest{
		Context: req.GetContext(),
		Details: &warehousingv1.RecallPickListRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		PickList: &warehousingv1.PickList{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}
}

// buildCountSellerListingTotalsRequestForGetRefundLifecycle maps what GetRefundLifecycle has to the request of
// acme.sellers.v1.SellerListingReportingService/CountSellerListingTotals.
func buildCountSellerListingTotalsRequestForGetRefundLifecycle(req *paymentsv1.GetRefundLifecycleRequest) *sellersv1.CountSellerListingTotalsRequest {
	return &sellersv1.CountSellerListingTotalsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.CountSellerListingTotalsRequest_Filter{
			OwnerId:      req.GetRefundId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
}

// buildCalculatePaymentRequestForListDisputeImports maps what ListDisputeImports has to the request of
// acme.payments.v1.PaymentCalculationService/CalculatePayment.
func buildCalculatePaymentRequestForListDisputeImports(req *paymentsv1.ListDisputeImportsRequest) *paymentsv1.CalculatePaymentRequest {
	return &paymentsv1.CalculatePaymentRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.CalculatePaymentRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Payment: &paymentsv1.Payment{
			Audit:    &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
			Metadata: req.GetFilter().GetLabels(),
		},
	}
}

// buildSuggestPaymentsRequestForCountRefundTotals maps what CountRefundTotals has to the request of
// acme.payments.v1.PaymentQueryService/SuggestPayments.
func buildSuggestPaymentsRequestForCountRefundTotals(req *paymentsv1.CountRefundTotalsRequest) *paymentsv1.SuggestPaymentsRequest {
	return &paymentsv1.SuggestPaymentsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.SuggestPaymentsRequest_Filter{
			Query:  req.GetFilter().GetQuery(),
			Region: req.GetFilter().GetRegion(),
		},
	}
}
