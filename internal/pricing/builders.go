package pricing

import (
	budgetingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	collectionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	devicesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/devices/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	routingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/routing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
)

// buildNormalizeCurrencyRateRequestForUpdatePriceList maps what UpdatePriceList has to the request of
// acme.pricing.v1.CurrencyRateValidationService/NormalizeCurrencyRate.
func buildNormalizeCurrencyRateRequestForUpdatePriceList(req *pricingv1.UpdatePriceListRequest, unsubscribeApiCredentialUpdatesReply *identityv1.UnsubscribeApiCredentialUpdatesResponse) *pricingv1.NormalizeCurrencyRateRequest {
	return &pricingv1.NormalizeCurrencyRateRequest{
		Context: req.GetContext(),
		Details: &pricingv1.NormalizeCurrencyRateRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetPriceList().GetOwner().GetEmail(),
				Phone: unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetContact().GetPhone(),
			},
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{DisplayName: req.GetPriceList().GetOwner().GetDisplayName()},
		},
	}
}

// buildRecalculateParcelsRequestForSearchPriceLists maps what SearchPriceLists has to the request of
// acme.shipping.v1.ParcelCalculationService/RecalculateParcels.
func buildRecalculateParcelsRequestForSearchPriceLists(req *pricingv1.SearchPriceListsRequest) *shippingv1.RecalculateParcelsRequest {
	return &shippingv1.RecalculateParcelsRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
			Owner:   &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildNormalizeCurrencyRateRequestForBrowsePriceQuotes maps what BrowsePriceQuotes has to the request of
// acme.pricing.v1.CurrencyRateValidationService/NormalizeCurrencyRate.
func buildNormalizeCurrencyRateRequestForBrowsePriceQuotes(req *pricingv1.BrowsePriceQuotesRequest) *pricingv1.NormalizeCurrencyRateRequest {
	return &pricingv1.NormalizeCurrencyRateRequest{
		Context: req.GetContext(),
		Details: &pricingv1.NormalizeCurrencyRateRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			Source:       req.GetFilter().GetOwnerId(),
			Measurements: []*pricingv1.CurrencyRateMeasurement{&pricingv1.CurrencyRateMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
		},
	}
}

// buildScheduleCurrencyRateExpiryRequestForPausePriceOverrideWorkflow maps what PausePriceOverrideWorkflow has to the
// request of acme.pricing.v1.CurrencyRateLifecycleService/ScheduleCurrencyRateExpiry.
func buildScheduleCurrencyRateExpiryRequestForPausePriceOverrideWorkflow(req *pricingv1.PausePriceOverrideWorkflowRequest) *pricingv1.ScheduleCurrencyRateExpiryRequest {
	return &pricingv1.ScheduleCurrencyRateExpiryRequest{
		Context: req.GetContext(),
		Details: &pricingv1.ScheduleCurrencyRateExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
			},
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetPriceOverride().GetOwner().GetDisplayName(),
				Email:       req.GetPriceOverride().GetOwner().GetEmail(),
			},
		},
	}
}

// buildDelegatePriceOverrideApprovalRequestForApproveCurrencyRate maps what ApproveCurrencyRate has to the request of
// acme.pricing.v1.PriceOverrideApprovalService/DelegatePriceOverrideApproval.
func buildDelegatePriceOverrideApprovalRequestForApproveCurrencyRate(req *pricingv1.ApproveCurrencyRateRequest, cancelPriceListWorkflowReply *pricingv1.CancelPriceListWorkflowResponse) *pricingv1.DelegatePriceOverrideApprovalRequest {
	return &pricingv1.DelegatePriceOverrideApprovalRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cancelPriceListWorkflowReply.GetPriceList().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildExplainPriceOverrideDecisionRequestForApproveCurrencyRate maps what ApproveCurrencyRate has to the request of
// acme.pricing.v1.PriceOverrideValidationService/ExplainPriceOverrideDecision.
func buildExplainPriceOverrideDecisionRequestForApproveCurrencyRate(req *pricingv1.ApproveCurrencyRateRequest, cancelPriceListWorkflowReply *pricingv1.CancelPriceListWorkflowResponse) *pricingv1.ExplainPriceOverrideDecisionRequest {
	return &pricingv1.ExplainPriceOverrideDecisionRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cancelPriceListWorkflowReply.GetPriceList().GetAudit().GetUpdatedBy(),
				CreatedBy: cancelPriceListWorkflowReply.GetPriceList().GetAudit().GetCreatedBy(),
			},
		},
	}
}

// buildValidatePriceQuoteRequestForCommentCurrencyRateApproval maps what CommentCurrencyRateApproval has to the request
// of acme.pricing.v1.PriceQuoteValidationService/ValidatePriceQuote.
func buildValidatePriceQuoteRequestForCommentCurrencyRateApproval(req *pricingv1.CommentCurrencyRateApprovalRequest, escalatePriceListReply *pricingv1.EscalatePriceListResponse) *pricingv1.ValidatePriceQuoteRequest {
	return &pricingv1.ValidatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Tax: &pricingv1.PriceQuoteLineTax{
					Amount: escalatePriceListReply.GetPriceList().GetDiscount().GetAmount(),
				},
			}},
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
}

// buildNormalizeUserRequestForFindPriceLists maps what FindPriceLists has to the request of
// acme.identity.v1.UserValidationService/NormalizeUser.
func buildNormalizeUserRequestForFindPriceLists(req *pricingv1.FindPriceListsRequest) *identityv1.NormalizeUserRequest {
	return &identityv1.NormalizeUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Details: &identityv1.NormalizeUserRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildEstimateHouseholdRequestForRetryPriceQuoteWorkflow maps what RetryPriceQuoteWorkflow has to the request of
// acme.customers.v1.HouseholdCalculationService/EstimateHousehold.
func buildEstimateHouseholdRequestForRetryPriceQuoteWorkflow(req *pricingv1.RetryPriceQuoteWorkflowRequest, escalatePriceListReply *pricingv1.EscalatePriceListResponse) *customersv1.EstimateHouseholdRequest {
	return &customersv1.EstimateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						Etag: escalatePriceListReply.GetPriceList().GetAudit().GetEtag(),
					},
					Manager: &identityv1.UserRef{
						Email: escalatePriceListReply.GetPriceList().GetOwner().GetEmail(),
					},
				}},
			}},
		},
	}
}

// buildSearchRoutesRequestForSubmitPriceOverride maps what SubmitPriceOverride has to the request of
// acme.routing.v1.RouteService/SearchRoutes.
func buildSearchRoutesRequestForSubmitPriceOverride(req *pricingv1.SubmitPriceOverrideRequest, delegatePriceQuoteApprovalReply *pricingv1.DelegatePriceQuoteApprovalResponse) *routingv1.SearchRoutesRequest {
	return &routingv1.SearchRoutesRequest{
		Context: req.GetContext(),
		Filter: &routingv1.SearchRoutesRequest_Filter{
			Region: delegatePriceQuoteApprovalReply.GetPriceQuote().GetBillingAddress().GetRegion(),
		},
	}
}

// buildRecalculateTelemetryReadingsRequestForRejectPriceOverride maps what RejectPriceOverride has to the request of
// acme.telematics.v1.TelemetryReadingCalculationService/RecalculateTelemetryReadings.
func buildRecalculateTelemetryReadingsRequestForRejectPriceOverride(req *pricingv1.RejectPriceOverrideRequest) *telematicsv1.RecalculateTelemetryReadingsRequest {
	return &telematicsv1.RecalculateTelemetryReadingsRequest{
		Context: req.GetContext(),
		Reason:  req.GetPriceOverride().GetBlock().GetReason(),
	}
}

// buildFindManagedDevicesRequestForCheckCurrencyRateEligibility maps what CheckCurrencyRateEligibility has to the
// request of acme.devices.v1.ManagedDeviceQueryService/FindManagedDevices.
func buildFindManagedDevicesRequestForCheckCurrencyRateEligibility(req *pricingv1.CheckCurrencyRateEligibilityRequest) *devicesv1.FindManagedDevicesRequest {
	return &devicesv1.FindManagedDevicesRequest{
		Context: req.GetContext(),
		Filter: &devicesv1.FindManagedDevicesRequest_Filter{
			Labels:       req.GetCurrencyRate().GetAttributes(),
			CreatedAfter: req.GetCurrencyRate().GetOccurredAt(),
		},
	}
}

// buildPullManifestsRequestForAdvancePriceListWorkflow maps what AdvancePriceListWorkflow has to the request of
// acme.shipping.v1.ManifestSyncService/PullManifests.
func buildPullManifestsRequestForAdvancePriceListWorkflow(req *pricingv1.AdvancePriceListWorkflowRequest) *shippingv1.PullManifestsRequest {
	return &shippingv1.PullManifestsRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			Audit:        &commonv1.AuditInfo{CreatedBy: req.GetPriceList().GetAudit().GetCreatedBy()},
			Counterparty: &customersv1.CustomerRef{DisplayName: req.GetPriceList().GetOwner().GetDisplayName()},
		},
	}
}

// buildCancelRefundWorkflowRequestForRetryPriceListWorkflow maps what RetryPriceListWorkflow has to the request of
// acme.payments.v1.RefundWorkflowService/CancelRefundWorkflow.
func buildCancelRefundWorkflowRequestForRetryPriceListWorkflow(req *pricingv1.RetryPriceListWorkflowRequest) *paymentsv1.CancelRefundWorkflowRequest {
	return &paymentsv1.CancelRefundWorkflowRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.CancelRefundWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{DisplayName: req.GetPriceList().GetOwner().GetDisplayName()},
		},
	}
}

// buildValidateConsentRecordRequestForSubmitPriceQuote maps what SubmitPriceQuote has to the request of
// acme.customers.v1.ConsentRecordValidationService/ValidateConsentRecord.
func buildValidateConsentRecordRequestForSubmitPriceQuote(req *pricingv1.SubmitPriceQuoteRequest, estimatePreferenceReply *customersv1.EstimatePreferenceResponse, scoreConsentRecordReply *customersv1.ScoreConsentRecordResponse) *customersv1.ValidateConsentRecordRequest {
	return &customersv1.ValidateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: estimatePreferenceReply.GetPreference().GetOwner().GetDisplayName(),
			},
			OccurredAt: scoreConsentRecordReply.GetConsentRecord().GetOccurredAt(),
		},
		Details: &customersv1.ValidateConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: estimatePreferenceReply.GetPreference().GetOwner().GetEmail(),
			},
		},
	}
}

// buildRecallRefundRequestForCommentPriceQuoteApproval maps what CommentPriceQuoteApproval has to the request of
// acme.payments.v1.RefundApprovalService/RecallRefund.
func buildRecallRefundRequestForCommentPriceQuoteApproval(req *pricingv1.CommentPriceQuoteApprovalRequest) *paymentsv1.RecallRefundRequest {
	return &paymentsv1.RecallRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Settlement: &paymentsv1.Refund_Bank{Bank: &paymentsv1.RefundBankSettlement{
				AccountHolder: req.GetPriceQuote().GetBank().GetAccountHolder(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: req.GetPriceQuote().GetBillingAddress().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPriceQuote().GetAudit().GetUpdatedBy()},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetPriceQuote().GetCounterparty().GetDisplayName(),
			},
		},
	}
}

// buildQuoteBackorderRequestForGetPriceOverrideRules maps what GetPriceOverrideRules has to the request of
// acme.orders.v1.BackorderCalculationService/QuoteBackorder.
func buildQuoteBackorderRequestForGetPriceOverrideRules(req *pricingv1.GetPriceOverrideRulesRequest) *ordersv1.QuoteBackorderRequest {
	return &ordersv1.QuoteBackorderRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Origin:   &ordersv1.Backorder_ExternalReference{ExternalReference: req.GetPriceOverrideId()},
			Comments: []*ordersv1.BackorderComment{&ordersv1.BackorderComment{CreatedAt: req.GetContext().GetDeadline()}},
		},
	}
}

// buildValidatePriceListRequestForExplainCurrencyRateCalculation maps what ExplainCurrencyRateCalculation has to the
// request of acme.pricing.v1.PriceListValidationService/ValidatePriceList.
func buildValidatePriceListRequestForExplainCurrencyRateCalculation(req *pricingv1.ExplainCurrencyRateCalculationRequest, checkPriceListEligibilityReply *pricingv1.CheckPriceListEligibilityResponse) *pricingv1.ValidatePriceListRequest {
	return &pricingv1.ValidatePriceListRequest{
		Context: req.GetContext(),
		Details: &pricingv1.ValidatePriceListRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: checkPriceListEligibilityReply.GetPriceList().GetOwner().GetEmail(),
			},
		},
		PriceList: &pricingv1.PriceList{
			Action: &pricingv1.PriceList_Route{Route: &pricingv1.PriceListRouteAction{
				Target: checkPriceListEligibilityReply.GetPriceList().GetRoute().GetTarget(),
			}},
		},
	}
}

// buildCheckPriceListEligibilityRequestForExplainPriceQuoteDecision maps what ExplainPriceQuoteDecision has to the
// request of acme.pricing.v1.PriceListValidationService/CheckPriceListEligibility.
func buildCheckPriceListEligibilityRequestForExplainPriceQuoteDecision(req *pricingv1.ExplainPriceQuoteDecisionRequest) *pricingv1.CheckPriceListEligibilityRequest {
	return &pricingv1.CheckPriceListEligibilityRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPriceQuote().GetAudit().GetCreatedAt(),
				UpdatedBy: req.GetPriceQuote().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &pricingv1.CheckPriceListEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetContact().GetName(),
			},
		},
	}
}

// buildLintPriceListsRequestForEstimatePriceOverride maps what EstimatePriceOverride has to the request of
// acme.pricing.v1.PriceListValidationService/LintPriceLists.
func buildLintPriceListsRequestForEstimatePriceOverride(req *pricingv1.EstimatePriceOverrideRequest) *pricingv1.LintPriceListsRequest {
	return &pricingv1.LintPriceListsRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Audit:  &commonv1.AuditInfo{CreatedBy: req.GetPriceOverride().GetAudit().GetCreatedBy()},
			Action: &pricingv1.PriceList_Route{Route: &pricingv1.PriceListRouteAction{Weight: req.GetPriceOverride().GetRoute().GetWeight()}},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetPriceOverride().GetOwner().GetDisplayName(),
			},
		},
		Details: &pricingv1.LintPriceListsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetPriceOverride().GetOwner().GetEmail()},
		},
	}
}

// buildCalculatePriceQuoteRequestForExplainPriceListDecision maps what ExplainPriceListDecision has to the request of
// acme.pricing.v1.PriceQuoteCalculationService/CalculatePriceQuote.
func buildCalculatePriceQuoteRequestForExplainPriceListDecision(req *pricingv1.ExplainPriceListDecisionRequest) *pricingv1.CalculatePriceQuoteRequest {
	return &pricingv1.CalculatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{Etag: req.GetPriceList().GetAudit().GetEtag()},
		},
	}
}

// buildCreateBudgetRequestForSimulatePriceQuote maps what SimulatePriceQuote has to the request of
// acme.budgeting.v1.BudgetService/CreateBudget.
func buildCreateBudgetRequestForSimulatePriceQuote(req *pricingv1.SimulatePriceQuoteRequest) *budgetingv1.CreateBudgetRequest {
	return &budgetingv1.CreateBudgetRequest{
		Context: req.GetContext(),
		Budget: &budgetingv1.Budget{
			Holder: &budgetingv1.ForecastRef{
				DisplayName: req.GetPriceQuote().GetCounterparty().GetDisplayName(),
			},
			Funding: &budgetingv1.Budget_Bank{Bank: &budgetingv1.BudgetBankFunding{
				AccountHolder: req.GetPriceQuote().GetBank().GetAccountHolder(),
			}},
		},
	}
}

// buildCompareSearchQueryVersionsRequestForScheduleCurrencyRateReport maps what ScheduleCurrencyRateReport has to the
// request of acme.search.v1.SearchQueryHistoryService/CompareSearchQueryVersions.
func buildCompareSearchQueryVersionsRequestForScheduleCurrencyRateReport(req *pricingv1.ScheduleCurrencyRateReportRequest, updateCollectionCaseReply *collectionsv1.UpdateCollectionCaseResponse) *searchv1.CompareSearchQueryVersionsRequest {
	return &searchv1.CompareSearchQueryVersionsRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: updateCollectionCaseReply.GetCollectionCase().GetCustomer().GetDisplayName(),
			},
			CorrelationId: updateCollectionCaseReply.GetResult().GetCorrelationId(),
		},
		Details: &searchv1.CompareSearchQueryVersionsRequest_Details{
			AuthorEmail: updateCollectionCaseReply.GetCollectionCase().GetRequester().GetEmail(),
		},
		Reason: req.GetReason(),
	}
}
