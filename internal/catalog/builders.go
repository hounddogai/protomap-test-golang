package catalog

import (
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	budgetingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	dispatchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/dispatch/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
)

// buildReconcileRedemptionsRequestForRecallBrand maps what RecallBrand has to the request of
// acme.promotions.v1.RedemptionSyncService/ReconcileRedemptions.
func buildReconcileRedemptionsRequestForRecallBrand(req *catalogv1.RecallBrandRequest) *promotionsv1.ReconcileRedemptionsRequest {
	return &promotionsv1.ReconcileRedemptionsRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Attributes: req.GetBrand().GetAttributes(),
		},
	}
}

// buildRecalculateCategoriesRequestForExplainAttributeDecision maps what ExplainAttributeDecision has to the request of
// acme.catalog.v1.CategoryCalculationService/RecalculateCategories.
func buildRecalculateCategoriesRequestForExplainAttributeDecision(req *catalogv1.ExplainAttributeDecisionRequest) *catalogv1.RecalculateCategoriesRequest {
	return &catalogv1.RecalculateCategoriesRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
				}},
			}},
		},
	}
}

// buildExplainProductCalculationRequestForFindProducts maps what FindProducts has to the request of
// acme.catalog.v1.ProductCalculationService/ExplainProductCalculation.
func buildExplainProductCalculationRequestForFindProducts(req *catalogv1.FindProductsRequest) *catalogv1.ExplainProductCalculationRequest {
	return &catalogv1.ExplainProductCalculationRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
			Owner:   &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &catalogv1.ExplainProductCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// collectCategoryNames lists the names of a category and of everything under it.
func collectCategoryNames(node *catalogv1.Category, names []string) []string {
	if node == nil {
		return names
	}
	names = append(names, node.GetName())
	for _, child := range node.GetChildren() {
		names = collectCategoryNames(child, names)
	}
	return names
}

// buildPreviewSessionRequestForSubmitProductVariant maps what SubmitProductVariant has to the request of
// acme.identity.v1.SessionValidationService/PreviewSession.
func buildPreviewSessionRequestForSubmitProductVariant(req *catalogv1.SubmitProductVariantRequest) *identityv1.PreviewSessionRequest {
	return &identityv1.PreviewSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{Etag: req.GetProductVariant().GetAudit().GetEtag()},
			Comments: []*identityv1.SessionComment{&identityv1.SessionComment{
				CreatedAt: req.GetProductVariant().GetAudit().GetCreatedAt(),
			}},
		},
	}
}

// buildListNotificationsRequestForCompareAttributeOptions maps what CompareAttributeOptions has to the request of
// acme.notifications.v1.NotificationService/ListNotifications.
func buildListNotificationsRequestForCompareAttributeOptions(req *catalogv1.CompareAttributeOptionsRequest) *notificationsv1.ListNotificationsRequest {
	return &notificationsv1.ListNotificationsRequest{
		Context: req.GetContext(),
		Filter:  &notificationsv1.ListNotificationsRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	}
}

// buildSummarizePlacementsRequestForExplainAttributeCalculation maps what ExplainAttributeCalculation has to the
// request of acme.merchandising.v1.PlacementQueryService/SummarizePlacements.
func buildSummarizePlacementsRequestForExplainAttributeCalculation(req *catalogv1.ExplainAttributeCalculationRequest) *merchandisingv1.SummarizePlacementsRequest {
	return &merchandisingv1.SummarizePlacementsRequest{
		Context: req.GetContext(),
		Reason:  req.GetAttribute().GetBlock().GetReason(),
	}
}

// buildRepairSearchIndexRequestForListProductWorkflows maps what ListProductWorkflows has to the request of
// acme.search.v1.SearchIndexAdminService/RepairSearchIndex.
func buildRepairSearchIndexRequestForListProductWorkflows(req *catalogv1.ListProductWorkflowsRequest) *searchv1.RepairSearchIndexRequest {
	return &searchv1.RepairSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
			Owner:   &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
}

// buildCompareProductOptionsRequestForValidateProductVariant maps what ValidateProductVariant has to the request of
// acme.catalog.v1.ProductCalculationService/CompareProductOptions.
func buildCompareProductOptionsRequestForValidateProductVariant(req *catalogv1.ValidateProductVariantRequest) *catalogv1.CompareProductOptionsRequest {
	return &catalogv1.CompareProductOptionsRequest{Context: req.GetContext()}
}

// buildExplainFeatureFlagDecisionRequestForLintProductVariants maps what LintProductVariants has to the request of
// acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision.
func buildExplainFeatureFlagDecisionRequestForLintProductVariants(req *catalogv1.LintProductVariantsRequest) *featureflagsv1.ExplainFeatureFlagDecisionRequest {
	return &featureflagsv1.ExplainFeatureFlagDecisionRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetProductVariant().GetAudit().GetUpdatedAt(),
			},
		},
	}
}

// buildCompareProductOptionsRequestForExplainBrandCalculation maps what ExplainBrandCalculation has to the request of
// acme.catalog.v1.ProductCalculationService/CompareProductOptions.
func buildCompareProductOptionsRequestForExplainBrandCalculation(req *catalogv1.ExplainBrandCalculationRequest, recalculateProductVariantsReply *catalogv1.RecalculateProductVariantsResponse) *catalogv1.CompareProductOptionsRequest {
	return &catalogv1.CompareProductOptionsRequest{
		Context: req.GetContext(),
		Details: &catalogv1.CompareProductOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: req.GetBrand().GetPrimaryAddress().GetLocality()},
		},
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculateProductVariantsReply.GetProductVariant().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildCreateDispatchOrderRequestForSuspendAttribute maps what SuspendAttribute has to the request of
// acme.dispatch.v1.DispatchOrderService/CreateDispatchOrder.
func buildCreateDispatchOrderRequestForSuspendAttribute(req *catalogv1.SuspendAttributeRequest) *dispatchv1.CreateDispatchOrderRequest {
	return &dispatchv1.CreateDispatchOrderRequest{
		Context: req.GetContext(),
		DispatchOrder: &dispatchv1.DispatchOrder{
			Counterparty: &dispatchv1.ZoneRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			DueAt: req.GetDetails().GetEffectiveAt(),
		},
	}
}

// buildCloneProductRequestForReinstateAttribute maps what ReinstateAttribute has to the request of
// acme.catalog.v1.ProductService/CloneProduct.
func buildCloneProductRequestForReinstateAttribute(req *catalogv1.ReinstateAttributeRequest) *catalogv1.CloneProductRequest {
	return &catalogv1.CloneProductRequest{
		Context: req.GetContext(),
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetDetails().GetEffectiveAt()},
		},
	}
}

// buildRetrySubscriptionPlanWorkflowRequestForApproveProduct maps what ApproveProduct has to the request of
// acme.subscriptions.v1.SubscriptionPlanWorkflowService/RetrySubscriptionPlanWorkflow.
func buildRetrySubscriptionPlanWorkflowRequestForApproveProduct(req *catalogv1.ApproveProductRequest) *subscriptionsv1.RetrySubscriptionPlanWorkflowRequest {
	return &subscriptionsv1.RetrySubscriptionPlanWorkflowRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
}

// buildSummarizeTranslationKeysRequestForEscalateProduct maps what EscalateProduct has to the request of
// acme.localization.v1.TranslationKeyQueryService/SummarizeTranslationKeys.
func buildSummarizeTranslationKeysRequestForEscalateProduct(req *catalogv1.EscalateProductRequest, summarizeVenuesReply *venuesv1.SummarizeVenuesResponse) *localizationv1.SummarizeTranslationKeysRequest {
	return &localizationv1.SummarizeTranslationKeysRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details:     &localizationv1.SummarizeTranslationKeysRequest_Details{Period: summarizeVenuesReply.GetPeriod()},
	}
}

// buildQuoteCategoryRequestForEstimateProductVariant maps what EstimateProductVariant has to the request of
// acme.catalog.v1.CategoryCalculationService/QuoteCategory.
func buildQuoteCategoryRequestForEstimateProductVariant(req *catalogv1.EstimateProductVariantRequest, calculateCategoryReply *catalogv1.CalculateCategoryResponse) *catalogv1.QuoteCategoryRequest {
	return &catalogv1.QuoteCategoryRequest{
		Context: req.GetContext(),
		Category: &catalogv1.Category{
			Children: []*catalogv1.Category{&catalogv1.Category{
				Children: []*catalogv1.Category{&catalogv1.Category{
					Audit: &commonv1.AuditInfo{
						CreatedBy: req.GetProductVariant().GetAudit().GetCreatedBy(),
					},
					Location: &commonv1.PostalAddress{
						Recipient: calculateCategoryReply.GetCategory().GetLocation().GetRecipient(),
					},
				}},
			}},
		},
	}
}

// buildGetTenantPreferencesRequestForSimulateProductVariant maps what SimulateProductVariant has to the request of
// acme.identity.v1.TenantNotificationService/GetTenantPreferences.
func buildGetTenantPreferencesRequestForSimulateProductVariant(req *catalogv1.SimulateProductVariantRequest) *identityv1.GetTenantPreferencesRequest {
	return &identityv1.GetTenantPreferencesRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
}

// buildCompareSearchQueryVersionsRequestForSuspendBrand maps what SuspendBrand has to the request of
// acme.search.v1.SearchQueryHistoryService/CompareSearchQueryVersions.
func buildCompareSearchQueryVersionsRequestForSuspendBrand(req *catalogv1.SuspendBrandRequest) *searchv1.CompareSearchQueryVersionsRequest {
	return &searchv1.CompareSearchQueryVersionsRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
}

// buildPreviewPaymentRequestForScoreProduct maps what ScoreProduct has to the request of
// acme.payments.v1.PaymentValidationService/PreviewPayment.
func buildPreviewPaymentRequestForScoreProduct(req *catalogv1.ScoreProductRequest) *paymentsv1.PreviewPaymentRequest {
	return &paymentsv1.PreviewPaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				Locality:    req.GetProduct().GetAddress().GetLocality(),
				CountryCode: req.GetProduct().GetAddress().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetProduct().GetAudit().GetEtag()},
		},
	}
}

// buildCalculateProductRequestForEstimateCategory maps what EstimateCategory has to the request of
// acme.catalog.v1.ProductCalculationService/CalculateProduct.
func buildCalculateProductRequestForEstimateCategory(req *catalogv1.EstimateCategoryRequest, quoteProductReply *catalogv1.QuoteProductResponse) *catalogv1.CalculateProductRequest {
	return &catalogv1.CalculateProductRequest{
		Context: req.GetContext(),
		Details: &catalogv1.CalculateProductRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: quoteProductReply.GetProduct().GetAddress().GetCountryCode(),
			},
		},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCategory().GetManager().GetDisplayName(),
			},
		},
	}
}

// buildCreateBudgetRequestForGetCategoryRates maps what GetCategoryRates has to the request of
// acme.budgeting.v1.BudgetService/CreateBudget.
func buildCreateBudgetRequestForGetCategoryRates(req *catalogv1.GetCategoryRatesRequest) *budgetingv1.CreateBudgetRequest {
	return &budgetingv1.CreateBudgetRequest{
		Context: req.GetContext(),
		Budget: &budgetingv1.Budget{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
	}
}

// buildListUsersRequestForActivateProductVariant maps what ActivateProductVariant has to the request of
// acme.identity.v1.UserService/ListUsers.
func buildListUsersRequestForActivateProductVariant(req *catalogv1.ActivateProductVariantRequest) *identityv1.ListUsersRequest {
	return &identityv1.ListUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListUsersRequest_Filter{
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
		},
	}
}

// buildRetrySynonymExportRequestForFetchBrandReport maps what FetchBrandReport has to the request of
// acme.search.v1.SynonymExportService/RetrySynonymExport.
func buildRetrySynonymExportRequestForFetchBrandReport(req *catalogv1.FetchBrandReportRequest) *searchv1.RetrySynonymExportRequest {
	return &searchv1.RetrySynonymExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.RetrySynonymExportRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
}

// buildExplainVenueDecisionRequestForScheduleBrandReport maps what ScheduleBrandReport has to the request of
// acme.venues.v1.VenueValidationService/ExplainVenueDecision.
func buildExplainVenueDecisionRequestForScheduleBrandReport(req *catalogv1.ScheduleBrandReportRequest, cancelSavedListWorkflowReply *cartv1.CancelSavedListWorkflowResponse) *venuesv1.ExplainVenueDecisionRequest {
	return &venuesv1.ExplainVenueDecisionRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{
				CountryCode: cancelSavedListWorkflowReply.GetSavedList().GetBillingAddress().GetCountryCode(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cancelSavedListWorkflowReply.GetSavedList().GetAudit().GetUpdatedBy(),
			},
		},
	}
}

// buildSubmitStockMovementRequestForShareBrandReport maps what ShareBrandReport has to the request of
// acme.inventory.v1.StockMovementApprovalService/SubmitStockMovement.
func buildSubmitStockMovementRequestForShareBrandReport(req *catalogv1.ShareBrandReportRequest) *inventoryv1.SubmitStockMovementRequest {
	return &inventoryv1.SubmitStockMovementRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
}

// buildAnnotateSearchIndexVersionRequestForSimulateProduct maps what SimulateProduct has to the request of
// acme.search.v1.SearchIndexHistoryService/AnnotateSearchIndexVersion.
func buildAnnotateSearchIndexVersionRequestForSimulateProduct(req *catalogv1.SimulateProductRequest, checkCartRuleEligibilityReply *cartv1.CheckCartRuleEligibilityResponse) *searchv1.AnnotateSearchIndexVersionRequest {
	return &searchv1.AnnotateSearchIndexVersionRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{PostalCode: req.GetProduct().GetAddress().GetPostalCode()}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkCartRuleEligibilityReply.GetCartRule().GetAudit().GetUpdatedAt(),
			},
		},
	}
}

// buildPeekCategoryRequestForSuspendCategory maps what SuspendCategory has to the request of
// acme.catalog.v1.CategoryQueryService/PeekCategory.
func buildPeekCategoryRequestForSuspendCategory(req *catalogv1.SuspendCategoryRequest) *catalogv1.PeekCategoryRequest {
	return &catalogv1.PeekCategoryRequest{
		Context:    req.GetContext(),
		CategoryId: req.GetCategoryId(),
	}
}

// buildNormalizeFeatureFlagRequestForTerminateCategory maps what TerminateCategory has to the request of
// acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag.
func buildNormalizeFeatureFlagRequestForTerminateCategory(req *catalogv1.TerminateCategoryRequest) *featureflagsv1.NormalizeFeatureFlagRequest {
	return &featureflagsv1.NormalizeFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
}

// buildScoreAccessPolicyRequestForGenerateProductVariantReport maps what GenerateProductVariantReport has to the
// request of acme.authorization.v1.AccessPolicyValidationService/ScoreAccessPolicy.
func buildScoreAccessPolicyRequestForGenerateProductVariantReport(req *catalogv1.GenerateProductVariantReportRequest, lockTenantReply *identityv1.LockTenantResponse, lintBrandsReply *catalogv1.LintBrandsResponse) *authorizationv1.ScoreAccessPolicyRequest {
	return &authorizationv1.ScoreAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{Email: lintBrandsReply.GetBrand().GetContact().GetEmail()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lockTenantReply.GetTenant().GetAudit().GetUpdatedAt(),
				CreatedAt: lintBrandsReply.GetBrand().GetAudit().GetCreatedAt(),
			},
		},
	}
}
