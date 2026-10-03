package promotions

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
)

// browseNotificationsOption sets part of a request of
// acme.notifications.v1.NotificationQueryService/BrowseNotifications.
type browseNotificationsOption func(*notificationsv1.BrowseNotificationsRequest)

// newBrowseNotificationsRequest builds a request of acme.notifications.v1.NotificationQueryService/BrowseNotifications
// from options.
func newBrowseNotificationsRequest(options ...browseNotificationsOption) *notificationsv1.BrowseNotificationsRequest {
	request := &notificationsv1.BrowseNotificationsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseNotificationsContext(value *commonv1.RequestContext) browseNotificationsOption {
	return func(request *notificationsv1.BrowseNotificationsRequest) { request.Context = value }
}

func withBrowseNotificationsFilter(value *notificationsv1.BrowseNotificationsRequest_Filter) browseNotificationsOption {
	return func(request *notificationsv1.BrowseNotificationsRequest) { request.Filter = value }
}

// explainRedemptionDecisionOption sets part of a request of
// acme.promotions.v1.RedemptionValidationService/ExplainRedemptionDecision.
type explainRedemptionDecisionOption func(*promotionsv1.ExplainRedemptionDecisionRequest)

// newExplainRedemptionDecisionRequest builds a request of
// acme.promotions.v1.RedemptionValidationService/ExplainRedemptionDecision from options.
func newExplainRedemptionDecisionRequest(options ...explainRedemptionDecisionOption) *promotionsv1.ExplainRedemptionDecisionRequest {
	request := &promotionsv1.ExplainRedemptionDecisionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainRedemptionDecisionContext(value *commonv1.RequestContext) explainRedemptionDecisionOption {
	return func(request *promotionsv1.ExplainRedemptionDecisionRequest) { request.Context = value }
}

func withExplainRedemptionDecisionRedemption(value *promotionsv1.Redemption) explainRedemptionDecisionOption {
	return func(request *promotionsv1.ExplainRedemptionDecisionRequest) { request.Redemption = value }
}

// reconcileCampaignsOption sets part of a request of acme.promotions.v1.CampaignSyncService/ReconcileCampaigns.
type reconcileCampaignsOption func(*promotionsv1.ReconcileCampaignsRequest)

// newReconcileCampaignsRequest builds a request of acme.promotions.v1.CampaignSyncService/ReconcileCampaigns from
// options.
func newReconcileCampaignsRequest(options ...reconcileCampaignsOption) *promotionsv1.ReconcileCampaignsRequest {
	request := &promotionsv1.ReconcileCampaignsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withReconcileCampaignsContext(value *commonv1.RequestContext) reconcileCampaignsOption {
	return func(request *promotionsv1.ReconcileCampaignsRequest) { request.Context = value }
}

func withReconcileCampaignsCampaign(value *promotionsv1.Campaign) reconcileCampaignsOption {
	return func(request *promotionsv1.ReconcileCampaignsRequest) { request.Campaign = value }
}

func withReconcileCampaignsDetails(value *promotionsv1.ReconcileCampaignsRequest_Details) reconcileCampaignsOption {
	return func(request *promotionsv1.ReconcileCampaignsRequest) { request.Details = value }
}

// resolveCampaignConflictOption sets part of a request of
// acme.promotions.v1.CampaignSyncService/ResolveCampaignConflict.
type resolveCampaignConflictOption func(*promotionsv1.ResolveCampaignConflictRequest)

// newResolveCampaignConflictRequest builds a request of acme.promotions.v1.CampaignSyncService/ResolveCampaignConflict
// from options.
func newResolveCampaignConflictRequest(options ...resolveCampaignConflictOption) *promotionsv1.ResolveCampaignConflictRequest {
	request := &promotionsv1.ResolveCampaignConflictRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withResolveCampaignConflictContext(value *commonv1.RequestContext) resolveCampaignConflictOption {
	return func(request *promotionsv1.ResolveCampaignConflictRequest) { request.Context = value }
}

func withResolveCampaignConflictCampaign(value *promotionsv1.Campaign) resolveCampaignConflictOption {
	return func(request *promotionsv1.ResolveCampaignConflictRequest) { request.Campaign = value }
}

func withResolveCampaignConflictDetails(value *promotionsv1.ResolveCampaignConflictRequest_Details) resolveCampaignConflictOption {
	return func(request *promotionsv1.ResolveCampaignConflictRequest) { request.Details = value }
}

// estimateSellerOption sets part of a request of acme.sellers.v1.SellerCalculationService/EstimateSeller.
type estimateSellerOption func(*sellersv1.EstimateSellerRequest)

// newEstimateSellerRequest builds a request of acme.sellers.v1.SellerCalculationService/EstimateSeller from options.
func newEstimateSellerRequest(options ...estimateSellerOption) *sellersv1.EstimateSellerRequest {
	request := &sellersv1.EstimateSellerRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withEstimateSellerContext(value *commonv1.RequestContext) estimateSellerOption {
	return func(request *sellersv1.EstimateSellerRequest) { request.Context = value }
}

func withEstimateSellerDetails(value *sellersv1.EstimateSellerRequest_Details) estimateSellerOption {
	return func(request *sellersv1.EstimateSellerRequest) { request.Details = value }
}

func withEstimateSellerSeller(value *sellersv1.Seller) estimateSellerOption {
	return func(request *sellersv1.EstimateSellerRequest) { request.Seller = value }
}

// retryRedemptionImportOption sets part of a request of
// acme.promotions.v1.RedemptionImportService/RetryRedemptionImport.
type retryRedemptionImportOption func(*promotionsv1.RetryRedemptionImportRequest)

// newRetryRedemptionImportRequest builds a request of acme.promotions.v1.RedemptionImportService/RetryRedemptionImport
// from options.
func newRetryRedemptionImportRequest(options ...retryRedemptionImportOption) *promotionsv1.RetryRedemptionImportRequest {
	request := &promotionsv1.RetryRedemptionImportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRetryRedemptionImportContext(value *commonv1.RequestContext) retryRedemptionImportOption {
	return func(request *promotionsv1.RetryRedemptionImportRequest) { request.Context = value }
}

func withRetryRedemptionImportRedemptionId(value string) retryRedemptionImportOption {
	return func(request *promotionsv1.RetryRedemptionImportRequest) { request.RedemptionId = value }
}

func withRetryRedemptionImportRedemption(value *promotionsv1.Redemption) retryRedemptionImportOption {
	return func(request *promotionsv1.RetryRedemptionImportRequest) { request.Redemption = value }
}

// sharePromotionReportOption sets part of a request of
// acme.promotions.v1.PromotionReportingService/SharePromotionReport.
type sharePromotionReportOption func(*promotionsv1.SharePromotionReportRequest)

// newSharePromotionReportRequest builds a request of acme.promotions.v1.PromotionReportingService/SharePromotionReport
// from options.
func newSharePromotionReportRequest(options ...sharePromotionReportOption) *promotionsv1.SharePromotionReportRequest {
	request := &promotionsv1.SharePromotionReportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSharePromotionReportContext(value *commonv1.RequestContext) sharePromotionReportOption {
	return func(request *promotionsv1.SharePromotionReportRequest) { request.Context = value }
}

func withSharePromotionReportDetails(value *promotionsv1.SharePromotionReportRequest_Details) sharePromotionReportOption {
	return func(request *promotionsv1.SharePromotionReportRequest) { request.Details = value }
}

func withSharePromotionReportAnnotations(value map[string]string) sharePromotionReportOption {
	return func(request *promotionsv1.SharePromotionReportRequest) { request.Annotations = value }
}

func withSharePromotionReportReason(value string) sharePromotionReportOption {
	return func(request *promotionsv1.SharePromotionReportRequest) { request.Reason = value }
}
