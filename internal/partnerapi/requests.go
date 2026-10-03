package partnerapi

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
)

// pausePartnerWebhookWorkflowOption sets part of a request of
// acme.partnerapi.v1.PartnerWebhookWorkflowService/PausePartnerWebhookWorkflow.
type pausePartnerWebhookWorkflowOption func(*partnerapiv1.PausePartnerWebhookWorkflowRequest)

// newPausePartnerWebhookWorkflowRequest builds a request of
// acme.partnerapi.v1.PartnerWebhookWorkflowService/PausePartnerWebhookWorkflow from options.
func newPausePartnerWebhookWorkflowRequest(options ...pausePartnerWebhookWorkflowOption) *partnerapiv1.PausePartnerWebhookWorkflowRequest {
	request := &partnerapiv1.PausePartnerWebhookWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withPausePartnerWebhookWorkflowContext(value *commonv1.RequestContext) pausePartnerWebhookWorkflowOption {
	return func(request *partnerapiv1.PausePartnerWebhookWorkflowRequest) { request.Context = value }
}

func withPausePartnerWebhookWorkflowPartnerWebhook(value *partnerapiv1.PartnerWebhook) pausePartnerWebhookWorkflowOption {
	return func(request *partnerapiv1.PausePartnerWebhookWorkflowRequest) { request.PartnerWebhook = value }
}

// schedulePartnerWebhookReportOption sets part of a request of
// acme.partnerapi.v1.PartnerWebhookReportingService/SchedulePartnerWebhookReport.
type schedulePartnerWebhookReportOption func(*partnerapiv1.SchedulePartnerWebhookReportRequest)

// newSchedulePartnerWebhookReportRequest builds a request of
// acme.partnerapi.v1.PartnerWebhookReportingService/SchedulePartnerWebhookReport from options.
func newSchedulePartnerWebhookReportRequest(options ...schedulePartnerWebhookReportOption) *partnerapiv1.SchedulePartnerWebhookReportRequest {
	request := &partnerapiv1.SchedulePartnerWebhookReportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSchedulePartnerWebhookReportContext(value *commonv1.RequestContext) schedulePartnerWebhookReportOption {
	return func(request *partnerapiv1.SchedulePartnerWebhookReportRequest) { request.Context = value }
}

func withSchedulePartnerWebhookReportDetails(value *partnerapiv1.SchedulePartnerWebhookReportRequest_Details) schedulePartnerWebhookReportOption {
	return func(request *partnerapiv1.SchedulePartnerWebhookReportRequest) { request.Details = value }
}

// listPendingGiftCardBatchesOption sets part of a request of
// acme.giftcards.v1.GiftCardBatchApprovalService/ListPendingGiftCardBatches.
type listPendingGiftCardBatchesOption func(*giftcardsv1.ListPendingGiftCardBatchesRequest)

// newListPendingGiftCardBatchesRequest builds a request of
// acme.giftcards.v1.GiftCardBatchApprovalService/ListPendingGiftCardBatches from options.
func newListPendingGiftCardBatchesRequest(options ...listPendingGiftCardBatchesOption) *giftcardsv1.ListPendingGiftCardBatchesRequest {
	request := &giftcardsv1.ListPendingGiftCardBatchesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withListPendingGiftCardBatchesContext(value *commonv1.RequestContext) listPendingGiftCardBatchesOption {
	return func(request *giftcardsv1.ListPendingGiftCardBatchesRequest) { request.Context = value }
}

func withListPendingGiftCardBatchesFilter(value *giftcardsv1.ListPendingGiftCardBatchesRequest_Filter) listPendingGiftCardBatchesOption {
	return func(request *giftcardsv1.ListPendingGiftCardBatchesRequest) { request.Filter = value }
}

func withListPendingGiftCardBatchesPage(value *commonv1.PageRequest) listPendingGiftCardBatchesOption {
	return func(request *giftcardsv1.ListPendingGiftCardBatchesRequest) { request.Page = value }
}

// delegateSellerApprovalOption sets part of a request of acme.sellers.v1.SellerApprovalService/DelegateSellerApproval.
type delegateSellerApprovalOption func(*sellersv1.DelegateSellerApprovalRequest)

// newDelegateSellerApprovalRequest builds a request of acme.sellers.v1.SellerApprovalService/DelegateSellerApproval
// from options.
func newDelegateSellerApprovalRequest(options ...delegateSellerApprovalOption) *sellersv1.DelegateSellerApprovalRequest {
	request := &sellersv1.DelegateSellerApprovalRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withDelegateSellerApprovalContext(value *commonv1.RequestContext) delegateSellerApprovalOption {
	return func(request *sellersv1.DelegateSellerApprovalRequest) { request.Context = value }
}

func withDelegateSellerApprovalAnnotations(value map[string]string) delegateSellerApprovalOption {
	return func(request *sellersv1.DelegateSellerApprovalRequest) { request.Annotations = value }
}

func withDelegateSellerApprovalSellerId(value string) delegateSellerApprovalOption {
	return func(request *sellersv1.DelegateSellerApprovalRequest) { request.SellerId = value }
}

// schedulePartnerOrderReportOption sets part of a request of
// acme.partnerapi.v1.PartnerOrderReportingService/SchedulePartnerOrderReport.
type schedulePartnerOrderReportOption func(*partnerapiv1.SchedulePartnerOrderReportRequest)

// newSchedulePartnerOrderReportRequest builds a request of
// acme.partnerapi.v1.PartnerOrderReportingService/SchedulePartnerOrderReport from options.
func newSchedulePartnerOrderReportRequest(options ...schedulePartnerOrderReportOption) *partnerapiv1.SchedulePartnerOrderReportRequest {
	request := &partnerapiv1.SchedulePartnerOrderReportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSchedulePartnerOrderReportContext(value *commonv1.RequestContext) schedulePartnerOrderReportOption {
	return func(request *partnerapiv1.SchedulePartnerOrderReportRequest) { request.Context = value }
}

func withSchedulePartnerOrderReportDetails(value *partnerapiv1.SchedulePartnerOrderReportRequest_Details) schedulePartnerOrderReportOption {
	return func(request *partnerapiv1.SchedulePartnerOrderReportRequest) { request.Details = value }
}

// escalatePartnerWebhookOption sets part of a request of
// acme.partnerapi.v1.PartnerWebhookApprovalService/EscalatePartnerWebhook.
type escalatePartnerWebhookOption func(*partnerapiv1.EscalatePartnerWebhookRequest)

// newEscalatePartnerWebhookRequest builds a request of
// acme.partnerapi.v1.PartnerWebhookApprovalService/EscalatePartnerWebhook from options.
func newEscalatePartnerWebhookRequest(options ...escalatePartnerWebhookOption) *partnerapiv1.EscalatePartnerWebhookRequest {
	request := &partnerapiv1.EscalatePartnerWebhookRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withEscalatePartnerWebhookContext(value *commonv1.RequestContext) escalatePartnerWebhookOption {
	return func(request *partnerapiv1.EscalatePartnerWebhookRequest) { request.Context = value }
}

func withEscalatePartnerWebhookDetails(value *partnerapiv1.EscalatePartnerWebhookRequest_Details) escalatePartnerWebhookOption {
	return func(request *partnerapiv1.EscalatePartnerWebhookRequest) { request.Details = value }
}

func withEscalatePartnerWebhookPartnerWebhook(value *partnerapiv1.PartnerWebhook) escalatePartnerWebhookOption {
	return func(request *partnerapiv1.EscalatePartnerWebhookRequest) { request.PartnerWebhook = value }
}

// getPartnerWebhookRulesOption sets part of a request of
// acme.partnerapi.v1.PartnerWebhookValidationService/GetPartnerWebhookRules.
type getPartnerWebhookRulesOption func(*partnerapiv1.GetPartnerWebhookRulesRequest)

// newGetPartnerWebhookRulesRequest builds a request of
// acme.partnerapi.v1.PartnerWebhookValidationService/GetPartnerWebhookRules from options.
func newGetPartnerWebhookRulesRequest(options ...getPartnerWebhookRulesOption) *partnerapiv1.GetPartnerWebhookRulesRequest {
	request := &partnerapiv1.GetPartnerWebhookRulesRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withGetPartnerWebhookRulesContext(value *commonv1.RequestContext) getPartnerWebhookRulesOption {
	return func(request *partnerapiv1.GetPartnerWebhookRulesRequest) { request.Context = value }
}

func withGetPartnerWebhookRulesPartnerWebhookId(value string) getPartnerWebhookRulesOption {
	return func(request *partnerapiv1.GetPartnerWebhookRulesRequest) { request.PartnerWebhookId = value }
}

// approveSubscriptionPlanOption sets part of a request of
// acme.subscriptions.v1.SubscriptionPlanApprovalService/ApproveSubscriptionPlan.
type approveSubscriptionPlanOption func(*subscriptionsv1.ApproveSubscriptionPlanRequest)

// newApproveSubscriptionPlanRequest builds a request of
// acme.subscriptions.v1.SubscriptionPlanApprovalService/ApproveSubscriptionPlan from options.
func newApproveSubscriptionPlanRequest(options ...approveSubscriptionPlanOption) *subscriptionsv1.ApproveSubscriptionPlanRequest {
	request := &subscriptionsv1.ApproveSubscriptionPlanRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withApproveSubscriptionPlanContext(value *commonv1.RequestContext) approveSubscriptionPlanOption {
	return func(request *subscriptionsv1.ApproveSubscriptionPlanRequest) { request.Context = value }
}

func withApproveSubscriptionPlanDetails(value *subscriptionsv1.ApproveSubscriptionPlanRequest_Details) approveSubscriptionPlanOption {
	return func(request *subscriptionsv1.ApproveSubscriptionPlanRequest) { request.Details = value }
}

// scorePartnerWebhookOption sets part of a request of
// acme.partnerapi.v1.PartnerWebhookValidationService/ScorePartnerWebhook.
type scorePartnerWebhookOption func(*partnerapiv1.ScorePartnerWebhookRequest)

// newScorePartnerWebhookRequest builds a request of
// acme.partnerapi.v1.PartnerWebhookValidationService/ScorePartnerWebhook from options.
func newScorePartnerWebhookRequest(options ...scorePartnerWebhookOption) *partnerapiv1.ScorePartnerWebhookRequest {
	request := &partnerapiv1.ScorePartnerWebhookRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withScorePartnerWebhookContext(value *commonv1.RequestContext) scorePartnerWebhookOption {
	return func(request *partnerapiv1.ScorePartnerWebhookRequest) { request.Context = value }
}

func withScorePartnerWebhookDetails(value *partnerapiv1.ScorePartnerWebhookRequest_Details) scorePartnerWebhookOption {
	return func(request *partnerapiv1.ScorePartnerWebhookRequest) { request.Details = value }
}
