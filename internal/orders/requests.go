package orders

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	yardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
)

// browseOrderAmendmentsOption sets part of a request of
// acme.orders.v1.OrderAmendmentQueryService/BrowseOrderAmendments.
type browseOrderAmendmentsOption func(*ordersv1.BrowseOrderAmendmentsRequest)

// newBrowseOrderAmendmentsRequest builds a request of acme.orders.v1.OrderAmendmentQueryService/BrowseOrderAmendments
// from options.
func newBrowseOrderAmendmentsRequest(options ...browseOrderAmendmentsOption) *ordersv1.BrowseOrderAmendmentsRequest {
	request := &ordersv1.BrowseOrderAmendmentsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseOrderAmendmentsContext(value *commonv1.RequestContext) browseOrderAmendmentsOption {
	return func(request *ordersv1.BrowseOrderAmendmentsRequest) { request.Context = value }
}

func withBrowseOrderAmendmentsFilter(value *ordersv1.BrowseOrderAmendmentsRequest_Filter) browseOrderAmendmentsOption {
	return func(request *ordersv1.BrowseOrderAmendmentsRequest) { request.Filter = value }
}

// validateOrderAmendmentOption sets part of a request of
// acme.orders.v1.OrderAmendmentValidationService/ValidateOrderAmendment.
type validateOrderAmendmentOption func(*ordersv1.ValidateOrderAmendmentRequest)

// newValidateOrderAmendmentRequest builds a request of
// acme.orders.v1.OrderAmendmentValidationService/ValidateOrderAmendment from options.
func newValidateOrderAmendmentRequest(options ...validateOrderAmendmentOption) *ordersv1.ValidateOrderAmendmentRequest {
	request := &ordersv1.ValidateOrderAmendmentRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withValidateOrderAmendmentContext(value *commonv1.RequestContext) validateOrderAmendmentOption {
	return func(request *ordersv1.ValidateOrderAmendmentRequest) { request.Context = value }
}

func withValidateOrderAmendmentDetails(value *ordersv1.ValidateOrderAmendmentRequest_Details) validateOrderAmendmentOption {
	return func(request *ordersv1.ValidateOrderAmendmentRequest) { request.Details = value }
}

func withValidateOrderAmendmentOrderAmendment(value *ordersv1.OrderAmendment) validateOrderAmendmentOption {
	return func(request *ordersv1.ValidateOrderAmendmentRequest) { request.OrderAmendment = value }
}

// normalizeOrderOption sets part of a request of acme.orders.v1.OrderValidationService/NormalizeOrder.
type normalizeOrderOption func(*ordersv1.NormalizeOrderRequest)

// newNormalizeOrderRequest builds a request of acme.orders.v1.OrderValidationService/NormalizeOrder from options.
func newNormalizeOrderRequest(options ...normalizeOrderOption) *ordersv1.NormalizeOrderRequest {
	request := &ordersv1.NormalizeOrderRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withNormalizeOrderContext(value *commonv1.RequestContext) normalizeOrderOption {
	return func(request *ordersv1.NormalizeOrderRequest) { request.Context = value }
}

func withNormalizeOrderOrder(value *ordersv1.Order) normalizeOrderOption {
	return func(request *ordersv1.NormalizeOrderRequest) { request.Order = value }
}

// cancelRefundWorkflowOption sets part of a request of acme.payments.v1.RefundWorkflowService/CancelRefundWorkflow.
type cancelRefundWorkflowOption func(*paymentsv1.CancelRefundWorkflowRequest)

// newCancelRefundWorkflowRequest builds a request of acme.payments.v1.RefundWorkflowService/CancelRefundWorkflow from
// options.
func newCancelRefundWorkflowRequest(options ...cancelRefundWorkflowOption) *paymentsv1.CancelRefundWorkflowRequest {
	request := &paymentsv1.CancelRefundWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCancelRefundWorkflowContext(value *commonv1.RequestContext) cancelRefundWorkflowOption {
	return func(request *paymentsv1.CancelRefundWorkflowRequest) { request.Context = value }
}

func withCancelRefundWorkflowRefund(value *paymentsv1.Refund) cancelRefundWorkflowOption {
	return func(request *paymentsv1.CancelRefundWorkflowRequest) { request.Refund = value }
}

func withCancelRefundWorkflowDetails(value *paymentsv1.CancelRefundWorkflowRequest_Details) cancelRefundWorkflowOption {
	return func(request *paymentsv1.CancelRefundWorkflowRequest) { request.Details = value }
}

// countFeatureFlagsOption sets part of a request of acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags.
type countFeatureFlagsOption func(*featureflagsv1.CountFeatureFlagsRequest)

// newCountFeatureFlagsRequest builds a request of acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags from
// options.
func newCountFeatureFlagsRequest(options ...countFeatureFlagsOption) *featureflagsv1.CountFeatureFlagsRequest {
	request := &featureflagsv1.CountFeatureFlagsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCountFeatureFlagsContext(value *commonv1.RequestContext) countFeatureFlagsOption {
	return func(request *featureflagsv1.CountFeatureFlagsRequest) { request.Context = value }
}

func withCountFeatureFlagsFilter(value *featureflagsv1.CountFeatureFlagsRequest_Filter) countFeatureFlagsOption {
	return func(request *featureflagsv1.CountFeatureFlagsRequest) { request.Filter = value }
}

// countOrderAmendmentsOption sets part of a request of acme.orders.v1.OrderAmendmentQueryService/CountOrderAmendments.
type countOrderAmendmentsOption func(*ordersv1.CountOrderAmendmentsRequest)

// newCountOrderAmendmentsRequest builds a request of acme.orders.v1.OrderAmendmentQueryService/CountOrderAmendments
// from options.
func newCountOrderAmendmentsRequest(options ...countOrderAmendmentsOption) *ordersv1.CountOrderAmendmentsRequest {
	request := &ordersv1.CountOrderAmendmentsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCountOrderAmendmentsContext(value *commonv1.RequestContext) countOrderAmendmentsOption {
	return func(request *ordersv1.CountOrderAmendmentsRequest) { request.Context = value }
}

func withCountOrderAmendmentsFilter(value *ordersv1.CountOrderAmendmentsRequest_Filter) countOrderAmendmentsOption {
	return func(request *ordersv1.CountOrderAmendmentsRequest) { request.Filter = value }
}

// deleteOrderReportOption sets part of a request of acme.orders.v1.OrderReportingService/DeleteOrderReport.
type deleteOrderReportOption func(*ordersv1.DeleteOrderReportRequest)

// newDeleteOrderReportRequest builds a request of acme.orders.v1.OrderReportingService/DeleteOrderReport from options.
func newDeleteOrderReportRequest(options ...deleteOrderReportOption) *ordersv1.DeleteOrderReportRequest {
	request := &ordersv1.DeleteOrderReportRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withDeleteOrderReportContext(value *commonv1.RequestContext) deleteOrderReportOption {
	return func(request *ordersv1.DeleteOrderReportRequest) { request.Context = value }
}

func withDeleteOrderReportOrderId(value string) deleteOrderReportOption {
	return func(request *ordersv1.DeleteOrderReportRequest) { request.OrderId = value }
}

func withDeleteOrderReportEtag(value string) deleteOrderReportOption {
	return func(request *ordersv1.DeleteOrderReportRequest) { request.Etag = value }
}

func withDeleteOrderReportReason(value string) deleteOrderReportOption {
	return func(request *ordersv1.DeleteOrderReportRequest) { request.Reason = value }
}

// createSearchIndexOption sets part of a request of acme.search.v1.SearchIndexService/CreateSearchIndex.
type createSearchIndexOption func(*searchv1.CreateSearchIndexRequest)

// newCreateSearchIndexRequest builds a request of acme.search.v1.SearchIndexService/CreateSearchIndex from options.
func newCreateSearchIndexRequest(options ...createSearchIndexOption) *searchv1.CreateSearchIndexRequest {
	request := &searchv1.CreateSearchIndexRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCreateSearchIndexContext(value *commonv1.RequestContext) createSearchIndexOption {
	return func(request *searchv1.CreateSearchIndexRequest) { request.Context = value }
}

func withCreateSearchIndexSearchIndex(value *searchv1.SearchIndex) createSearchIndexOption {
	return func(request *searchv1.CreateSearchIndexRequest) { request.SearchIndex = value }
}

// previewOrderAmendmentOption sets part of a request of
// acme.orders.v1.OrderAmendmentValidationService/PreviewOrderAmendment.
type previewOrderAmendmentOption func(*ordersv1.PreviewOrderAmendmentRequest)

// newPreviewOrderAmendmentRequest builds a request of
// acme.orders.v1.OrderAmendmentValidationService/PreviewOrderAmendment from options.
func newPreviewOrderAmendmentRequest(options ...previewOrderAmendmentOption) *ordersv1.PreviewOrderAmendmentRequest {
	request := &ordersv1.PreviewOrderAmendmentRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withPreviewOrderAmendmentContext(value *commonv1.RequestContext) previewOrderAmendmentOption {
	return func(request *ordersv1.PreviewOrderAmendmentRequest) { request.Context = value }
}

func withPreviewOrderAmendmentOrderAmendment(value *ordersv1.OrderAmendment) previewOrderAmendmentOption {
	return func(request *ordersv1.PreviewOrderAmendmentRequest) { request.OrderAmendment = value }
}

func withPreviewOrderAmendmentDetails(value *ordersv1.PreviewOrderAmendmentRequest_Details) previewOrderAmendmentOption {
	return func(request *ordersv1.PreviewOrderAmendmentRequest) { request.Details = value }
}

// simulateOrderNoteOption sets part of a request of acme.orders.v1.OrderNoteCalculationService/SimulateOrderNote.
type simulateOrderNoteOption func(*ordersv1.SimulateOrderNoteRequest)

// newSimulateOrderNoteRequest builds a request of acme.orders.v1.OrderNoteCalculationService/SimulateOrderNote from
// options.
func newSimulateOrderNoteRequest(options ...simulateOrderNoteOption) *ordersv1.SimulateOrderNoteRequest {
	request := &ordersv1.SimulateOrderNoteRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSimulateOrderNoteContext(value *commonv1.RequestContext) simulateOrderNoteOption {
	return func(request *ordersv1.SimulateOrderNoteRequest) { request.Context = value }
}

func withSimulateOrderNoteDetails(value *ordersv1.SimulateOrderNoteRequest_Details) simulateOrderNoteOption {
	return func(request *ordersv1.SimulateOrderNoteRequest) { request.Details = value }
}

func withSimulateOrderNoteOrderNote(value *ordersv1.OrderNote) simulateOrderNoteOption {
	return func(request *ordersv1.SimulateOrderNoteRequest) { request.OrderNote = value }
}

// delegateInspectionApprovalOption sets part of a request of
// acme.returns.v1.InspectionApprovalService/DelegateInspectionApproval.
type delegateInspectionApprovalOption func(*returnsv1.DelegateInspectionApprovalRequest)

// newDelegateInspectionApprovalRequest builds a request of
// acme.returns.v1.InspectionApprovalService/DelegateInspectionApproval from options.
func newDelegateInspectionApprovalRequest(options ...delegateInspectionApprovalOption) *returnsv1.DelegateInspectionApprovalRequest {
	request := &returnsv1.DelegateInspectionApprovalRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withDelegateInspectionApprovalContext(value *commonv1.RequestContext) delegateInspectionApprovalOption {
	return func(request *returnsv1.DelegateInspectionApprovalRequest) { request.Context = value }
}

func withDelegateInspectionApprovalInspection(value *returnsv1.Inspection) delegateInspectionApprovalOption {
	return func(request *returnsv1.DelegateInspectionApprovalRequest) { request.Inspection = value }
}

// estimateOrderOption sets part of a request of acme.orders.v1.OrderCalculationService/EstimateOrder.
type estimateOrderOption func(*ordersv1.EstimateOrderRequest)

// newEstimateOrderRequest builds a request of acme.orders.v1.OrderCalculationService/EstimateOrder from options.
func newEstimateOrderRequest(options ...estimateOrderOption) *ordersv1.EstimateOrderRequest {
	request := &ordersv1.EstimateOrderRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withEstimateOrderContext(value *commonv1.RequestContext) estimateOrderOption {
	return func(request *ordersv1.EstimateOrderRequest) { request.Context = value }
}

func withEstimateOrderOrder(value *ordersv1.Order) estimateOrderOption {
	return func(request *ordersv1.EstimateOrderRequest) { request.Order = value }
}

func withEstimateOrderDetails(value *ordersv1.EstimateOrderRequest_Details) estimateOrderOption {
	return func(request *ordersv1.EstimateOrderRequest) { request.Details = value }
}

// normalizeFeatureFlagOption sets part of a request of
// acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag.
type normalizeFeatureFlagOption func(*featureflagsv1.NormalizeFeatureFlagRequest)

// newNormalizeFeatureFlagRequest builds a request of
// acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag from options.
func newNormalizeFeatureFlagRequest(options ...normalizeFeatureFlagOption) *featureflagsv1.NormalizeFeatureFlagRequest {
	request := &featureflagsv1.NormalizeFeatureFlagRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withNormalizeFeatureFlagContext(value *commonv1.RequestContext) normalizeFeatureFlagOption {
	return func(request *featureflagsv1.NormalizeFeatureFlagRequest) { request.Context = value }
}

func withNormalizeFeatureFlagDetails(value *featureflagsv1.NormalizeFeatureFlagRequest_Details) normalizeFeatureFlagOption {
	return func(request *featureflagsv1.NormalizeFeatureFlagRequest) { request.Details = value }
}

// calculateOrderOption sets part of a request of acme.orders.v1.OrderCalculationService/CalculateOrder.
type calculateOrderOption func(*ordersv1.CalculateOrderRequest)

// newCalculateOrderRequest builds a request of acme.orders.v1.OrderCalculationService/CalculateOrder from options.
func newCalculateOrderRequest(options ...calculateOrderOption) *ordersv1.CalculateOrderRequest {
	request := &ordersv1.CalculateOrderRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCalculateOrderContext(value *commonv1.RequestContext) calculateOrderOption {
	return func(request *ordersv1.CalculateOrderRequest) { request.Context = value }
}

func withCalculateOrderOrder(value *ordersv1.Order) calculateOrderOption {
	return func(request *ordersv1.CalculateOrderRequest) { request.Order = value }
}

// simulateOrderOption sets part of a request of acme.orders.v1.OrderCalculationService/SimulateOrder.
type simulateOrderOption func(*ordersv1.SimulateOrderRequest)

// newSimulateOrderRequest builds a request of acme.orders.v1.OrderCalculationService/SimulateOrder from options.
func newSimulateOrderRequest(options ...simulateOrderOption) *ordersv1.SimulateOrderRequest {
	request := &ordersv1.SimulateOrderRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSimulateOrderContext(value *commonv1.RequestContext) simulateOrderOption {
	return func(request *ordersv1.SimulateOrderRequest) { request.Context = value }
}

func withSimulateOrderOrder(value *ordersv1.Order) simulateOrderOption {
	return func(request *ordersv1.SimulateOrderRequest) { request.Order = value }
}

// searchYardsOption sets part of a request of acme.yards.v1.YardService/SearchYards.
type searchYardsOption func(*yardsv1.SearchYardsRequest)

// newSearchYardsRequest builds a request of acme.yards.v1.YardService/SearchYards from options.
func newSearchYardsRequest(options ...searchYardsOption) *yardsv1.SearchYardsRequest {
	request := &yardsv1.SearchYardsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSearchYardsContext(value *commonv1.RequestContext) searchYardsOption {
	return func(request *yardsv1.SearchYardsRequest) { request.Context = value }
}

func withSearchYardsFilter(value *yardsv1.SearchYardsRequest_Filter) searchYardsOption {
	return func(request *yardsv1.SearchYardsRequest) { request.Filter = value }
}

// scoreGiftCardTransactionOption sets part of a request of
// acme.giftcards.v1.GiftCardTransactionValidationService/ScoreGiftCardTransaction.
type scoreGiftCardTransactionOption func(*giftcardsv1.ScoreGiftCardTransactionRequest)

// newScoreGiftCardTransactionRequest builds a request of
// acme.giftcards.v1.GiftCardTransactionValidationService/ScoreGiftCardTransaction from options.
func newScoreGiftCardTransactionRequest(options ...scoreGiftCardTransactionOption) *giftcardsv1.ScoreGiftCardTransactionRequest {
	request := &giftcardsv1.ScoreGiftCardTransactionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withScoreGiftCardTransactionContext(value *commonv1.RequestContext) scoreGiftCardTransactionOption {
	return func(request *giftcardsv1.ScoreGiftCardTransactionRequest) { request.Context = value }
}

func withScoreGiftCardTransactionDetails(value *giftcardsv1.ScoreGiftCardTransactionRequest_Details) scoreGiftCardTransactionOption {
	return func(request *giftcardsv1.ScoreGiftCardTransactionRequest) { request.Details = value }
}

func withScoreGiftCardTransactionGiftCardTransaction(value *giftcardsv1.GiftCardTransaction) scoreGiftCardTransactionOption {
	return func(request *giftcardsv1.ScoreGiftCardTransactionRequest) { request.GiftCardTransaction = value }
}

// countOrderTotalsOption sets part of a request of acme.orders.v1.OrderReportingService/CountOrderTotals.
type countOrderTotalsOption func(*ordersv1.CountOrderTotalsRequest)

// newCountOrderTotalsRequest builds a request of acme.orders.v1.OrderReportingService/CountOrderTotals from options.
func newCountOrderTotalsRequest(options ...countOrderTotalsOption) *ordersv1.CountOrderTotalsRequest {
	request := &ordersv1.CountOrderTotalsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCountOrderTotalsContext(value *commonv1.RequestContext) countOrderTotalsOption {
	return func(request *ordersv1.CountOrderTotalsRequest) { request.Context = value }
}

func withCountOrderTotalsFilter(value *ordersv1.CountOrderTotalsRequest_Filter) countOrderTotalsOption {
	return func(request *ordersv1.CountOrderTotalsRequest) { request.Filter = value }
}

// explainOrderDecisionOption sets part of a request of acme.orders.v1.OrderValidationService/ExplainOrderDecision.
type explainOrderDecisionOption func(*ordersv1.ExplainOrderDecisionRequest)

// newExplainOrderDecisionRequest builds a request of acme.orders.v1.OrderValidationService/ExplainOrderDecision from
// options.
func newExplainOrderDecisionRequest(options ...explainOrderDecisionOption) *ordersv1.ExplainOrderDecisionRequest {
	request := &ordersv1.ExplainOrderDecisionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainOrderDecisionContext(value *commonv1.RequestContext) explainOrderDecisionOption {
	return func(request *ordersv1.ExplainOrderDecisionRequest) { request.Context = value }
}

func withExplainOrderDecisionOrder(value *ordersv1.Order) explainOrderDecisionOption {
	return func(request *ordersv1.ExplainOrderDecisionRequest) { request.Order = value }
}

// explainPutawayTaskDecisionOption sets part of a request of
// acme.warehousing.v1.PutawayTaskValidationService/ExplainPutawayTaskDecision.
type explainPutawayTaskDecisionOption func(*warehousingv1.ExplainPutawayTaskDecisionRequest)

// newExplainPutawayTaskDecisionRequest builds a request of
// acme.warehousing.v1.PutawayTaskValidationService/ExplainPutawayTaskDecision from options.
func newExplainPutawayTaskDecisionRequest(options ...explainPutawayTaskDecisionOption) *warehousingv1.ExplainPutawayTaskDecisionRequest {
	request := &warehousingv1.ExplainPutawayTaskDecisionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainPutawayTaskDecisionContext(value *commonv1.RequestContext) explainPutawayTaskDecisionOption {
	return func(request *warehousingv1.ExplainPutawayTaskDecisionRequest) { request.Context = value }
}

func withExplainPutawayTaskDecisionPutawayTask(value *warehousingv1.PutawayTask) explainPutawayTaskDecisionOption {
	return func(request *warehousingv1.ExplainPutawayTaskDecisionRequest) { request.PutawayTask = value }
}

// lintCustomersOption sets part of a request of acme.customers.v1.CustomerValidationService/LintCustomers.
type lintCustomersOption func(*customersv1.LintCustomersRequest)

// newLintCustomersRequest builds a request of acme.customers.v1.CustomerValidationService/LintCustomers from options.
func newLintCustomersRequest(options ...lintCustomersOption) *customersv1.LintCustomersRequest {
	request := &customersv1.LintCustomersRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withLintCustomersContext(value *commonv1.RequestContext) lintCustomersOption {
	return func(request *customersv1.LintCustomersRequest) { request.Context = value }
}

func withLintCustomersCustomer(value *customersv1.Customer) lintCustomersOption {
	return func(request *customersv1.LintCustomersRequest) { request.Customer = value }
}

// resumePackingSlipWorkflowOption sets part of a request of
// acme.fulfillment.v1.PackingSlipWorkflowService/ResumePackingSlipWorkflow.
type resumePackingSlipWorkflowOption func(*fulfillmentv1.ResumePackingSlipWorkflowRequest)

// newResumePackingSlipWorkflowRequest builds a request of
// acme.fulfillment.v1.PackingSlipWorkflowService/ResumePackingSlipWorkflow from options.
func newResumePackingSlipWorkflowRequest(options ...resumePackingSlipWorkflowOption) *fulfillmentv1.ResumePackingSlipWorkflowRequest {
	request := &fulfillmentv1.ResumePackingSlipWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withResumePackingSlipWorkflowContext(value *commonv1.RequestContext) resumePackingSlipWorkflowOption {
	return func(request *fulfillmentv1.ResumePackingSlipWorkflowRequest) { request.Context = value }
}

// recalculateOrderAmendmentsOption sets part of a request of
// acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments.
type recalculateOrderAmendmentsOption func(*ordersv1.RecalculateOrderAmendmentsRequest)

// newRecalculateOrderAmendmentsRequest builds a request of
// acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments from options.
func newRecalculateOrderAmendmentsRequest(options ...recalculateOrderAmendmentsOption) *ordersv1.RecalculateOrderAmendmentsRequest {
	request := &ordersv1.RecalculateOrderAmendmentsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withRecalculateOrderAmendmentsContext(value *commonv1.RequestContext) recalculateOrderAmendmentsOption {
	return func(request *ordersv1.RecalculateOrderAmendmentsRequest) { request.Context = value }
}

func withRecalculateOrderAmendmentsOrderAmendment(value *ordersv1.OrderAmendment) recalculateOrderAmendmentsOption {
	return func(request *ordersv1.RecalculateOrderAmendmentsRequest) { request.OrderAmendment = value }
}

func withRecalculateOrderAmendmentsReason(value string) recalculateOrderAmendmentsOption {
	return func(request *ordersv1.RecalculateOrderAmendmentsRequest) { request.Reason = value }
}

// validateHouseholdOption sets part of a request of acme.customers.v1.HouseholdValidationService/ValidateHousehold.
type validateHouseholdOption func(*customersv1.ValidateHouseholdRequest)

// newValidateHouseholdRequest builds a request of acme.customers.v1.HouseholdValidationService/ValidateHousehold from
// options.
func newValidateHouseholdRequest(options ...validateHouseholdOption) *customersv1.ValidateHouseholdRequest {
	request := &customersv1.ValidateHouseholdRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withValidateHouseholdContext(value *commonv1.RequestContext) validateHouseholdOption {
	return func(request *customersv1.ValidateHouseholdRequest) { request.Context = value }
}

func withValidateHouseholdHousehold(value *customersv1.Household) validateHouseholdOption {
	return func(request *customersv1.ValidateHouseholdRequest) { request.Household = value }
}
