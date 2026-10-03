package fraud

import (
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
)

// findAuditEventsOption sets part of a request of acme.audit.v1.AuditEventQueryService/FindAuditEvents.
type findAuditEventsOption func(*auditv1.FindAuditEventsRequest)

// newFindAuditEventsRequest builds a request of acme.audit.v1.AuditEventQueryService/FindAuditEvents from options.
func newFindAuditEventsRequest(options ...findAuditEventsOption) *auditv1.FindAuditEventsRequest {
	request := &auditv1.FindAuditEventsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withFindAuditEventsContext(value *commonv1.RequestContext) findAuditEventsOption {
	return func(request *auditv1.FindAuditEventsRequest) { request.Context = value }
}

func withFindAuditEventsFilter(value *auditv1.FindAuditEventsRequest_Filter) findAuditEventsOption {
	return func(request *auditv1.FindAuditEventsRequest) { request.Filter = value }
}

// cloneWarehouseOption sets part of a request of acme.warehousing.v1.WarehouseService/CloneWarehouse.
type cloneWarehouseOption func(*warehousingv1.CloneWarehouseRequest)

// newCloneWarehouseRequest builds a request of acme.warehousing.v1.WarehouseService/CloneWarehouse from options.
func newCloneWarehouseRequest(options ...cloneWarehouseOption) *warehousingv1.CloneWarehouseRequest {
	request := &warehousingv1.CloneWarehouseRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCloneWarehouseContext(value *commonv1.RequestContext) cloneWarehouseOption {
	return func(request *warehousingv1.CloneWarehouseRequest) { request.Context = value }
}

func withCloneWarehouseWarehouse(value *warehousingv1.Warehouse) cloneWarehouseOption {
	return func(request *warehousingv1.CloneWarehouseRequest) { request.Warehouse = value }
}

// explainUsageRecordCalculationOption sets part of a request of
// acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation.
type explainUsageRecordCalculationOption func(*subscriptionsv1.ExplainUsageRecordCalculationRequest)

// newExplainUsageRecordCalculationRequest builds a request of
// acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation from options.
func newExplainUsageRecordCalculationRequest(options ...explainUsageRecordCalculationOption) *subscriptionsv1.ExplainUsageRecordCalculationRequest {
	request := &subscriptionsv1.ExplainUsageRecordCalculationRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainUsageRecordCalculationContext(value *commonv1.RequestContext) explainUsageRecordCalculationOption {
	return func(request *subscriptionsv1.ExplainUsageRecordCalculationRequest) { request.Context = value }
}

func withExplainUsageRecordCalculationUsageRecord(value *subscriptionsv1.UsageRecord) explainUsageRecordCalculationOption {
	return func(request *subscriptionsv1.ExplainUsageRecordCalculationRequest) { request.UsageRecord = value }
}

// compareReturnRequestOptionsOption sets part of a request of
// acme.returns.v1.ReturnRequestCalculationService/CompareReturnRequestOptions.
type compareReturnRequestOptionsOption func(*returnsv1.CompareReturnRequestOptionsRequest)

// newCompareReturnRequestOptionsRequest builds a request of
// acme.returns.v1.ReturnRequestCalculationService/CompareReturnRequestOptions from options.
func newCompareReturnRequestOptionsRequest(options ...compareReturnRequestOptionsOption) *returnsv1.CompareReturnRequestOptionsRequest {
	request := &returnsv1.CompareReturnRequestOptionsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withCompareReturnRequestOptionsContext(value *commonv1.RequestContext) compareReturnRequestOptionsOption {
	return func(request *returnsv1.CompareReturnRequestOptionsRequest) { request.Context = value }
}

func withCompareReturnRequestOptionsReturnRequest(value *returnsv1.ReturnRequest) compareReturnRequestOptionsOption {
	return func(request *returnsv1.CompareReturnRequestOptionsRequest) { request.ReturnRequest = value }
}

func withCompareReturnRequestOptionsReturnRequestId(value string) compareReturnRequestOptionsOption {
	return func(request *returnsv1.CompareReturnRequestOptionsRequest) { request.ReturnRequestId = value }
}

// browseCustomersOption sets part of a request of acme.customers.v1.CustomerQueryService/BrowseCustomers.
type browseCustomersOption func(*customersv1.BrowseCustomersRequest)

// newBrowseCustomersRequest builds a request of acme.customers.v1.CustomerQueryService/BrowseCustomers from options.
func newBrowseCustomersRequest(options ...browseCustomersOption) *customersv1.BrowseCustomersRequest {
	request := &customersv1.BrowseCustomersRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withBrowseCustomersContext(value *commonv1.RequestContext) browseCustomersOption {
	return func(request *customersv1.BrowseCustomersRequest) { request.Context = value }
}

func withBrowseCustomersFilter(value *customersv1.BrowseCustomersRequest_Filter) browseCustomersOption {
	return func(request *customersv1.BrowseCustomersRequest) { request.Filter = value }
}

// listSellerScorecardConflictsOption sets part of a request of
// acme.sellers.v1.SellerScorecardSyncService/ListSellerScorecardConflicts.
type listSellerScorecardConflictsOption func(*sellersv1.ListSellerScorecardConflictsRequest)

// newListSellerScorecardConflictsRequest builds a request of
// acme.sellers.v1.SellerScorecardSyncService/ListSellerScorecardConflicts from options.
func newListSellerScorecardConflictsRequest(options ...listSellerScorecardConflictsOption) *sellersv1.ListSellerScorecardConflictsRequest {
	request := &sellersv1.ListSellerScorecardConflictsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withListSellerScorecardConflictsContext(value *commonv1.RequestContext) listSellerScorecardConflictsOption {
	return func(request *sellersv1.ListSellerScorecardConflictsRequest) { request.Context = value }
}

func withListSellerScorecardConflictsFilter(value *sellersv1.ListSellerScorecardConflictsRequest_Filter) listSellerScorecardConflictsOption {
	return func(request *sellersv1.ListSellerScorecardConflictsRequest) { request.Filter = value }
}

// explainFraudRuleDecisionOption sets part of a request of
// acme.fraud.v1.FraudRuleValidationService/ExplainFraudRuleDecision.
type explainFraudRuleDecisionOption func(*fraudv1.ExplainFraudRuleDecisionRequest)

// newExplainFraudRuleDecisionRequest builds a request of
// acme.fraud.v1.FraudRuleValidationService/ExplainFraudRuleDecision from options.
func newExplainFraudRuleDecisionRequest(options ...explainFraudRuleDecisionOption) *fraudv1.ExplainFraudRuleDecisionRequest {
	request := &fraudv1.ExplainFraudRuleDecisionRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainFraudRuleDecisionContext(value *commonv1.RequestContext) explainFraudRuleDecisionOption {
	return func(request *fraudv1.ExplainFraudRuleDecisionRequest) { request.Context = value }
}

func withExplainFraudRuleDecisionFraudRule(value *fraudv1.FraudRule) explainFraudRuleDecisionOption {
	return func(request *fraudv1.ExplainFraudRuleDecisionRequest) { request.FraudRule = value }
}

func withExplainFraudRuleDecisionDetails(value *fraudv1.ExplainFraudRuleDecisionRequest_Details) explainFraudRuleDecisionOption {
	return func(request *fraudv1.ExplainFraudRuleDecisionRequest) { request.Details = value }
}

// estimateWatchlistOption sets part of a request of acme.fraud.v1.WatchlistCalculationService/EstimateWatchlist.
type estimateWatchlistOption func(*fraudv1.EstimateWatchlistRequest)

// newEstimateWatchlistRequest builds a request of acme.fraud.v1.WatchlistCalculationService/EstimateWatchlist from
// options.
func newEstimateWatchlistRequest(options ...estimateWatchlistOption) *fraudv1.EstimateWatchlistRequest {
	request := &fraudv1.EstimateWatchlistRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withEstimateWatchlistContext(value *commonv1.RequestContext) estimateWatchlistOption {
	return func(request *fraudv1.EstimateWatchlistRequest) { request.Context = value }
}

func withEstimateWatchlistWatchlist(value *fraudv1.Watchlist) estimateWatchlistOption {
	return func(request *fraudv1.EstimateWatchlistRequest) { request.Watchlist = value }
}
