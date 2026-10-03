package inventory

import (
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
)

// resumePlacementWorkflowOption sets part of a request of
// acme.merchandising.v1.PlacementWorkflowService/ResumePlacementWorkflow.
type resumePlacementWorkflowOption func(*merchandisingv1.ResumePlacementWorkflowRequest)

// newResumePlacementWorkflowRequest builds a request of
// acme.merchandising.v1.PlacementWorkflowService/ResumePlacementWorkflow from options.
func newResumePlacementWorkflowRequest(options ...resumePlacementWorkflowOption) *merchandisingv1.ResumePlacementWorkflowRequest {
	request := &merchandisingv1.ResumePlacementWorkflowRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withResumePlacementWorkflowContext(value *commonv1.RequestContext) resumePlacementWorkflowOption {
	return func(request *merchandisingv1.ResumePlacementWorkflowRequest) { request.Context = value }
}

func withResumePlacementWorkflowPlacement(value *merchandisingv1.Placement) resumePlacementWorkflowOption {
	return func(request *merchandisingv1.ResumePlacementWorkflowRequest) { request.Placement = value }
}

func withResumePlacementWorkflowPlacementId(value string) resumePlacementWorkflowOption {
	return func(request *merchandisingv1.ResumePlacementWorkflowRequest) { request.PlacementId = value }
}

// findStockReservationsOption sets part of a request of
// acme.inventory.v1.StockReservationQueryService/FindStockReservations.
type findStockReservationsOption func(*inventoryv1.FindStockReservationsRequest)

// newFindStockReservationsRequest builds a request of
// acme.inventory.v1.StockReservationQueryService/FindStockReservations from options.
func newFindStockReservationsRequest(options ...findStockReservationsOption) *inventoryv1.FindStockReservationsRequest {
	request := &inventoryv1.FindStockReservationsRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withFindStockReservationsContext(value *commonv1.RequestContext) findStockReservationsOption {
	return func(request *inventoryv1.FindStockReservationsRequest) { request.Context = value }
}

func withFindStockReservationsFilter(value *inventoryv1.FindStockReservationsRequest_Filter) findStockReservationsOption {
	return func(request *inventoryv1.FindStockReservationsRequest) { request.Filter = value }
}

// quoteReplenishmentPlanOption sets part of a request of
// acme.inventory.v1.ReplenishmentPlanCalculationService/QuoteReplenishmentPlan.
type quoteReplenishmentPlanOption func(*inventoryv1.QuoteReplenishmentPlanRequest)

// newQuoteReplenishmentPlanRequest builds a request of
// acme.inventory.v1.ReplenishmentPlanCalculationService/QuoteReplenishmentPlan from options.
func newQuoteReplenishmentPlanRequest(options ...quoteReplenishmentPlanOption) *inventoryv1.QuoteReplenishmentPlanRequest {
	request := &inventoryv1.QuoteReplenishmentPlanRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withQuoteReplenishmentPlanContext(value *commonv1.RequestContext) quoteReplenishmentPlanOption {
	return func(request *inventoryv1.QuoteReplenishmentPlanRequest) { request.Context = value }
}

func withQuoteReplenishmentPlanReplenishmentPlan(value *inventoryv1.ReplenishmentPlan) quoteReplenishmentPlanOption {
	return func(request *inventoryv1.QuoteReplenishmentPlanRequest) { request.ReplenishmentPlan = value }
}

// explainReplenishmentPlanCalculationOption sets part of a request of
// acme.inventory.v1.ReplenishmentPlanCalculationService/ExplainReplenishmentPlanCalculation.
type explainReplenishmentPlanCalculationOption func(*inventoryv1.ExplainReplenishmentPlanCalculationRequest)

// newExplainReplenishmentPlanCalculationRequest builds a request of
// acme.inventory.v1.ReplenishmentPlanCalculationService/ExplainReplenishmentPlanCalculation from options.
func newExplainReplenishmentPlanCalculationRequest(options ...explainReplenishmentPlanCalculationOption) *inventoryv1.ExplainReplenishmentPlanCalculationRequest {
	request := &inventoryv1.ExplainReplenishmentPlanCalculationRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withExplainReplenishmentPlanCalculationContext(value *commonv1.RequestContext) explainReplenishmentPlanCalculationOption {
	return func(request *inventoryv1.ExplainReplenishmentPlanCalculationRequest) { request.Context = value }
}

func withExplainReplenishmentPlanCalculationReplenishmentPlan(value *inventoryv1.ReplenishmentPlan) explainReplenishmentPlanCalculationOption {
	return func(request *inventoryv1.ExplainReplenishmentPlanCalculationRequest) {
		request.ReplenishmentPlan = value
	}
}

// simulateReplenishmentPlanOption sets part of a request of
// acme.inventory.v1.ReplenishmentPlanCalculationService/SimulateReplenishmentPlan.
type simulateReplenishmentPlanOption func(*inventoryv1.SimulateReplenishmentPlanRequest)

// newSimulateReplenishmentPlanRequest builds a request of
// acme.inventory.v1.ReplenishmentPlanCalculationService/SimulateReplenishmentPlan from options.
func newSimulateReplenishmentPlanRequest(options ...simulateReplenishmentPlanOption) *inventoryv1.SimulateReplenishmentPlanRequest {
	request := &inventoryv1.SimulateReplenishmentPlanRequest{}
	for _, option := range options {
		option(request)
	}
	return request
}

func withSimulateReplenishmentPlanContext(value *commonv1.RequestContext) simulateReplenishmentPlanOption {
	return func(request *inventoryv1.SimulateReplenishmentPlanRequest) { request.Context = value }
}

func withSimulateReplenishmentPlanReplenishmentPlan(value *inventoryv1.ReplenishmentPlan) simulateReplenishmentPlanOption {
	return func(request *inventoryv1.SimulateReplenishmentPlanRequest) { request.ReplenishmentPlan = value }
}
