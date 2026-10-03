package inventory

import (
	"context"
	"time"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredCustomerSegmentValidation records how long each call to acme.customers.v1.CustomerSegmentValidationService
// takes.
type meteredCustomerSegmentValidation struct {
	customersv1.CustomerSegmentValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomerSegmentValidation) LintCustomerSegments(ctx context.Context, request *customersv1.LintCustomerSegmentsRequest, options ...grpc.CallOption) (*customersv1.LintCustomerSegmentsResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments", time.Now())
	return c.CustomerSegmentValidationServiceClient.LintCustomerSegments(ctx, request, options...)
}

// meteredReplenishmentPlanCalculation records how long each call to
// acme.inventory.v1.ReplenishmentPlanCalculationService takes.
type meteredReplenishmentPlanCalculation struct {
	inventoryv1.ReplenishmentPlanCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredReplenishmentPlanCalculation) CompareReplenishmentPlanOptions(ctx context.Context, request *inventoryv1.CompareReplenishmentPlanOptionsRequest, options ...grpc.CallOption) (*inventoryv1.CompareReplenishmentPlanOptionsResponse, error) {
	defer c.recorder.Observe("acme.inventory.v1.ReplenishmentPlanCalculationService/CompareReplenishmentPlanOptions", time.Now())
	return c.ReplenishmentPlanCalculationServiceClient.CompareReplenishmentPlanOptions(ctx, request, options...)
}

// meteredStockReservationQuery records how long each call to acme.inventory.v1.StockReservationQueryService takes.
type meteredStockReservationQuery struct {
	inventoryv1.StockReservationQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredStockReservationQuery) SummarizeStockReservations(ctx context.Context, request *inventoryv1.SummarizeStockReservationsRequest, options ...grpc.CallOption) (*inventoryv1.SummarizeStockReservationsResponse, error) {
	defer c.recorder.Observe("acme.inventory.v1.StockReservationQueryService/SummarizeStockReservations", time.Now())
	return c.StockReservationQueryServiceClient.SummarizeStockReservations(ctx, request, options...)
}
