package inventory

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// householdQueryPort is what the inventory domain needs of acme.customers.v1.HouseholdQueryService.
type householdQueryPort interface {
	ResolveHouseholds(ctx context.Context, request *customersv1.ResolveHouseholdsRequest) (*customersv1.ResolveHouseholdsResponse, error)
}

// grpcHouseholdQueryPort reaches acme.customers.v1.HouseholdQueryService over gRPC.
type grpcHouseholdQueryPort struct {
	client customersv1.HouseholdQueryServiceClient
}

// sandboxHouseholdQueryPort answers in the sandbox environment, where acme.customers.v1.HouseholdQueryService is not
// deployed.
type sandboxHouseholdQueryPort struct{ log *slog.Logger }

func (p grpcHouseholdQueryPort) ResolveHouseholds(ctx context.Context, request *customersv1.ResolveHouseholdsRequest) (*customersv1.ResolveHouseholdsResponse, error) {
	return p.client.ResolveHouseholds(ctx, request)
}

func (p sandboxHouseholdQueryPort) ResolveHouseholds(ctx context.Context, request *customersv1.ResolveHouseholdsRequest) (*customersv1.ResolveHouseholdsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ResolveHouseholds")
	return &customersv1.ResolveHouseholdsResponse{}, nil
}

func newHouseholdQueryPort(deps platform.Deps) householdQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxHouseholdQueryPort{log: deps.Logger}
	}
	return grpcHouseholdQueryPort{client: customersv1.NewHouseholdQueryServiceClient(deps.Conn)}
}

// productReviewValidationPort is what the inventory domain needs of acme.reviews.v1.ProductReviewValidationService.
type productReviewValidationPort interface {
	PreviewProductReview(ctx context.Context, request *reviewsv1.PreviewProductReviewRequest) (*reviewsv1.PreviewProductReviewResponse, error)
}

// grpcProductReviewValidationPort reaches acme.reviews.v1.ProductReviewValidationService over gRPC.
type grpcProductReviewValidationPort struct {
	client reviewsv1.ProductReviewValidationServiceClient
}

// sandboxProductReviewValidationPort answers in the sandbox environment, where
// acme.reviews.v1.ProductReviewValidationService is not deployed.
type sandboxProductReviewValidationPort struct{ log *slog.Logger }

func (p grpcProductReviewValidationPort) PreviewProductReview(ctx context.Context, request *reviewsv1.PreviewProductReviewRequest) (*reviewsv1.PreviewProductReviewResponse, error) {
	return p.client.PreviewProductReview(ctx, request)
}

func (p sandboxProductReviewValidationPort) PreviewProductReview(ctx context.Context, request *reviewsv1.PreviewProductReviewRequest) (*reviewsv1.PreviewProductReviewResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewProductReview")
	return &reviewsv1.PreviewProductReviewResponse{}, nil
}

func newProductReviewValidationPort(deps platform.Deps) productReviewValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxProductReviewValidationPort{log: deps.Logger}
	}
	return grpcProductReviewValidationPort{client: reviewsv1.NewProductReviewValidationServiceClient(deps.Conn)}
}

// stockItemQueryPort is what the inventory domain needs of acme.inventory.v1.StockItemQueryService.
type stockItemQueryPort interface {
	BrowseStockItems(ctx context.Context, request *inventoryv1.BrowseStockItemsRequest) (*inventoryv1.BrowseStockItemsResponse, error)
}

// grpcStockItemQueryPort reaches acme.inventory.v1.StockItemQueryService over gRPC.
type grpcStockItemQueryPort struct {
	client inventoryv1.StockItemQueryServiceClient
}

// sandboxStockItemQueryPort answers in the sandbox environment, where acme.inventory.v1.StockItemQueryService is not
// deployed.
type sandboxStockItemQueryPort struct{ log *slog.Logger }

func (p grpcStockItemQueryPort) BrowseStockItems(ctx context.Context, request *inventoryv1.BrowseStockItemsRequest) (*inventoryv1.BrowseStockItemsResponse, error) {
	return p.client.BrowseStockItems(ctx, request)
}

func (p sandboxStockItemQueryPort) BrowseStockItems(ctx context.Context, request *inventoryv1.BrowseStockItemsRequest) (*inventoryv1.BrowseStockItemsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers BrowseStockItems")
	return &inventoryv1.BrowseStockItemsResponse{}, nil
}

func newStockItemQueryPort(deps platform.Deps) stockItemQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxStockItemQueryPort{log: deps.Logger}
	}
	return grpcStockItemQueryPort{client: inventoryv1.NewStockItemQueryServiceClient(deps.Conn)}
}
