package catalog

import (
	"context"
	"time"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	dispatchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/dispatch/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredBrandValidation records how long each call to acme.catalog.v1.BrandValidationService takes.
type meteredBrandValidation struct {
	catalogv1.BrandValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredBrandValidation) NormalizeBrand(ctx context.Context, request *catalogv1.NormalizeBrandRequest, options ...grpc.CallOption) (*catalogv1.NormalizeBrandResponse, error) {
	defer c.recorder.Observe("acme.catalog.v1.BrandValidationService/NormalizeBrand", time.Now())
	return c.BrandValidationServiceClient.NormalizeBrand(ctx, request, options...)
}

// meteredCartCalculation records how long each call to acme.cart.v1.CartCalculationService takes.
type meteredCartCalculation struct {
	cartv1.CartCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCartCalculation) QuoteCart(ctx context.Context, request *cartv1.QuoteCartRequest, options ...grpc.CallOption) (*cartv1.QuoteCartResponse, error) {
	defer c.recorder.Observe("acme.cart.v1.CartCalculationService/QuoteCart", time.Now())
	return c.CartCalculationServiceClient.QuoteCart(ctx, request, options...)
}

// meteredCategoryCalculation records how long each call to acme.catalog.v1.CategoryCalculationService takes.
type meteredCategoryCalculation struct {
	catalogv1.CategoryCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCategoryCalculation) QuoteCategory(ctx context.Context, request *catalogv1.QuoteCategoryRequest, options ...grpc.CallOption) (*catalogv1.QuoteCategoryResponse, error) {
	defer c.recorder.Observe("acme.catalog.v1.CategoryCalculationService/QuoteCategory", time.Now())
	return c.CategoryCalculationServiceClient.QuoteCategory(ctx, request, options...)
}

// meteredCategoryValidation records how long each call to acme.catalog.v1.CategoryValidationService takes.
type meteredCategoryValidation struct {
	catalogv1.CategoryValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCategoryValidation) CheckCategoryEligibility(ctx context.Context, request *catalogv1.CheckCategoryEligibilityRequest, options ...grpc.CallOption) (*catalogv1.CheckCategoryEligibilityResponse, error) {
	defer c.recorder.Observe("acme.catalog.v1.CategoryValidationService/CheckCategoryEligibility", time.Now())
	return c.CategoryValidationServiceClient.CheckCategoryEligibility(ctx, request, options...)
}

// meteredDispatchOrder records how long each call to acme.dispatch.v1.DispatchOrderService takes.
type meteredDispatchOrder struct {
	dispatchv1.DispatchOrderServiceClient
	recorder *metrics.Recorder
}

func (c meteredDispatchOrder) BatchGetDispatchOrders(ctx context.Context, request *dispatchv1.BatchGetDispatchOrdersRequest, options ...grpc.CallOption) (*dispatchv1.BatchGetDispatchOrdersResponse, error) {
	defer c.recorder.Observe("acme.dispatch.v1.DispatchOrderService/BatchGetDispatchOrders", time.Now())
	return c.DispatchOrderServiceClient.BatchGetDispatchOrders(ctx, request, options...)
}

// meteredForm records how long each call to acme.forms.v1.FormService takes.
type meteredForm struct {
	formsv1.FormServiceClient
	recorder *metrics.Recorder
}

func (c meteredForm) CreateForm(ctx context.Context, request *formsv1.CreateFormRequest, options ...grpc.CallOption) (*formsv1.CreateFormResponse, error) {
	defer c.recorder.Observe("acme.forms.v1.FormService/CreateForm", time.Now())
	return c.FormServiceClient.CreateForm(ctx, request, options...)
}

// meteredFuelCard records how long each call to acme.fuel.v1.FuelCardService takes.
type meteredFuelCard struct {
	fuelv1.FuelCardServiceClient
	recorder *metrics.Recorder
}

func (c meteredFuelCard) CloneFuelCard(ctx context.Context, request *fuelv1.CloneFuelCardRequest, options ...grpc.CallOption) (*fuelv1.CloneFuelCardResponse, error) {
	defer c.recorder.Observe("acme.fuel.v1.FuelCardService/CloneFuelCard", time.Now())
	return c.FuelCardServiceClient.CloneFuelCard(ctx, request, options...)
}

// meteredFulfillment records how long each call to acme.fulfillment.v1.FulfillmentService takes.
type meteredFulfillment struct {
	fulfillmentv1.FulfillmentServiceClient
	recorder *metrics.Recorder
}

func (c meteredFulfillment) BatchGetFulfillments(ctx context.Context, request *fulfillmentv1.BatchGetFulfillmentsRequest, options ...grpc.CallOption) (*fulfillmentv1.BatchGetFulfillmentsResponse, error) {
	defer c.recorder.Observe("acme.fulfillment.v1.FulfillmentService/BatchGetFulfillments", time.Now())
	return c.FulfillmentServiceClient.BatchGetFulfillments(ctx, request, options...)
}

// meteredGiftCardBatchApproval records how long each call to acme.giftcards.v1.GiftCardBatchApprovalService takes.
type meteredGiftCardBatchApproval struct {
	giftcardsv1.GiftCardBatchApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredGiftCardBatchApproval) DelegateGiftCardBatchApproval(ctx context.Context, request *giftcardsv1.DelegateGiftCardBatchApprovalRequest, options ...grpc.CallOption) (*giftcardsv1.DelegateGiftCardBatchApprovalResponse, error) {
	defer c.recorder.Observe("acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval", time.Now())
	return c.GiftCardBatchApprovalServiceClient.DelegateGiftCardBatchApproval(ctx, request, options...)
}

// meteredProductApproval records how long each call to acme.catalog.v1.ProductApprovalService takes.
type meteredProductApproval struct {
	catalogv1.ProductApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredProductApproval) EscalateProduct(ctx context.Context, request *catalogv1.EscalateProductRequest, options ...grpc.CallOption) (*catalogv1.EscalateProductResponse, error) {
	defer c.recorder.Observe("acme.catalog.v1.ProductApprovalService/EscalateProduct", time.Now())
	return c.ProductApprovalServiceClient.EscalateProduct(ctx, request, options...)
}

// meteredProductCalculation records how long each call to acme.catalog.v1.ProductCalculationService takes.
type meteredProductCalculation struct {
	catalogv1.ProductCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredProductCalculation) SimulateProduct(ctx context.Context, request *catalogv1.SimulateProductRequest, options ...grpc.CallOption) (*catalogv1.SimulateProductResponse, error) {
	defer c.recorder.Observe("acme.catalog.v1.ProductCalculationService/SimulateProduct", time.Now())
	return c.ProductCalculationServiceClient.SimulateProduct(ctx, request, options...)
}

// meteredProductValidation records how long each call to acme.catalog.v1.ProductValidationService takes.
type meteredProductValidation struct {
	catalogv1.ProductValidationServiceClient
	recorder *metrics.Recorder
}

// meteredProductVariantValidation records how long each call to acme.catalog.v1.ProductVariantValidationService takes.
type meteredProductVariantValidation struct {
	catalogv1.ProductVariantValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredProductVariantValidation) PreviewProductVariant(ctx context.Context, request *catalogv1.PreviewProductVariantRequest, options ...grpc.CallOption) (*catalogv1.PreviewProductVariantResponse, error) {
	defer c.recorder.Observe("acme.catalog.v1.ProductVariantValidationService/PreviewProductVariant", time.Now())
	return c.ProductVariantValidationServiceClient.PreviewProductVariant(ctx, request, options...)
}

func (c meteredProductVariantValidation) ValidateProductVariant(ctx context.Context, request *catalogv1.ValidateProductVariantRequest, options ...grpc.CallOption) (*catalogv1.ValidateProductVariantResponse, error) {
	defer c.recorder.Observe("acme.catalog.v1.ProductVariantValidationService/ValidateProductVariant", time.Now())
	return c.ProductVariantValidationServiceClient.ValidateProductVariant(ctx, request, options...)
}

// meteredSearchIndexHistory records how long each call to acme.search.v1.SearchIndexHistoryService takes.
type meteredSearchIndexHistory struct {
	searchv1.SearchIndexHistoryServiceClient
	recorder *metrics.Recorder
}

// meteredTelemetryReadingCalculation records how long each call to
// acme.telematics.v1.TelemetryReadingCalculationService takes.
type meteredTelemetryReadingCalculation struct {
	telematicsv1.TelemetryReadingCalculationServiceClient
	recorder *metrics.Recorder
}

// meteredTierReporting records how long each call to acme.loyalty.v1.TierReportingService takes.
type meteredTierReporting struct {
	loyaltyv1.TierReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredTierReporting) ShareTierReport(ctx context.Context, request *loyaltyv1.ShareTierReportRequest, options ...grpc.CallOption) (*loyaltyv1.ShareTierReportResponse, error) {
	defer c.recorder.Observe("acme.loyalty.v1.TierReportingService/ShareTierReport", time.Now())
	return c.TierReportingServiceClient.ShareTierReport(ctx, request, options...)
}

// meteredUserHistory records how long each call to acme.identity.v1.UserHistoryService takes.
type meteredUserHistory struct {
	identityv1.UserHistoryServiceClient
	recorder *metrics.Recorder
}

func (c meteredUserHistory) ListUserVersions(ctx context.Context, request *identityv1.ListUserVersionsRequest, options ...grpc.CallOption) (*identityv1.ListUserVersionsResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.UserHistoryService/ListUserVersions", time.Now())
	return c.UserHistoryServiceClient.ListUserVersions(ctx, request, options...)
}

// meteredUserQuery records how long each call to acme.identity.v1.UserQueryService takes.
type meteredUserQuery struct {
	identityv1.UserQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredUserQuery) FindUsers(ctx context.Context, request *identityv1.FindUsersRequest, options ...grpc.CallOption) (*identityv1.FindUsersResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.UserQueryService/FindUsers", time.Now())
	return c.UserQueryServiceClient.FindUsers(ctx, request, options...)
}
