package promotions

import (
	"context"
	"time"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredCartQuery records how long each call to acme.cart.v1.CartQueryService takes.
type meteredCartQuery struct {
	cartv1.CartQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredCartQuery) SuggestCarts(ctx context.Context, request *cartv1.SuggestCartsRequest, options ...grpc.CallOption) (*cartv1.SuggestCartsResponse, error) {
	defer c.recorder.Observe("acme.cart.v1.CartQueryService/SuggestCarts", time.Now())
	return c.CartQueryServiceClient.SuggestCarts(ctx, request, options...)
}

// meteredCouponValidation records how long each call to acme.promotions.v1.CouponValidationService takes.
type meteredCouponValidation struct {
	promotionsv1.CouponValidationServiceClient
	recorder *metrics.Recorder
}

// meteredPackingSlipApproval records how long each call to acme.fulfillment.v1.PackingSlipApprovalService takes.
type meteredPackingSlipApproval struct {
	fulfillmentv1.PackingSlipApprovalServiceClient
	recorder *metrics.Recorder
}

// meteredParcelWorkflow records how long each call to acme.shipping.v1.ParcelWorkflowService takes.
type meteredParcelWorkflow struct {
	shippingv1.ParcelWorkflowServiceClient
	recorder *metrics.Recorder
}

// meteredPickListApproval records how long each call to acme.warehousing.v1.PickListApprovalService takes.
type meteredPickListApproval struct {
	warehousingv1.PickListApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredPickListApproval) SubmitPickList(ctx context.Context, request *warehousingv1.SubmitPickListRequest, options ...grpc.CallOption) (*warehousingv1.SubmitPickListResponse, error) {
	defer c.recorder.Observe("acme.warehousing.v1.PickListApprovalService/SubmitPickList", time.Now())
	return c.PickListApprovalServiceClient.SubmitPickList(ctx, request, options...)
}

// meteredRedemptionSync records how long each call to acme.promotions.v1.RedemptionSyncService takes.
type meteredRedemptionSync struct {
	promotionsv1.RedemptionSyncServiceClient
	recorder *metrics.Recorder
}

func (c meteredRedemptionSync) ReconcileRedemptions(ctx context.Context, request *promotionsv1.ReconcileRedemptionsRequest, options ...grpc.CallOption) (*promotionsv1.ReconcileRedemptionsResponse, error) {
	defer c.recorder.Observe("acme.promotions.v1.RedemptionSyncService/ReconcileRedemptions", time.Now())
	return c.RedemptionSyncServiceClient.ReconcileRedemptions(ctx, request, options...)
}

// meteredReturnLabelWorkflow records how long each call to acme.returns.v1.ReturnLabelWorkflowService takes.
type meteredReturnLabelWorkflow struct {
	returnsv1.ReturnLabelWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredReturnLabelWorkflow) StartReturnLabelWorkflow(ctx context.Context, request *returnsv1.StartReturnLabelWorkflowRequest, options ...grpc.CallOption) (*returnsv1.StartReturnLabelWorkflowResponse, error) {
	defer c.recorder.Observe("acme.returns.v1.ReturnLabelWorkflowService/StartReturnLabelWorkflow", time.Now())
	return c.ReturnLabelWorkflowServiceClient.StartReturnLabelWorkflow(ctx, request, options...)
}

// meteredReviewModerationApproval records how long each call to acme.reviews.v1.ReviewModerationApprovalService takes.
type meteredReviewModerationApproval struct {
	reviewsv1.ReviewModerationApprovalServiceClient
	recorder *metrics.Recorder
}

// meteredRiskAssessmentWorkflow records how long each call to acme.fraud.v1.RiskAssessmentWorkflowService takes.
type meteredRiskAssessmentWorkflow struct {
	fraudv1.RiskAssessmentWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredRiskAssessmentWorkflow) CancelRiskAssessmentWorkflow(ctx context.Context, request *fraudv1.CancelRiskAssessmentWorkflowRequest, options ...grpc.CallOption) (*fraudv1.CancelRiskAssessmentWorkflowResponse, error) {
	defer c.recorder.Observe("acme.fraud.v1.RiskAssessmentWorkflowService/CancelRiskAssessmentWorkflow", time.Now())
	return c.RiskAssessmentWorkflowServiceClient.CancelRiskAssessmentWorkflow(ctx, request, options...)
}

// meteredSearchIndexQuery records how long each call to acme.search.v1.SearchIndexQueryService takes.
type meteredSearchIndexQuery struct {
	searchv1.SearchIndexQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredSearchIndexQuery) SummarizeSearchIndexes(ctx context.Context, request *searchv1.SummarizeSearchIndexesRequest, options ...grpc.CallOption) (*searchv1.SummarizeSearchIndexesResponse, error) {
	defer c.recorder.Observe("acme.search.v1.SearchIndexQueryService/SummarizeSearchIndexes", time.Now())
	return c.SearchIndexQueryServiceClient.SummarizeSearchIndexes(ctx, request, options...)
}

// meteredStockItemApproval records how long each call to acme.inventory.v1.StockItemApprovalService takes.
type meteredStockItemApproval struct {
	inventoryv1.StockItemApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredStockItemApproval) EscalateStockItem(ctx context.Context, request *inventoryv1.EscalateStockItemRequest, options ...grpc.CallOption) (*inventoryv1.EscalateStockItemResponse, error) {
	defer c.recorder.Observe("acme.inventory.v1.StockItemApprovalService/EscalateStockItem", time.Now())
	return c.StockItemApprovalServiceClient.EscalateStockItem(ctx, request, options...)
}
