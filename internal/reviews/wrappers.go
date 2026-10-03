package reviews

import (
	"context"
	"time"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredCollection records how long each call to acme.merchandising.v1.CollectionService takes.
type meteredCollection struct {
	merchandisingv1.CollectionServiceClient
	recorder *metrics.Recorder
}

// meteredGiftCardTransactionApproval records how long each call to acme.giftcards.v1.GiftCardTransactionApprovalService
// takes.
type meteredGiftCardTransactionApproval struct {
	giftcardsv1.GiftCardTransactionApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredGiftCardTransactionApproval) SubmitGiftCardTransaction(ctx context.Context, request *giftcardsv1.SubmitGiftCardTransactionRequest, options ...grpc.CallOption) (*giftcardsv1.SubmitGiftCardTransactionResponse, error) {
	defer c.recorder.Observe("acme.giftcards.v1.GiftCardTransactionApprovalService/SubmitGiftCardTransaction", time.Now())
	return c.GiftCardTransactionApprovalServiceClient.SubmitGiftCardTransaction(ctx, request, options...)
}

// meteredHouseholdValidation records how long each call to acme.customers.v1.HouseholdValidationService takes.
type meteredHouseholdValidation struct {
	customersv1.HouseholdValidationServiceClient
	recorder *metrics.Recorder
}

// meteredPipelineCalculation records how long each call to acme.pipelines.v1.PipelineCalculationService takes.
type meteredPipelineCalculation struct {
	pipelinesv1.PipelineCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPipelineCalculation) RecalculatePipelines(ctx context.Context, request *pipelinesv1.RecalculatePipelinesRequest, options ...grpc.CallOption) (*pipelinesv1.RecalculatePipelinesResponse, error) {
	defer c.recorder.Observe("acme.pipelines.v1.PipelineCalculationService/RecalculatePipelines", time.Now())
	return c.PipelineCalculationServiceClient.RecalculatePipelines(ctx, request, options...)
}

// meteredPlacementValidation records how long each call to acme.merchandising.v1.PlacementValidationService takes.
type meteredPlacementValidation struct {
	merchandisingv1.PlacementValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPlacementValidation) ValidatePlacement(ctx context.Context, request *merchandisingv1.ValidatePlacementRequest, options ...grpc.CallOption) (*merchandisingv1.ValidatePlacementResponse, error) {
	defer c.recorder.Observe("acme.merchandising.v1.PlacementValidationService/ValidatePlacement", time.Now())
	return c.PlacementValidationServiceClient.ValidatePlacement(ctx, request, options...)
}

// meteredProductReview records how long each call to acme.reviews.v1.ProductReviewService takes.
type meteredProductReview struct {
	reviewsv1.ProductReviewServiceClient
	recorder *metrics.Recorder
}

// meteredSynonymExport records how long each call to acme.search.v1.SynonymExportService takes.
type meteredSynonymExport struct {
	searchv1.SynonymExportServiceClient
	recorder *metrics.Recorder
}

func (c meteredSynonymExport) ScheduleSynonymExport(ctx context.Context, request *searchv1.ScheduleSynonymExportRequest, options ...grpc.CallOption) (*searchv1.ScheduleSynonymExportResponse, error) {
	defer c.recorder.Observe("acme.search.v1.SynonymExportService/ScheduleSynonymExport", time.Now())
	return c.SynonymExportServiceClient.ScheduleSynonymExport(ctx, request, options...)
}
