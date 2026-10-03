package warehousing

import (
	"context"
	"time"

	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredDocumentValidation records how long each call to acme.documents.v1.DocumentValidationService takes.
type meteredDocumentValidation struct {
	documentsv1.DocumentValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredDocumentValidation) PreviewDocument(ctx context.Context, request *documentsv1.PreviewDocumentRequest, options ...grpc.CallOption) (*documentsv1.PreviewDocumentResponse, error) {
	defer c.recorder.Observe("acme.documents.v1.DocumentValidationService/PreviewDocument", time.Now())
	return c.DocumentValidationServiceClient.PreviewDocument(ctx, request, options...)
}

// meteredPutawayTaskValidation records how long each call to acme.warehousing.v1.PutawayTaskValidationService takes.
type meteredPutawayTaskValidation struct {
	warehousingv1.PutawayTaskValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPutawayTaskValidation) NormalizePutawayTask(ctx context.Context, request *warehousingv1.NormalizePutawayTaskRequest, options ...grpc.CallOption) (*warehousingv1.NormalizePutawayTaskResponse, error) {
	defer c.recorder.Observe("acme.warehousing.v1.PutawayTaskValidationService/NormalizePutawayTask", time.Now())
	return c.PutawayTaskValidationServiceClient.NormalizePutawayTask(ctx, request, options...)
}

// meteredReviewVoteWorkflow records how long each call to acme.reviews.v1.ReviewVoteWorkflowService takes.
type meteredReviewVoteWorkflow struct {
	reviewsv1.ReviewVoteWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredReviewVoteWorkflow) SignalReviewVoteWorkflow(ctx context.Context, request *reviewsv1.SignalReviewVoteWorkflowRequest, options ...grpc.CallOption) (*reviewsv1.SignalReviewVoteWorkflowResponse, error) {
	defer c.recorder.Observe("acme.reviews.v1.ReviewVoteWorkflowService/SignalReviewVoteWorkflow", time.Now())
	return c.ReviewVoteWorkflowServiceClient.SignalReviewVoteWorkflow(ctx, request, options...)
}

// meteredSellerValidation records how long each call to acme.sellers.v1.SellerValidationService takes.
type meteredSellerValidation struct {
	sellersv1.SellerValidationServiceClient
	recorder *metrics.Recorder
}

// meteredTierReporting records how long each call to acme.loyalty.v1.TierReportingService takes.
type meteredTierReporting struct {
	loyaltyv1.TierReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredTierReporting) CountTierTotals(ctx context.Context, request *loyaltyv1.CountTierTotalsRequest, options ...grpc.CallOption) (*loyaltyv1.CountTierTotalsResponse, error) {
	defer c.recorder.Observe("acme.loyalty.v1.TierReportingService/CountTierTotals", time.Now())
	return c.TierReportingServiceClient.CountTierTotals(ctx, request, options...)
}

// meteredVenue records how long each call to acme.venues.v1.VenueService takes.
type meteredVenue struct {
	venuesv1.VenueServiceClient
	recorder *metrics.Recorder
}
