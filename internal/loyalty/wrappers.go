package loyalty

import (
	"context"
	"time"

	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	treasuryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/treasury/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredApiCredentialNotification records how long each call to acme.identity.v1.ApiCredentialNotificationService
// takes.
type meteredApiCredentialNotification struct {
	identityv1.ApiCredentialNotificationServiceClient
	recorder *metrics.Recorder
}

func (c meteredApiCredentialNotification) NotifyApiCredentialWatchers(ctx context.Context, request *identityv1.NotifyApiCredentialWatchersRequest, options ...grpc.CallOption) (*identityv1.NotifyApiCredentialWatchersResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.ApiCredentialNotificationService/NotifyApiCredentialWatchers", time.Now())
	return c.ApiCredentialNotificationServiceClient.NotifyApiCredentialWatchers(ctx, request, options...)
}

// meteredCashPosition records how long each call to acme.treasury.v1.CashPositionService takes.
type meteredCashPosition struct {
	treasuryv1.CashPositionServiceClient
	recorder *metrics.Recorder
}

func (c meteredCashPosition) CloneCashPosition(ctx context.Context, request *treasuryv1.CloneCashPositionRequest, options ...grpc.CallOption) (*treasuryv1.CloneCashPositionResponse, error) {
	defer c.recorder.Observe("acme.treasury.v1.CashPositionService/CloneCashPosition", time.Now())
	return c.CashPositionServiceClient.CloneCashPosition(ctx, request, options...)
}

// meteredGiftCardTransactionWorkflow records how long each call to acme.giftcards.v1.GiftCardTransactionWorkflowService
// takes.
type meteredGiftCardTransactionWorkflow struct {
	giftcardsv1.GiftCardTransactionWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredGiftCardTransactionWorkflow) ListGiftCardTransactionWorkflows(ctx context.Context, request *giftcardsv1.ListGiftCardTransactionWorkflowsRequest, options ...grpc.CallOption) (*giftcardsv1.ListGiftCardTransactionWorkflowsResponse, error) {
	defer c.recorder.Observe("acme.giftcards.v1.GiftCardTransactionWorkflowService/ListGiftCardTransactionWorkflows", time.Now())
	return c.GiftCardTransactionWorkflowServiceClient.ListGiftCardTransactionWorkflows(ctx, request, options...)
}

// meteredPointsLedgerApproval records how long each call to acme.loyalty.v1.PointsLedgerApprovalService takes.
type meteredPointsLedgerApproval struct {
	loyaltyv1.PointsLedgerApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredPointsLedgerApproval) ListPendingPointsLedgers(ctx context.Context, request *loyaltyv1.ListPendingPointsLedgersRequest, options ...grpc.CallOption) (*loyaltyv1.ListPendingPointsLedgersResponse, error) {
	defer c.recorder.Observe("acme.loyalty.v1.PointsLedgerApprovalService/ListPendingPointsLedgers", time.Now())
	return c.PointsLedgerApprovalServiceClient.ListPendingPointsLedgers(ctx, request, options...)
}

// meteredPointsLedgerCalculation records how long each call to acme.loyalty.v1.PointsLedgerCalculationService takes.
type meteredPointsLedgerCalculation struct {
	loyaltyv1.PointsLedgerCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPointsLedgerCalculation) SimulatePointsLedger(ctx context.Context, request *loyaltyv1.SimulatePointsLedgerRequest, options ...grpc.CallOption) (*loyaltyv1.SimulatePointsLedgerResponse, error) {
	defer c.recorder.Observe("acme.loyalty.v1.PointsLedgerCalculationService/SimulatePointsLedger", time.Now())
	return c.PointsLedgerCalculationServiceClient.SimulatePointsLedger(ctx, request, options...)
}

// meteredPointsLedgerWorkflow records how long each call to acme.loyalty.v1.PointsLedgerWorkflowService takes.
type meteredPointsLedgerWorkflow struct {
	loyaltyv1.PointsLedgerWorkflowServiceClient
	recorder *metrics.Recorder
}

// meteredReviewVoteCalculation records how long each call to acme.reviews.v1.ReviewVoteCalculationService takes.
type meteredReviewVoteCalculation struct {
	reviewsv1.ReviewVoteCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredReviewVoteCalculation) EstimateReviewVote(ctx context.Context, request *reviewsv1.EstimateReviewVoteRequest, options ...grpc.CallOption) (*reviewsv1.EstimateReviewVoteResponse, error) {
	defer c.recorder.Observe("acme.reviews.v1.ReviewVoteCalculationService/EstimateReviewVote", time.Now())
	return c.ReviewVoteCalculationServiceClient.EstimateReviewVote(ctx, request, options...)
}

// meteredSellerCalculation records how long each call to acme.sellers.v1.SellerCalculationService takes.
type meteredSellerCalculation struct {
	sellersv1.SellerCalculationServiceClient
	recorder *metrics.Recorder
}

// meteredTenantExport records how long each call to acme.identity.v1.TenantExportService takes.
type meteredTenantExport struct {
	identityv1.TenantExportServiceClient
	recorder *metrics.Recorder
}

func (c meteredTenantExport) RetryTenantExport(ctx context.Context, request *identityv1.RetryTenantExportRequest, options ...grpc.CallOption) (*identityv1.RetryTenantExportResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.TenantExportService/RetryTenantExport", time.Now())
	return c.TenantExportServiceClient.RetryTenantExport(ctx, request, options...)
}

// meteredTierReporting records how long each call to acme.loyalty.v1.TierReportingService takes.
type meteredTierReporting struct {
	loyaltyv1.TierReportingServiceClient
	recorder *metrics.Recorder
}

// meteredVenueValidation records how long each call to acme.venues.v1.VenueValidationService takes.
type meteredVenueValidation struct {
	venuesv1.VenueValidationServiceClient
	recorder *metrics.Recorder
}
