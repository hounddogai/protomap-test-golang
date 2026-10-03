package subscriptions

import (
	"context"
	"time"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredAuditEventQuery records how long each call to acme.audit.v1.AuditEventQueryService takes.
type meteredAuditEventQuery struct {
	auditv1.AuditEventQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredAuditEventQuery) SuggestAuditEvents(ctx context.Context, request *auditv1.SuggestAuditEventsRequest, options ...grpc.CallOption) (*auditv1.SuggestAuditEventsResponse, error) {
	defer c.recorder.Observe("acme.audit.v1.AuditEventQueryService/SuggestAuditEvents", time.Now())
	return c.AuditEventQueryServiceClient.SuggestAuditEvents(ctx, request, options...)
}

// meteredGiftCardBatchQuery records how long each call to acme.giftcards.v1.GiftCardBatchQueryService takes.
type meteredGiftCardBatchQuery struct {
	giftcardsv1.GiftCardBatchQueryServiceClient
	recorder *metrics.Recorder
}

// meteredMetricSeries records how long each call to acme.metrics.v1.MetricSeriesService takes.
type meteredMetricSeries struct {
	metricsv1.MetricSeriesServiceClient
	recorder *metrics.Recorder
}

func (c meteredMetricSeries) ListMetricSerieses(ctx context.Context, request *metricsv1.ListMetricSeriesesRequest, options ...grpc.CallOption) (*metricsv1.ListMetricSeriesesResponse, error) {
	defer c.recorder.Observe("acme.metrics.v1.MetricSeriesService/ListMetricSerieses", time.Now())
	return c.MetricSeriesServiceClient.ListMetricSerieses(ctx, request, options...)
}

// meteredPreferenceValidation records how long each call to acme.customers.v1.PreferenceValidationService takes.
type meteredPreferenceValidation struct {
	customersv1.PreferenceValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPreferenceValidation) CheckPreferenceEligibility(ctx context.Context, request *customersv1.CheckPreferenceEligibilityRequest, options ...grpc.CallOption) (*customersv1.CheckPreferenceEligibilityResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.PreferenceValidationService/CheckPreferenceEligibility", time.Now())
	return c.PreferenceValidationServiceClient.CheckPreferenceEligibility(ctx, request, options...)
}

// meteredProductReview records how long each call to acme.reviews.v1.ProductReviewService takes.
type meteredProductReview struct {
	reviewsv1.ProductReviewServiceClient
	recorder *metrics.Recorder
}

func (c meteredProductReview) BatchGetProductReviews(ctx context.Context, request *reviewsv1.BatchGetProductReviewsRequest, options ...grpc.CallOption) (*reviewsv1.BatchGetProductReviewsResponse, error) {
	defer c.recorder.Observe("acme.reviews.v1.ProductReviewService/BatchGetProductReviews", time.Now())
	return c.ProductReviewServiceClient.BatchGetProductReviews(ctx, request, options...)
}

// meteredSubscriptionPlanApproval records how long each call to acme.subscriptions.v1.SubscriptionPlanApprovalService
// takes.
type meteredSubscriptionPlanApproval struct {
	subscriptionsv1.SubscriptionPlanApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredSubscriptionPlanApproval) ListPendingSubscriptionPlans(ctx context.Context, request *subscriptionsv1.ListPendingSubscriptionPlansRequest, options ...grpc.CallOption) (*subscriptionsv1.ListPendingSubscriptionPlansResponse, error) {
	defer c.recorder.Observe("acme.subscriptions.v1.SubscriptionPlanApprovalService/ListPendingSubscriptionPlans", time.Now())
	return c.SubscriptionPlanApprovalServiceClient.ListPendingSubscriptionPlans(ctx, request, options...)
}

// meteredSubscriptionPlanValidation records how long each call to
// acme.subscriptions.v1.SubscriptionPlanValidationService takes.
type meteredSubscriptionPlanValidation struct {
	subscriptionsv1.SubscriptionPlanValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredSubscriptionPlanValidation) NormalizeSubscriptionPlan(ctx context.Context, request *subscriptionsv1.NormalizeSubscriptionPlanRequest, options ...grpc.CallOption) (*subscriptionsv1.NormalizeSubscriptionPlanResponse, error) {
	defer c.recorder.Observe("acme.subscriptions.v1.SubscriptionPlanValidationService/NormalizeSubscriptionPlan", time.Now())
	return c.SubscriptionPlanValidationServiceClient.NormalizeSubscriptionPlan(ctx, request, options...)
}

// meteredUsageRecordCalculation records how long each call to acme.subscriptions.v1.UsageRecordCalculationService
// takes.
type meteredUsageRecordCalculation struct {
	subscriptionsv1.UsageRecordCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredUsageRecordCalculation) ExplainUsageRecordCalculation(ctx context.Context, request *subscriptionsv1.ExplainUsageRecordCalculationRequest, options ...grpc.CallOption) (*subscriptionsv1.ExplainUsageRecordCalculationResponse, error) {
	defer c.recorder.Observe("acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation", time.Now())
	return c.UsageRecordCalculationServiceClient.ExplainUsageRecordCalculation(ctx, request, options...)
}
