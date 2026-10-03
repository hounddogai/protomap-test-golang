package customers

import (
	"context"
	"time"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	deliveriesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/deliveries/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredAuditEventQuery records how long each call to acme.audit.v1.AuditEventQueryService takes.
type meteredAuditEventQuery struct {
	auditv1.AuditEventQueryServiceClient
	recorder *metrics.Recorder
}

// meteredBrandLifecycle records how long each call to acme.catalog.v1.BrandLifecycleService takes.
type meteredBrandLifecycle struct {
	catalogv1.BrandLifecycleServiceClient
	recorder *metrics.Recorder
}

func (c meteredBrandLifecycle) ScheduleBrandExpiry(ctx context.Context, request *catalogv1.ScheduleBrandExpiryRequest, options ...grpc.CallOption) (*catalogv1.ScheduleBrandExpiryResponse, error) {
	defer c.recorder.Observe("acme.catalog.v1.BrandLifecycleService/ScheduleBrandExpiry", time.Now())
	return c.BrandLifecycleServiceClient.ScheduleBrandExpiry(ctx, request, options...)
}

// meteredConsentRecordReporting records how long each call to acme.customers.v1.ConsentRecordReportingService takes.
type meteredConsentRecordReporting struct {
	customersv1.ConsentRecordReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredConsentRecordReporting) ShareConsentRecordReport(ctx context.Context, request *customersv1.ShareConsentRecordReportRequest, options ...grpc.CallOption) (*customersv1.ShareConsentRecordReportResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.ConsentRecordReportingService/ShareConsentRecordReport", time.Now())
	return c.ConsentRecordReportingServiceClient.ShareConsentRecordReport(ctx, request, options...)
}

// meteredCustomerCalculation records how long each call to acme.customers.v1.CustomerCalculationService takes.
type meteredCustomerCalculation struct {
	customersv1.CustomerCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomerCalculation) ExplainCustomerCalculation(ctx context.Context, request *customersv1.ExplainCustomerCalculationRequest, options ...grpc.CallOption) (*customersv1.ExplainCustomerCalculationResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation", time.Now())
	return c.CustomerCalculationServiceClient.ExplainCustomerCalculation(ctx, request, options...)
}

// meteredCustomerQuery records how long each call to acme.customers.v1.CustomerQueryService takes.
type meteredCustomerQuery struct {
	customersv1.CustomerQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomerQuery) SummarizeCustomers(ctx context.Context, request *customersv1.SummarizeCustomersRequest, options ...grpc.CallOption) (*customersv1.SummarizeCustomersResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerQueryService/SummarizeCustomers", time.Now())
	return c.CustomerQueryServiceClient.SummarizeCustomers(ctx, request, options...)
}

// meteredCustomerSegmentCalculation records how long each call to acme.customers.v1.CustomerSegmentCalculationService
// takes.
type meteredCustomerSegmentCalculation struct {
	customersv1.CustomerSegmentCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomerSegmentCalculation) ExplainCustomerSegmentCalculation(ctx context.Context, request *customersv1.ExplainCustomerSegmentCalculationRequest, options ...grpc.CallOption) (*customersv1.ExplainCustomerSegmentCalculationResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerSegmentCalculationService/ExplainCustomerSegmentCalculation", time.Now())
	return c.CustomerSegmentCalculationServiceClient.ExplainCustomerSegmentCalculation(ctx, request, options...)
}

// meteredCustomerValidation records how long each call to acme.customers.v1.CustomerValidationService takes.
type meteredCustomerValidation struct {
	customersv1.CustomerValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomerValidation) CheckCustomerEligibility(ctx context.Context, request *customersv1.CheckCustomerEligibilityRequest, options ...grpc.CallOption) (*customersv1.CheckCustomerEligibilityResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerValidationService/CheckCustomerEligibility", time.Now())
	return c.CustomerValidationServiceClient.CheckCustomerEligibility(ctx, request, options...)
}

// meteredDelivery records how long each call to acme.deliveries.v1.DeliveryService takes.
type meteredDelivery struct {
	deliveriesv1.DeliveryServiceClient
	recorder *metrics.Recorder
}

func (c meteredDelivery) CloneDelivery(ctx context.Context, request *deliveriesv1.CloneDeliveryRequest, options ...grpc.CallOption) (*deliveriesv1.CloneDeliveryResponse, error) {
	defer c.recorder.Observe("acme.deliveries.v1.DeliveryService/CloneDelivery", time.Now())
	return c.DeliveryServiceClient.CloneDelivery(ctx, request, options...)
}

// meteredHouseholdValidation records how long each call to acme.customers.v1.HouseholdValidationService takes.
type meteredHouseholdValidation struct {
	customersv1.HouseholdValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredHouseholdValidation) NormalizeHousehold(ctx context.Context, request *customersv1.NormalizeHouseholdRequest, options ...grpc.CallOption) (*customersv1.NormalizeHouseholdResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.HouseholdValidationService/NormalizeHousehold", time.Now())
	return c.HouseholdValidationServiceClient.NormalizeHousehold(ctx, request, options...)
}

// meteredMetricSeriesQuery records how long each call to acme.metrics.v1.MetricSeriesQueryService takes.
type meteredMetricSeriesQuery struct {
	metricsv1.MetricSeriesQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredMetricSeriesQuery) SuggestMetricSerieses(ctx context.Context, request *metricsv1.SuggestMetricSeriesesRequest, options ...grpc.CallOption) (*metricsv1.SuggestMetricSeriesesResponse, error) {
	defer c.recorder.Observe("acme.metrics.v1.MetricSeriesQueryService/SuggestMetricSerieses", time.Now())
	return c.MetricSeriesQueryServiceClient.SuggestMetricSerieses(ctx, request, options...)
}

// meteredMetricSeriesValidation records how long each call to acme.metrics.v1.MetricSeriesValidationService takes.
type meteredMetricSeriesValidation struct {
	metricsv1.MetricSeriesValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredMetricSeriesValidation) LintMetricSerieses(ctx context.Context, request *metricsv1.LintMetricSeriesesRequest, options ...grpc.CallOption) (*metricsv1.LintMetricSeriesesResponse, error) {
	defer c.recorder.Observe("acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses", time.Now())
	return c.MetricSeriesValidationServiceClient.LintMetricSerieses(ctx, request, options...)
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

// meteredPickListWorkflow records how long each call to acme.warehousing.v1.PickListWorkflowService takes.
type meteredPickListWorkflow struct {
	warehousingv1.PickListWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredPickListWorkflow) ListPickListWorkflows(ctx context.Context, request *warehousingv1.ListPickListWorkflowsRequest, options ...grpc.CallOption) (*warehousingv1.ListPickListWorkflowsResponse, error) {
	defer c.recorder.Observe("acme.warehousing.v1.PickListWorkflowService/ListPickListWorkflows", time.Now())
	return c.PickListWorkflowServiceClient.ListPickListWorkflows(ctx, request, options...)
}

func (c meteredPickListWorkflow) ResumePickListWorkflow(ctx context.Context, request *warehousingv1.ResumePickListWorkflowRequest, options ...grpc.CallOption) (*warehousingv1.ResumePickListWorkflowResponse, error) {
	defer c.recorder.Observe("acme.warehousing.v1.PickListWorkflowService/ResumePickListWorkflow", time.Now())
	return c.PickListWorkflowServiceClient.ResumePickListWorkflow(ctx, request, options...)
}

// meteredPriceQuoteQuery records how long each call to acme.pricing.v1.PriceQuoteQueryService takes.
type meteredPriceQuoteQuery struct {
	pricingv1.PriceQuoteQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredPriceQuoteQuery) SuggestPriceQuotes(ctx context.Context, request *pricingv1.SuggestPriceQuotesRequest, options ...grpc.CallOption) (*pricingv1.SuggestPriceQuotesResponse, error) {
	defer c.recorder.Observe("acme.pricing.v1.PriceQuoteQueryService/SuggestPriceQuotes", time.Now())
	return c.PriceQuoteQueryServiceClient.SuggestPriceQuotes(ctx, request, options...)
}

// meteredRewardCalculation records how long each call to acme.loyalty.v1.RewardCalculationService takes.
type meteredRewardCalculation struct {
	loyaltyv1.RewardCalculationServiceClient
	recorder *metrics.Recorder
}

// meteredSavedListApproval records how long each call to acme.cart.v1.SavedListApprovalService takes.
type meteredSavedListApproval struct {
	cartv1.SavedListApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredSavedListApproval) DelegateSavedListApproval(ctx context.Context, request *cartv1.DelegateSavedListApprovalRequest, options ...grpc.CallOption) (*cartv1.DelegateSavedListApprovalResponse, error) {
	defer c.recorder.Observe("acme.cart.v1.SavedListApprovalService/DelegateSavedListApproval", time.Now())
	return c.SavedListApprovalServiceClient.DelegateSavedListApproval(ctx, request, options...)
}

// meteredShipmentApproval records how long each call to acme.shipping.v1.ShipmentApprovalService takes.
type meteredShipmentApproval struct {
	shippingv1.ShipmentApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredShipmentApproval) ApproveShipment(ctx context.Context, request *shippingv1.ApproveShipmentRequest, options ...grpc.CallOption) (*shippingv1.ApproveShipmentResponse, error) {
	defer c.recorder.Observe("acme.shipping.v1.ShipmentApprovalService/ApproveShipment", time.Now())
	return c.ShipmentApprovalServiceClient.ApproveShipment(ctx, request, options...)
}

// meteredSponsorCalculation records how long each call to acme.sponsorships.v1.SponsorCalculationService takes.
type meteredSponsorCalculation struct {
	sponsorshipsv1.SponsorCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredSponsorCalculation) QuoteSponsor(ctx context.Context, request *sponsorshipsv1.QuoteSponsorRequest, options ...grpc.CallOption) (*sponsorshipsv1.QuoteSponsorResponse, error) {
	defer c.recorder.Observe("acme.sponsorships.v1.SponsorCalculationService/QuoteSponsor", time.Now())
	return c.SponsorCalculationServiceClient.QuoteSponsor(ctx, request, options...)
}
