package shipping

import (
	"context"
	"time"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	performersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/performers/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	ticketingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ticketing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredCarrierRateCalculation records how long each call to acme.shipping.v1.CarrierRateCalculationService takes.
type meteredCarrierRateCalculation struct {
	shippingv1.CarrierRateCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCarrierRateCalculation) QuoteCarrierRate(ctx context.Context, request *shippingv1.QuoteCarrierRateRequest, options ...grpc.CallOption) (*shippingv1.QuoteCarrierRateResponse, error) {
	defer c.recorder.Observe("acme.shipping.v1.CarrierRateCalculationService/QuoteCarrierRate", time.Now())
	return c.CarrierRateCalculationServiceClient.QuoteCarrierRate(ctx, request, options...)
}

// meteredCarrierRateReporting records how long each call to acme.shipping.v1.CarrierRateReportingService takes.
type meteredCarrierRateReporting struct {
	shippingv1.CarrierRateReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredCarrierRateReporting) CountCarrierRateTotals(ctx context.Context, request *shippingv1.CountCarrierRateTotalsRequest, options ...grpc.CallOption) (*shippingv1.CountCarrierRateTotalsResponse, error) {
	defer c.recorder.Observe("acme.shipping.v1.CarrierRateReportingService/CountCarrierRateTotals", time.Now())
	return c.CarrierRateReportingServiceClient.CountCarrierRateTotals(ctx, request, options...)
}

func (c meteredCarrierRateReporting) ListCarrierRateReports(ctx context.Context, request *shippingv1.ListCarrierRateReportsRequest, options ...grpc.CallOption) (*shippingv1.ListCarrierRateReportsResponse, error) {
	defer c.recorder.Observe("acme.shipping.v1.CarrierRateReportingService/ListCarrierRateReports", time.Now())
	return c.CarrierRateReportingServiceClient.ListCarrierRateReports(ctx, request, options...)
}

// meteredCustomerValidation records how long each call to acme.customers.v1.CustomerValidationService takes.
type meteredCustomerValidation struct {
	customersv1.CustomerValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomerValidation) ValidateCustomer(ctx context.Context, request *customersv1.ValidateCustomerRequest, options ...grpc.CallOption) (*customersv1.ValidateCustomerResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerValidationService/ValidateCustomer", time.Now())
	return c.CustomerValidationServiceClient.ValidateCustomer(ctx, request, options...)
}

// meteredGiftCardApproval records how long each call to acme.giftcards.v1.GiftCardApprovalService takes.
type meteredGiftCardApproval struct {
	giftcardsv1.GiftCardApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredGiftCardApproval) DelegateGiftCardApproval(ctx context.Context, request *giftcardsv1.DelegateGiftCardApprovalRequest, options ...grpc.CallOption) (*giftcardsv1.DelegateGiftCardApprovalResponse, error) {
	defer c.recorder.Observe("acme.giftcards.v1.GiftCardApprovalService/DelegateGiftCardApproval", time.Now())
	return c.GiftCardApprovalServiceClient.DelegateGiftCardApproval(ctx, request, options...)
}

// meteredLedgerAccount records how long each call to acme.ledger.v1.LedgerAccountService takes.
type meteredLedgerAccount struct {
	ledgerv1.LedgerAccountServiceClient
	recorder *metrics.Recorder
}

func (c meteredLedgerAccount) ListLedgerAccounts(ctx context.Context, request *ledgerv1.ListLedgerAccountsRequest, options ...grpc.CallOption) (*ledgerv1.ListLedgerAccountsResponse, error) {
	defer c.recorder.Observe("acme.ledger.v1.LedgerAccountService/ListLedgerAccounts", time.Now())
	return c.LedgerAccountServiceClient.ListLedgerAccounts(ctx, request, options...)
}

// meteredManifestReporting records how long each call to acme.shipping.v1.ManifestReportingService takes.
type meteredManifestReporting struct {
	shippingv1.ManifestReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredManifestReporting) GenerateManifestReport(ctx context.Context, request *shippingv1.GenerateManifestReportRequest, options ...grpc.CallOption) (*shippingv1.GenerateManifestReportResponse, error) {
	defer c.recorder.Observe("acme.shipping.v1.ManifestReportingService/GenerateManifestReport", time.Now())
	return c.ManifestReportingServiceClient.GenerateManifestReport(ctx, request, options...)
}

// meteredPerformerQuery records how long each call to acme.performers.v1.PerformerQueryService takes.
type meteredPerformerQuery struct {
	performersv1.PerformerQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredPerformerQuery) CountPerformers(ctx context.Context, request *performersv1.CountPerformersRequest, options ...grpc.CallOption) (*performersv1.CountPerformersResponse, error) {
	defer c.recorder.Observe("acme.performers.v1.PerformerQueryService/CountPerformers", time.Now())
	return c.PerformerQueryServiceClient.CountPerformers(ctx, request, options...)
}

// meteredSearchQueryValidation records how long each call to acme.search.v1.SearchQueryValidationService takes.
type meteredSearchQueryValidation struct {
	searchv1.SearchQueryValidationServiceClient
	recorder *metrics.Recorder
}

// meteredShipmentCalculation records how long each call to acme.shipping.v1.ShipmentCalculationService takes.
type meteredShipmentCalculation struct {
	shippingv1.ShipmentCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredShipmentCalculation) ExplainShipmentCalculation(ctx context.Context, request *shippingv1.ExplainShipmentCalculationRequest, options ...grpc.CallOption) (*shippingv1.ExplainShipmentCalculationResponse, error) {
	defer c.recorder.Observe("acme.shipping.v1.ShipmentCalculationService/ExplainShipmentCalculation", time.Now())
	return c.ShipmentCalculationServiceClient.ExplainShipmentCalculation(ctx, request, options...)
}

// meteredShipmentValidation records how long each call to acme.shipping.v1.ShipmentValidationService takes.
type meteredShipmentValidation struct {
	shippingv1.ShipmentValidationServiceClient
	recorder *metrics.Recorder
}

// meteredTicketQuery records how long each call to acme.ticketing.v1.TicketQueryService takes.
type meteredTicketQuery struct {
	ticketingv1.TicketQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredTicketQuery) FindTickets(ctx context.Context, request *ticketingv1.FindTicketsRequest, options ...grpc.CallOption) (*ticketingv1.FindTicketsResponse, error) {
	defer c.recorder.Observe("acme.ticketing.v1.TicketQueryService/FindTickets", time.Now())
	return c.TicketQueryServiceClient.FindTickets(ctx, request, options...)
}

// meteredTrackingEventValidation records how long each call to acme.shipping.v1.TrackingEventValidationService takes.
type meteredTrackingEventValidation struct {
	shippingv1.TrackingEventValidationServiceClient
	recorder *metrics.Recorder
}
