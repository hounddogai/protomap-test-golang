package pricing

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// currencyRateReportingPort is what the pricing domain needs of acme.pricing.v1.CurrencyRateReportingService.
type currencyRateReportingPort interface {
	ListCurrencyRateReports(ctx context.Context, request *pricingv1.ListCurrencyRateReportsRequest) (*pricingv1.ListCurrencyRateReportsResponse, error)
}

// grpcCurrencyRateReportingPort reaches acme.pricing.v1.CurrencyRateReportingService over gRPC.
type grpcCurrencyRateReportingPort struct {
	client pricingv1.CurrencyRateReportingServiceClient
}

// sandboxCurrencyRateReportingPort answers in the sandbox environment, where
// acme.pricing.v1.CurrencyRateReportingService is not deployed.
type sandboxCurrencyRateReportingPort struct{ log *slog.Logger }

func (p grpcCurrencyRateReportingPort) ListCurrencyRateReports(ctx context.Context, request *pricingv1.ListCurrencyRateReportsRequest) (*pricingv1.ListCurrencyRateReportsResponse, error) {
	return p.client.ListCurrencyRateReports(ctx, request)
}

func (p sandboxCurrencyRateReportingPort) ListCurrencyRateReports(ctx context.Context, request *pricingv1.ListCurrencyRateReportsRequest) (*pricingv1.ListCurrencyRateReportsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListCurrencyRateReports")
	return &pricingv1.ListCurrencyRateReportsResponse{}, nil
}

func newCurrencyRateReportingPort(deps platform.Deps) currencyRateReportingPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCurrencyRateReportingPort{log: deps.Logger}
	}
	return grpcCurrencyRateReportingPort{client: pricingv1.NewCurrencyRateReportingServiceClient(deps.Conn)}
}

// giftCardTransactionApprovalPort is what the pricing domain needs of
// acme.giftcards.v1.GiftCardTransactionApprovalService.
type giftCardTransactionApprovalPort interface {
	DelegateGiftCardTransactionApproval(ctx context.Context, request *giftcardsv1.DelegateGiftCardTransactionApprovalRequest) (*giftcardsv1.DelegateGiftCardTransactionApprovalResponse, error)
}

// grpcGiftCardTransactionApprovalPort reaches acme.giftcards.v1.GiftCardTransactionApprovalService over gRPC.
type grpcGiftCardTransactionApprovalPort struct {
	client giftcardsv1.GiftCardTransactionApprovalServiceClient
}

// sandboxGiftCardTransactionApprovalPort answers in the sandbox environment, where
// acme.giftcards.v1.GiftCardTransactionApprovalService is not deployed.
type sandboxGiftCardTransactionApprovalPort struct{ log *slog.Logger }

func (p grpcGiftCardTransactionApprovalPort) DelegateGiftCardTransactionApproval(ctx context.Context, request *giftcardsv1.DelegateGiftCardTransactionApprovalRequest) (*giftcardsv1.DelegateGiftCardTransactionApprovalResponse, error) {
	return p.client.DelegateGiftCardTransactionApproval(ctx, request)
}

func (p sandboxGiftCardTransactionApprovalPort) DelegateGiftCardTransactionApproval(ctx context.Context, request *giftcardsv1.DelegateGiftCardTransactionApprovalRequest) (*giftcardsv1.DelegateGiftCardTransactionApprovalResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers DelegateGiftCardTransactionApproval")
	return &giftcardsv1.DelegateGiftCardTransactionApprovalResponse{}, nil
}

func newGiftCardTransactionApprovalPort(deps platform.Deps) giftCardTransactionApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardTransactionApprovalPort{log: deps.Logger}
	}
	return grpcGiftCardTransactionApprovalPort{client: giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn)}
}

// householdQueryPort is what the pricing domain needs of acme.customers.v1.HouseholdQueryService.
type householdQueryPort interface {
	FindHouseholds(ctx context.Context, request *customersv1.FindHouseholdsRequest) (*customersv1.FindHouseholdsResponse, error)
}

// grpcHouseholdQueryPort reaches acme.customers.v1.HouseholdQueryService over gRPC.
type grpcHouseholdQueryPort struct {
	client customersv1.HouseholdQueryServiceClient
}

// sandboxHouseholdQueryPort answers in the sandbox environment, where acme.customers.v1.HouseholdQueryService is not
// deployed.
type sandboxHouseholdQueryPort struct{ log *slog.Logger }

func (p grpcHouseholdQueryPort) FindHouseholds(ctx context.Context, request *customersv1.FindHouseholdsRequest) (*customersv1.FindHouseholdsResponse, error) {
	return p.client.FindHouseholds(ctx, request)
}

func (p sandboxHouseholdQueryPort) FindHouseholds(ctx context.Context, request *customersv1.FindHouseholdsRequest) (*customersv1.FindHouseholdsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers FindHouseholds")
	return &customersv1.FindHouseholdsResponse{}, nil
}

func newHouseholdQueryPort(deps platform.Deps) householdQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxHouseholdQueryPort{log: deps.Logger}
	}
	return grpcHouseholdQueryPort{client: customersv1.NewHouseholdQueryServiceClient(deps.Conn)}
}

// priceListValidationPort is what the pricing domain needs of acme.pricing.v1.PriceListValidationService.
type priceListValidationPort interface {
	LintPriceLists(ctx context.Context, request *pricingv1.LintPriceListsRequest) (*pricingv1.LintPriceListsResponse, error)
}

// grpcPriceListValidationPort reaches acme.pricing.v1.PriceListValidationService over gRPC.
type grpcPriceListValidationPort struct {
	client pricingv1.PriceListValidationServiceClient
}

// sandboxPriceListValidationPort answers in the sandbox environment, where acme.pricing.v1.PriceListValidationService
// is not deployed.
type sandboxPriceListValidationPort struct{ log *slog.Logger }

func (p grpcPriceListValidationPort) LintPriceLists(ctx context.Context, request *pricingv1.LintPriceListsRequest) (*pricingv1.LintPriceListsResponse, error) {
	return p.client.LintPriceLists(ctx, request)
}

func (p sandboxPriceListValidationPort) LintPriceLists(ctx context.Context, request *pricingv1.LintPriceListsRequest) (*pricingv1.LintPriceListsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers LintPriceLists")
	return &pricingv1.LintPriceListsResponse{}, nil
}

func newPriceListValidationPort(deps platform.Deps) priceListValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPriceListValidationPort{log: deps.Logger}
	}
	return grpcPriceListValidationPort{client: pricingv1.NewPriceListValidationServiceClient(deps.Conn)}
}

// priceOverrideCalculationPort is what the pricing domain needs of acme.pricing.v1.PriceOverrideCalculationService.
type priceOverrideCalculationPort interface {
	CalculatePriceOverride(ctx context.Context, request *pricingv1.CalculatePriceOverrideRequest) (*pricingv1.CalculatePriceOverrideResponse, error)
}

// grpcPriceOverrideCalculationPort reaches acme.pricing.v1.PriceOverrideCalculationService over gRPC.
type grpcPriceOverrideCalculationPort struct {
	client pricingv1.PriceOverrideCalculationServiceClient
}

// sandboxPriceOverrideCalculationPort answers in the sandbox environment, where
// acme.pricing.v1.PriceOverrideCalculationService is not deployed.
type sandboxPriceOverrideCalculationPort struct{ log *slog.Logger }

func (p grpcPriceOverrideCalculationPort) CalculatePriceOverride(ctx context.Context, request *pricingv1.CalculatePriceOverrideRequest) (*pricingv1.CalculatePriceOverrideResponse, error) {
	return p.client.CalculatePriceOverride(ctx, request)
}

func (p sandboxPriceOverrideCalculationPort) CalculatePriceOverride(ctx context.Context, request *pricingv1.CalculatePriceOverrideRequest) (*pricingv1.CalculatePriceOverrideResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculatePriceOverride")
	return &pricingv1.CalculatePriceOverrideResponse{}, nil
}

func newPriceOverrideCalculationPort(deps platform.Deps) priceOverrideCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPriceOverrideCalculationPort{log: deps.Logger}
	}
	return grpcPriceOverrideCalculationPort{client: pricingv1.NewPriceOverrideCalculationServiceClient(deps.Conn)}
}

// priceQuoteCalculationPort is what the pricing domain needs of acme.pricing.v1.PriceQuoteCalculationService.
type priceQuoteCalculationPort interface {
	CalculatePriceQuote(ctx context.Context, request *pricingv1.CalculatePriceQuoteRequest) (*pricingv1.CalculatePriceQuoteResponse, error)
}

// grpcPriceQuoteCalculationPort reaches acme.pricing.v1.PriceQuoteCalculationService over gRPC.
type grpcPriceQuoteCalculationPort struct {
	client pricingv1.PriceQuoteCalculationServiceClient
}

// sandboxPriceQuoteCalculationPort answers in the sandbox environment, where
// acme.pricing.v1.PriceQuoteCalculationService is not deployed.
type sandboxPriceQuoteCalculationPort struct{ log *slog.Logger }

func (p grpcPriceQuoteCalculationPort) CalculatePriceQuote(ctx context.Context, request *pricingv1.CalculatePriceQuoteRequest) (*pricingv1.CalculatePriceQuoteResponse, error) {
	return p.client.CalculatePriceQuote(ctx, request)
}

func (p sandboxPriceQuoteCalculationPort) CalculatePriceQuote(ctx context.Context, request *pricingv1.CalculatePriceQuoteRequest) (*pricingv1.CalculatePriceQuoteResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculatePriceQuote")
	return &pricingv1.CalculatePriceQuoteResponse{}, nil
}

func newPriceQuoteCalculationPort(deps platform.Deps) priceQuoteCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPriceQuoteCalculationPort{log: deps.Logger}
	}
	return grpcPriceQuoteCalculationPort{client: pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn)}
}
