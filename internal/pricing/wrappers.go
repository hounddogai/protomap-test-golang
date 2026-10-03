package pricing

import (
	"context"
	"time"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredCartCalculation records how long each call to acme.cart.v1.CartCalculationService takes.
type meteredCartCalculation struct {
	cartv1.CartCalculationServiceClient
	recorder *metrics.Recorder
}

// meteredCouponValidation records how long each call to acme.promotions.v1.CouponValidationService takes.
type meteredCouponValidation struct {
	promotionsv1.CouponValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCouponValidation) NormalizeCoupon(ctx context.Context, request *promotionsv1.NormalizeCouponRequest, options ...grpc.CallOption) (*promotionsv1.NormalizeCouponResponse, error) {
	defer c.recorder.Observe("acme.promotions.v1.CouponValidationService/NormalizeCoupon", time.Now())
	return c.CouponValidationServiceClient.NormalizeCoupon(ctx, request, options...)
}

// meteredHouseholdValidation records how long each call to acme.customers.v1.HouseholdValidationService takes.
type meteredHouseholdValidation struct {
	customersv1.HouseholdValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredHouseholdValidation) ValidateHousehold(ctx context.Context, request *customersv1.ValidateHouseholdRequest, options ...grpc.CallOption) (*customersv1.ValidateHouseholdResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.HouseholdValidationService/ValidateHousehold", time.Now())
	return c.HouseholdValidationServiceClient.ValidateHousehold(ctx, request, options...)
}

// meteredPriceOverrideCalculation records how long each call to acme.pricing.v1.PriceOverrideCalculationService takes.
type meteredPriceOverrideCalculation struct {
	pricingv1.PriceOverrideCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPriceOverrideCalculation) SimulatePriceOverride(ctx context.Context, request *pricingv1.SimulatePriceOverrideRequest, options ...grpc.CallOption) (*pricingv1.SimulatePriceOverrideResponse, error) {
	defer c.recorder.Observe("acme.pricing.v1.PriceOverrideCalculationService/SimulatePriceOverride", time.Now())
	return c.PriceOverrideCalculationServiceClient.SimulatePriceOverride(ctx, request, options...)
}

// meteredPriceOverrideValidation records how long each call to acme.pricing.v1.PriceOverrideValidationService takes.
type meteredPriceOverrideValidation struct {
	pricingv1.PriceOverrideValidationServiceClient
	recorder *metrics.Recorder
}

// meteredPriceQuoteApproval records how long each call to acme.pricing.v1.PriceQuoteApprovalService takes.
type meteredPriceQuoteApproval struct {
	pricingv1.PriceQuoteApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredPriceQuoteApproval) DelegatePriceQuoteApproval(ctx context.Context, request *pricingv1.DelegatePriceQuoteApprovalRequest, options ...grpc.CallOption) (*pricingv1.DelegatePriceQuoteApprovalResponse, error) {
	defer c.recorder.Observe("acme.pricing.v1.PriceQuoteApprovalService/DelegatePriceQuoteApproval", time.Now())
	return c.PriceQuoteApprovalServiceClient.DelegatePriceQuoteApproval(ctx, request, options...)
}

func (c meteredPriceQuoteApproval) RecallPriceQuote(ctx context.Context, request *pricingv1.RecallPriceQuoteRequest, options ...grpc.CallOption) (*pricingv1.RecallPriceQuoteResponse, error) {
	defer c.recorder.Observe("acme.pricing.v1.PriceQuoteApprovalService/RecallPriceQuote", time.Now())
	return c.PriceQuoteApprovalServiceClient.RecallPriceQuote(ctx, request, options...)
}

// meteredPriceQuoteCalculation records how long each call to acme.pricing.v1.PriceQuoteCalculationService takes.
type meteredPriceQuoteCalculation struct {
	pricingv1.PriceQuoteCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPriceQuoteCalculation) SimulatePriceQuote(ctx context.Context, request *pricingv1.SimulatePriceQuoteRequest, options ...grpc.CallOption) (*pricingv1.SimulatePriceQuoteResponse, error) {
	defer c.recorder.Observe("acme.pricing.v1.PriceQuoteCalculationService/SimulatePriceQuote", time.Now())
	return c.PriceQuoteCalculationServiceClient.SimulatePriceQuote(ctx, request, options...)
}

// meteredSynonymNotification records how long each call to acme.search.v1.SynonymNotificationService takes.
type meteredSynonymNotification struct {
	searchv1.SynonymNotificationServiceClient
	recorder *metrics.Recorder
}

// meteredUserValidation records how long each call to acme.identity.v1.UserValidationService takes.
type meteredUserValidation struct {
	identityv1.UserValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredUserValidation) LintUsers(ctx context.Context, request *identityv1.LintUsersRequest, options ...grpc.CallOption) (*identityv1.LintUsersResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.UserValidationService/LintUsers", time.Now())
	return c.UserValidationServiceClient.LintUsers(ctx, request, options...)
}
