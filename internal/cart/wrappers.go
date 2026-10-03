package cart

import (
	"context"
	"time"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredApiCredentialValidation records how long each call to acme.identity.v1.ApiCredentialValidationService takes.
type meteredApiCredentialValidation struct {
	identityv1.ApiCredentialValidationServiceClient
	recorder *metrics.Recorder
}

// meteredCartCalculation records how long each call to acme.cart.v1.CartCalculationService takes.
type meteredCartCalculation struct {
	cartv1.CartCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCartCalculation) RecalculateCarts(ctx context.Context, request *cartv1.RecalculateCartsRequest, options ...grpc.CallOption) (*cartv1.RecalculateCartsResponse, error) {
	defer c.recorder.Observe("acme.cart.v1.CartCalculationService/RecalculateCarts", time.Now())
	return c.CartCalculationServiceClient.RecalculateCarts(ctx, request, options...)
}

// meteredCartRuleCalculation records how long each call to acme.cart.v1.CartRuleCalculationService takes.
type meteredCartRuleCalculation struct {
	cartv1.CartRuleCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCartRuleCalculation) CompareCartRuleOptions(ctx context.Context, request *cartv1.CompareCartRuleOptionsRequest, options ...grpc.CallOption) (*cartv1.CompareCartRuleOptionsResponse, error) {
	defer c.recorder.Observe("acme.cart.v1.CartRuleCalculationService/CompareCartRuleOptions", time.Now())
	return c.CartRuleCalculationServiceClient.CompareCartRuleOptions(ctx, request, options...)
}

// meteredCartValidation records how long each call to acme.cart.v1.CartValidationService takes.
type meteredCartValidation struct {
	cartv1.CartValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCartValidation) ScoreCart(ctx context.Context, request *cartv1.ScoreCartRequest, options ...grpc.CallOption) (*cartv1.ScoreCartResponse, error) {
	defer c.recorder.Observe("acme.cart.v1.CartValidationService/ScoreCart", time.Now())
	return c.CartValidationServiceClient.ScoreCart(ctx, request, options...)
}

// meteredPaymentCalculation records how long each call to acme.payments.v1.PaymentCalculationService takes.
type meteredPaymentCalculation struct {
	paymentsv1.PaymentCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPaymentCalculation) QuotePayment(ctx context.Context, request *paymentsv1.QuotePaymentRequest, options ...grpc.CallOption) (*paymentsv1.QuotePaymentResponse, error) {
	defer c.recorder.Observe("acme.payments.v1.PaymentCalculationService/QuotePayment", time.Now())
	return c.PaymentCalculationServiceClient.QuotePayment(ctx, request, options...)
}

// meteredSavedListCalculation records how long each call to acme.cart.v1.SavedListCalculationService takes.
type meteredSavedListCalculation struct {
	cartv1.SavedListCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredSavedListCalculation) QuoteSavedList(ctx context.Context, request *cartv1.QuoteSavedListRequest, options ...grpc.CallOption) (*cartv1.QuoteSavedListResponse, error) {
	defer c.recorder.Observe("acme.cart.v1.SavedListCalculationService/QuoteSavedList", time.Now())
	return c.SavedListCalculationServiceClient.QuoteSavedList(ctx, request, options...)
}

// meteredUsageRecordApproval records how long each call to acme.subscriptions.v1.UsageRecordApprovalService takes.
type meteredUsageRecordApproval struct {
	subscriptionsv1.UsageRecordApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredUsageRecordApproval) CommentUsageRecordApproval(ctx context.Context, request *subscriptionsv1.CommentUsageRecordApprovalRequest, options ...grpc.CallOption) (*subscriptionsv1.CommentUsageRecordApprovalResponse, error) {
	defer c.recorder.Observe("acme.subscriptions.v1.UsageRecordApprovalService/CommentUsageRecordApproval", time.Now())
	return c.UsageRecordApprovalServiceClient.CommentUsageRecordApproval(ctx, request, options...)
}
