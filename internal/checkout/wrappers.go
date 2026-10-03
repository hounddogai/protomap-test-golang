package checkout

import (
	"context"
	"time"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredCheckoutSessionApproval records how long each call to acme.checkout.v1.CheckoutSessionApprovalService takes.
type meteredCheckoutSessionApproval struct {
	checkoutv1.CheckoutSessionApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredCheckoutSessionApproval) RecallCheckoutSession(ctx context.Context, request *checkoutv1.RecallCheckoutSessionRequest, options ...grpc.CallOption) (*checkoutv1.RecallCheckoutSessionResponse, error) {
	defer c.recorder.Observe("acme.checkout.v1.CheckoutSessionApprovalService/RecallCheckoutSession", time.Now())
	return c.CheckoutSessionApprovalServiceClient.RecallCheckoutSession(ctx, request, options...)
}

// meteredCheckoutSessionQuery records how long each call to acme.checkout.v1.CheckoutSessionQueryService takes.
type meteredCheckoutSessionQuery struct {
	checkoutv1.CheckoutSessionQueryServiceClient
	recorder *metrics.Recorder
}

// meteredCustomerValidation records how long each call to acme.customers.v1.CustomerValidationService takes.
type meteredCustomerValidation struct {
	customersv1.CustomerValidationServiceClient
	recorder *metrics.Recorder
}

// meteredSynonymHistory records how long each call to acme.search.v1.SynonymHistoryService takes.
type meteredSynonymHistory struct {
	searchv1.SynonymHistoryServiceClient
	recorder *metrics.Recorder
}

func (c meteredSynonymHistory) AnnotateSynonymVersion(ctx context.Context, request *searchv1.AnnotateSynonymVersionRequest, options ...grpc.CallOption) (*searchv1.AnnotateSynonymVersionResponse, error) {
	defer c.recorder.Observe("acme.search.v1.SynonymHistoryService/AnnotateSynonymVersion", time.Now())
	return c.SynonymHistoryServiceClient.AnnotateSynonymVersion(ctx, request, options...)
}
