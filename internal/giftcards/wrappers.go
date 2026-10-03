package giftcards

import (
	"context"
	"time"

	concessionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/concessions/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	patientsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patients/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredConcessionStandCalculation records how long each call to acme.concessions.v1.ConcessionStandCalculationService
// takes.
type meteredConcessionStandCalculation struct {
	concessionsv1.ConcessionStandCalculationServiceClient
	recorder *metrics.Recorder
}

// meteredFeatureFlagValidation records how long each call to acme.featureflags.v1.FeatureFlagValidationService takes.
type meteredFeatureFlagValidation struct {
	featureflagsv1.FeatureFlagValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredFeatureFlagValidation) ValidateFeatureFlag(ctx context.Context, request *featureflagsv1.ValidateFeatureFlagRequest, options ...grpc.CallOption) (*featureflagsv1.ValidateFeatureFlagResponse, error) {
	defer c.recorder.Observe("acme.featureflags.v1.FeatureFlagValidationService/ValidateFeatureFlag", time.Now())
	return c.FeatureFlagValidationServiceClient.ValidateFeatureFlag(ctx, request, options...)
}

// meteredGiftCardBatchCalculation records how long each call to acme.giftcards.v1.GiftCardBatchCalculationService
// takes.
type meteredGiftCardBatchCalculation struct {
	giftcardsv1.GiftCardBatchCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredGiftCardBatchCalculation) QuoteGiftCardBatch(ctx context.Context, request *giftcardsv1.QuoteGiftCardBatchRequest, options ...grpc.CallOption) (*giftcardsv1.QuoteGiftCardBatchResponse, error) {
	defer c.recorder.Observe("acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch", time.Now())
	return c.GiftCardBatchCalculationServiceClient.QuoteGiftCardBatch(ctx, request, options...)
}

// meteredGiftCardCalculation records how long each call to acme.giftcards.v1.GiftCardCalculationService takes.
type meteredGiftCardCalculation struct {
	giftcardsv1.GiftCardCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredGiftCardCalculation) CompareGiftCardOptions(ctx context.Context, request *giftcardsv1.CompareGiftCardOptionsRequest, options ...grpc.CallOption) (*giftcardsv1.CompareGiftCardOptionsResponse, error) {
	defer c.recorder.Observe("acme.giftcards.v1.GiftCardCalculationService/CompareGiftCardOptions", time.Now())
	return c.GiftCardCalculationServiceClient.CompareGiftCardOptions(ctx, request, options...)
}

// meteredGiftCardTransactionValidation records how long each call to
// acme.giftcards.v1.GiftCardTransactionValidationService takes.
type meteredGiftCardTransactionValidation struct {
	giftcardsv1.GiftCardTransactionValidationServiceClient
	recorder *metrics.Recorder
}

// meteredGiftCardValidation records how long each call to acme.giftcards.v1.GiftCardValidationService takes.
type meteredGiftCardValidation struct {
	giftcardsv1.GiftCardValidationServiceClient
	recorder *metrics.Recorder
}

// meteredLoanApplicationQuery records how long each call to acme.lending.v1.LoanApplicationQueryService takes.
type meteredLoanApplicationQuery struct {
	lendingv1.LoanApplicationQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredLoanApplicationQuery) CountLoanApplications(ctx context.Context, request *lendingv1.CountLoanApplicationsRequest, options ...grpc.CallOption) (*lendingv1.CountLoanApplicationsResponse, error) {
	defer c.recorder.Observe("acme.lending.v1.LoanApplicationQueryService/CountLoanApplications", time.Now())
	return c.LoanApplicationQueryServiceClient.CountLoanApplications(ctx, request, options...)
}

// meteredOrderNoteValidation records how long each call to acme.orders.v1.OrderNoteValidationService takes.
type meteredOrderNoteValidation struct {
	ordersv1.OrderNoteValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredOrderNoteValidation) ValidateOrderNote(ctx context.Context, request *ordersv1.ValidateOrderNoteRequest, options ...grpc.CallOption) (*ordersv1.ValidateOrderNoteResponse, error) {
	defer c.recorder.Observe("acme.orders.v1.OrderNoteValidationService/ValidateOrderNote", time.Now())
	return c.OrderNoteValidationServiceClient.ValidateOrderNote(ctx, request, options...)
}

// meteredPatientQuery records how long each call to acme.patients.v1.PatientQueryService takes.
type meteredPatientQuery struct {
	patientsv1.PatientQueryServiceClient
	recorder *metrics.Recorder
}

// meteredPointsLedgerQuery records how long each call to acme.loyalty.v1.PointsLedgerQueryService takes.
type meteredPointsLedgerQuery struct {
	loyaltyv1.PointsLedgerQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredPointsLedgerQuery) CountPointsLedgers(ctx context.Context, request *loyaltyv1.CountPointsLedgersRequest, options ...grpc.CallOption) (*loyaltyv1.CountPointsLedgersResponse, error) {
	defer c.recorder.Observe("acme.loyalty.v1.PointsLedgerQueryService/CountPointsLedgers", time.Now())
	return c.PointsLedgerQueryServiceClient.CountPointsLedgers(ctx, request, options...)
}

// meteredSynonymValidation records how long each call to acme.search.v1.SynonymValidationService takes.
type meteredSynonymValidation struct {
	searchv1.SynonymValidationServiceClient
	recorder *metrics.Recorder
}
