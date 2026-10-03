package giftcards

import (
	"context"
	"log/slog"

	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// giftCardBatchQueryPort is what the giftcards domain needs of acme.giftcards.v1.GiftCardBatchQueryService.
type giftCardBatchQueryPort interface {
	SummarizeGiftCardBatches(ctx context.Context, request *giftcardsv1.SummarizeGiftCardBatchesRequest) (*giftcardsv1.SummarizeGiftCardBatchesResponse, error)
}

// grpcGiftCardBatchQueryPort reaches acme.giftcards.v1.GiftCardBatchQueryService over gRPC.
type grpcGiftCardBatchQueryPort struct {
	client giftcardsv1.GiftCardBatchQueryServiceClient
}

// sandboxGiftCardBatchQueryPort answers in the sandbox environment, where acme.giftcards.v1.GiftCardBatchQueryService
// is not deployed.
type sandboxGiftCardBatchQueryPort struct{ log *slog.Logger }

func (p grpcGiftCardBatchQueryPort) SummarizeGiftCardBatches(ctx context.Context, request *giftcardsv1.SummarizeGiftCardBatchesRequest) (*giftcardsv1.SummarizeGiftCardBatchesResponse, error) {
	return p.client.SummarizeGiftCardBatches(ctx, request)
}

func (p sandboxGiftCardBatchQueryPort) SummarizeGiftCardBatches(ctx context.Context, request *giftcardsv1.SummarizeGiftCardBatchesRequest) (*giftcardsv1.SummarizeGiftCardBatchesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SummarizeGiftCardBatches")
	return &giftcardsv1.SummarizeGiftCardBatchesResponse{}, nil
}

func newGiftCardBatchQueryPort(deps platform.Deps) giftCardBatchQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardBatchQueryPort{log: deps.Logger}
	}
	return grpcGiftCardBatchQueryPort{client: giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn)}
}

// giftCardCalculationPort is what the giftcards domain needs of acme.giftcards.v1.GiftCardCalculationService.
type giftCardCalculationPort interface {
	CompareGiftCardOptions(ctx context.Context, request *giftcardsv1.CompareGiftCardOptionsRequest) (*giftcardsv1.CompareGiftCardOptionsResponse, error)
}

// grpcGiftCardCalculationPort reaches acme.giftcards.v1.GiftCardCalculationService over gRPC.
type grpcGiftCardCalculationPort struct {
	client giftcardsv1.GiftCardCalculationServiceClient
}

// sandboxGiftCardCalculationPort answers in the sandbox environment, where acme.giftcards.v1.GiftCardCalculationService
// is not deployed.
type sandboxGiftCardCalculationPort struct{ log *slog.Logger }

func (p grpcGiftCardCalculationPort) CompareGiftCardOptions(ctx context.Context, request *giftcardsv1.CompareGiftCardOptionsRequest) (*giftcardsv1.CompareGiftCardOptionsResponse, error) {
	return p.client.CompareGiftCardOptions(ctx, request)
}

func (p sandboxGiftCardCalculationPort) CompareGiftCardOptions(ctx context.Context, request *giftcardsv1.CompareGiftCardOptionsRequest) (*giftcardsv1.CompareGiftCardOptionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CompareGiftCardOptions")
	return &giftcardsv1.CompareGiftCardOptionsResponse{}, nil
}

func newGiftCardCalculationPort(deps platform.Deps) giftCardCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardCalculationPort{log: deps.Logger}
	}
	return grpcGiftCardCalculationPort{client: giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn)}
}

// giftCardTransactionCalculationPort is what the giftcards domain needs of
// acme.giftcards.v1.GiftCardTransactionCalculationService.
type giftCardTransactionCalculationPort interface {
	EstimateGiftCardTransaction(ctx context.Context, request *giftcardsv1.EstimateGiftCardTransactionRequest) (*giftcardsv1.EstimateGiftCardTransactionResponse, error)
	GetGiftCardTransactionRates(ctx context.Context, request *giftcardsv1.GetGiftCardTransactionRatesRequest) (*giftcardsv1.GetGiftCardTransactionRatesResponse, error)
	RecalculateGiftCardTransactions(ctx context.Context, request *giftcardsv1.RecalculateGiftCardTransactionsRequest) (*giftcardsv1.RecalculateGiftCardTransactionsResponse, error)
}

// grpcGiftCardTransactionCalculationPort reaches acme.giftcards.v1.GiftCardTransactionCalculationService over gRPC.
type grpcGiftCardTransactionCalculationPort struct {
	client giftcardsv1.GiftCardTransactionCalculationServiceClient
}

// sandboxGiftCardTransactionCalculationPort answers in the sandbox environment, where
// acme.giftcards.v1.GiftCardTransactionCalculationService is not deployed.
type sandboxGiftCardTransactionCalculationPort struct{ log *slog.Logger }

func (p grpcGiftCardTransactionCalculationPort) EstimateGiftCardTransaction(ctx context.Context, request *giftcardsv1.EstimateGiftCardTransactionRequest) (*giftcardsv1.EstimateGiftCardTransactionResponse, error) {
	return p.client.EstimateGiftCardTransaction(ctx, request)
}

func (p sandboxGiftCardTransactionCalculationPort) EstimateGiftCardTransaction(ctx context.Context, request *giftcardsv1.EstimateGiftCardTransactionRequest) (*giftcardsv1.EstimateGiftCardTransactionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateGiftCardTransaction")
	return &giftcardsv1.EstimateGiftCardTransactionResponse{}, nil
}

func (p grpcGiftCardTransactionCalculationPort) GetGiftCardTransactionRates(ctx context.Context, request *giftcardsv1.GetGiftCardTransactionRatesRequest) (*giftcardsv1.GetGiftCardTransactionRatesResponse, error) {
	return p.client.GetGiftCardTransactionRates(ctx, request)
}

func (p sandboxGiftCardTransactionCalculationPort) GetGiftCardTransactionRates(ctx context.Context, request *giftcardsv1.GetGiftCardTransactionRatesRequest) (*giftcardsv1.GetGiftCardTransactionRatesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetGiftCardTransactionRates")
	return &giftcardsv1.GetGiftCardTransactionRatesResponse{}, nil
}

func (p grpcGiftCardTransactionCalculationPort) RecalculateGiftCardTransactions(ctx context.Context, request *giftcardsv1.RecalculateGiftCardTransactionsRequest) (*giftcardsv1.RecalculateGiftCardTransactionsResponse, error) {
	return p.client.RecalculateGiftCardTransactions(ctx, request)
}

func (p sandboxGiftCardTransactionCalculationPort) RecalculateGiftCardTransactions(ctx context.Context, request *giftcardsv1.RecalculateGiftCardTransactionsRequest) (*giftcardsv1.RecalculateGiftCardTransactionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecalculateGiftCardTransactions")
	return &giftcardsv1.RecalculateGiftCardTransactionsResponse{}, nil
}

func newGiftCardTransactionCalculationPort(deps platform.Deps) giftCardTransactionCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardTransactionCalculationPort{log: deps.Logger}
	}
	return grpcGiftCardTransactionCalculationPort{client: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn)}
}

// giftCardTransactionValidationPort is what the giftcards domain needs of
// acme.giftcards.v1.GiftCardTransactionValidationService.
type giftCardTransactionValidationPort interface {
	ValidateGiftCardTransaction(ctx context.Context, request *giftcardsv1.ValidateGiftCardTransactionRequest) (*giftcardsv1.ValidateGiftCardTransactionResponse, error)
}

// grpcGiftCardTransactionValidationPort reaches acme.giftcards.v1.GiftCardTransactionValidationService over gRPC.
type grpcGiftCardTransactionValidationPort struct {
	client giftcardsv1.GiftCardTransactionValidationServiceClient
}

// sandboxGiftCardTransactionValidationPort answers in the sandbox environment, where
// acme.giftcards.v1.GiftCardTransactionValidationService is not deployed.
type sandboxGiftCardTransactionValidationPort struct{ log *slog.Logger }

func (p grpcGiftCardTransactionValidationPort) ValidateGiftCardTransaction(ctx context.Context, request *giftcardsv1.ValidateGiftCardTransactionRequest) (*giftcardsv1.ValidateGiftCardTransactionResponse, error) {
	return p.client.ValidateGiftCardTransaction(ctx, request)
}

func (p sandboxGiftCardTransactionValidationPort) ValidateGiftCardTransaction(ctx context.Context, request *giftcardsv1.ValidateGiftCardTransactionRequest) (*giftcardsv1.ValidateGiftCardTransactionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidateGiftCardTransaction")
	return &giftcardsv1.ValidateGiftCardTransactionResponse{}, nil
}

func newGiftCardTransactionValidationPort(deps platform.Deps) giftCardTransactionValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardTransactionValidationPort{log: deps.Logger}
	}
	return grpcGiftCardTransactionValidationPort{client: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn)}
}

// giftCardWorkflowPort is what the giftcards domain needs of acme.giftcards.v1.GiftCardWorkflowService.
type giftCardWorkflowPort interface {
	SignalGiftCardWorkflow(ctx context.Context, request *giftcardsv1.SignalGiftCardWorkflowRequest) (*giftcardsv1.SignalGiftCardWorkflowResponse, error)
}

// grpcGiftCardWorkflowPort reaches acme.giftcards.v1.GiftCardWorkflowService over gRPC.
type grpcGiftCardWorkflowPort struct {
	client giftcardsv1.GiftCardWorkflowServiceClient
}

// sandboxGiftCardWorkflowPort answers in the sandbox environment, where acme.giftcards.v1.GiftCardWorkflowService is
// not deployed.
type sandboxGiftCardWorkflowPort struct{ log *slog.Logger }

func (p grpcGiftCardWorkflowPort) SignalGiftCardWorkflow(ctx context.Context, request *giftcardsv1.SignalGiftCardWorkflowRequest) (*giftcardsv1.SignalGiftCardWorkflowResponse, error) {
	return p.client.SignalGiftCardWorkflow(ctx, request)
}

func (p sandboxGiftCardWorkflowPort) SignalGiftCardWorkflow(ctx context.Context, request *giftcardsv1.SignalGiftCardWorkflowRequest) (*giftcardsv1.SignalGiftCardWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SignalGiftCardWorkflow")
	return &giftcardsv1.SignalGiftCardWorkflowResponse{}, nil
}

func newGiftCardWorkflowPort(deps platform.Deps) giftCardWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardWorkflowPort{log: deps.Logger}
	}
	return grpcGiftCardWorkflowPort{client: giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn)}
}

// synonymHistoryPort is what the giftcards domain needs of acme.search.v1.SynonymHistoryService.
type synonymHistoryPort interface {
	AnnotateSynonymVersion(ctx context.Context, request *searchv1.AnnotateSynonymVersionRequest) (*searchv1.AnnotateSynonymVersionResponse, error)
}

// grpcSynonymHistoryPort reaches acme.search.v1.SynonymHistoryService over gRPC.
type grpcSynonymHistoryPort struct {
	client searchv1.SynonymHistoryServiceClient
}

// sandboxSynonymHistoryPort answers in the sandbox environment, where acme.search.v1.SynonymHistoryService is not
// deployed.
type sandboxSynonymHistoryPort struct{ log *slog.Logger }

func (p grpcSynonymHistoryPort) AnnotateSynonymVersion(ctx context.Context, request *searchv1.AnnotateSynonymVersionRequest) (*searchv1.AnnotateSynonymVersionResponse, error) {
	return p.client.AnnotateSynonymVersion(ctx, request)
}

func (p sandboxSynonymHistoryPort) AnnotateSynonymVersion(ctx context.Context, request *searchv1.AnnotateSynonymVersionRequest) (*searchv1.AnnotateSynonymVersionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers AnnotateSynonymVersion")
	return &searchv1.AnnotateSynonymVersionResponse{}, nil
}

func newSynonymHistoryPort(deps platform.Deps) synonymHistoryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSynonymHistoryPort{log: deps.Logger}
	}
	return grpcSynonymHistoryPort{client: searchv1.NewSynonymHistoryServiceClient(deps.Conn)}
}

// synonymValidationPort is what the giftcards domain needs of acme.search.v1.SynonymValidationService.
type synonymValidationPort interface {
	NormalizeSynonym(ctx context.Context, request *searchv1.NormalizeSynonymRequest) (*searchv1.NormalizeSynonymResponse, error)
}

// grpcSynonymValidationPort reaches acme.search.v1.SynonymValidationService over gRPC.
type grpcSynonymValidationPort struct {
	client searchv1.SynonymValidationServiceClient
}

// sandboxSynonymValidationPort answers in the sandbox environment, where acme.search.v1.SynonymValidationService is not
// deployed.
type sandboxSynonymValidationPort struct{ log *slog.Logger }

func (p grpcSynonymValidationPort) NormalizeSynonym(ctx context.Context, request *searchv1.NormalizeSynonymRequest) (*searchv1.NormalizeSynonymResponse, error) {
	return p.client.NormalizeSynonym(ctx, request)
}

func (p sandboxSynonymValidationPort) NormalizeSynonym(ctx context.Context, request *searchv1.NormalizeSynonymRequest) (*searchv1.NormalizeSynonymResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizeSynonym")
	return &searchv1.NormalizeSynonymResponse{}, nil
}

func newSynonymValidationPort(deps platform.Deps) synonymValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSynonymValidationPort{log: deps.Logger}
	}
	return grpcSynonymValidationPort{client: searchv1.NewSynonymValidationServiceClient(deps.Conn)}
}

// venuePort is what the giftcards domain needs of acme.venues.v1.VenueService.
type venuePort interface {
	UpdateVenue(ctx context.Context, request *venuesv1.UpdateVenueRequest) (*venuesv1.UpdateVenueResponse, error)
}

// grpcVenuePort reaches acme.venues.v1.VenueService over gRPC.
type grpcVenuePort struct{ client venuesv1.VenueServiceClient }

// sandboxVenuePort answers in the sandbox environment, where acme.venues.v1.VenueService is not deployed.
type sandboxVenuePort struct{ log *slog.Logger }

func (p grpcVenuePort) UpdateVenue(ctx context.Context, request *venuesv1.UpdateVenueRequest) (*venuesv1.UpdateVenueResponse, error) {
	return p.client.UpdateVenue(ctx, request)
}

func (p sandboxVenuePort) UpdateVenue(ctx context.Context, request *venuesv1.UpdateVenueRequest) (*venuesv1.UpdateVenueResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers UpdateVenue")
	return &venuesv1.UpdateVenueResponse{}, nil
}

func newVenuePort(deps platform.Deps) venuePort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxVenuePort{log: deps.Logger}
	}
	return grpcVenuePort{client: venuesv1.NewVenueServiceClient(deps.Conn)}
}
