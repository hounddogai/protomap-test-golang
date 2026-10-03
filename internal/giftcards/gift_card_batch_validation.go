package giftcards

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	patientsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patients/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	showsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shows/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// GiftCardBatchValidationServer implements `acme.giftcards.v1.GiftCardBatchValidationService`.
// Checks gift card batches against the rules before anything acts on them.
type GiftCardBatchValidationServer struct {
	giftcardsv1.UnimplementedGiftCardBatchValidationServiceServer
	store                                *Store
	log                                  *slog.Logger
	giftCardBatchCalculationClient       giftcardsv1.GiftCardBatchCalculationServiceClient
	recalculateGiftCardsFn               func(context.Context, *giftcardsv1.RecalculateGiftCardsRequest, ...grpc.CallOption) (*giftcardsv1.RecalculateGiftCardsResponse, error)
	ledgerAccountValidationClient        ledgerv1.LedgerAccountValidationServiceClient
	giftCardValidationClient             giftcardsv1.GiftCardValidationServiceClient
	meteredSynonymValidation             meteredSynonymValidation
	meteredGiftCardCalculation           meteredGiftCardCalculation
	clients                              *registry.Registry
	bus                                  *eventbus.Bus
	showQueryClient                      showsv1.ShowQueryServiceClient
	conn                                 *grpc.ClientConn
	meteredPatientQuery                  meteredPatientQuery
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
}

// NewGiftCardBatchValidationServer wires a GiftCardBatchValidationServer.
func NewGiftCardBatchValidationServer(deps platform.Deps) *GiftCardBatchValidationServer {
	giftCardCalculation := giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn)
	return &GiftCardBatchValidationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.giftcards.v1.GiftCardBatchValidationService"),
		giftCardBatchCalculationClient:       giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		recalculateGiftCardsFn:               giftCardCalculation.RecalculateGiftCards,
		ledgerAccountValidationClient:        ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		giftCardValidationClient:             giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		meteredSynonymValidation:             meteredSynonymValidation{SynonymValidationServiceClient: searchv1.NewSynonymValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		meteredGiftCardCalculation:           meteredGiftCardCalculation{GiftCardCalculationServiceClient: giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                              deps.Clients,
		bus:                                  deps.Bus,
		showQueryClient:                      showsv1.NewShowQueryServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		meteredPatientQuery:                  meteredPatientQuery{PatientQueryServiceClient: patientsv1.NewPatientQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
	}
}

// ValidateGiftCardBatch validates a gift card batch.
func (s *GiftCardBatchValidationServer) ValidateGiftCardBatch(ctx context.Context, req *giftcardsv1.ValidateGiftCardBatchRequest) (*giftcardsv1.ValidateGiftCardBatchResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidateGiftCardBatch", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "validate gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*giftcardsv1.QuoteGiftCardBatchResponse, len(req.GetGiftCardBatch().GetLines()))
	for i, line := range req.GetGiftCardBatch().GetLines() {
		group.Go(func() error {
			reply, err := s.giftCardBatchCalculationClient.QuoteGiftCardBatch(groupCtx, &giftcardsv1.QuoteGiftCardBatchRequest{
				Context: req.GetContext(),
				GiftCardBatch: &giftcardsv1.GiftCardBatch{
					Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{
						Tax: &giftcardsv1.GiftCardBatchLineTax{
							Rate:       line.GetTax().GetRate(),
							Exemptions: line.GetTax().GetExemptions(),
						},
						Amount:    line.GetTax().GetAmount(),
						UnitPrice: line.GetUnitPrice(),
					}},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	recalculateGiftCardsRequest := &giftcardsv1.RecalculateGiftCardsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RecalculateGiftCardsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: req.GetGiftCardBatch().GetBillingAddress().GetRegion(),
			},
		},
	}
	recalculateGiftCardsReply, err := s.recalculateGiftCardsFn(ctx, recalculateGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards")
	}
	return &giftcardsv1.ValidateGiftCardBatchResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: recalculateGiftCardsReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			DueAt:           req.GetGiftCardBatch().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewGiftCardBatch previews a gift card batch.
func (s *GiftCardBatchValidationServer) PreviewGiftCardBatch(ctx context.Context, req *giftcardsv1.PreviewGiftCardBatchRequest) (*giftcardsv1.PreviewGiftCardBatchResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewGiftCardBatch", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "preview gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	var holderName string
	switch choice := req.GetGiftCardBatch().GetSettlement().(type) {
	case *giftcardsv1.GiftCardBatch_Card:
		holderName = choice.Card.GetHolderName()
	case *giftcardsv1.GiftCardBatch_Bank:
		holderName = choice.Bank.GetIban()
	case *giftcardsv1.GiftCardBatch_VoucherCode:
		holderName = choice.VoucherCode
	}
	explainLedgerAccountDecisionReply, err := s.ledgerAccountValidationClient.ExplainLedgerAccountDecision(ctx, &ledgerv1.ExplainLedgerAccountDecisionRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Funding: &ledgerv1.LedgerAccount_Card{Card: &ledgerv1.LedgerAccountCardFunding{
				Token:      req.GetGiftCardBatch().GetCard().GetToken(),
				HolderName: holderName,
			}},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetGiftCardBatch().GetAudit().GetUpdatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ExplainLedgerAccountDecision")
	}
	previewGiftCardRequest := &giftcardsv1.PreviewGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				Last4:   explainLedgerAccountDecisionReply.GetLedgerAccount().GetCard().GetLast4(),
				Network: req.GetGiftCardBatch().GetCard().GetNetwork(),
			}},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetGiftCardBatch().GetAudit().GetCreatedAt()},
		},
		Details: &giftcardsv1.PreviewGiftCardRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	previewGiftCardReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*giftcardsv1.PreviewGiftCardResponse, error) {
		return s.giftCardValidationClient.PreviewGiftCard(ctx, previewGiftCardRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/PreviewGiftCard")
	}
	return &giftcardsv1.PreviewGiftCardBatchResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName:  explainLedgerAccountDecisionReply.GetLedgerAccount().GetHolder().GetDisplayName(),
				CurrencyCode: previewGiftCardReply.GetGiftCard().GetCurrencyCode(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Status:          req.GetGiftCardBatch().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckGiftCardBatchEligibility checks a gift card batch.
func (s *GiftCardBatchValidationServer) CheckGiftCardBatchEligibility(ctx context.Context, req *giftcardsv1.CheckGiftCardBatchEligibilityRequest) (*giftcardsv1.CheckGiftCardBatchEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckGiftCardBatchEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "check gift card batch eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetGiftCardBatch()).(*giftcardsv1.GiftCardBatch)
	validateSynonymRequest := &searchv1.ValidateSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cloned.GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
		},
	}
	validateSynonymReply, err := s.meteredSynonymValidation.ValidateSynonym(ctx, validateSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ValidateSynonym")
	}
	compareGiftCardOptionsRequest := &giftcardsv1.CompareGiftCardOptionsRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{Bic: req.GetGiftCardBatch().GetBank().GetBic()}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateSynonymReply.GetSynonym().GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
			},
		},
	}
	compareGiftCardOptionsReply, err := s.meteredGiftCardCalculation.CompareGiftCardOptions(ctx, compareGiftCardOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CompareGiftCardOptions")
	}
	return &giftcardsv1.CheckGiftCardBatchEligibilityResponse{
		Outcome: &giftcardsv1.CheckGiftCardBatchEligibilityResponse_Outcome{
			ReferenceId: validateSynonymReply.GetOutcome().GetReferenceId(),
			Message:     compareGiftCardOptionsReply.GetResult().GetMessage(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Counterparty:    req.GetGiftCardBatch().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeGiftCardBatch normalizes a gift card batch.
func (s *GiftCardBatchValidationServer) NormalizeGiftCardBatch(ctx context.Context, req *giftcardsv1.NormalizeGiftCardBatchRequest) (*giftcardsv1.NormalizeGiftCardBatchResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeGiftCardBatch", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "normalize gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	giftCardBatchCalculation, ok := s.clients.Get("acme.giftcards.v1.GiftCardBatchCalculationService").(giftcardsv1.GiftCardBatchCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardBatchCalculationService")
	}
	getGiftCardBatchRatesRequest := &giftcardsv1.GetGiftCardBatchRatesRequest{
		Context:         req.GetContext(),
		GiftCardBatchId: req.GetGiftCardBatch().GetGiftCardBatchId(),
	}
	getGiftCardBatchRatesReply, err := giftCardBatchCalculation.GetGiftCardBatchRates(ctx, getGiftCardBatchRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/GetGiftCardBatchRates")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "giftcards.normalize_gift_card_batch.validate_gift_card",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"gift_card.holder.display_name": getGiftCardBatchRatesReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			"gift_card.bank.account_holder": getGiftCardBatchRatesReply.GetGiftCardBatch().GetBank().GetAccountHolder(),
			"gift_card.audit.etag":          req.GetGiftCardBatch().GetAudit().GetEtag(),
			"context.tenant_id":             req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	giftCardBatchCalculation2, ok := s.clients.Get("acme.giftcards.v1.GiftCardBatchCalculationService").(giftcardsv1.GiftCardBatchCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardBatchCalculationService")
	}
	var network string
	switch choice := req.GetGiftCardBatch().GetSettlement().(type) {
	case *giftcardsv1.GiftCardBatch_Card:
		network = choice.Card.GetNetwork()
	case *giftcardsv1.GiftCardBatch_Bank:
		network = choice.Bank.GetIban()
	case *giftcardsv1.GiftCardBatch_VoucherCode:
		network = choice.VoucherCode
	}
	calculateGiftCardBatchRequest := &giftcardsv1.CalculateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Settlement: &giftcardsv1.GiftCardBatch_Card{Card: &giftcardsv1.GiftCardBatchCardSettlement{Network: network}},
			BillingAddress: &commonv1.PostalAddress{
				Lines: getGiftCardBatchRatesReply.GetGiftCardBatch().GetBillingAddress().GetLines(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: getGiftCardBatchRatesReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
		},
		Details: &giftcardsv1.CalculateGiftCardBatchRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetGiftCardBatch().GetBillingAddress().GetLocality(),
			},
		},
	}
	calculateGiftCardBatchReply, err := giftCardBatchCalculation2.CalculateGiftCardBatch(ctx, calculateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/CalculateGiftCardBatch")
	}
	giftCardBatchCache := cache.New[string, *giftcardsv1.GiftCardBatch](1)
	giftCardBatchCache.Put(req.GetContext().GetTraceId(), req.GetGiftCardBatch())
	cachedGiftCardBatch, _ := giftCardBatchCache.Get(req.GetContext().GetTraceId())
	quoteGiftCardBatchRequest := newQuoteGiftCardBatchRequest(withQuoteGiftCardBatchContext(req.GetContext()), withQuoteGiftCardBatchDetails(&giftcardsv1.QuoteGiftCardBatchRequest_Details{
		Destination: &commonv1.PostalAddress{
			Recipient: cachedGiftCardBatch.GetBillingAddress().GetRecipient(),
		},
	}), withQuoteGiftCardBatchGiftCardBatch(&giftcardsv1.GiftCardBatch{
		Settlement: &giftcardsv1.GiftCardBatch_Card{Card: &giftcardsv1.GiftCardBatchCardSettlement{Last4: req.GetGiftCardBatch().GetCard().GetLast4()}},
	}))
	quoteGiftCardBatchReply, err := s.giftCardBatchCalculationClient.QuoteGiftCardBatch(ctx, quoteGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch")
	}
	return &giftcardsv1.NormalizeGiftCardBatchResponse{
		Result: &commonv1.OperationResult{Status: getGiftCardBatchRatesReply.GetResult().GetStatus()},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: calculateGiftCardBatchReply.GetGiftCardBatch().GetBillingAddress().GetRecipient(),
			},
			Metadata:        quoteGiftCardBatchReply.GetGiftCardBatch().GetMetadata(),
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Status:          req.GetGiftCardBatch().GetStatus(),
		},
	}, nil
}

// ExplainGiftCardBatchDecision explains a gift card batch.
func (s *GiftCardBatchValidationServer) ExplainGiftCardBatchDecision(ctx context.Context, req *giftcardsv1.ExplainGiftCardBatchDecisionRequest) (*giftcardsv1.ExplainGiftCardBatchDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "explain gift card batch decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	browseShowsRequest := &showsv1.BrowseShowsRequest{
		Context: req.GetContext(),
		Filter: &showsv1.BrowseShowsRequest_Filter{
			Region: req.GetGiftCardBatch().GetBillingAddress().GetRegion(),
		},
	}
	browseShowsReply, err := showsv1.ShowQueryServiceClient.BrowseShows(s.showQueryClient, ctx, browseShowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shows.v1.ShowQueryService/BrowseShows")
	}
	listApiCredentialVersionsRequest := &identityv1.ListApiCredentialVersionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListApiCredentialVersionsRequest_Filter{
			Region:       req.GetGiftCardBatch().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetGiftCardBatch().GetAudit().GetCreatedAt(),
		},
	}
	listApiCredentialVersionsReply, err := grpcx.NewClient(s.conn, identityv1.NewApiCredentialHistoryServiceClient).ListApiCredentialVersions(ctx, listApiCredentialVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/ListApiCredentialVersions")
	}
	return &giftcardsv1.ExplainGiftCardBatchDecisionResponse{
		Result: &commonv1.OperationResult{
			Message:       browseShowsReply.GetResult().GetMessage(),
			CorrelationId: listApiCredentialVersionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &giftcardsv1.ExplainGiftCardBatchDecisionResponse_Outcome{
			Status: listApiCredentialVersionsReply.GetResult().GetStatus(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Notes:           req.GetGiftCardBatch().GetNotes(),
		},
	}, nil
}

// ScoreGiftCardBatch scores a gift card batch.
func (s *GiftCardBatchValidationServer) ScoreGiftCardBatch(ctx context.Context, req *giftcardsv1.ScoreGiftCardBatchRequest) (*giftcardsv1.ScoreGiftCardBatchResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreGiftCardBatch", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "score gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetGiftCardBatch()).(*giftcardsv1.GiftCardBatch)
	metadata2 := make(map[string]string, len(req.GetGiftCardBatch().GetMetadata()))
	for key, value := range req.GetGiftCardBatch().GetMetadata() {
		metadata2[key] = value
	}
	browsePatientsRequest := &patientsv1.BrowsePatientsRequest{
		Context: req.GetContext(),
		Filter: &patientsv1.BrowsePatientsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: cloned.GetDueAt(),
			Labels:       metadata2,
			OwnerId:      req.GetGiftCardBatch().GetCounterparty().GetGiftCardId(),
		},
	}
	browsePatientsReply, err := s.meteredPatientQuery.BrowsePatients(ctx, browsePatientsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.patients.v1.PatientQueryService/BrowsePatients")
	}
	var holderName string
	switch choice := req.GetGiftCardBatch().GetSettlement().(type) {
	case *giftcardsv1.GiftCardBatch_Card:
		holderName = choice.Card.GetHolderName()
	case *giftcardsv1.GiftCardBatch_Bank:
		holderName = choice.Bank.GetIban()
	case *giftcardsv1.GiftCardBatch_VoucherCode:
		holderName = choice.VoucherCode
	}
	explainGiftCardBatchCalculationRequest := &giftcardsv1.ExplainGiftCardBatchCalculationRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Settlement: &giftcardsv1.GiftCardBatch_Card{Card: &giftcardsv1.GiftCardBatchCardSettlement{HolderName: holderName}},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetGiftCardBatch().GetBillingAddress().GetCountryCode(),
				Lines:       req.GetDetails().GetDestination().GetLines(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetGiftCardBatch().GetAudit().GetCreatedAt()},
		},
	}
	type explainGiftCardBatchCalculationResult struct {
		reply *giftcardsv1.ExplainGiftCardBatchCalculationResponse
		err   error
	}
	explainGiftCardBatchCalculationResults := make(chan explainGiftCardBatchCalculationResult, 1)
	go func() {
		reply, err := s.giftCardBatchCalculationClient.ExplainGiftCardBatchCalculation(ctx, explainGiftCardBatchCalculationRequest)
		explainGiftCardBatchCalculationResults <- explainGiftCardBatchCalculationResult{reply, err}
	}()
	var explainGiftCardBatchCalculationReply *giftcardsv1.ExplainGiftCardBatchCalculationResponse
	select {
	case result := <-explainGiftCardBatchCalculationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardBatchCalculationService/ExplainGiftCardBatchCalculation")
		}
		explainGiftCardBatchCalculationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	_ = explainGiftCardBatchCalculationReply
	return &giftcardsv1.ScoreGiftCardBatchResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browsePatientsReply.GetResult().GetReferenceIds(),
			Message:      browsePatientsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Counterparty:    req.GetGiftCardBatch().GetCounterparty(),
		},
	}, nil
}

// LintGiftCardBatches lints a gift card batch.
func (s *GiftCardBatchValidationServer) LintGiftCardBatches(ctx context.Context, req *giftcardsv1.LintGiftCardBatchesRequest) (*giftcardsv1.LintGiftCardBatchesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "lint gift card batches", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetGiftCardBatch().GetLines(), func(candidate *giftcardsv1.GiftCardBatchLine) bool { return candidate != nil }) {
		if _, err := s.giftCardValidationClient.CheckGiftCardEligibility(ctx, &giftcardsv1.CheckGiftCardEligibilityRequest{
			Context: req.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{Amount: line.GetTax().GetAmount()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/CheckGiftCardEligibility")
		}
	}
	explainGiftCardTransactionCalculationRequest := &giftcardsv1.ExplainGiftCardTransactionCalculationRequest{}
	explainGiftCardTransactionCalculationRequest.Context = req.GetContext()
	explainGiftCardTransactionCalculationRequest.Details = &giftcardsv1.ExplainGiftCardTransactionCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
	}
	explainGiftCardTransactionCalculationReply, err := s.giftCardTransactionCalculationClient.ExplainGiftCardTransactionCalculation(ctx, explainGiftCardTransactionCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/ExplainGiftCardTransactionCalculation")
	}
	estimateGiftCardBatchRequest := &giftcardsv1.EstimateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetGiftCardBatch().GetAudit().GetUpdatedAt()},
		},
		Details: &giftcardsv1.EstimateGiftCardBatchRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetGiftCardBatch().GetBillingAddress().GetLocality(),
			},
		},
	}
	estimateGiftCardBatchReply, err := grpcx.Call(ctx, s.giftCardBatchCalculationClient.EstimateGiftCardBatch, estimateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/EstimateGiftCardBatch")
	}
	return &giftcardsv1.LintGiftCardBatchesResponse{
		Outcome: &giftcardsv1.LintGiftCardBatchesResponse_Outcome{
			ReferenceId: explainGiftCardTransactionCalculationReply.GetOutcome().GetReferenceId(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: estimateGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Metadata:        req.GetGiftCardBatch().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: estimateGiftCardBatchReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// GetGiftCardBatchRules returns a gift card batch.
func (s *GiftCardBatchValidationServer) GetGiftCardBatchRules(ctx context.Context, req *giftcardsv1.GetGiftCardBatchRulesRequest) (*giftcardsv1.GetGiftCardBatchRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_batch", req.GetGiftCardBatchId(), "get_gift_card_batch_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get gift card batch rules", "tenant", req.GetContext().GetTenantId())
	return &giftcardsv1.GetGiftCardBatchRulesResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{GiftCardBatchId: req.GetGiftCardBatchId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}
