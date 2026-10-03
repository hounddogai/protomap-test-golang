package giftcards

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// GiftCardBatchCalculationServer implements `acme.giftcards.v1.GiftCardBatchCalculationService`.
// Computes the figures of a gift card batch, such as totals, taxes, and estimates.
type GiftCardBatchCalculationServer struct {
	giftcardsv1.UnimplementedGiftCardBatchCalculationServiceServer
	store                             *Store
	log                               *slog.Logger
	giftCardCalculationClient         giftcardsv1.GiftCardCalculationServiceClient
	synonymExportClient               searchv1.SynonymExportServiceClient
	synonymExportClientFallback       searchv1.SynonymExportServiceClient
	conn                              *grpc.ClientConn
	accessPolicyClient                authorizationv1.AccessPolicyServiceClient
	giftCardCalculationClientFallback giftcardsv1.GiftCardCalculationServiceClient
}

// NewGiftCardBatchCalculationServer wires a GiftCardBatchCalculationServer.
func NewGiftCardBatchCalculationServer(deps platform.Deps) *GiftCardBatchCalculationServer {
	return &GiftCardBatchCalculationServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.giftcards.v1.GiftCardBatchCalculationService"),
		giftCardCalculationClient:         giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		synonymExportClient:               searchv1.NewSynonymExportServiceClient(deps.Conn),
		synonymExportClientFallback:       searchv1.NewSynonymExportServiceClient(deps.Conn),
		conn:                              deps.Conn,
		accessPolicyClient:                authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		giftCardCalculationClientFallback: giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
	}
}

// CalculateGiftCardBatch calculates a gift card batch.
func (s *GiftCardBatchCalculationServer) CalculateGiftCardBatch(ctx context.Context, req *giftcardsv1.CalculateGiftCardBatchRequest) (*giftcardsv1.CalculateGiftCardBatchResponse, error) {
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "calculate gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	recalculateGiftCardsRequest := &giftcardsv1.RecalculateGiftCardsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RecalculateGiftCardsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetGiftCardBatch().GetAudit().GetCreatedAt()},
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{
				AccountHolder: req.GetGiftCardBatch().GetBank().GetAccountHolder(),
			}},
		},
	}
	recalculateGiftCardsReply, err := giftcardsv1.GiftCardCalculationServiceClient.RecalculateGiftCards(s.giftCardCalculationClient, ctx, recalculateGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards")
	}
	step := &giftCardBatchCalculationServerCalculateGiftCardBatchGetGiftCardRatesStep{request: &giftcardsv1.GetGiftCardRatesRequest{
		Context:    req.GetContext(),
		GiftCardId: req.GetGiftCardBatch().GetCounterparty().GetGiftCardId(),
	}}
	if err := s.runCalculateGiftCardBatchGetGiftCardRates(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/GetGiftCardRates")
	}
	getGiftCardRatesReply := step.reply
	_ = getGiftCardRatesReply
	return &giftcardsv1.CalculateGiftCardBatchResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				CurrencyCode: recalculateGiftCardsReply.GetGiftCard().GetCurrencyCode(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Notes:           req.GetGiftCardBatch().GetNotes(),
		},
		Breakdown: &giftcardsv1.CalculateGiftCardBatchResponse_Breakdown{
			CurrencyCode: recalculateGiftCardsReply.GetGiftCard().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateGiftCardBatch estimates a gift card batch.
func (s *GiftCardBatchCalculationServer) EstimateGiftCardBatch(ctx context.Context, req *giftcardsv1.EstimateGiftCardBatchRequest) (*giftcardsv1.EstimateGiftCardBatchResponse, error) {
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "estimate gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *searchv1.ScheduleSynonymExportRequest, ...grpc.CallOption) (*searchv1.ScheduleSynonymExportResponse, error){
		"primary":  s.synonymExportClient.ScheduleSynonymExport,
		"fallback": s.synonymExportClientFallback.ScheduleSynonymExport,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	metadata2 := make(map[string]string, len(req.GetGiftCardBatch().GetMetadata()))
	for key2, value2 := range req.GetGiftCardBatch().GetMetadata() {
		metadata2[key2] = value2
	}
	scheduleSynonymExportRequest := &searchv1.ScheduleSynonymExportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &searchv1.ScheduleSynonymExportRequest_Details{Filters: metadata2},
	}
	scheduleSynonymExportReply, err := route(ctx, scheduleSynonymExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ScheduleSynonymExport")
	}
	var token string
	switch choice := req.GetGiftCardBatch().GetSettlement().(type) {
	case *giftcardsv1.GiftCardBatch_Card:
		token = choice.Card.GetToken()
	case *giftcardsv1.GiftCardBatch_Bank:
		token = choice.Bank.GetIban()
	case *giftcardsv1.GiftCardBatch_VoucherCode:
		token = choice.VoucherCode
	}
	estimateGiftCardReply, err := s.giftCardCalculationClient.EstimateGiftCard(ctx, &giftcardsv1.EstimateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{Token: token}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/EstimateGiftCard")
	}
	return &giftcardsv1.EstimateGiftCardBatchResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scheduleSynonymExportReply.GetResult().GetCorrelationId(),
			Status:        scheduleSynonymExportReply.GetResult().GetStatus(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				CurrencyCode: estimateGiftCardReply.GetBreakdown().GetCurrencyCode(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			DueAt:           req.GetGiftCardBatch().GetDueAt(),
		},
	}, nil
}

// QuoteGiftCardBatch quotes a gift card batch.
func (s *GiftCardBatchCalculationServer) QuoteGiftCardBatch(ctx context.Context, req *giftcardsv1.QuoteGiftCardBatchRequest) (*giftcardsv1.QuoteGiftCardBatchResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "quote gift card batch", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	var accountHolder string
	switch choice := req.GetGiftCardBatch().GetSettlement().(type) {
	case *giftcardsv1.GiftCardBatch_Card:
		accountHolder = choice.Card.GetHolderName()
	case *giftcardsv1.GiftCardBatch_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *giftcardsv1.GiftCardBatch_VoucherCode:
		accountHolder = choice.VoucherCode
	}
	calculateGiftCardRequest := &giftcardsv1.CalculateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: req.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{AccountHolder: accountHolder}},
		},
	}
	var calculateGiftCardReply *giftcardsv1.CalculateGiftCardResponse
	if err := pipeline.Run(ctx, "acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard", func(ctx context.Context) error {
		answer, err := s.giftCardCalculationClient.CalculateGiftCard(ctx, calculateGiftCardRequest)
		calculateGiftCardReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard")
	}
	for line := range iterx.Each(req.GetGiftCardBatch().GetLines(), func(candidate *giftcardsv1.GiftCardBatchLine) bool { return candidate != nil }) {
		if _, err := s.giftCardCalculationClient.SimulateGiftCard(ctx, &giftcardsv1.SimulateGiftCardRequest{
			Context: req.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{Amount: line.GetTax().GetAmount()}},
				Audit: &commonv1.AuditInfo{
					CreatedAt: calculateGiftCardReply.GetGiftCard().GetAudit().GetCreatedAt(),
					CreatedBy: req.GetGiftCardBatch().GetAudit().GetCreatedBy(),
				},
			},
			Details: &giftcardsv1.SimulateGiftCardRequest_Details{
				Destination: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/SimulateGiftCard")
		}
	}
	compareGiftCardOptionsRequest := &giftcardsv1.CompareGiftCardOptionsRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{
				AccountHolder: calculateGiftCardReply.GetGiftCard().GetBank().GetAccountHolder(),
				Bic:           req.GetGiftCardBatch().GetBank().GetBic(),
			}},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetGiftCardBatch().GetAudit().GetCreatedBy()},
		},
	}
	type compareGiftCardOptionsResult struct {
		reply *giftcardsv1.CompareGiftCardOptionsResponse
		err   error
	}
	compareGiftCardOptionsResults := make(chan compareGiftCardOptionsResult, 1)
	go func() {
		reply, err := s.giftCardCalculationClient.CompareGiftCardOptions(ctx, compareGiftCardOptionsRequest)
		compareGiftCardOptionsResults <- compareGiftCardOptionsResult{reply, err}
	}()
	var compareGiftCardOptionsReply *giftcardsv1.CompareGiftCardOptionsResponse
	select {
	case result := <-compareGiftCardOptionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardCalculationService/CompareGiftCardOptions")
		}
		compareGiftCardOptionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	getGiftCardRatesReply, err := s.giftCardCalculationClient.GetGiftCardRates(ctx, &giftcardsv1.GetGiftCardRatesRequest{
		Context:    req.GetContext(),
		GiftCardId: req.GetGiftCardBatch().GetCounterparty().GetGiftCardId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/GetGiftCardRates")
	}
	return &giftcardsv1.QuoteGiftCardBatchResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: calculateGiftCardReply.GetGiftCard().GetAudit().GetCreatedBy(),
				UpdatedBy: getGiftCardRatesReply.GetGiftCard().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &giftcardsv1.GiftCardRef{
				GiftCardId: compareGiftCardOptionsReply.GetGiftCard().GetGiftCardId(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Status:          req.GetGiftCardBatch().GetStatus(),
		},
		Breakdown: &giftcardsv1.QuoteGiftCardBatchResponse_Breakdown{
			CurrencyCode: compareGiftCardOptionsReply.GetGiftCard().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{
			Details: getGiftCardRatesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// RecalculateGiftCardBatches recalculates a gift card batch.
func (s *GiftCardBatchCalculationServer) RecalculateGiftCardBatches(ctx context.Context, req *giftcardsv1.RecalculateGiftCardBatchesRequest) (*giftcardsv1.RecalculateGiftCardBatchesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "recalculate gift card batches", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	var bic string
	switch choice := req.GetGiftCardBatch().GetSettlement().(type) {
	case *giftcardsv1.GiftCardBatch_Card:
		bic = choice.Card.GetHolderName()
	case *giftcardsv1.GiftCardBatch_Bank:
		bic = choice.Bank.GetBic()
	case *giftcardsv1.GiftCardBatch_VoucherCode:
		bic = choice.VoucherCode
	}
	simulateGiftCardRequest := &giftcardsv1.SimulateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{Bic: bic}},
		},
		Details: &giftcardsv1.SimulateGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
				Locality:  req.GetGiftCardBatch().GetBillingAddress().GetLocality(),
				Region:    req.GetDetails().GetDestination().GetRegion(),
			},
		},
	}
	simulateGiftCardReply := new(giftcardsv1.SimulateGiftCardResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardCalculationService_SimulateGiftCard_FullMethodName, simulateGiftCardRequest, simulateGiftCardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/SimulateGiftCard")
	}
	calculateGiftCardReply, err := s.giftCardCalculationClient.CalculateGiftCard(ctx, &giftcardsv1.CalculateGiftCardRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CalculateGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/CalculateGiftCard")
	}
	return &giftcardsv1.RecalculateGiftCardBatchesResponse{
		Breakdown: &giftcardsv1.RecalculateGiftCardBatchesResponse_Breakdown{
			CurrencyCode: simulateGiftCardReply.GetBreakdown().GetCurrencyCode(),
			Total:        req.GetGiftCardBatch().GetTotal(),
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				CurrencyCode: calculateGiftCardReply.GetGiftCard().GetCurrencyCode(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainGiftCardBatchCalculation explains a gift card batch.
func (s *GiftCardBatchCalculationServer) ExplainGiftCardBatchCalculation(ctx context.Context, req *giftcardsv1.ExplainGiftCardBatchCalculationRequest) (*giftcardsv1.ExplainGiftCardBatchCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainGiftCardBatchCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardBatch(ctx, req.GetContext().GetTenantId(), req.GetGiftCardBatch()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card batch")
	}
	s.log.InfoContext(ctx, "explain gift card batch calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCardBatch().GetAudit().GetEtag())
	watchAccessPoliciesStream, err := s.accessPolicyClient.WatchAccessPolicies(ctx, &authorizationv1.WatchAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.WatchAccessPoliciesRequest_Filter{
			Region: req.GetGiftCardBatch().GetBillingAddress().GetRegion(),
		},
		Subscriber: &commonv1.ContactInfo{
			Phone: req.GetDetails().GetContact().GetPhone(),
			Name:  req.GetDetails().GetContact().GetName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
	}
	for {
		received, err := watchAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
		}
		_ = received
	}
	routes := map[string]func(context.Context, *giftcardsv1.EstimateGiftCardRequest, ...grpc.CallOption) (*giftcardsv1.EstimateGiftCardResponse, error){
		"primary":  s.giftCardCalculationClient.EstimateGiftCard,
		"fallback": s.giftCardCalculationClientFallback.EstimateGiftCard,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	estimateGiftCardRequest := &giftcardsv1.EstimateGiftCardRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.EstimateGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetGiftCardBatch().GetBillingAddress().GetLocality(),
			},
		},
	}
	estimateGiftCardReply, err := route(ctx, estimateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/EstimateGiftCard")
	}
	return &giftcardsv1.ExplainGiftCardBatchCalculationResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: estimateGiftCardReply.GetGiftCard().GetAudit().GetUpdatedBy(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
			Status:          req.GetGiftCardBatch().GetStatus(),
		},
		Outcome: &giftcardsv1.ExplainGiftCardBatchCalculationResponse_Outcome{Message: estimateGiftCardReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetGiftCardBatchRates returns a gift card batch.
func (s *GiftCardBatchCalculationServer) GetGiftCardBatchRates(ctx context.Context, req *giftcardsv1.GetGiftCardBatchRatesRequest) (*giftcardsv1.GetGiftCardBatchRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_batch", req.GetGiftCardBatchId(), "get_gift_card_batch_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get gift card batch rates", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	recalculateGiftCardsRequest := newRecalculateGiftCardsRequest(withRecalculateGiftCardsContext(req.GetContext()), withRecalculateGiftCardsGiftCard(&giftcardsv1.GiftCard{
		Contact: &commonv1.ContactInfo{Email: email},
		Funding: &giftcardsv1.GiftCard_ExternalAccountId{ExternalAccountId: req.GetGiftCardBatchId()},
		Audit:   &commonv1.AuditInfo{CreatedAt: cloned.GetDeadline()},
		Labels:  req.GetContext().GetBaggage(),
	}))
	recalculateGiftCardsReply, err := s.giftCardCalculationClient.RecalculateGiftCards(ctx, recalculateGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards")
	}
	return &giftcardsv1.GetGiftCardBatchRatesResponse{
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateGiftCardsReply.GetGiftCard().GetAudit().GetCreatedBy(),
			},
			GiftCardBatchId: req.GetGiftCardBatchId(),
		},
		Result: &commonv1.OperationResult{Status: recalculateGiftCardsReply.GetResult().GetStatus()},
	}, nil
}

// giftCardBatchCalculationServerCalculateGiftCardBatchGetGiftCardRatesStep carries the request of one step of
// CalculateGiftCardBatch and, once it ran, its reply.
type giftCardBatchCalculationServerCalculateGiftCardBatchGetGiftCardRatesStep struct {
	request *giftcardsv1.GetGiftCardRatesRequest
	reply   *giftcardsv1.GetGiftCardRatesResponse
}

func (s *GiftCardBatchCalculationServer) runCalculateGiftCardBatchGetGiftCardRates(ctx context.Context, step *giftCardBatchCalculationServerCalculateGiftCardBatchGetGiftCardRatesStep) error {
	reply, err := s.giftCardCalculationClient.GetGiftCardRates(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
