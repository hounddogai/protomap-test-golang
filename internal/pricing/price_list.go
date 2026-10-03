package pricing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PriceListServer implements `acme.pricing.v1.PriceListService`.
// Manages price lists: creates, reads, changes, and lists them.
type PriceListServer struct {
	pricingv1.UnimplementedPriceListServiceServer
	store                           *Store
	log                             *slog.Logger
	currencyRateCalculationClient   pricingv1.CurrencyRateCalculationServiceClient
	priceQuoteQueryClient           pricingv1.PriceQuoteQueryServiceClient
	priceQuoteValidationClient      pricingv1.PriceQuoteValidationServiceClient
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
	currencyRateValidationClient    pricingv1.CurrencyRateValidationServiceClient
	conn                            *grpc.ClientConn
	orderAmendmentValidationOnce    sync.Once
	orderAmendmentValidationCached  ordersv1.OrderAmendmentValidationServiceClient
	documentValidationClient        documentsv1.DocumentValidationServiceClient
	parcelCalculationClient         shippingv1.ParcelCalculationServiceClient
	synonymValidationClient         searchv1.SynonymValidationServiceClient
	metricSeriesQueryClient         metricsv1.MetricSeriesQueryServiceClient
	orderAmendmentValidationClient  ordersv1.OrderAmendmentValidationServiceClient
}

// NewPriceListServer wires a PriceListServer.
func NewPriceListServer(deps platform.Deps) *PriceListServer {
	return &PriceListServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.pricing.v1.PriceListService"),
		currencyRateCalculationClient:   pricingv1.NewCurrencyRateCalculationServiceClient(deps.Conn),
		priceQuoteQueryClient:           pricingv1.NewPriceQuoteQueryServiceClient(deps.Conn),
		priceQuoteValidationClient:      pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		currencyRateValidationClient:    pricingv1.NewCurrencyRateValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		documentValidationClient:        documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		parcelCalculationClient:         shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		synonymValidationClient:         searchv1.NewSynonymValidationServiceClient(deps.Conn),
		metricSeriesQueryClient:         metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		orderAmendmentValidationClient:  ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn),
	}
}

// CreatePriceList creates a price list.
func (s *PriceListServer) CreatePriceList(ctx context.Context, req *pricingv1.CreatePriceListRequest) (*pricingv1.CreatePriceListResponse, error) {
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "create price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	var notifyEmail string
	switch choice := req.GetPriceList().GetAction().(type) {
	case *pricingv1.PriceList_Route:
		notifyEmail = choice.Route.GetTarget()
	case *pricingv1.PriceList_Block:
		notifyEmail = choice.Block.GetNotifyEmail()
	}
	ctx = tenancy.With(ctx, "display_name", req.GetPriceList().GetOwner().GetDisplayName())
	step := &priceListServerCreatePriceListSimulateCurrencyRateStep{request: &pricingv1.SimulateCurrencyRateRequest{
		Context: req.GetContext(),
		Reason:  req.GetPriceList().GetBlock().GetReason(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				Email:       notifyEmail,
				DisplayName: tenancy.Value(ctx, "display_name"),
			},
			Measurements: []*pricingv1.CurrencyRateMeasurement{&pricingv1.CurrencyRateMeasurement{ObservedAt: req.GetPriceList().GetAudit().GetUpdatedAt()}},
		},
	}}
	if err := s.runCreatePriceListSimulateCurrencyRate(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateCalculationService/SimulateCurrencyRate")
	}
	simulateCurrencyRateReply := step.reply
	ctx = tenancy.With(ctx, "created_by", req.GetPriceList().GetAudit().GetCreatedBy())
	scorePriceQuoteRequest := &pricingv1.ScorePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: simulateCurrencyRateReply.GetCurrencyRate().GetSubject().GetDisplayName(),
				PriceListId: req.GetPriceList().GetPriceListId(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Tax: &pricingv1.PriceQuoteLineTax{Amount: req.GetPriceList().GetDiscount().GetAmount()},
			}},
		},
	}
	scorePriceQuoteReply, err := scorePriceQuoteCall(ctx, scorePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/ScorePriceQuote")
	}
	return &pricingv1.CreatePriceListResponse{
		Result: &commonv1.OperationResult{
			Details:       simulateCurrencyRateReply.GetResult().GetDetails(),
			ReferenceIds:  simulateCurrencyRateReply.GetResult().GetReferenceIds(),
			CorrelationId: scorePriceQuoteReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PriceList: &pricingv1.PriceList{
			Effective:  req.GetPriceList().GetEffective(),
			Parameters: req.GetPriceList().GetParameters(),
		},
	}, nil
}

// GetPriceList returns a price list.
func (s *PriceListServer) GetPriceList(ctx context.Context, req *pricingv1.GetPriceListRequest) (*pricingv1.GetPriceListResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_list", req.GetPriceListId(), "get_price_list"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get price list", "tenant", req.GetContext().GetTenantId())
	var cachedName string
	if entry, ok := recentPriceLists.Load(recordKey{req.GetContext().GetTenantId(), req.GetPriceListId()}); ok {
		if previous, ok := entry.(*pricingv1.PriceList); ok {
			cachedName = previous.GetName()
		}
	}
	suggestPriceQuotesRequest := &pricingv1.SuggestPriceQuotesRequest{}
	suggestPriceQuotesRequest.Context = req.GetContext()
	suggestPriceQuotesRequest.Filter = &pricingv1.SuggestPriceQuotesRequest_Filter{OwnerId: req.GetPriceListId()}
	suggestPriceQuotesRequest.OrderBy = cachedName
	suggestPriceQuotesReply, err := s.priceQuoteQueryClient.SuggestPriceQuotes(ctx, suggestPriceQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteQueryService/SuggestPriceQuotes")
	}
	scorePriceQuoteRequest := newScorePriceQuoteRequest(withScorePriceQuoteContext(req.GetContext()), withScorePriceQuotePriceQuote(&pricingv1.PriceQuote{
		Counterparty: &pricingv1.PriceListRef{PriceListId: req.GetPriceListId()},
		Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
			Attributes: suggestPriceQuotesReply.GetResult().GetDetails(),
		}},
		Audit:    &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		Metadata: req.GetContext().GetBaggage(),
	}))
	scorePriceQuoteReply, err := s.priceQuoteValidationClient.ScorePriceQuote(ctx, scorePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/ScorePriceQuote")
	}
	return &pricingv1.GetPriceListResponse{
		Result: &commonv1.OperationResult{Status: suggestPriceQuotesReply.GetResult().GetStatus()},
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				Etag: scorePriceQuoteReply.GetPriceQuote().GetAudit().GetEtag(),
			},
			PriceListId: req.GetPriceListId(),
		},
	}, nil
}

// UpdatePriceList changes a price list.
func (s *PriceListServer) UpdatePriceList(ctx context.Context, req *pricingv1.UpdatePriceListRequest) (*pricingv1.UpdatePriceListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "update price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	unsubscribeApiCredentialUpdatesRequest := &identityv1.UnsubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetPriceList().GetOwner().GetEmail()},
		},
		Reason: req.GetPriceList().GetBlock().GetReason(),
	}
	var unsubscribeApiCredentialUpdatesReply *identityv1.UnsubscribeApiCredentialUpdatesResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.ApiCredentialNotificationService/UnsubscribeApiCredentialUpdates", func(ctx context.Context) error {
		answer, err := s.apiCredentialNotificationClient.UnsubscribeApiCredentialUpdates(ctx, unsubscribeApiCredentialUpdatesRequest)
		unsubscribeApiCredentialUpdatesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/UnsubscribeApiCredentialUpdates")
	}
	normalizeCurrencyRateRequest := buildNormalizeCurrencyRateRequestForUpdatePriceList(req, unsubscribeApiCredentialUpdatesReply)
	normalizeCurrencyRateReply, err := s.currencyRateValidationClient.NormalizeCurrencyRate(ctx, normalizeCurrencyRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/NormalizeCurrencyRate")
	}
	recentPriceLists.Store(recordKey{req.GetContext().GetTenantId(), req.GetPriceList().GetPriceListId()}, req.GetPriceList())
	return &pricingv1.UpdatePriceListResponse{
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: normalizeCurrencyRateReply.GetCurrencyRate().GetSubject().GetDisplayName(),
			},
			Description: req.GetPriceList().GetDescription(),
			Status:      req.GetPriceList().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DeletePriceList deletes a price list.
func (s *PriceListServer) DeletePriceList(ctx context.Context, req *pricingv1.DeletePriceListRequest) (*pricingv1.DeletePriceListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_list", req.GetPriceListId(), "delete_price_list"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete price list", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "price_list_id", req.GetPriceListId())
	scorePriceQuoteRequest := &pricingv1.ScorePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{PriceListId: tenancy.Value(ctx, "price_list_id")},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetEtag(),
				UpdatedAt: req.GetContext().GetDeadline(),
			},
		},
		Reason: req.GetReason(),
	}
	scorePriceQuoteReply := new(pricingv1.ScorePriceQuoteResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.pricing.v1.PriceQuoteValidationService/ScorePriceQuote", scorePriceQuoteRequest, scorePriceQuoteReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/ScorePriceQuote")
	}
	return &pricingv1.DeletePriceListResponse{
		Result: &commonv1.OperationResult{
			Message: scorePriceQuoteReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ListPriceLists lists price lists a page at a time.
func (s *PriceListServer) ListPriceLists(ctx context.Context, req *pricingv1.ListPriceListsRequest) (*pricingv1.ListPriceListsResponse, error) {
	s.log.InfoContext(ctx, "list price lists", "tenant", req.GetContext().GetTenantId())
	return &pricingv1.ListPriceListsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// BatchGetPriceLists returns several price lists at once.
func (s *PriceListServer) BatchGetPriceLists(ctx context.Context, req *pricingv1.BatchGetPriceListsRequest) (*pricingv1.BatchGetPriceListsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BatchGetPriceLists", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get price lists", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "locale", req.GetContext().GetLocale())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	explainOrderAmendmentDecisionRequest := &ordersv1.ExplainOrderAmendmentDecisionRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ExplainOrderAmendmentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  tenancy.Value(ctx, "locale"),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
		Annotations: baggage,
	}
	explainOrderAmendmentDecisionReply, err := s.orderAmendmentValidationLazy().ExplainOrderAmendmentDecision(ctx, explainOrderAmendmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/ExplainOrderAmendmentDecision")
	}
	previewDocumentRequest := &documentsv1.PreviewDocumentRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Counterparty: &documentsv1.DocumentFolderRef{
				DisplayName: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
			Settlement: &documentsv1.Document_Bank{Bank: &documentsv1.DocumentBankSettlement{
				AccountHolder: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetBank().GetAccountHolder(),
				Bic:           explainOrderAmendmentDecisionReply.GetOrderAmendment().GetBank().GetBic(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetBillingAddress().GetRecipient(),
			},
		},
	}
	previewDocumentReply, err := grpcx.Call(ctx, s.documentValidationClient.PreviewDocument, previewDocumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/PreviewDocument")
	}
	return &pricingv1.BatchGetPriceListsResponse{
		PriceLists: []*pricingv1.PriceList{&pricingv1.PriceList{
			Priority: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetCounterparty().GetPriority(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetAudit().GetUpdatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{
			Status:  previewDocumentReply.GetOutcome().GetStatus(),
			Details: previewDocumentReply.GetResult().GetDetails(),
		},
	}, nil
}

// SearchPriceLists lists price lists a page at a time.
func (s *PriceListServer) SearchPriceLists(ctx context.Context, req *pricingv1.SearchPriceListsRequest) (*pricingv1.SearchPriceListsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "search price lists", "tenant", req.GetContext().GetTenantId())
	recalculateParcelsRequest := buildRecalculateParcelsRequestForSearchPriceLists(req)
	recalculateParcelsReply, err := s.parcelCalculationClient.RecalculateParcels(ctx, recalculateParcelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/RecalculateParcels")
	}
	return &pricingv1.SearchPriceListsResponse{
		PriceLists: []*pricingv1.PriceList{&pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculateParcelsReply.GetParcel().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BatchCreatePriceLists takes a stream of price lists and answers once at the end.
func (s *PriceListServer) BatchCreatePriceLists(stream grpc.ClientStreamingServer[pricingv1.BatchCreatePriceListsRequest, pricingv1.BatchCreatePriceListsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create price lists", "tenant", msg.GetContext().GetTenantId())
		if _, err := s.synonymValidationClient.LintSynonyms(ctx, &searchv1.LintSynonymsRequest{
			Context: msg.GetContext(),
			Synonym: &searchv1.Synonym{
				Action: &searchv1.Synonym_Discount{Discount: &searchv1.SynonymDiscountAction{Amount: msg.GetRecord().GetDiscount().GetAmount()}},
			},
			Details: &searchv1.LintSynonymsRequest_Details{
				Contact: &commonv1.ContactInfo{
					Phone:            msg.GetHeader().GetSubmittedBy().GetPhone(),
					PreferredChannel: msg.GetHeader().GetSubmittedBy().GetPreferredChannel(),
				},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/LintSynonyms")
		}
		summarizeMetricSeriesesRequest := &metricsv1.SummarizeMetricSeriesesRequest{
			Context: msg.GetContext(),
			Details: &metricsv1.SummarizeMetricSeriesesRequest_Details{Format: msg.GetHeader().GetFormat()},
		}
		if _, err := grpcx.Call(ctx, s.metricSeriesQueryClient.SummarizeMetricSerieses, summarizeMetricSeriesesRequest); err != nil {
			return grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses")
		}
		accepted++
	}
	return stream.SendAndClose(&pricingv1.BatchCreatePriceListsResponse{Accepted: accepted})
}

// ClonePriceList creates a price list.
func (s *PriceListServer) ClonePriceList(ctx context.Context, req *pricingv1.ClonePriceListRequest) (*pricingv1.ClonePriceListResponse, error) {
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "clone price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	priceListCache := cache.New[string, *pricingv1.PriceList](1)
	priceListCache.Put(req.GetContext().GetTraceId(), req.GetPriceList())
	cachedPriceList, _ := priceListCache.Get(req.GetContext().GetTraceId())
	explainOrderAmendmentDecisionRequest := &ordersv1.ExplainOrderAmendmentDecisionRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPriceList().GetAudit().GetCreatedBy(),
				Etag:      cachedPriceList.GetAudit().GetEtag(),
			},
			Lines: []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{
				Tax: &ordersv1.OrderAmendmentLineTax{Amount: req.GetPriceList().GetDiscount().GetAmount()},
			}},
		},
	}
	type explainOrderAmendmentDecisionResult struct {
		reply *ordersv1.ExplainOrderAmendmentDecisionResponse
		err   error
	}
	explainOrderAmendmentDecisionResults := make(chan explainOrderAmendmentDecisionResult, 1)
	go func() {
		reply, err := s.orderAmendmentValidationClient.ExplainOrderAmendmentDecision(ctx, explainOrderAmendmentDecisionRequest)
		explainOrderAmendmentDecisionResults <- explainOrderAmendmentDecisionResult{reply, err}
	}()
	var explainOrderAmendmentDecisionReply *ordersv1.ExplainOrderAmendmentDecisionResponse
	select {
	case result := <-explainOrderAmendmentDecisionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderAmendmentValidationService/ExplainOrderAmendmentDecision")
		}
		explainOrderAmendmentDecisionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	checkPriceQuoteEligibilityRequest := &pricingv1.CheckPriceQuoteEligibilityRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Tax: &pricingv1.PriceQuoteLineTax{Amount: req.GetPriceList().GetDiscount().GetAmount()},
			}},
			BillingAddress: &commonv1.PostalAddress{
				Locality: explainOrderAmendmentDecisionReply.GetOrderAmendment().GetBillingAddress().GetLocality(),
			},
		},
	}
	checkPriceQuoteEligibilityReply := new(pricingv1.CheckPriceQuoteEligibilityResponse)
	if err := grpc.Invoke(ctx, "/acme.pricing.v1.PriceQuoteValidationService/CheckPriceQuoteEligibility", checkPriceQuoteEligibilityRequest, checkPriceQuoteEligibilityReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/CheckPriceQuoteEligibility")
	}
	return &pricingv1.ClonePriceListResponse{
		Result: &commonv1.OperationResult{
			Status: explainOrderAmendmentDecisionReply.GetResult().GetStatus(),
		},
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkPriceQuoteEligibilityReply.GetPriceQuote().GetAudit().GetUpdatedBy(),
			},
			Status:     req.GetPriceList().GetStatus(),
			Parameters: req.GetPriceList().GetParameters(),
		},
	}, nil
}

// priceListServerCreatePriceListSimulateCurrencyRateStep carries the request of one step of CreatePriceList and, once
// it ran, its reply.
type priceListServerCreatePriceListSimulateCurrencyRateStep struct {
	request *pricingv1.SimulateCurrencyRateRequest
	reply   *pricingv1.SimulateCurrencyRateResponse
}

func (s *PriceListServer) runCreatePriceListSimulateCurrencyRate(ctx context.Context, step *priceListServerCreatePriceListSimulateCurrencyRateStep) error {
	reply, err := s.currencyRateCalculationClient.SimulateCurrencyRate(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// orderAmendmentValidationLazy dials acme.orders.v1.OrderAmendmentValidationService the first time a handler needs it.
func (s *PriceListServer) orderAmendmentValidationLazy() ordersv1.OrderAmendmentValidationServiceClient {
	s.orderAmendmentValidationOnce.Do(func() { s.orderAmendmentValidationCached = ordersv1.NewOrderAmendmentValidationServiceClient(s.conn) })
	return s.orderAmendmentValidationCached
}
