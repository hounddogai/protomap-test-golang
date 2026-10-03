package sellers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	audiencesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audiences/v1"
	collectionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// SellerCalculationServer implements `acme.sellers.v1.SellerCalculationService`.
// Computes the figures of a seller, such as totals, taxes, and estimates.
type SellerCalculationServer struct {
	sellersv1.UnimplementedSellerCalculationServiceServer
	store                                *Store
	log                                  *slog.Logger
	conn                                 *grpc.ClientConn
	giftCardValidationClient             giftcardsv1.GiftCardValidationServiceClient
	synonymExportClient                  searchv1.SynonymExportServiceClient
	giftCardBatchValidationClient        giftcardsv1.GiftCardBatchValidationServiceClient
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	audienceQueryOnce                    sync.Once
	audienceQueryCached                  audiencesv1.AudienceQueryServiceClient
	clients                              *registry.Registry
	giftCardQueryClient                  giftcardsv1.GiftCardQueryServiceClient
	giftCardBatchQueryClient             giftcardsv1.GiftCardBatchQueryServiceClient
	searchQueryValidationOnce            sync.Once
	searchQueryValidationCached          searchv1.SearchQueryValidationServiceClient
	synonymExportPort                    synonymExportPort
}

// NewSellerCalculationServer wires a SellerCalculationServer.
func NewSellerCalculationServer(deps platform.Deps) *SellerCalculationServer {
	return &SellerCalculationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.sellers.v1.SellerCalculationService"),
		conn:                                 deps.Conn,
		giftCardValidationClient:             giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		synonymExportClient:                  searchv1.NewSynonymExportServiceClient(deps.Conn),
		giftCardBatchValidationClient:        giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		clients:                              deps.Clients,
		giftCardQueryClient:                  giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
		giftCardBatchQueryClient:             giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		synonymExportPort:                    newSynonymExportPort(deps),
	}
}

// CalculateSeller calculates a seller.
func (s *SellerCalculationServer) CalculateSeller(ctx context.Context, req *sellersv1.CalculateSellerRequest) (*sellersv1.CalculateSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateSeller", "error", err)
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "calculate seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	validateGiftCardRequest := &giftcardsv1.ValidateGiftCardRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ValidateGiftCardRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetSeller().GetContact().GetName()},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Audit:   &commonv1.AuditInfo{Etag: req.GetSeller().GetAudit().GetEtag()},
			Contact: &commonv1.ContactInfo{Email: req.GetSeller().GetContact().GetEmail()},
		},
	}
	validateGiftCardReply := new(giftcardsv1.ValidateGiftCardResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardValidationService_ValidateGiftCard_FullMethodName, validateGiftCardRequest, validateGiftCardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	}
	ctx = tenancy.With(ctx, "email", req.GetSeller().GetContact().GetEmail())
	explainGiftCardDecisionReply, err := s.giftCardValidationClient.ExplainGiftCardDecision(ctx, &giftcardsv1.ExplainGiftCardDecisionRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{
				Iban: validateGiftCardReply.GetGiftCard().GetBank().GetIban(),
			}},
		},
		Details: &giftcardsv1.ExplainGiftCardDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ExplainGiftCardDecision")
	}
	return &sellersv1.CalculateSellerResponse{
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: validateGiftCardReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{
				Email: explainGiftCardDecisionReply.GetGiftCard().GetContact().GetEmail(),
			},
			SellerId: req.GetSellerId(),
			Status:   req.GetSeller().GetStatus(),
		},
		Breakdown: &sellersv1.CalculateSellerResponse_Breakdown{
			CurrencyCode: validateGiftCardReply.GetGiftCard().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: explainGiftCardDecisionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// EstimateSeller estimates a seller.
func (s *SellerCalculationServer) EstimateSeller(ctx context.Context, req *sellersv1.EstimateSellerRequest) (*sellersv1.EstimateSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateSeller", "error", err)
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "estimate seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	attributes := make(map[string]string, len(req.GetSeller().GetAttributes()))
	for key, value := range req.GetSeller().GetAttributes() {
		attributes[key] = value
	}
	streamSynonymExportStream, err := s.synonymExportClient.StreamSynonymExport(ctx, &searchv1.StreamSynonymExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSynonymExportRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetSeller().GetPassport().GetExpiresAt(),
			OwnerId:      req.GetSeller().GetSellerId(),
			Labels:       attributes,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
	}
	for {
		received, err := streamSynonymExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "postal_code", req.GetSeller().GetPrimaryAddress().GetPostalCode())
	scoreGiftCardBatchReply, err := s.giftCardBatchValidationClient.ScoreGiftCardBatch(ctx, &giftcardsv1.ScoreGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit:          &commonv1.AuditInfo{CreatedBy: req.GetSeller().GetAudit().GetCreatedBy()},
			BillingAddress: &commonv1.PostalAddress{PostalCode: tenancy.Value(ctx, "postal_code")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/ScoreGiftCardBatch")
	}
	explainGiftCardTransactionCalculationRequest := buildExplainGiftCardTransactionCalculationRequestForEstimateSeller(req)
	explainGiftCardTransactionCalculationReply, err := s.giftCardTransactionCalculationClient.ExplainGiftCardTransactionCalculation(ctx, explainGiftCardTransactionCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/ExplainGiftCardTransactionCalculation")
	}
	return &sellersv1.EstimateSellerResponse{
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: explainGiftCardTransactionCalculationReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			SellerId:   req.GetSellerId(),
			Attributes: req.GetSeller().GetAttributes(),
		},
		Result: &commonv1.OperationResult{
			Message: explainGiftCardTransactionCalculationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// QuoteSeller quotes a seller.
func (s *SellerCalculationServer) QuoteSeller(ctx context.Context, req *sellersv1.QuoteSellerRequest) (*sellersv1.QuoteSellerResponse, error) {
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "quote seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	resolveAudiencesRequest := &audiencesv1.ResolveAudiencesRequest{Context: req.GetContext()}
	resolveAudiencesReply, err := s.audienceQueryLazy().ResolveAudiences(ctx, resolveAudiencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceQueryService/ResolveAudiences")
	}
	return &sellersv1.QuoteSellerResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveAudiencesReply.GetResult().GetCorrelationId(),
			Details:       resolveAudiencesReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Seller: &sellersv1.Seller{
			SellerId:       req.GetSellerId(),
			PrimaryAddress: req.GetSeller().GetPrimaryAddress(),
		},
	}, nil
}

// CompareSellerOptions compares a seller.
func (s *SellerCalculationServer) CompareSellerOptions(ctx context.Context, req *sellersv1.CompareSellerOptionsRequest) (*sellersv1.CompareSellerOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareSellerOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "compare seller options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	collectionCase, err := registry.Lookup[collectionsv1.CollectionCaseServiceClient](s.clients, "acme.collections.v1.CollectionCaseService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloneCollectionCaseRequest := &collectionsv1.CloneCollectionCaseRequest{
		Context: req.GetContext(),
		CollectionCase: &collectionsv1.CollectionCase{
			Assignee: &identityv1.UserRef{Email: req.GetSeller().GetContact().GetEmail()},
		},
	}
	cloneCollectionCaseReply, err := collectionCase.CloneCollectionCase(ctx, cloneCollectionCaseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.collections.v1.CollectionCaseService/CloneCollectionCase")
	}
	resolveGiftCardsRequest := buildResolveGiftCardsRequestForCompareSellerOptions(req)
	resolveGiftCardsReply, err := s.giftCardQueryClient.ResolveGiftCards(ctx, resolveGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/ResolveGiftCards")
	}
	return &sellersv1.CompareSellerOptionsResponse{
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{
				Email: cloneCollectionCaseReply.GetCollectionCase().GetRequester().GetEmail(),
			},
			SellerId: req.GetSellerId(),
			Name:     req.GetSeller().GetName(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: resolveGiftCardsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ExplainSellerCalculation explains a seller.
func (s *SellerCalculationServer) ExplainSellerCalculation(ctx context.Context, req *sellersv1.ExplainSellerCalculationRequest) (*sellersv1.ExplainSellerCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "explain seller calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeGiftCardBatchesReply, err := s.giftCardBatchQueryClient.SummarizeGiftCardBatches(ctx, &giftcardsv1.SummarizeGiftCardBatchesRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/SummarizeGiftCardBatches")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded sellersv1.ExplainSellerCalculationRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetSeller())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateSearchQueryRequest := &searchv1.ValidateSearchQueryRequest{
		Context: req.GetContext(),
		Details: &searchv1.ValidateSearchQueryRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            (&decoded).GetContact().GetPhone(),
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Email:            req.GetSeller().GetRelationshipManager().GetEmail(),
			},
		},
		SearchQuery: &searchv1.SearchQuery{Detail: packed},
	}
	validateSearchQueryReply, err := s.searchQueryValidationLazy().ValidateSearchQuery(ctx, validateSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ValidateSearchQuery")
	}
	return &sellersv1.ExplainSellerCalculationResponse{
		Outcome: &sellersv1.ExplainSellerCalculationResponse_Outcome{
			Status:    summarizeGiftCardBatchesReply.GetResult().GetStatus(),
			NextSteps: validateSearchQueryReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: validateSearchQueryReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Seller: &sellersv1.Seller{
			SellerId:       req.GetSellerId(),
			PrimaryAddress: req.GetSeller().GetPrimaryAddress(),
		},
	}, nil
}

// GetSellerRates returns a seller.
func (s *SellerCalculationServer) GetSellerRates(ctx context.Context, req *sellersv1.GetSellerRatesRequest) (*sellersv1.GetSellerRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "get_seller_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller rates", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainGiftCardTransactionCalculationRequest := &giftcardsv1.ExplainGiftCardTransactionCalculationRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ExplainGiftCardTransactionCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	type explainGiftCardTransactionCalculationResult struct {
		reply *giftcardsv1.ExplainGiftCardTransactionCalculationResponse
		err   error
	}
	explainGiftCardTransactionCalculationResults := make(chan explainGiftCardTransactionCalculationResult, 1)
	go func() {
		reply, err := s.giftCardTransactionCalculationClient.ExplainGiftCardTransactionCalculation(ctx, explainGiftCardTransactionCalculationRequest)
		explainGiftCardTransactionCalculationResults <- explainGiftCardTransactionCalculationResult{reply, err}
	}()
	var explainGiftCardTransactionCalculationReply *giftcardsv1.ExplainGiftCardTransactionCalculationResponse
	select {
	case result := <-explainGiftCardTransactionCalculationResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardTransactionCalculationService/ExplainGiftCardTransactionCalculation")
		}
		explainGiftCardTransactionCalculationReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	retrySynonymExportRequest := &searchv1.RetrySynonymExportRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: explainGiftCardTransactionCalculationReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
				Email:       req.GetContext().GetActor().GetEmail(),
				UserId:      req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainGiftCardTransactionCalculationReply.GetOutcome().GetEffectiveAt(),
			},
		},
	}
	retrySynonymExportReply, err := s.synonymExportPort.RetrySynonymExport(ctx, retrySynonymExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/RetrySynonymExport")
	}
	return &sellersv1.GetSellerRatesResponse{
		Result: &commonv1.OperationResult{
			Message: explainGiftCardTransactionCalculationReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				Etag: retrySynonymExportReply.GetSynonym().GetAudit().GetEtag(),
			},
			SellerId: req.GetSellerId(),
		},
	}, nil
}

// audienceQueryLazy dials acme.audiences.v1.AudienceQueryService the first time a handler needs it.
func (s *SellerCalculationServer) audienceQueryLazy() audiencesv1.AudienceQueryServiceClient {
	s.audienceQueryOnce.Do(func() { s.audienceQueryCached = audiencesv1.NewAudienceQueryServiceClient(s.conn) })
	return s.audienceQueryCached
}

// searchQueryValidationLazy dials acme.search.v1.SearchQueryValidationService the first time a handler needs it.
func (s *SellerCalculationServer) searchQueryValidationLazy() searchv1.SearchQueryValidationServiceClient {
	s.searchQueryValidationOnce.Do(func() { s.searchQueryValidationCached = searchv1.NewSearchQueryValidationServiceClient(s.conn) })
	return s.searchQueryValidationCached
}
