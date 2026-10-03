package sellers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	treasuryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/treasury/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SellerValidationServer implements `acme.sellers.v1.SellerValidationService`.
// Checks sellers against the rules before anything acts on them.
type SellerValidationServer struct {
	sellersv1.UnimplementedSellerValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	giftCardCalculationPort         giftCardCalculationPort
	clients                         *registry.Registry
	conn                            *grpc.ClientConn
	giftCardClient                  giftcardsv1.GiftCardServiceClient
	giftCardBatchCalculationClient  giftcardsv1.GiftCardBatchCalculationServiceClient
	giftCardQueryClient             giftcardsv1.GiftCardQueryServiceClient
	sellerCalculationClient         sellersv1.SellerCalculationServiceClient
	cashPositionCalculationClient   treasuryv1.CashPositionCalculationServiceClient
	sellerListingCalculationClient  sellersv1.SellerListingCalculationServiceClient
	meteredSellerListingCalculation meteredSellerListingCalculation
	sellerCalculationClientFallback sellersv1.SellerCalculationServiceClient
	apiCredentialHistoryClient      identityv1.ApiCredentialHistoryServiceClient
	searchIndexQueryClient          searchv1.SearchIndexQueryServiceClient
}

// NewSellerValidationServer wires a SellerValidationServer.
func NewSellerValidationServer(deps platform.Deps) SellerValidationServer {
	return SellerValidationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.sellers.v1.SellerValidationService"),
		giftCardCalculationPort:         newGiftCardCalculationPort(deps),
		clients:                         deps.Clients,
		conn:                            deps.Conn,
		giftCardClient:                  giftcardsv1.NewGiftCardServiceClient(deps.Conn),
		giftCardBatchCalculationClient:  giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		giftCardQueryClient:             giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
		sellerCalculationClient:         sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		cashPositionCalculationClient:   treasuryv1.NewCashPositionCalculationServiceClient(deps.Conn),
		sellerListingCalculationClient:  sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		meteredSellerListingCalculation: meteredSellerListingCalculation{SellerListingCalculationServiceClient: sellersv1.NewSellerListingCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerCalculationClientFallback: sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		apiCredentialHistoryClient:      identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		searchIndexQueryClient:          searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
	}
}

// ValidateSeller validates a seller.
func (s SellerValidationServer) ValidateSeller(ctx context.Context, req *sellersv1.ValidateSellerRequest) (*sellersv1.ValidateSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "validate seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	recalculateGiftCardsRequest := &giftcardsv1.RecalculateGiftCardsRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.RecalculateGiftCardsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetSeller().GetPrimaryAddress().GetCountryCode(),
				PostalCode:  req.GetSeller().GetPrimaryAddress().GetPostalCode(),
			},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetSeller().GetContact().GetPreferredChannel(),
			},
		},
	}
	recalculateGiftCardsReply, err := s.giftCardCalculationPort.RecalculateGiftCards(ctx, recalculateGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/RecalculateGiftCards")
	}
	sellerCalculation, ok := s.clients.Get("acme.sellers.v1.SellerCalculationService").(sellersv1.SellerCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerCalculationService")
	}
	calculateSellerRequest := &sellersv1.CalculateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateGiftCardsReply.GetGiftCard().GetAudit().GetCreatedBy(),
				CreatedAt: req.GetSeller().GetAudit().GetCreatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
		},
	}
	calculateSellerReply, err := sellerCalculation.CalculateSeller(ctx, calculateSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/CalculateSeller")
	}
	findGiftCardsRequest := &giftcardsv1.FindGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.FindGiftCardsRequest_Filter{
			Region: calculateSellerReply.GetSeller().GetPrimaryAddress().GetRegion(),
		},
	}
	findGiftCardsReply := new(giftcardsv1.FindGiftCardsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", giftcardsGiftCardQueryServiceName, "FindGiftCards"), findGiftCardsRequest, findGiftCardsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/FindGiftCards")
	}
	return &sellersv1.ValidateSellerResponse{
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateGiftCardsReply.GetGiftCard().GetAudit().GetCreatedBy(),
				CreatedAt: calculateSellerReply.GetSeller().GetAudit().GetCreatedAt(),
			},
			SellerId:       req.GetSellerId(),
			PrimaryAddress: req.GetSeller().GetPrimaryAddress(),
		},
		Result: &commonv1.OperationResult{Status: findGiftCardsReply.GetResult().GetStatus()},
	}, nil
}

// PreviewSeller previews a seller.
func (s SellerValidationServer) PreviewSeller(ctx context.Context, req *sellersv1.PreviewSellerRequest) (*sellersv1.PreviewSellerResponse, error) {
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "preview seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	watchGiftCardsStream, err := s.giftCardClient.WatchGiftCards(ctx, &giftcardsv1.WatchGiftCardsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/WatchGiftCards")
	}
	for {
		received, err := watchGiftCardsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/WatchGiftCards")
		}
		_ = received
	}
	estimateGiftCardBatchRequest := &giftcardsv1.EstimateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetSeller().GetAudit().GetUpdatedAt()},
		},
		Details: &giftcardsv1.EstimateGiftCardBatchRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetSeller().GetPrimaryAddress().GetRegion()},
		},
	}
	type estimateGiftCardBatchResult struct {
		reply *giftcardsv1.EstimateGiftCardBatchResponse
		err   error
	}
	estimateGiftCardBatchResults := make(chan estimateGiftCardBatchResult, 1)
	go func() {
		reply, err := s.giftCardBatchCalculationClient.EstimateGiftCardBatch(ctx, estimateGiftCardBatchRequest)
		estimateGiftCardBatchResults <- estimateGiftCardBatchResult{reply, err}
	}()
	var estimateGiftCardBatchReply *giftcardsv1.EstimateGiftCardBatchResponse
	select {
	case result := <-estimateGiftCardBatchResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardBatchCalculationService/EstimateGiftCardBatch")
		}
		estimateGiftCardBatchReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &sellersv1.PreviewSellerResponse{
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Region: estimateGiftCardBatchReply.GetGiftCardBatch().GetBillingAddress().GetRegion(),
			},
			SellerId: req.GetSellerId(),
			Contact:  req.GetDetails().GetContact(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckSellerEligibility checks a seller.
func (s SellerValidationServer) CheckSellerEligibility(ctx context.Context, req *sellersv1.CheckSellerEligibilityRequest) (*sellersv1.CheckSellerEligibilityResponse, error) {
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "check seller eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	giftCardTransactionCalculation, ok := s.clients.Get("acme.giftcards.v1.GiftCardTransactionCalculationService").(giftcardsv1.GiftCardTransactionCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardTransactionCalculationService")
	}
	calculateGiftCardTransactionRequest := &giftcardsv1.CalculateGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: req.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
		},
		Details: &giftcardsv1.CalculateGiftCardTransactionRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetSeller().GetPrimaryAddress().GetCountryCode(),
			},
		},
	}
	calculateGiftCardTransactionReply, err := giftCardTransactionCalculation.CalculateGiftCardTransaction(ctx, calculateGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/CalculateGiftCardTransaction")
	}
	findGiftCardsRequest := buildFindGiftCardsRequestForCheckSellerEligibility(req)
	findGiftCardsReply, err := s.giftCardQueryClient.FindGiftCards(ctx, findGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/FindGiftCards")
	}
	return &sellersv1.CheckSellerEligibilityResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateGiftCardTransactionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &sellersv1.CheckSellerEligibilityResponse_Outcome{Message: findGiftCardsReply.GetResult().GetMessage()},
		Seller: &sellersv1.Seller{
			SellerId: req.GetSellerId(),
			Name:     req.GetSeller().GetName(),
		},
	}, nil
}

// NormalizeSeller normalizes a seller.
func (s SellerValidationServer) NormalizeSeller(ctx context.Context, req *sellersv1.NormalizeSellerRequest) (*sellersv1.NormalizeSellerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "normalize seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	getSellerRatesRequest := &sellersv1.GetSellerRatesRequest{
		Context:  req.GetContext(),
		SellerId: req.GetSeller().GetSellerId(),
	}
	send := s.getSellerRatesSender(getSellerRatesRequest)
	getSellerRatesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/GetSellerRates")
	}
	var number string
	switch choice := req.GetSeller().GetIdentityDocument().(type) {
	case *sellersv1.Seller_Passport:
		number = choice.Passport.GetNumber()
	case *sellersv1.Seller_NationalId:
		number = choice.NationalId.GetNumber()
	case *sellersv1.Seller_TaxNumber:
		number = choice.TaxNumber
	}
	explainCashPositionCalculationRequest := &treasuryv1.ExplainCashPositionCalculationRequest{
		Context: req.GetContext(),
		Details: &treasuryv1.ExplainCashPositionCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetSeller().GetContact().GetPreferredChannel(),
			},
		},
		CashPosition: &treasuryv1.CashPosition{
			Contact: &commonv1.ContactInfo{
				Email: getSellerRatesReply.GetSeller().GetRelationshipManager().GetEmail(),
			},
			Holder: &treasuryv1.FxHedgeRef{Number: number},
		},
	}
	explainCashPositionCalculationReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*treasuryv1.ExplainCashPositionCalculationResponse, error) {
		return s.cashPositionCalculationClient.ExplainCashPositionCalculation(ctx, explainCashPositionCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.treasury.v1.CashPositionCalculationService/ExplainCashPositionCalculation")
	}
	return &sellersv1.NormalizeSellerResponse{
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: getSellerRatesReply.GetSeller().GetAudit().GetUpdatedBy(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: getSellerRatesReply.GetSeller().GetPrimaryAddress().GetRecipient(),
			},
			SellerId: req.GetSellerId(),
			Name:     req.GetSeller().GetName(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: explainCashPositionCalculationReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ExplainSellerDecision explains a seller.
func (s SellerValidationServer) ExplainSellerDecision(ctx context.Context, req *sellersv1.ExplainSellerDecisionRequest) (*sellersv1.ExplainSellerDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "explain seller decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetSeller().GetPrimaryAddress().GetRegion())
	quoteSellerListingRequest := &sellersv1.QuoteSellerListingRequest{
		Context: req.GetContext(),
		Details: &sellersv1.QuoteSellerListingRequest_Details{
			Destination: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
	}
	quoteSellerListingReply, err := grpcx.Call(ctx, s.sellerListingCalculationClient.QuoteSellerListing, quoteSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/QuoteSellerListing")
	}
	calculateSellerListingRequest := &sellersv1.CalculateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Components: map[string]*sellersv1.SellerListingComponent{"primary": &sellersv1.SellerListingComponent{
				Quantity: quoteSellerListingReply.GetSellerListing().GetComponents()["primary"].GetQuantity(),
			}},
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetSeller().GetAudit().GetUpdatedBy()},
		},
	}
	calculateSellerListingReply, err := s.meteredSellerListingCalculation.CalculateSellerListing(ctx, calculateSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/CalculateSellerListing")
	}
	return &sellersv1.ExplainSellerDecisionResponse{
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: quoteSellerListingReply.GetSellerListing().GetAddress().GetCountryCode(),
				Recipient:   calculateSellerListingReply.GetSellerListing().GetAddress().GetRecipient(),
			},
			RelationshipManager: &identityv1.UserRef{
				UserId: calculateSellerListingReply.GetSellerListing().GetOwner().GetUserId(),
			},
			SellerId: req.GetSellerId(),
			Name:     req.GetSeller().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreSeller scores a seller.
func (s SellerValidationServer) ScoreSeller(ctx context.Context, req *sellersv1.ScoreSellerRequest) (*sellersv1.ScoreSellerResponse, error) {
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "score seller", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *sellersv1.CompareSellerOptionsRequest, ...grpc.CallOption) (*sellersv1.CompareSellerOptionsResponse, error){
		"primary":  s.sellerCalculationClient.CompareSellerOptions,
		"fallback": s.sellerCalculationClientFallback.CompareSellerOptions,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	compareSellerOptionsRequest := &sellersv1.CompareSellerOptionsRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{Etag: req.GetSeller().GetAudit().GetEtag()},
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: req.GetSeller().GetPrimaryAddress().GetPostalCode(),
			},
		},
	}
	compareSellerOptionsReply, err := route(ctx, compareSellerOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/CompareSellerOptions")
	}
	source := req.GetSeller().GetContact().ProtoReflect()
	phone := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("phone"))).String()
	compareApiCredentialVersionsReply, err := s.apiCredentialHistoryClient.CompareApiCredentialVersions(ctx, &identityv1.CompareApiCredentialVersionsRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{
				Phone:            phone,
				PreferredChannel: req.GetSeller().GetContact().GetPreferredChannel(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetSeller().GetAudit().GetUpdatedAt(),
				Etag:      compareSellerOptionsReply.GetSeller().GetAudit().GetEtag(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/CompareApiCredentialVersions")
	}
	return &sellersv1.ScoreSellerResponse{
		Result: &commonv1.OperationResult{
			Details: compareSellerOptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: compareSellerOptionsReply.GetSeller().GetPrimaryAddress().GetLocality(),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: compareApiCredentialVersionsReply.GetApiCredential().GetContact().GetPreferredChannel(),
			},
			SellerId: req.GetSellerId(),
		},
		Breakdown: &sellersv1.ScoreSellerResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// LintSellers lints a seller.
func (s SellerValidationServer) LintSellers(ctx context.Context, req *sellersv1.LintSellersRequest) (*sellersv1.LintSellersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSeller(ctx, req.GetContext().GetTenantId(), req.GetSeller()); err != nil {
		return nil, grpcx.Wrap(err, "save seller")
	}
	s.log.InfoContext(ctx, "lint sellers", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSeller().GetAudit().GetEtag())
	explainSellerCalculationRequest := &sellersv1.ExplainSellerCalculationRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetSeller().GetAudit().GetUpdatedBy()},
		},
	}
	explainSellerCalculationReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*sellersv1.ExplainSellerCalculationResponse, error) {
		return s.sellerCalculationClient.ExplainSellerCalculation(ctx, explainSellerCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation")
	}
	quoteSellerRequest := &sellersv1.QuoteSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			IdentityDocument: &sellersv1.Seller_NationalId{NationalId: &sellersv1.SellerNationalId{
				IssuingCountry: req.GetSeller().GetPassport().GetIssuingCountry(),
				Number:         explainSellerCalculationReply.GetSeller().GetNationalId().GetNumber(),
			}},
			Preferences: &sellersv1.SellerPreferences{
				MarketingOptIn: explainSellerCalculationReply.GetSeller().GetPreferences().GetMarketingOptIn(),
			},
		},
	}
	type quoteSellerResult struct {
		reply *sellersv1.QuoteSellerResponse
		err   error
	}
	quoteSellerResults := make(chan quoteSellerResult, 1)
	go func() {
		reply, err := s.sellerCalculationClient.QuoteSeller(ctx, quoteSellerRequest)
		quoteSellerResults <- quoteSellerResult{reply, err}
	}()
	var quoteSellerReply *sellersv1.QuoteSellerResponse
	select {
	case result := <-quoteSellerResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
		}
		quoteSellerReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	previewGiftCardRequest := &giftcardsv1.PreviewGiftCardRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.PreviewGiftCardRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetSeller().GetRelationshipManager().GetEmail()},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainSellerCalculationReply.GetSeller().GetAudit().GetUpdatedAt(),
			},
		},
	}
	previewGiftCardReply := new(giftcardsv1.PreviewGiftCardResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.giftcards.v1.GiftCardValidationService/PreviewGiftCard", previewGiftCardRequest, previewGiftCardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/PreviewGiftCard")
	}
	sellerCalculation, err := registry.Lookup[sellersv1.SellerCalculationServiceClient](s.clients, "acme.sellers.v1.SellerCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	getSellerRatesRequest := &sellersv1.GetSellerRatesRequest{
		Context:  req.GetContext(),
		SellerId: req.GetSellerId(),
	}
	getSellerRatesReply, err := sellerCalculation.GetSellerRates(ctx, getSellerRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/GetSellerRates")
	}
	_ = getSellerRatesReply
	return &sellersv1.LintSellersResponse{
		Result: &commonv1.OperationResult{
			Details: explainSellerCalculationReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Seller: &sellersv1.Seller{
			PrimaryAddress: quoteSellerReply.GetSeller().GetPrimaryAddress(),
			Contact:        previewGiftCardReply.GetGiftCard().GetContact(),
			SellerId:       req.GetSellerId(),
			Name:           req.GetSeller().GetName(),
		},
	}, nil
}

// GetSellerRules returns a seller.
func (s SellerValidationServer) GetSellerRules(ctx context.Context, req *sellersv1.GetSellerRulesRequest) (*sellersv1.GetSellerRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller", req.GetSellerId(), "get_seller_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller rules", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	findSearchIndexesRequest := &searchv1.FindSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.FindSearchIndexesRequest_Filter{
			CreatedAfter: req.GetContext().GetDeadline(),
			OwnerId:      req.GetSellerId(),
			Labels:       baggage,
		},
	}
	findSearchIndexesReply, err := grpcx.Call(ctx, s.searchIndexQueryClient.FindSearchIndexes, findSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/FindSearchIndexes")
	}
	return &sellersv1.GetSellerRulesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: findSearchIndexesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Seller: &sellersv1.Seller{SellerId: req.GetSellerId()},
	}, nil
}

// getSellerRatesSender returns a function that sends one request to
// acme.sellers.v1.SellerCalculationService/GetSellerRates.
func (s SellerValidationServer) getSellerRatesSender(request *sellersv1.GetSellerRatesRequest) func(context.Context) (*sellersv1.GetSellerRatesResponse, error) {
	return func(ctx context.Context) (*sellersv1.GetSellerRatesResponse, error) {
		return s.sellerCalculationClient.GetSellerRates(ctx, request)
	}
}
