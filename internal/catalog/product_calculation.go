package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ProductCalculationServer implements `acme.catalog.v1.ProductCalculationService`.
// Computes the figures of a product, such as totals, taxes, and estimates.
type ProductCalculationServer struct {
	catalogv1.UnimplementedProductCalculationServiceServer
	store                                *Store
	log                                  *slog.Logger
	featureFlagValidationClient          featureflagsv1.FeatureFlagValidationServiceClient
	conn                                 *grpc.ClientConn
	cartRuleValidationClient             cartv1.CartRuleValidationServiceClient
	reviewModerationQueryClient          reviewsv1.ReviewModerationQueryServiceClient
	compareSynonymVersionsFn             func(context.Context, *searchv1.CompareSynonymVersionsRequest, ...grpc.CallOption) (*searchv1.CompareSynonymVersionsResponse, error)
	checkUserEligibilityFn               func(context.Context, *identityv1.CheckUserEligibilityRequest, ...grpc.CallOption) (*identityv1.CheckUserEligibilityResponse, error)
	meteredCartCalculation               meteredCartCalculation
	returnRequestCalculationClient       returnsv1.ReturnRequestCalculationServiceClient
	cartRuleValidationPort               cartRuleValidationPort
	productReviewClient                  reviewsv1.ProductReviewServiceClient
	meteredFuelCard                      meteredFuelCard
	sessionNotificationClient            identityv1.SessionNotificationServiceClient
	bus                                  *eventbus.Bus
	clients                              *registry.Registry
	searchIndexHistoryClient             searchv1.SearchIndexHistoryServiceClient
	riskAssessmentQueryClient            fraudv1.RiskAssessmentQueryServiceClient
	metricSeriesValidationClient         metricsv1.MetricSeriesValidationServiceClient
	metricSeriesValidationClientFallback metricsv1.MetricSeriesValidationServiceClient
	ledgerAccountClient                  ledgerv1.LedgerAccountServiceClient
}

// NewProductCalculationServer wires a ProductCalculationServer.
func NewProductCalculationServer(deps platform.Deps) *ProductCalculationServer {
	synonymHistory := searchv1.NewSynonymHistoryServiceClient(deps.Conn)
	userValidation := identityv1.NewUserValidationServiceClient(deps.Conn)
	return &ProductCalculationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.catalog.v1.ProductCalculationService"),
		featureFlagValidationClient:          featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		cartRuleValidationClient:             cartv1.NewCartRuleValidationServiceClient(deps.Conn),
		reviewModerationQueryClient:          reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn),
		compareSynonymVersionsFn:             synonymHistory.CompareSynonymVersions,
		checkUserEligibilityFn:               userValidation.CheckUserEligibility,
		meteredCartCalculation:               meteredCartCalculation{CartCalculationServiceClient: cartv1.NewCartCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		returnRequestCalculationClient:       returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn),
		cartRuleValidationPort:               newCartRuleValidationPort(deps),
		productReviewClient:                  reviewsv1.NewProductReviewServiceClient(deps.Conn),
		meteredFuelCard:                      meteredFuelCard{FuelCardServiceClient: fuelv1.NewFuelCardServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sessionNotificationClient:            identityv1.NewSessionNotificationServiceClient(deps.Conn),
		bus:                                  deps.Bus,
		clients:                              deps.Clients,
		searchIndexHistoryClient:             searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		riskAssessmentQueryClient:            fraudv1.NewRiskAssessmentQueryServiceClient(deps.Conn),
		metricSeriesValidationClient:         metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		metricSeriesValidationClientFallback: metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		ledgerAccountClient:                  ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
	}
}

// CalculateProduct calculates a product.
func (s *ProductCalculationServer) CalculateProduct(ctx context.Context, req *catalogv1.CalculateProductRequest) (*catalogv1.CalculateProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateProduct", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "calculate product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetProduct()).(*catalogv1.Product)
	checkFeatureFlagEligibilityRequest := &featureflagsv1.CheckFeatureFlagEligibilityRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{UpdatedAt: cloned.GetAudit().GetUpdatedAt()},
		},
	}
	type checkFeatureFlagEligibilityResult struct {
		reply *featureflagsv1.CheckFeatureFlagEligibilityResponse
		err   error
	}
	checkFeatureFlagEligibilityResults := make(chan checkFeatureFlagEligibilityResult, 1)
	go func() {
		reply, err := s.featureFlagValidationClient.CheckFeatureFlagEligibility(ctx, checkFeatureFlagEligibilityRequest)
		checkFeatureFlagEligibilityResults <- checkFeatureFlagEligibilityResult{reply, err}
	}()
	var checkFeatureFlagEligibilityReply *featureflagsv1.CheckFeatureFlagEligibilityResponse
	select {
	case result := <-checkFeatureFlagEligibilityResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.featureflags.v1.FeatureFlagValidationService/CheckFeatureFlagEligibility")
		}
		checkFeatureFlagEligibilityReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	peekUserRequest := &identityv1.PeekUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetProduct().GetOwner().GetUserId(),
	}
	peekUserReply := new(identityv1.PeekUserResponse)
	if err := s.conn.Invoke(ctx, identityUserQueryServicePeekUserMethod, peekUserRequest, peekUserReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/PeekUser")
	}
	var holder struct{ Region string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	browseRiskAssessmentsRequest := &fraudv1.BrowseRiskAssessmentsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.BrowseRiskAssessmentsRequest_Filter{
			Region:       holder.Region,
			CreatedAfter: peekUserReply.GetUser().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetProduct().GetComponents()["primary"].GetComponentId(),
		},
	}
	browseRiskAssessmentsReply := new(fraudv1.BrowseRiskAssessmentsResponse)
	if err := grpc.Invoke(ctx, "/acme.fraud.v1.RiskAssessmentQueryService/BrowseRiskAssessments", browseRiskAssessmentsRequest, browseRiskAssessmentsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentQueryService/BrowseRiskAssessments")
	}
	previewCartRuleRequest := &cartv1.PreviewCartRuleRequest{
		Context: req.GetContext(),
		Details: &cartv1.PreviewCartRuleRequest_Details{
			Contact: &commonv1.ContactInfo{Email: peekUserReply.GetUser().GetContact().GetEmail()},
		},
	}
	previewCartRuleReply, err := cartv1.CartRuleValidationServiceClient.PreviewCartRule(s.cartRuleValidationClient, ctx, previewCartRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/PreviewCartRule")
	}
	return &catalogv1.CalculateProductResponse{
		Result: &commonv1.OperationResult{
			Message:      checkFeatureFlagEligibilityReply.GetResult().GetMessage(),
			Status:       peekUserReply.GetResult().GetStatus(),
			ReferenceIds: browseRiskAssessmentsReply.GetResult().GetReferenceIds(),
		},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				Email: checkFeatureFlagEligibilityReply.GetFeatureFlag().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      peekUserReply.GetUser().GetAudit().GetEtag(),
				UpdatedAt: previewCartRuleReply.GetCartRule().GetAudit().GetUpdatedAt(),
			},
			ProductId: req.GetProductId(),
			Code:      req.GetProduct().GetCode(),
		},
	}, nil
}

// EstimateProduct estimates a product.
func (s *ProductCalculationServer) EstimateProduct(ctx context.Context, req *catalogv1.EstimateProductRequest) (*catalogv1.EstimateProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "estimate product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	specs := make(map[string]string, len(req.GetProduct().GetSpecs()))
	for key2, value2 := range req.GetProduct().GetSpecs() {
		specs[key2] = value2
	}
	summarizeReviewModerationsReply, err := s.reviewModerationQueryClient.SummarizeReviewModerations(ctx, &reviewsv1.SummarizeReviewModerationsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &reviewsv1.SummarizeReviewModerationsRequest_Details{Filters: specs},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationQueryService/SummarizeReviewModerations")
	}
	lookupUserRequest := &identityv1.LookupUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetProduct().GetOwner().GetUserId(),
	}
	lookupUserReply, err := grpcx.NewClient(s.conn, identityv1.NewUserQueryServiceClient).LookupUser(ctx, lookupUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/LookupUser")
	}
	return &catalogv1.EstimateProductResponse{
		Result: &commonv1.OperationResult{
			Status: summarizeReviewModerationsReply.GetResult().GetStatus(),
		},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				UserId: lookupUserReply.GetUser().GetRelationshipManager().GetUserId(),
			},
			ProductId: req.GetProductId(),
			Specs:     req.GetProduct().GetSpecs(),
		},
	}, nil
}

// QuoteProduct quotes a product.
func (s *ProductCalculationServer) QuoteProduct(ctx context.Context, req *catalogv1.QuoteProductRequest) (*catalogv1.QuoteProductResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteProduct", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "quote product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetProduct())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareSynonymVersionsRequest := &searchv1.CompareSynonymVersionsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				UserId: req.GetProduct().GetOwner().GetUserId(),
				Email:  req.GetProduct().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetProduct().GetAudit().GetCreatedAt(),
				UpdatedAt: req.GetProduct().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &searchv1.CompareSynonymVersionsRequest_Details{Snapshot: snapshot},
	}
	compareSynonymVersionsReply, err := s.compareSynonymVersionsFn(ctx, compareSynonymVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/CompareSynonymVersions")
	}
	checkUserEligibilityRequest := &identityv1.CheckUserEligibilityRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy()},
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
			RelationshipManager: &identityv1.UserRef{
				UserId: compareSynonymVersionsReply.GetSynonym().GetOwner().GetUserId(),
			},
		},
	}
	checkUserEligibilityReply, err := s.checkUserEligibilityFn(ctx, checkUserEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	return &catalogv1.QuoteProductResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: compareSynonymVersionsReply.GetSynonym().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				Email: checkUserEligibilityReply.GetUser().GetContact().GetEmail(),
			},
			ProductId: req.GetProductId(),
			Specs:     req.GetProduct().GetSpecs(),
		},
		Breakdown: &catalogv1.QuoteProductResponse_Breakdown{
			Lines: []*catalogv1.QuoteProductResponse_Breakdown_Line{&catalogv1.QuoteProductResponse_Breakdown_Line{
				Amount: compareSynonymVersionsReply.GetSynonym().GetDiscount().GetAmount(),
			}},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareProductOptions compares a product.
func (s *ProductCalculationServer) CompareProductOptions(ctx context.Context, req *catalogv1.CompareProductOptionsRequest) (*catalogv1.CompareProductOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareProductOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "compare product options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	var postalCode string
	switch choice := req.GetProduct().GetLocator().(type) {
	case *catalogv1.Product_Address:
		postalCode = choice.Address.GetPostalCode()
	case *catalogv1.Product_FacilityCode:
		postalCode = choice.FacilityCode
	}
	quoteCartRequest := &cartv1.QuoteCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
				Lines:      req.GetProduct().GetAddress().GetLines(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetProduct().GetAudit().GetCreatedBy()},
		},
		Details: &cartv1.QuoteCartRequest_Details{
			Destination: &commonv1.PostalAddress{PostalCode: postalCode},
		},
	}
	quoteCartReply, err := s.meteredCartCalculation.QuoteCart(ctx, quoteCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/QuoteCart")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := productCalculationServerSimulateReturnRequestSteps[mode]
	simulateReturnRequestRequest := &returnsv1.SimulateReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Settlement: &returnsv1.ReturnRequest_Bank{Bank: &returnsv1.ReturnRequestBankSettlement{Bic: quoteCartReply.GetCart().GetBank().GetBic()}},
			Audit:      &commonv1.AuditInfo{Etag: req.GetProduct().GetAudit().GetEtag()},
		},
	}
	simulateReturnRequestReply, err := run(s, ctx, simulateReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestCalculationService/SimulateReturnRequest")
	}
	normalizeCartRuleRequest := &cartv1.NormalizeCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateReturnRequestReply.GetReturnRequest().GetAudit().GetCreatedAt(),
			},
		},
	}
	normalizeCartRuleReply, err := s.cartRuleValidationPort.NormalizeCartRule(ctx, normalizeCartRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/NormalizeCartRule")
	}
	watchProductReviewsStream, err := s.productReviewClient.WatchProductReviews(ctx, &reviewsv1.WatchProductReviewsRequest{
		Context: req.GetContext(),
		Filter:  &reviewsv1.WatchProductReviewsRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/WatchProductReviews")
	}
	for {
		received, err := watchProductReviewsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewService/WatchProductReviews")
		}
		_ = received
	}
	return &catalogv1.CompareProductOptionsResponse{
		Result: &commonv1.OperationResult{Status: quoteCartReply.GetResult().GetStatus()},
		Breakdown: &catalogv1.CompareProductOptionsResponse_Breakdown{
			CurrencyCode: quoteCartReply.GetBreakdown().GetCurrencyCode(),
		},
		Product: &catalogv1.Product{
			Owner: &identityv1.UserRef{
				Email: simulateReturnRequestReply.GetReturnRequest().GetCounterparty().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      simulateReturnRequestReply.GetReturnRequest().GetAudit().GetEtag(),
				UpdatedAt: normalizeCartRuleReply.GetCartRule().GetAudit().GetUpdatedAt(),
			},
			ProductId: req.GetProductId(),
			Specs:     req.GetProduct().GetSpecs(),
		},
	}, nil
}

// RecalculateProducts recalculates a product.
func (s *ProductCalculationServer) RecalculateProducts(ctx context.Context, req *catalogv1.RecalculateProductsRequest) (*catalogv1.RecalculateProductsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateProducts", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "recalculate products", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	cloneFuelCardRequest := &fuelv1.CloneFuelCardRequest{
		Context: req.GetContext(),
		FuelCard: &fuelv1.FuelCard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetProduct().GetAudit().GetCreatedBy(),
				Etag:      req.GetProduct().GetAudit().GetEtag(),
				CreatedAt: req.GetProduct().GetAudit().GetCreatedAt(),
				UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy(),
			},
		},
	}
	cloneFuelCardReply, err := s.meteredFuelCard.CloneFuelCard(ctx, cloneFuelCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardService/CloneFuelCard")
	}
	notifySessionWatchersReply, err := s.sessionNotificationClient.NotifySessionWatchers(ctx, &identityv1.NotifySessionWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifySessionWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            cloneFuelCardReply.GetFuelCard().GetContact().GetEmail(),
				PreferredChannel: cloneFuelCardReply.GetFuelCard().GetContact().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/NotifySessionWatchers")
	}
	return &catalogv1.RecalculateProductsResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cloneFuelCardReply.GetFuelCard().GetAudit().GetCreatedBy(),
			},
			ProductId: req.GetProductId(),
			Status:    req.GetProduct().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details: notifySessionWatchersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// SimulateProduct simulates a product.
func (s *ProductCalculationServer) SimulateProduct(ctx context.Context, req *catalogv1.SimulateProductRequest) (*catalogv1.SimulateProductResponse, error) {
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "simulate product", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "catalog.simulate_product.list_search_index_versions",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     req.GetDetails().GetDestination().GetRegion(),
			"filter.owner_id":   req.GetProduct().GetOwner().GetUserId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	cartRuleValidation, ok := s.clients.Get("acme.cart.v1.CartRuleValidationService").(cartv1.CartRuleValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.cart.v1.CartRuleValidationService")
	}
	checkCartRuleEligibilityRequest := &cartv1.CheckCartRuleEligibilityRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{UserId: req.GetProduct().GetOwner().GetUserId()},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetProduct().GetAudit().GetCreatedAt()},
		},
		Details: &cartv1.CheckCartRuleEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetProduct().GetOwner().GetEmail()},
		},
	}
	checkCartRuleEligibilityReply, err := cartRuleValidation.CheckCartRuleEligibility(ctx, checkCartRuleEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/CheckCartRuleEligibility")
	}
	annotateSearchIndexVersionRequest := buildAnnotateSearchIndexVersionRequestForSimulateProduct(req, checkCartRuleEligibilityReply)
	annotateSearchIndexVersionReply, err := s.searchIndexHistoryClient.AnnotateSearchIndexVersion(ctx, annotateSearchIndexVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/AnnotateSearchIndexVersion")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeRiskAssessmentsRequest := &fraudv1.SummarizeRiskAssessmentsRequest{
		Context:     req.GetContext(),
		Reason:      checkCartRuleEligibilityReply.GetCartRule().GetBlock().GetReason(),
		Annotations: annotations,
		Details: &fraudv1.SummarizeRiskAssessmentsRequest_Details{
			Period: checkCartRuleEligibilityReply.GetCartRule().GetEffective(),
		},
	}
	summarizeRiskAssessmentsReply, err := grpcx.Call(ctx, s.riskAssessmentQueryClient.SummarizeRiskAssessments, summarizeRiskAssessmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentQueryService/SummarizeRiskAssessments")
	}
	return &catalogv1.SimulateProductResponse{
		Breakdown: &catalogv1.SimulateProductResponse_Breakdown{
			Lines: []*catalogv1.SimulateProductResponse_Breakdown_Line{&catalogv1.SimulateProductResponse_Breakdown_Line{
				Amount: checkCartRuleEligibilityReply.GetCartRule().GetDiscount().GetAmount(),
			}},
		},
		Product: &catalogv1.Product{
			Tags: annotateSearchIndexVersionReply.GetSearchIndex().GetTags(),
			Components: map[string]*catalogv1.ProductComponent{"primary": &catalogv1.ProductComponent{
				Description: annotateSearchIndexVersionReply.GetSearchIndex().GetComponents()["primary"].GetDescription(),
			}},
			ProductId: req.GetProductId(),
			Status:    req.GetProduct().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: summarizeRiskAssessmentsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ExplainProductCalculation explains a product.
func (s *ProductCalculationServer) ExplainProductCalculation(ctx context.Context, req *catalogv1.ExplainProductCalculationRequest) (*catalogv1.ExplainProductCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainProductCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveProduct(ctx, req.GetContext().GetTenantId(), req.GetProduct()); err != nil {
		return nil, grpcx.Wrap(err, "save product")
	}
	s.log.InfoContext(ctx, "explain product calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetProduct().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *metricsv1.NormalizeMetricSeriesRequest, ...grpc.CallOption) (*metricsv1.NormalizeMetricSeriesResponse, error){
		"primary":  s.metricSeriesValidationClient.NormalizeMetricSeries,
		"fallback": s.metricSeriesValidationClientFallback.NormalizeMetricSeries,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded catalogv1.ExplainProductCalculationRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	normalizeMetricSeriesRequest := &metricsv1.NormalizeMetricSeriesRequest{
		Context: req.GetContext(),
		Details: &metricsv1.NormalizeMetricSeriesRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: (&decoded).GetContact().GetPreferredChannel(),
				Phone:            req.GetDetails().GetContact().GetPhone(),
			},
		},
		MetricSeries: &metricsv1.MetricSeries{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetProduct().GetAudit().GetUpdatedBy(),
				Etag:      req.GetProduct().GetAudit().GetEtag(),
			},
		},
	}
	normalizeMetricSeriesReply, err := route(ctx, normalizeMetricSeriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/NormalizeMetricSeries")
	}
	var region string
	switch choice := req.GetProduct().GetLocator().(type) {
	case *catalogv1.Product_Address:
		region = choice.Address.GetRegion()
	case *catalogv1.Product_FacilityCode:
		region = choice.FacilityCode
	}
	suggestRiskAssessmentsReply, err := s.riskAssessmentQueryClient.SuggestRiskAssessments(ctx, &fraudv1.SuggestRiskAssessmentsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.SuggestRiskAssessmentsRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetProduct().GetAudit().GetUpdatedAt(),
			OwnerId:      normalizeMetricSeriesReply.GetMetricSeries().GetComponents()["primary"].GetComponentId(),
			Labels:       normalizeMetricSeriesReply.GetMetricSeries().GetSpecs(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentQueryService/SuggestRiskAssessments")
	}
	lockTenantRequest := &identityv1.LockTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.LockTenantRequest_Details{
			NewOwner: &identityv1.UserRef{
				Email:  normalizeMetricSeriesReply.GetMetricSeries().GetOwner().GetEmail(),
				UserId: normalizeMetricSeriesReply.GetMetricSeries().GetOwner().GetUserId(),
			},
		},
	}
	lockTenantReply, err := lockTenantCall(ctx, lockTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/LockTenant")
	}
	return &catalogv1.ExplainProductCalculationResponse{
		Product: &catalogv1.Product{
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizeMetricSeriesReply.GetMetricSeries().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId: lockTenantReply.GetTenant().GetManager().GetUserId(),
			},
			ProductId: req.GetProductId(),
			Code:      req.GetProduct().GetCode(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: normalizeMetricSeriesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &catalogv1.ExplainProductCalculationResponse_Outcome{
			Message: suggestRiskAssessmentsReply.GetResult().GetMessage(),
		},
	}, nil
}

// GetProductRates returns a product.
func (s *ProductCalculationServer) GetProductRates(ctx context.Context, req *catalogv1.GetProductRatesRequest) (*catalogv1.GetProductRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "product", req.GetProductId(), "get_product_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get product rates", "tenant", req.GetContext().GetTenantId())
	searchLedgerAccountsRequest := &ledgerv1.SearchLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter:  &ledgerv1.SearchLedgerAccountsRequest_Filter{OwnerId: req.GetProductId()},
	}
	searchLedgerAccountsReply, err := grpcx.Call(ctx, s.ledgerAccountClient.SearchLedgerAccounts, searchLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	return &catalogv1.GetProductRatesResponse{
		Result: &commonv1.OperationResult{
			Status:  searchLedgerAccountsReply.GetResult().GetStatus(),
			Details: searchLedgerAccountsReply.GetResult().GetDetails(),
		},
		Product: &catalogv1.Product{ProductId: req.GetProductId()},
	}, nil
}

// productCalculationServerSimulateReturnRequestSteps are the ways to run
// acme.returns.v1.ReturnRequestCalculationService/SimulateReturnRequest, by name.
var productCalculationServerSimulateReturnRequestSteps = map[string]func(*ProductCalculationServer, context.Context, *returnsv1.SimulateReturnRequestRequest) (*returnsv1.SimulateReturnRequestResponse, error){
	"default": (*ProductCalculationServer).simulateReturnRequestStep,
	"audited": (*ProductCalculationServer).simulateReturnRequestAuditedStep,
}

func (s *ProductCalculationServer) simulateReturnRequestStep(ctx context.Context, request *returnsv1.SimulateReturnRequestRequest) (*returnsv1.SimulateReturnRequestResponse, error) {
	return s.returnRequestCalculationClient.SimulateReturnRequest(ctx, request)
}

func (s *ProductCalculationServer) simulateReturnRequestAuditedStep(ctx context.Context, request *returnsv1.SimulateReturnRequestRequest) (*returnsv1.SimulateReturnRequestResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.returns.v1.ReturnRequestCalculationService/SimulateReturnRequest")
	return s.returnRequestCalculationClient.SimulateReturnRequest(ctx, request)
}
