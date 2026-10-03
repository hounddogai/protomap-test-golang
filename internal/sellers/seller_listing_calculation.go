package sellers

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	labelingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/labeling/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SellerListingCalculationServer implements `acme.sellers.v1.SellerListingCalculationService`.
// Computes the figures of a seller listing, such as totals, taxes, and estimates.
type SellerListingCalculationServer struct {
	sellersv1.UnimplementedSellerListingCalculationServiceServer
	store                         *Store
	log                           *slog.Logger
	sellerCalculationClient       sellersv1.SellerCalculationServiceClient
	clients                       *registry.Registry
	conn                          *grpc.ClientConn
	ledgerAccountQueryOnce        sync.Once
	ledgerAccountQueryCached      ledgerv1.LedgerAccountQueryServiceClient
	featureFlagValidationClient   featureflagsv1.FeatureFlagValidationServiceClient
	giftCardValidationClient      giftcardsv1.GiftCardValidationServiceClient
	labelingTaskCalculationClient labelingv1.LabelingTaskCalculationServiceClient
	quoteGiftCardBatchFn          func(context.Context, *giftcardsv1.QuoteGiftCardBatchRequest, ...grpc.CallOption) (*giftcardsv1.QuoteGiftCardBatchResponse, error)
	tenantExportClient            identityv1.TenantExportServiceClient
	sellerCalculationOnce         sync.Once
	sellerCalculationCached       sellersv1.SellerCalculationServiceClient
}

// NewSellerListingCalculationServer wires a SellerListingCalculationServer.
func NewSellerListingCalculationServer(deps platform.Deps) *SellerListingCalculationServer {
	giftCardBatchCalculation := giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn)
	return &SellerListingCalculationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.sellers.v1.SellerListingCalculationService"),
		sellerCalculationClient:       sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		clients:                       deps.Clients,
		conn:                          deps.Conn,
		featureFlagValidationClient:   featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		giftCardValidationClient:      giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		labelingTaskCalculationClient: labelingv1.NewLabelingTaskCalculationServiceClient(deps.Conn),
		quoteGiftCardBatchFn:          giftCardBatchCalculation.QuoteGiftCardBatch,
		tenantExportClient:            identityv1.NewTenantExportServiceClient(deps.Conn),
	}
}

// CalculateSellerListing calculates a seller listing.
func (s *SellerListingCalculationServer) CalculateSellerListing(ctx context.Context, req *sellersv1.CalculateSellerListingRequest) (*sellersv1.CalculateSellerListingResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "calculate seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "postal_code", req.GetDetails().GetDestination().GetPostalCode())
	calculateSellerRequest := &sellersv1.CalculateSellerRequest{}
	calculateSellerRequest.Context = req.GetContext()
	calculateSellerRequest.Details = &sellersv1.CalculateSellerRequest_Details{
		Destination: &commonv1.PostalAddress{
			Locality:   req.GetDetails().GetDestination().GetLocality(),
			Region:     req.GetDetails().GetDestination().GetRegion(),
			PostalCode: tenancy.Value(ctx, "postal_code"),
		},
	}
	calculateSellerRequest.Seller = &sellersv1.Seller{
		PrimaryAddress: &commonv1.PostalAddress{Locality: req.GetSellerListing().GetAddress().GetLocality()},
	}
	calculateSellerReply, err := s.sellerCalculationClient.CalculateSeller(ctx, calculateSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/CalculateSeller")
	}
	sellerCalculation, ok := s.clients.Get("acme.sellers.v1.SellerCalculationService").(sellersv1.SellerCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerCalculationService")
	}
	estimateSellerRequest := &sellersv1.EstimateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetSellerListing().GetOwner().GetDisplayName(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Locality: req.GetSellerListing().GetAddress().GetLocality()},
		},
	}
	estimateSellerReply, err := sellerCalculation.EstimateSeller(ctx, estimateSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/EstimateSeller")
	}
	return &sellersv1.CalculateSellerListingResponse{
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateSellerReply.GetSeller().GetAudit().GetCreatedAt(),
			},
			SellerListingId: req.GetSellerListingId(),
			Capacity:        req.GetSellerListing().GetCapacity(),
		},
		Breakdown: &sellersv1.CalculateSellerListingResponse_Breakdown{
			CurrencyCode: estimateSellerReply.GetBreakdown().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateSellerListing estimates a seller listing.
func (s *SellerListingCalculationServer) EstimateSellerListing(ctx context.Context, req *sellersv1.EstimateSellerListingRequest) (*sellersv1.EstimateSellerListingResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "estimate seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	resolveLedgerAccountsRequest := &ledgerv1.ResolveLedgerAccountsRequest{Context: req.GetContext()}
	resolveLedgerAccountsReply, err := s.ledgerAccountQueryLazy().ResolveLedgerAccounts(ctx, resolveLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ResolveLedgerAccounts")
	}
	explainSellerCalculationRequest := &sellersv1.ExplainSellerCalculationRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress:      &commonv1.PostalAddress{Lines: req.GetSellerListing().GetAddress().GetLines()},
			RelationshipManager: &identityv1.UserRef{Email: req.GetSellerListing().GetOwner().GetEmail()},
		},
	}
	explainSellerCalculationReply := new(sellersv1.ExplainSellerCalculationResponse)
	if err := grpcx.Invoke(ctx, s.conn, sellersv1.SellerCalculationService_ExplainSellerCalculation_FullMethodName, explainSellerCalculationRequest, explainSellerCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation")
	}
	scoreFeatureFlagRequest := &featureflagsv1.ScoreFeatureFlagRequest{
		Context: req.GetContext(),
		Details: &featureflagsv1.ScoreFeatureFlagRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:   req.GetDetails().GetDestination().GetRegion(),
				Locality: explainSellerCalculationReply.GetSeller().GetPrimaryAddress().GetLocality(),
			},
		},
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetSellerListing().GetAudit().GetCreatedBy()},
		},
	}
	scoreFeatureFlagReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*featureflagsv1.ScoreFeatureFlagResponse, error) {
		return s.featureFlagValidationClient.ScoreFeatureFlag(ctx, scoreFeatureFlagRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag")
	}
	return &sellersv1.EstimateSellerListingResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveLedgerAccountsReply.GetResult().GetCorrelationId(),
			Details:       resolveLedgerAccountsReply.GetResult().GetDetails(),
			Message:       explainSellerCalculationReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email: explainSellerCalculationReply.GetSeller().GetRelationshipManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreFeatureFlagReply.GetFeatureFlag().GetAudit().GetCreatedBy(),
			},
			SellerListingId: req.GetSellerListingId(),
		},
		Breakdown: &sellersv1.EstimateSellerListingResponse_Breakdown{
			Lines: []*sellersv1.EstimateSellerListingResponse_Breakdown_Line{&sellersv1.EstimateSellerListingResponse_Breakdown_Line{
				Amount: scoreFeatureFlagReply.GetFeatureFlag().GetDiscount().GetAmount(),
			}},
			CurrencyCode: req.GetDetails().GetCurrencyCode(),
		},
	}, nil
}

// QuoteSellerListing quotes a seller listing.
func (s *SellerListingCalculationServer) QuoteSellerListing(ctx context.Context, req *sellersv1.QuoteSellerListingRequest) (*sellersv1.QuoteSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "quote seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	checkGiftCardEligibilityRequest := &giftcardsv1.CheckGiftCardEligibilityRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CheckGiftCardEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetSellerListing().GetOwner().GetEmail()},
		},
	}
	checkGiftCardEligibilityReply, err := grpcx.Call(ctx, s.giftCardValidationClient.CheckGiftCardEligibility, checkGiftCardEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/CheckGiftCardEligibility")
	}
	return &sellersv1.QuoteSellerListingResponse{
		Result: &commonv1.OperationResult{
			Message: checkGiftCardEligibilityReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkGiftCardEligibilityReply.GetGiftCard().GetAudit().GetUpdatedBy(),
			},
			SellerListingId: req.GetSellerListingId(),
			Code:            req.GetSellerListing().GetCode(),
		},
	}, nil
}

// CompareSellerListingOptions compares a seller listing.
func (s *SellerListingCalculationServer) CompareSellerListingOptions(ctx context.Context, req *sellersv1.CompareSellerListingOptionsRequest) (*sellersv1.CompareSellerListingOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareSellerListingOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "compare seller listing options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	explainLabelingTaskCalculationRequest := buildExplainLabelingTaskCalculationRequestForCompareSellerListingOptions(req)
	explainLabelingTaskCalculationReply, err := s.labelingTaskCalculationClient.ExplainLabelingTaskCalculation(ctx, explainLabelingTaskCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.labeling.v1.LabelingTaskCalculationService/ExplainLabelingTaskCalculation")
	}
	var postalCode string
	switch choice := req.GetSellerListing().GetLocator().(type) {
	case *sellersv1.SellerListing_Address:
		postalCode = choice.Address.GetPostalCode()
	case *sellersv1.SellerListing_FacilityCode:
		postalCode = choice.FacilityCode
	}
	quoteGiftCardBatchRequest := &giftcardsv1.QuoteGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				Etag: explainLabelingTaskCalculationReply.GetLabelingTask().GetAudit().GetEtag(),
			},
		},
		Details: &giftcardsv1.QuoteGiftCardBatchRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: postalCode,
				Lines:      req.GetSellerListing().GetAddress().GetLines(),
			},
		},
	}
	quoteGiftCardBatchReply, err := s.quoteGiftCardBatchFn(ctx, quoteGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch")
	}
	return &sellersv1.CompareSellerListingOptionsResponse{
		Result: &commonv1.OperationResult{
			Message: explainLabelingTaskCalculationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				DisplayName: explainLabelingTaskCalculationReply.GetLabelingTask().GetCustomer().GetDisplayName(),
			},
			SellerListingId: req.GetSellerListingId(),
			Capacity:        req.GetSellerListing().GetCapacity(),
		},
		Breakdown: &sellersv1.CompareSellerListingOptionsResponse_Breakdown{Total: quoteGiftCardBatchReply.GetBreakdown().GetTotal()},
	}, nil
}

// RecalculateSellerListings recalculates a seller listing.
func (s *SellerListingCalculationServer) RecalculateSellerListings(ctx context.Context, req *sellersv1.RecalculateSellerListingsRequest) (*sellersv1.RecalculateSellerListingsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "recalculate seller listings", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	scheduleTenantExportRequest := &identityv1.ScheduleTenantExportRequest{
		Context:     req.GetContext(),
		Reason:      tenancy.Value(ctx, "reason"),
		Annotations: req.GetAnnotations(),
		TenantId:    req.GetContext().GetTenantId(),
	}
	scheduleTenantExportReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*identityv1.ScheduleTenantExportResponse, error) {
		return s.tenantExportClient.ScheduleTenantExport(ctx, scheduleTenantExportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/ScheduleTenantExport")
	}
	return &sellersv1.RecalculateSellerListingsResponse{
		Result: &commonv1.OperationResult{
			Status:  scheduleTenantExportReply.GetResult().GetStatus(),
			Details: scheduleTenantExportReply.GetResult().GetDetails(),
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Capacity:        req.GetSellerListing().GetCapacity(),
		},
	}, nil
}

// SimulateSellerListing simulates a seller listing.
func (s *SellerListingCalculationServer) SimulateSellerListing(ctx context.Context, req *sellersv1.SimulateSellerListingRequest) (*sellersv1.SimulateSellerListingResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "simulate seller listing", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	resolveGiftCardBatchesRequest := &giftcardsv1.ResolveGiftCardBatchesRequest{Context: req.GetContext()}
	resolveGiftCardBatchesReply, err := resolveGiftCardBatchesCall(ctx, resolveGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/ResolveGiftCardBatches")
	}
	return &sellersv1.SimulateSellerListingResponse{
		Result: &commonv1.OperationResult{
			Details: resolveGiftCardBatchesReply.GetResult().GetDetails(),
			Status:  resolveGiftCardBatchesReply.GetResult().GetStatus(),
		},
		SellerListing: &sellersv1.SellerListing{
			SellerListingId: req.GetSellerListingId(),
			Owner:           req.GetSellerListing().GetOwner(),
		},
	}, nil
}

// ExplainSellerListingCalculation explains a seller listing.
func (s *SellerListingCalculationServer) ExplainSellerListingCalculation(ctx context.Context, req *sellersv1.ExplainSellerListingCalculationRequest) (*sellersv1.ExplainSellerListingCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainSellerListingCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveSellerListing(ctx, req.GetContext().GetTenantId(), req.GetSellerListing()); err != nil {
		return nil, grpcx.Wrap(err, "save seller listing")
	}
	s.log.InfoContext(ctx, "explain seller listing calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSellerListing().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	quoteSellerRequest := &sellersv1.QuoteSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
			PrimaryAddress:      &commonv1.PostalAddress{Locality: req.GetSellerListing().GetAddress().GetLocality()},
			Contact:             &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
		},
	}
	quoteSellerReply, err := s.sellerCalculationLazy().QuoteSeller(ctx, quoteSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
	}
	return &sellersv1.ExplainSellerListingCalculationResponse{
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				CreatedAt: quoteSellerReply.GetSeller().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				UserId: quoteSellerReply.GetSeller().GetRelationshipManager().GetUserId(),
			},
			SellerListingId: req.GetSellerListingId(),
			Name:            req.GetSellerListing().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSellerListingRates returns a seller listing.
func (s *SellerListingCalculationServer) GetSellerListingRates(ctx context.Context, req *sellersv1.GetSellerListingRatesRequest) (*sellersv1.GetSellerListingRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSellerListingRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "seller_listing", req.GetSellerListingId(), "get_seller_listing_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get seller listing rates", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	compareSellerOptionsRequest := &sellersv1.CompareSellerOptionsRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				Email:  email,
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	compareSellerOptionsReply, err := compareSellerOptionsCall(ctx, compareSellerOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/CompareSellerOptions")
	}
	return &sellersv1.GetSellerListingRatesResponse{
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				UserId:      compareSellerOptionsReply.GetSeller().GetRelationshipManager().GetUserId(),
				DisplayName: compareSellerOptionsReply.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
			SellerListingId: req.GetSellerListingId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ledgerAccountQueryLazy dials acme.ledger.v1.LedgerAccountQueryService the first time a handler needs it.
func (s *SellerListingCalculationServer) ledgerAccountQueryLazy() ledgerv1.LedgerAccountQueryServiceClient {
	s.ledgerAccountQueryOnce.Do(func() { s.ledgerAccountQueryCached = ledgerv1.NewLedgerAccountQueryServiceClient(s.conn) })
	return s.ledgerAccountQueryCached
}

// sellerCalculationLazy dials acme.sellers.v1.SellerCalculationService the first time a handler needs it.
func (s *SellerListingCalculationServer) sellerCalculationLazy() sellersv1.SellerCalculationServiceClient {
	s.sellerCalculationOnce.Do(func() { s.sellerCalculationCached = sellersv1.NewSellerCalculationServiceClient(s.conn) })
	return s.sellerCalculationCached
}
