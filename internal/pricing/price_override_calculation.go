package pricing

import (
	"context"
	"log/slog"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	lineagev1 "github.com/hounddogai/protomap-test-golang/gen/acme/lineage/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PriceOverrideCalculationServer implements `acme.pricing.v1.PriceOverrideCalculationService`.
// Computes the figures of a price override, such as totals, taxes, and estimates.
type PriceOverrideCalculationServer struct {
	pricingv1.UnimplementedPriceOverrideCalculationServiceServer
	store                               *Store
	log                                 *slog.Logger
	priceQuoteCalculationPort           priceQuoteCalculationPort
	priceListValidationClient           pricingv1.PriceListValidationServiceClient
	priceQuoteCalculationClient         pricingv1.PriceQuoteCalculationServiceClient
	priceQuoteCalculationClientFallback pricingv1.PriceQuoteCalculationServiceClient
	couponValidationClient              promotionsv1.CouponValidationServiceClient
	clients                             *registry.Registry
	meteredCouponValidation             meteredCouponValidation
	synonymValidationClient             searchv1.SynonymValidationServiceClient
	checkoutSessionClient               checkoutv1.CheckoutSessionServiceClient
	tenantExportClient                  identityv1.TenantExportServiceClient
}

// NewPriceOverrideCalculationServer wires a PriceOverrideCalculationServer.
func NewPriceOverrideCalculationServer(deps platform.Deps) *PriceOverrideCalculationServer {
	return &PriceOverrideCalculationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.pricing.v1.PriceOverrideCalculationService"),
		priceQuoteCalculationPort:           newPriceQuoteCalculationPort(deps),
		priceListValidationClient:           pricingv1.NewPriceListValidationServiceClient(deps.Conn),
		priceQuoteCalculationClient:         pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn),
		priceQuoteCalculationClientFallback: pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn),
		couponValidationClient:              promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		meteredCouponValidation:             meteredCouponValidation{CouponValidationServiceClient: promotionsv1.NewCouponValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		synonymValidationClient:             searchv1.NewSynonymValidationServiceClient(deps.Conn),
		checkoutSessionClient:               checkoutv1.NewCheckoutSessionServiceClient(deps.Conn),
		tenantExportClient:                  identityv1.NewTenantExportServiceClient(deps.Conn),
	}
}

// CalculatePriceOverride calculates a price override.
func (s *PriceOverrideCalculationServer) CalculatePriceOverride(ctx context.Context, req *pricingv1.CalculatePriceOverrideRequest) (*pricingv1.CalculatePriceOverrideResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "calculate price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "locality", req.GetDetails().GetDestination().GetLocality())
	calculatePriceQuoteRequest := &pricingv1.CalculatePriceQuoteRequest{
		Context: req.GetContext(),
		Details: &pricingv1.CalculatePriceQuoteRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
				Locality:   tenancy.Value(ctx, "locality"),
			},
		},
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: req.GetPriceOverride().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPriceOverride().GetAudit().GetUpdatedAt()},
		},
	}
	calculatePriceQuoteReply, err := s.priceQuoteCalculationPort.CalculatePriceQuote(ctx, calculatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/CalculatePriceQuote")
	}
	return &pricingv1.CalculatePriceOverrideResponse{
		Result: &commonv1.OperationResult{Status: calculatePriceQuoteReply.GetResult().GetStatus()},
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: calculatePriceQuoteReply.GetPriceQuote().GetAudit().GetUpdatedAt(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Status:          req.GetPriceOverride().GetStatus(),
		},
	}, nil
}

// EstimatePriceOverride estimates a price override.
func (s *PriceOverrideCalculationServer) EstimatePriceOverride(ctx context.Context, req *pricingv1.EstimatePriceOverrideRequest) (*pricingv1.EstimatePriceOverrideResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "estimate price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	lintPriceListsRequest := buildLintPriceListsRequestForEstimatePriceOverride(req)
	lintPriceListsReply, err := s.priceListValidationClient.LintPriceLists(ctx, lintPriceListsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/LintPriceLists")
	}
	explainPriceQuoteCalculationReply, err := s.priceQuoteCalculationClient.ExplainPriceQuoteCalculation(ctx, &pricingv1.ExplainPriceQuoteCalculationRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintPriceListsReply.GetPriceList().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &pricingv1.ExplainPriceQuoteCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetPriceOverride().GetOwner().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/ExplainPriceQuoteCalculation")
	}
	return &pricingv1.EstimatePriceOverrideResponse{
		Result: &commonv1.OperationResult{
			Details: lintPriceListsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Breakdown: &pricingv1.EstimatePriceOverrideResponse_Breakdown{
			Total: explainPriceQuoteCalculationReply.GetPriceQuote().GetTotal(),
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Status:          req.GetPriceOverride().GetStatus(),
		},
	}, nil
}

// QuotePriceOverride quotes a price override.
func (s *PriceOverrideCalculationServer) QuotePriceOverride(ctx context.Context, req *pricingv1.QuotePriceOverrideRequest) (*pricingv1.QuotePriceOverrideResponse, error) {
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "quote price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	for _, condition := range req.GetPriceOverride().GetConditions() {
		if _, err := s.priceListValidationClient.NormalizePriceList(ctx, &pricingv1.NormalizePriceListRequest{
			Context: req.GetContext(),
			PriceList: &pricingv1.PriceList{
				Conditions: []*pricingv1.PriceListCondition{&pricingv1.PriceListCondition{Value: condition.GetValue()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/NormalizePriceList")
		}
	}
	resolveLineageNodesRequest := &lineagev1.ResolveLineageNodesRequest{Context: req.GetContext()}
	resolveLineageNodesReply, err := resolveLineageNodesCall(ctx, resolveLineageNodesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lineage.v1.LineageNodeQueryService/ResolveLineageNodes")
	}
	explainPriceQuoteCalculationRequest := newExplainPriceQuoteCalculationRequest(withExplainPriceQuoteCalculationContext(req.GetContext()), withExplainPriceQuoteCalculationPriceQuote(&pricingv1.PriceQuote{
		Counterparty: &pricingv1.PriceListRef{
			DisplayName: req.GetPriceOverride().GetOwner().GetDisplayName(),
		},
		Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPriceOverride().GetAudit().GetUpdatedAt()},
	}))
	explainPriceQuoteCalculationReply, err := s.priceQuoteCalculationClient.ExplainPriceQuoteCalculation(ctx, explainPriceQuoteCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/ExplainPriceQuoteCalculation")
	}
	return &pricingv1.QuotePriceOverrideResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: resolveLineageNodesReply.GetResult().GetReferenceIds(),
			Status:       explainPriceQuoteCalculationReply.GetOutcome().GetStatus(),
			Message:      explainPriceQuoteCalculationReply.GetResult().GetMessage(),
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Owner:           req.GetPriceOverride().GetOwner(),
		},
	}, nil
}

// ComparePriceOverrideOptions compares a price override.
func (s *PriceOverrideCalculationServer) ComparePriceOverrideOptions(ctx context.Context, req *pricingv1.ComparePriceOverrideOptionsRequest) (*pricingv1.ComparePriceOverrideOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ComparePriceOverrideOptions", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "compare price override options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	for _, condition := range req.GetPriceOverride().GetConditions() {
		if _, err := s.priceListValidationClient.LintPriceLists(ctx, &pricingv1.LintPriceListsRequest{
			Context: req.GetContext(),
			PriceList: &pricingv1.PriceList{
				Conditions: []*pricingv1.PriceListCondition{&pricingv1.PriceListCondition{
					Attribute: condition.GetAttribute(),
					Value:     condition.GetValue(),
					Negate:    condition.GetNegate(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/LintPriceLists")
		}
	}
	routes := map[string]func(context.Context, *pricingv1.EstimatePriceQuoteRequest, ...grpc.CallOption) (*pricingv1.EstimatePriceQuoteResponse, error){
		"primary":  s.priceQuoteCalculationClient.EstimatePriceQuote,
		"fallback": s.priceQuoteCalculationClientFallback.EstimatePriceQuote,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	source := req.GetDetails().GetDestination().ProtoReflect()
	countryCode := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("country_code"))).String()
	estimatePriceQuoteRequest := &pricingv1.EstimatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines:          []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{Amount: req.GetPriceOverride().GetDiscount().GetAmount()}},
			Audit:          &commonv1.AuditInfo{UpdatedBy: req.GetPriceOverride().GetAudit().GetUpdatedBy()},
			BillingAddress: &commonv1.PostalAddress{CountryCode: countryCode},
		},
	}
	estimatePriceQuoteReply, err := route(ctx, estimatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/EstimatePriceQuote")
	}
	recalculatePriceQuotesReply, err := s.priceQuoteCalculationClient.RecalculatePriceQuotes(ctx, &pricingv1.RecalculatePriceQuotesRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{Amount: req.GetPriceOverride().GetDiscount().GetAmount()}},
			Settlement: &pricingv1.PriceQuote_Card{Card: &pricingv1.PriceQuoteCardSettlement{
				HolderName: estimatePriceQuoteReply.GetPriceQuote().GetCard().GetHolderName(),
				Last4:      estimatePriceQuoteReply.GetPriceQuote().GetCard().GetLast4(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Region: estimatePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetRegion(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/RecalculatePriceQuotes")
	}
	return &pricingv1.ComparePriceOverrideOptionsResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: estimatePriceQuoteReply.GetPriceQuote().GetAudit().GetUpdatedAt(),
				CreatedBy: recalculatePriceQuotesReply.GetPriceQuote().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: estimatePriceQuoteReply.GetPriceQuote().GetCounterparty().GetDisplayName(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Version:         req.GetPriceOverride().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecalculatePriceOverrides recalculates a price override.
func (s *PriceOverrideCalculationServer) RecalculatePriceOverrides(ctx context.Context, req *pricingv1.RecalculatePriceOverridesRequest) (*pricingv1.RecalculatePriceOverridesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "recalculate price overrides", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	step := &priceOverrideCalculationServerRecalculatePriceOverridesPreviewCouponStep{request: &promotionsv1.PreviewCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetPriceOverride().GetAudit().GetCreatedBy()},
			Holder: &promotionsv1.PromotionRef{
				DisplayName: req.GetPriceOverride().GetOwner().GetDisplayName(),
			},
		},
	}}
	if err := s.runRecalculatePriceOverridesPreviewCoupon(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/PreviewCoupon")
	}
	previewCouponReply := step.reply
	sessionQuery, err := registry.Lookup[identityv1.SessionQueryServiceClient](s.clients, "acme.identity.v1.SessionQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	browseSessionsRequest := &identityv1.BrowseSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.BrowseSessionsRequest_Filter{
			Labels: previewCouponReply.GetCoupon().GetLabels(),
			Region: req.GetDetails().GetDestination().GetRegion(),
		},
	}
	browseSessionsReply, err := sessionQuery.BrowseSessions(ctx, browseSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/BrowseSessions")
	}
	return &pricingv1.RecalculatePriceOverridesResponse{
		Breakdown: &pricingv1.RecalculatePriceOverridesResponse_Breakdown{
			CurrencyCode: previewCouponReply.GetCoupon().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: browseSessionsReply.GetResult().GetReferenceIds(),
			Message:      browseSessionsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Description:     req.GetPriceOverride().GetDescription(),
		},
	}, nil
}

// SimulatePriceOverride simulates a price override.
func (s *PriceOverrideCalculationServer) SimulatePriceOverride(ctx context.Context, req *pricingv1.SimulatePriceOverrideRequest) (*pricingv1.SimulatePriceOverrideResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "simulate price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetPriceOverride().GetOwner().GetEmail())
	normalizeCouponRequest := &promotionsv1.NormalizeCouponRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.NormalizeCouponRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetPriceOverride().GetAudit().GetCreatedBy()},
		},
	}
	normalizeCouponReply, err := s.meteredCouponValidation.NormalizeCoupon(ctx, normalizeCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/NormalizeCoupon")
	}
	return &pricingv1.SimulatePriceOverrideResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Audit:           &commonv1.AuditInfo{Etag: normalizeCouponReply.GetCoupon().GetAudit().GetEtag()},
			PriceOverrideId: req.GetPriceOverrideId(),
			Owner:           req.GetPriceOverride().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: normalizeCouponReply.GetResult().GetStatus()},
	}, nil
}

// ExplainPriceOverrideCalculation explains a price override.
func (s *PriceOverrideCalculationServer) ExplainPriceOverrideCalculation(ctx context.Context, req *pricingv1.ExplainPriceOverrideCalculationRequest) (*pricingv1.ExplainPriceOverrideCalculationResponse, error) {
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "explain price override calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := priceOverrideCalculationServerCalculatePriceQuoteSteps[mode]
	cloned := proto.Clone(req.GetPriceOverride()).(*pricingv1.PriceOverride)
	calculatePriceQuoteRequest := &pricingv1.CalculatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cloned.GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetPriceOverride().GetAudit().GetCreatedBy(),
			},
		},
	}
	calculatePriceQuoteReply, err := run(s, ctx, calculatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/CalculatePriceQuote")
	}
	for _, condition := range req.GetPriceOverride().GetConditions() {
		if _, err := s.synonymValidationClient.ScoreSynonym(ctx, &searchv1.ScoreSynonymRequest{
			Context: req.GetContext(),
			Synonym: &searchv1.Synonym{
				Conditions: []*searchv1.SynonymCondition{&searchv1.SynonymCondition{
					Attribute: condition.GetAttribute(),
					Operator:  condition.GetOperator(),
					Value:     condition.GetValue(),
					Negate:    condition.GetNegate(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ScoreSynonym")
		}
	}
	searchCheckoutSessionsRequest := &checkoutv1.SearchCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.SearchCheckoutSessionsRequest_Filter{
			Region: calculatePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetRegion(),
		},
	}
	type searchCheckoutSessionsResult struct {
		reply *checkoutv1.SearchCheckoutSessionsResponse
		err   error
	}
	searchCheckoutSessionsResults := make(chan searchCheckoutSessionsResult, 1)
	go func() {
		reply, err := s.checkoutSessionClient.SearchCheckoutSessions(ctx, searchCheckoutSessionsRequest)
		searchCheckoutSessionsResults <- searchCheckoutSessionsResult{reply, err}
	}()
	var searchCheckoutSessionsReply *checkoutv1.SearchCheckoutSessionsResponse
	select {
	case result := <-searchCheckoutSessionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.checkout.v1.CheckoutSessionService/SearchCheckoutSessions")
		}
		searchCheckoutSessionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	recalculatePriceQuotesRequest := &pricingv1.RecalculatePriceQuotesRequest{
		Context: req.GetContext(),
		Details: &pricingv1.RecalculatePriceQuotesRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: calculatePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetRecipient(),
			},
		},
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				Etag: calculatePriceQuoteReply.GetPriceQuote().GetAudit().GetEtag(),
			},
		},
	}
	recalculatePriceQuotesReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*pricingv1.RecalculatePriceQuotesResponse, error) {
		return s.priceQuoteCalculationClient.RecalculatePriceQuotes(ctx, recalculatePriceQuotesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/RecalculatePriceQuotes")
	}
	return &pricingv1.ExplainPriceOverrideCalculationResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Version: calculatePriceQuoteReply.GetPriceQuote().GetCounterparty().GetVersion(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: calculatePriceQuoteReply.GetPriceQuote().GetAudit().GetUpdatedAt(),
				CreatedBy: recalculatePriceQuotesReply.GetPriceQuote().GetAudit().GetCreatedBy(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Priority:        req.GetPriceOverride().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			Message: searchCheckoutSessionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &pricingv1.ExplainPriceOverrideCalculationResponse_Outcome{
			Message: searchCheckoutSessionsReply.GetResult().GetMessage(),
		},
	}, nil
}

// GetPriceOverrideRates returns a price override.
func (s *PriceOverrideCalculationServer) GetPriceOverrideRates(ctx context.Context, req *pricingv1.GetPriceOverrideRatesRequest) (*pricingv1.GetPriceOverrideRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_override", req.GetPriceOverrideId(), "get_price_override_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get price override rates", "tenant", req.GetContext().GetTenantId())
	var holder struct{ TenantId string }
	if record := req.GetContext(); record != nil {
		if err := mapper.CopyFields(&holder, record, "TenantId"); err != nil {
			s.log.WarnContext(ctx, "copy tenant_id", "error", err)
		}
	}
	getTenantExportReply, err := s.tenantExportClient.GetTenantExport(ctx, &identityv1.GetTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: holder.TenantId,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/GetTenantExport")
	}
	return &pricingv1.GetPriceOverrideRatesResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: getTenantExportReply.GetTenant().GetAudit().GetUpdatedBy(),
				CreatedAt: getTenantExportReply.GetTenant().GetAudit().GetCreatedAt(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// priceOverrideCalculationServerRecalculatePriceOverridesPreviewCouponStep carries the request of one step of
// RecalculatePriceOverrides and, once it ran, its reply.
type priceOverrideCalculationServerRecalculatePriceOverridesPreviewCouponStep struct {
	request *promotionsv1.PreviewCouponRequest
	reply   *promotionsv1.PreviewCouponResponse
}

func (s *PriceOverrideCalculationServer) runRecalculatePriceOverridesPreviewCoupon(ctx context.Context, step *priceOverrideCalculationServerRecalculatePriceOverridesPreviewCouponStep) error {
	reply, err := s.couponValidationClient.PreviewCoupon(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// priceOverrideCalculationServerCalculatePriceQuoteSteps are the ways to run
// acme.pricing.v1.PriceQuoteCalculationService/CalculatePriceQuote, by name.
var priceOverrideCalculationServerCalculatePriceQuoteSteps = map[string]func(*PriceOverrideCalculationServer, context.Context, *pricingv1.CalculatePriceQuoteRequest) (*pricingv1.CalculatePriceQuoteResponse, error){
	"default": (*PriceOverrideCalculationServer).calculatePriceQuoteStep,
	"audited": (*PriceOverrideCalculationServer).calculatePriceQuoteAuditedStep,
}

func (s *PriceOverrideCalculationServer) calculatePriceQuoteStep(ctx context.Context, request *pricingv1.CalculatePriceQuoteRequest) (*pricingv1.CalculatePriceQuoteResponse, error) {
	return s.priceQuoteCalculationClient.CalculatePriceQuote(ctx, request)
}

func (s *PriceOverrideCalculationServer) calculatePriceQuoteAuditedStep(ctx context.Context, request *pricingv1.CalculatePriceQuoteRequest) (*pricingv1.CalculatePriceQuoteResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.pricing.v1.PriceQuoteCalculationService/CalculatePriceQuote")
	return s.priceQuoteCalculationClient.CalculatePriceQuote(ctx, request)
}
