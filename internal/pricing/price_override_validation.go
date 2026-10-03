package pricing

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PriceOverrideValidationServer implements `acme.pricing.v1.PriceOverrideValidationService`.
// Checks price overrides against the rules before anything acts on them.
type PriceOverrideValidationServer struct {
	pricingv1.UnimplementedPriceOverrideValidationServiceServer
	store                      *Store
	log                        *slog.Logger
	searchQueryQueryClient     searchv1.SearchQueryQueryServiceClient
	conn                       *grpc.ClientConn
	priceQuoteValidationClient pricingv1.PriceQuoteValidationServiceClient
	packingSlipQueryClient     fulfillmentv1.PackingSlipQueryServiceClient
	priceListValidationPort    priceListValidationPort
	auditEventValidationClient auditv1.AuditEventValidationServiceClient
	priceListValidationClient  pricingv1.PriceListValidationServiceClient
	clients                    *registry.Registry
	fulfillmentClient          fulfillmentv1.FulfillmentServiceClient
	backorderCalculationClient ordersv1.BackorderCalculationServiceClient
}

// NewPriceOverrideValidationServer wires a PriceOverrideValidationServer.
func NewPriceOverrideValidationServer(deps platform.Deps) *PriceOverrideValidationServer {
	return &PriceOverrideValidationServer{
		store:                      NewStore(deps.DB),
		log:                        deps.Logger.With("service", "acme.pricing.v1.PriceOverrideValidationService"),
		searchQueryQueryClient:     searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		conn:                       deps.Conn,
		priceQuoteValidationClient: pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		packingSlipQueryClient:     fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn),
		priceListValidationPort:    newPriceListValidationPort(deps),
		auditEventValidationClient: auditv1.NewAuditEventValidationServiceClient(deps.Conn),
		priceListValidationClient:  pricingv1.NewPriceListValidationServiceClient(deps.Conn),
		clients:                    deps.Clients,
		fulfillmentClient:          fulfillmentv1.NewFulfillmentServiceClient(deps.Conn),
		backorderCalculationClient: ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
	}
}

// ValidatePriceOverride validates a price override.
func (s *PriceOverrideValidationServer) ValidatePriceOverride(ctx context.Context, req *pricingv1.ValidatePriceOverrideRequest) (*pricingv1.ValidatePriceOverrideResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "validate price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	suggestSearchQueriesReply, err := s.searchQueryQueryClient.SuggestSearchQueries(ctx, &searchv1.SuggestSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.SuggestSearchQueriesRequest_Filter{
			CreatedAfter: req.GetPriceOverride().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/SuggestSearchQueries")
	}
	return &pricingv1.ValidatePriceOverrideResponse{
		Result: &commonv1.OperationResult{
			Message: suggestSearchQueriesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Owner:           req.GetPriceOverride().GetOwner(),
		},
	}, nil
}

// PreviewPriceOverride previews a price override.
func (s *PriceOverrideValidationServer) PreviewPriceOverride(ctx context.Context, req *pricingv1.PreviewPriceOverrideRequest) (*pricingv1.PreviewPriceOverrideResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "preview price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	validatePriceListRequest := &pricingv1.ValidatePriceListRequest{
		Context: req.GetContext(),
		Details: &pricingv1.ValidatePriceListRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	validatePriceListReply := new(pricingv1.ValidatePriceListResponse)
	if err := grpcx.Invoke(ctx, s.conn, pricingv1.PriceListValidationService_ValidatePriceList_FullMethodName, validatePriceListRequest, validatePriceListReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/ValidatePriceList")
	}
	normalizePriceQuoteRequest := &pricingv1.NormalizePriceQuoteRequest{}
	normalizePriceQuoteRequest.Context = req.GetContext()
	normalizePriceQuoteRequest.PriceQuote = &pricingv1.PriceQuote{
		Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
			Tax: &pricingv1.PriceQuoteLineTax{
				Amount: validatePriceListReply.GetPriceList().GetDiscount().GetAmount(),
			},
		}},
		Audit: &commonv1.AuditInfo{
			Etag: validatePriceListReply.GetPriceList().GetAudit().GetEtag(),
		},
	}
	normalizePriceQuoteReply, err := s.priceQuoteValidationClient.NormalizePriceQuote(ctx, normalizePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/NormalizePriceQuote")
	}
	countPackingSlipsRequest := &fulfillmentv1.CountPackingSlipsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.CountPackingSlipsRequest_Filter{
			Region:       normalizePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetRegion(),
			CreatedAfter: normalizePriceQuoteReply.GetPriceQuote().GetAudit().GetUpdatedAt(),
		},
	}
	countPackingSlipsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*fulfillmentv1.CountPackingSlipsResponse, error) {
		return s.packingSlipQueryClient.CountPackingSlips(ctx, countPackingSlipsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/CountPackingSlips")
	}
	lintPriceListsRequest := &pricingv1.LintPriceListsRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Action: &pricingv1.PriceList_Discount{Discount: &pricingv1.PriceListDiscountAction{Amount: req.GetPriceOverride().GetDiscount().GetAmount()}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validatePriceListReply.GetPriceList().GetAudit().GetUpdatedAt(),
			},
		},
	}
	lintPriceListsReply, err := s.priceListValidationPort.LintPriceLists(ctx, lintPriceListsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/LintPriceLists")
	}
	return &pricingv1.PreviewPriceOverrideResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Description: validatePriceListReply.GetPriceList().GetDescription(),
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintPriceListsReply.GetPriceList().GetAudit().GetCreatedAt(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Status:          req.GetPriceOverride().GetStatus(),
		},
		Outcome: &pricingv1.PreviewPriceOverrideResponse_Outcome{Status: validatePriceListReply.GetOutcome().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: normalizePriceQuoteReply.GetOutcome().GetMessage(),
			Status:  countPackingSlipsReply.GetResult().GetStatus(),
		},
	}, nil
}

// NormalizePriceOverride normalizes a price override.
func (s *PriceOverrideValidationServer) NormalizePriceOverride(ctx context.Context, req *pricingv1.NormalizePriceOverrideRequest) (*pricingv1.NormalizePriceOverrideResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "normalize price override", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "user_id", req.GetPriceOverride().GetOwner().GetUserId())
	browseSearchQueriesRequest := &searchv1.BrowseSearchQueriesRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.BrowseSearchQueriesRequest_Filter{OwnerId: tenancy.Value(ctx, "user_id")},
	}
	browseSearchQueriesReply := new(searchv1.BrowseSearchQueriesResponse)
	if err := s.conn.Invoke(ctx, searchSearchQueryQueryServiceBrowseSearchQueriesMethod, browseSearchQueriesRequest, browseSearchQueriesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/BrowseSearchQueries")
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetContact().GetPhone())
	explainAuditEventDecisionRequest := &auditv1.ExplainAuditEventDecisionRequest{
		Context: req.GetContext(),
		Details: &auditv1.ExplainAuditEventDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetContact().GetEmail(),
				Phone: tenancy.Value(ctx, "phone"),
			},
		},
	}
	explainAuditEventDecisionReply, err := grpcx.Call(ctx, s.auditEventValidationClient.ExplainAuditEventDecision, explainAuditEventDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/ExplainAuditEventDecision")
	}
	for _, condition := range req.GetPriceOverride().GetConditions() {
		if _, err := s.priceListValidationClient.ValidatePriceList(ctx, &pricingv1.ValidatePriceListRequest{
			Context: req.GetContext(),
			PriceList: &pricingv1.PriceList{
				Conditions: []*pricingv1.PriceListCondition{&pricingv1.PriceListCondition{
					Attribute: condition.GetAttribute(),
					Value:     condition.GetValue(),
					Operator:  condition.GetOperator(),
					Negate:    condition.GetNegate(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/ValidatePriceList")
		}
	}
	_ = explainAuditEventDecisionReply
	return &pricingv1.NormalizePriceOverrideResponse{
		Outcome: &pricingv1.NormalizePriceOverrideResponse_Outcome{Status: browseSearchQueriesReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: browseSearchQueriesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Owner:           req.GetPriceOverride().GetOwner(),
		},
	}, nil
}

// ExplainPriceOverrideDecision explains a price override.
func (s *PriceOverrideValidationServer) ExplainPriceOverrideDecision(ctx context.Context, req *pricingv1.ExplainPriceOverrideDecisionRequest) (*pricingv1.ExplainPriceOverrideDecisionResponse, error) {
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "explain price override decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPriceOverride()).(*pricingv1.PriceOverride)
	previewPriceQuoteRequest := &pricingv1.PreviewPriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPriceOverride().GetAudit().GetCreatedBy(),
				UpdatedBy: req.GetPriceOverride().GetAudit().GetUpdatedBy(),
				CreatedAt: cloned.GetAudit().GetCreatedAt(),
			},
		},
	}
	previewPriceQuoteReply, err := pricingv1.PriceQuoteValidationServiceClient.PreviewPriceQuote(s.priceQuoteValidationClient, ctx, previewPriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/PreviewPriceQuote")
	}
	comparePriceQuoteOptionsRequest := &pricingv1.ComparePriceQuoteOptionsRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Settlement: &pricingv1.PriceQuote_Bank{Bank: &pricingv1.PriceQuoteBankSettlement{
				AccountHolder: previewPriceQuoteReply.GetPriceQuote().GetBank().GetAccountHolder(),
				Iban:          previewPriceQuoteReply.GetPriceQuote().GetBank().GetIban(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: previewPriceQuoteReply.GetPriceQuote().GetAudit().GetCreatedAt(),
			},
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: previewPriceQuoteReply.GetPriceQuote().GetCounterparty().GetDisplayName(),
			},
		},
	}
	comparePriceQuoteOptionsReply := new(pricingv1.ComparePriceQuoteOptionsResponse)
	if err := s.conn.Invoke(ctx, pricingPriceQuoteCalculationServiceComparePriceQuoteOptionsMethod, comparePriceQuoteOptionsRequest, comparePriceQuoteOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/ComparePriceQuoteOptions")
	}
	return &pricingv1.ExplainPriceOverrideDecisionResponse{
		Outcome: &pricingv1.ExplainPriceOverrideDecisionResponse_Outcome{Message: previewPriceQuoteReply.GetOutcome().GetMessage()},
		Result: &commonv1.OperationResult{
			Status: comparePriceQuoteOptionsReply.GetResult().GetStatus(),
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Status:          req.GetPriceOverride().GetStatus(),
		},
	}, nil
}

// LintPriceOverrides lints a price override.
func (s *PriceOverrideValidationServer) LintPriceOverrides(ctx context.Context, req *pricingv1.LintPriceOverridesRequest) (*pricingv1.LintPriceOverridesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "lint price overrides", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	priceQuoteValidation, err := registry.Lookup[pricingv1.PriceQuoteValidationServiceClient](s.clients, "acme.pricing.v1.PriceQuoteValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	explainPriceQuoteDecisionRequest := &pricingv1.ExplainPriceQuoteDecisionRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetPriceOverride().GetAudit().GetCreatedAt()},
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: req.GetPriceOverride().GetOwner().GetDisplayName(),
			},
		},
	}
	explainPriceQuoteDecisionReply, err := priceQuoteValidation.ExplainPriceQuoteDecision(ctx, explainPriceQuoteDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/ExplainPriceQuoteDecision")
	}
	lintPriceListsRequest := &pricingv1.LintPriceListsRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Owner:  &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
			Action: &pricingv1.PriceList_Route{Route: &pricingv1.PriceListRouteAction{Weight: req.GetPriceOverride().GetRoute().GetWeight()}},
		},
	}
	lintPriceListsReply := new(pricingv1.LintPriceListsResponse)
	if err := grpcx.Invoke(ctx, s.conn, pricingv1.PriceListValidationService_LintPriceLists_FullMethodName, lintPriceListsRequest, lintPriceListsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/LintPriceLists")
	}
	watchFulfillmentsStream, err := s.fulfillmentClient.WatchFulfillments(ctx, &fulfillmentv1.WatchFulfillmentsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/WatchFulfillments")
	}
	for {
		received, err := watchFulfillmentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/WatchFulfillments")
		}
		_ = received
	}
	return &pricingv1.LintPriceOverridesResponse{
		Outcome: &pricingv1.LintPriceOverridesResponse_Outcome{
			EffectiveAt: explainPriceQuoteDecisionReply.GetOutcome().GetEffectiveAt(),
		},
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainPriceQuoteDecisionReply.GetPriceQuote().GetAudit().GetUpdatedBy(),
			},
			Priority:        lintPriceListsReply.GetPriceList().GetPriority(),
			PriceOverrideId: req.GetPriceOverrideId(),
			Owner:           req.GetPriceOverride().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPriceOverrideRules returns a price override.
func (s *PriceOverrideValidationServer) GetPriceOverrideRules(ctx context.Context, req *pricingv1.GetPriceOverrideRulesRequest) (*pricingv1.GetPriceOverrideRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_override", req.GetPriceOverrideId(), "get_price_override_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get price override rules", "tenant", req.GetContext().GetTenantId())
	quoteBackorderRequest := buildQuoteBackorderRequestForGetPriceOverrideRules(req)
	quoteBackorderReply, err := s.backorderCalculationClient.QuoteBackorder(ctx, quoteBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/QuoteBackorder")
	}
	return &pricingv1.GetPriceOverrideRulesResponse{
		Result: &commonv1.OperationResult{
			Message: quoteBackorderReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{PriceOverrideId: req.GetPriceOverrideId()},
	}, nil
}
