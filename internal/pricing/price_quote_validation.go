package pricing

import (
	"context"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	datasetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datasets/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PriceQuoteValidationServer implements `acme.pricing.v1.PriceQuoteValidationService`.
// Checks price quotes against the rules before anything acts on them.
type PriceQuoteValidationServer struct {
	pricingv1.UnimplementedPriceQuoteValidationServiceServer
	store                              *Store
	log                                *slog.Logger
	meteredCartCalculation             meteredCartCalculation
	conn                               *grpc.ClientConn
	priceListValidationClient          pricingv1.PriceListValidationServiceClient
	priceListValidationClientFallback  pricingv1.PriceListValidationServiceClient
	savedListCalculationClient         cartv1.SavedListCalculationServiceClient
	savedListCalculationClientFallback cartv1.SavedListCalculationServiceClient
	priceQuoteCalculationClient        pricingv1.PriceQuoteCalculationServiceClient
	clients                            *registry.Registry
	paymentCalculationClient           paymentsv1.PaymentCalculationServiceClient
	datasetClient                      datasetsv1.DatasetServiceClient
}

// NewPriceQuoteValidationServer wires a PriceQuoteValidationServer.
func NewPriceQuoteValidationServer(deps platform.Deps) *PriceQuoteValidationServer {
	return &PriceQuoteValidationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.pricing.v1.PriceQuoteValidationService"),
		meteredCartCalculation:             meteredCartCalculation{CartCalculationServiceClient: cartv1.NewCartCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                               deps.Conn,
		priceListValidationClient:          pricingv1.NewPriceListValidationServiceClient(deps.Conn),
		priceListValidationClientFallback:  pricingv1.NewPriceListValidationServiceClient(deps.Conn),
		savedListCalculationClient:         cartv1.NewSavedListCalculationServiceClient(deps.Conn),
		savedListCalculationClientFallback: cartv1.NewSavedListCalculationServiceClient(deps.Conn),
		priceQuoteCalculationClient:        pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn),
		clients:                            deps.Clients,
		paymentCalculationClient:           paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		datasetClient:                      datasetsv1.NewDatasetServiceClient(deps.Conn),
	}
}

// ValidatePriceQuote validates a price quote.
func (s *PriceQuoteValidationServer) ValidatePriceQuote(ctx context.Context, req *pricingv1.ValidatePriceQuoteRequest) (*pricingv1.ValidatePriceQuoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidatePriceQuote", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "validate price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	explainCartCalculationRequest := &cartv1.ExplainCartCalculationRequest{
		Context: req.GetContext(),
		Details: &cartv1.ExplainCartCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	explainCartCalculationReply, err := s.meteredCartCalculation.ExplainCartCalculation(ctx, explainCartCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/ExplainCartCalculation")
	}
	checkPriceListEligibilityRequest := &pricingv1.CheckPriceListEligibilityRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPriceQuote().GetAudit().GetCreatedAt(),
				CreatedBy: explainCartCalculationReply.GetCart().GetAudit().GetCreatedBy(),
			},
		},
	}
	checkPriceListEligibilityReply := new(pricingv1.CheckPriceListEligibilityResponse)
	if err := grpcx.Invoke(ctx, s.conn, pricingv1.PriceListValidationService_CheckPriceListEligibility_FullMethodName, checkPriceListEligibilityRequest, checkPriceListEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/CheckPriceListEligibility")
	}
	return &pricingv1.ValidatePriceQuoteResponse{
		Outcome: &pricingv1.ValidatePriceQuoteResponse_Outcome{
			NextSteps: explainCartCalculationReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			Status: checkPriceListEligibilityReply.GetOutcome().GetStatus(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			PriceQuoteId:   req.GetPriceQuoteId(),
			BillingAddress: req.GetPriceQuote().GetBillingAddress(),
		},
	}, nil
}

// PreviewPriceQuote previews a price quote.
func (s *PriceQuoteValidationServer) PreviewPriceQuote(ctx context.Context, req *pricingv1.PreviewPriceQuoteRequest) (*pricingv1.PreviewPriceQuoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "preview price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *pricingv1.NormalizePriceListRequest, ...grpc.CallOption) (*pricingv1.NormalizePriceListResponse, error){
		"primary":  s.priceListValidationClient.NormalizePriceList,
		"fallback": s.priceListValidationClientFallback.NormalizePriceList,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	var holder struct{ Etag string }
	if record := req.GetPriceQuote().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Etag"); err != nil {
			s.log.WarnContext(ctx, "copy etag", "error", err)
		}
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetPriceQuote().GetAudit().GetUpdatedBy())
	normalizePriceListRequest := &pricingv1.NormalizePriceListRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				Etag:      holder.Etag,
				UpdatedBy: tenancy.Value(ctx, "updated_by"),
			},
		},
		Details: &pricingv1.NormalizePriceListRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	normalizePriceListReply, err := route(ctx, normalizePriceListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/NormalizePriceList")
	}
	routes2 := map[string]func(context.Context, *cartv1.ExplainSavedListCalculationRequest, ...grpc.CallOption) (*cartv1.ExplainSavedListCalculationResponse, error){
		"primary":  s.savedListCalculationClient.ExplainSavedListCalculation,
		"fallback": s.savedListCalculationClientFallback.ExplainSavedListCalculation,
	}
	route2, ok := routes2[req.GetContext().GetLocale()]
	if !ok {
		route2 = routes2["primary"]
	}
	explainSavedListCalculationRequest := &cartv1.ExplainSavedListCalculationRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Counterparty: &cartv1.CartRuleRef{Version: req.GetPriceQuote().GetCounterparty().GetVersion()},
			Settlement:   &cartv1.SavedList_Card{Card: &cartv1.SavedListCardSettlement{Token: req.GetPriceQuote().GetCard().GetToken()}},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPriceQuote().GetAudit().GetEtag(),
				UpdatedAt: normalizePriceListReply.GetPriceList().GetAudit().GetUpdatedAt(),
			},
		},
	}
	explainSavedListCalculationReply, err := route2(ctx, explainSavedListCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/ExplainSavedListCalculation")
	}
	return &pricingv1.PreviewPriceQuoteResponse{
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: normalizePriceListReply.GetPriceList().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainSavedListCalculationReply.GetSavedList().GetAudit().GetUpdatedAt(),
			},
			DueAt:        explainSavedListCalculationReply.GetSavedList().GetDueAt(),
			PriceQuoteId: req.GetPriceQuoteId(),
			IssuedAt:     req.GetPriceQuote().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckPriceQuoteEligibility checks a price quote.
func (s *PriceQuoteValidationServer) CheckPriceQuoteEligibility(ctx context.Context, req *pricingv1.CheckPriceQuoteEligibilityRequest) (*pricingv1.CheckPriceQuoteEligibilityResponse, error) {
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "check price quote eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	simulatePriceQuoteRequest := &pricingv1.SimulatePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Settlement: &pricingv1.PriceQuote_Card{Card: &pricingv1.PriceQuoteCardSettlement{HolderName: req.GetPriceQuote().GetCard().GetHolderName()}},
		},
	}
	simulatePriceQuoteReply, err := pricingv1.PriceQuoteCalculationServiceClient.SimulatePriceQuote(s.priceQuoteCalculationClient, ctx, simulatePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/SimulatePriceQuote")
	}
	priceOverrideCalculation, err := registry.Lookup[pricingv1.PriceOverrideCalculationServiceClient](s.clients, "acme.pricing.v1.PriceOverrideCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	calculatePriceOverrideRequest := &pricingv1.CalculatePriceOverrideRequest{
		Context: req.GetContext(),
		Details: &pricingv1.CalculatePriceOverrideRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: simulatePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	calculatePriceOverrideReply, err := priceOverrideCalculation.CalculatePriceOverride(ctx, calculatePriceOverrideRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/CalculatePriceOverride")
	}
	return &pricingv1.CheckPriceQuoteEligibilityResponse{
		PriceQuote: &pricingv1.PriceQuote{
			Number: simulatePriceQuoteReply.GetPriceQuote().GetNumber(),
			Counterparty: &pricingv1.PriceListRef{
				Version: calculatePriceOverrideReply.GetPriceOverride().GetVersion(),
			},
			PriceQuoteId:   req.GetPriceQuoteId(),
			BillingAddress: req.GetPriceQuote().GetBillingAddress(),
		},
		Outcome: &pricingv1.CheckPriceQuoteEligibilityResponse_Outcome{Status: calculatePriceOverrideReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizePriceQuote normalizes a price quote.
func (s *PriceQuoteValidationServer) NormalizePriceQuote(ctx context.Context, req *pricingv1.NormalizePriceQuoteRequest) (*pricingv1.NormalizePriceQuoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizePriceQuote", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "normalize price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	quotePriceQuoteRequest := &pricingv1.QuotePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				PriceListId: req.GetPriceQuote().GetCounterparty().GetPriceListId(),
			},
		},
	}
	type quotePriceQuoteResult struct {
		reply *pricingv1.QuotePriceQuoteResponse
		err   error
	}
	quotePriceQuoteResults := make(chan quotePriceQuoteResult, 1)
	go func() {
		reply, err := s.priceQuoteCalculationClient.QuotePriceQuote(ctx, quotePriceQuoteRequest)
		quotePriceQuoteResults <- quotePriceQuoteResult{reply, err}
	}()
	var quotePriceQuoteReply *pricingv1.QuotePriceQuoteResponse
	select {
	case result := <-quotePriceQuoteResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.pricing.v1.PriceQuoteCalculationService/QuotePriceQuote")
		}
		quotePriceQuoteReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	estimatePaymentReply, err := s.paymentCalculationClient.EstimatePayment(ctx, &paymentsv1.EstimatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetPriceQuote().GetCounterparty().GetDisplayName(),
			},
			Settlement: &paymentsv1.Payment_Bank{Bank: &paymentsv1.PaymentBankSettlement{
				Iban: quotePriceQuoteReply.GetPriceQuote().GetBank().GetIban(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: quotePriceQuoteReply.GetPriceQuote().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Lines: quotePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetLines(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/EstimatePayment")
	}
	return &pricingv1.NormalizePriceQuoteResponse{
		PriceQuote: &pricingv1.PriceQuote{
			Metadata: quotePriceQuoteReply.GetPriceQuote().GetMetadata(),
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: estimatePaymentReply.GetPayment().GetBillingAddress().GetCountryCode(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			Number:       req.GetPriceQuote().GetNumber(),
		},
		Outcome: &pricingv1.NormalizePriceQuoteResponse_Outcome{Status: quotePriceQuoteReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: estimatePaymentReply.GetResult().GetStatus()},
	}, nil
}

// ExplainPriceQuoteDecision explains a price quote.
func (s *PriceQuoteValidationServer) ExplainPriceQuoteDecision(ctx context.Context, req *pricingv1.ExplainPriceQuoteDecisionRequest) (*pricingv1.ExplainPriceQuoteDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "explain price quote decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	checkPriceListEligibilityRequest := buildCheckPriceListEligibilityRequestForExplainPriceQuoteDecision(req)
	checkPriceListEligibilityReply, err := s.priceListValidationClient.CheckPriceListEligibility(ctx, checkPriceListEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/CheckPriceListEligibility")
	}
	explainPriceListDecisionReply, err := s.priceListValidationClient.ExplainPriceListDecision(ctx, &pricingv1.ExplainPriceListDecisionRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Action: &pricingv1.PriceList_Block{Block: &pricingv1.PriceListBlockAction{
				Reason: checkPriceListEligibilityReply.GetPriceList().GetBlock().GetReason(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/ExplainPriceListDecision")
	}
	previewPriceListReply, err := s.priceListValidationClient.PreviewPriceList(ctx, &pricingv1.PreviewPriceListRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				Etag: explainPriceListDecisionReply.GetPriceList().GetAudit().GetEtag(),
			},
		},
		Details: &pricingv1.PreviewPriceListRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/PreviewPriceList")
	}
	return &pricingv1.ExplainPriceQuoteDecisionResponse{
		Result: &commonv1.OperationResult{
			Details: checkPriceListEligibilityReply.GetResult().GetDetails(),
			Status:  explainPriceListDecisionReply.GetResult().GetStatus(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: previewPriceListReply.GetPriceList().GetAudit().GetUpdatedAt(),
			},
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: previewPriceListReply.GetPriceList().GetOwner().GetDisplayName(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			DueAt:        req.GetPriceQuote().GetDueAt(),
		},
	}, nil
}

// ScorePriceQuote scores a price quote.
func (s *PriceQuoteValidationServer) ScorePriceQuote(ctx context.Context, req *pricingv1.ScorePriceQuoteRequest) (*pricingv1.ScorePriceQuoteResponse, error) {
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "score price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	for _, line := range req.GetPriceQuote().GetLines() {
		if _, err := s.datasetClient.CloneDataset(ctx, &datasetsv1.CloneDatasetRequest{
			Context: req.GetContext(),
			Dataset: &datasetsv1.Dataset{
				Components: map[string]*datasetsv1.DatasetComponent{"primary": &datasetsv1.DatasetComponent{Description: line.GetDescription()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.datasets.v1.DatasetService/CloneDataset")
		}
	}
	validatePriceListRequest := &pricingv1.ValidatePriceListRequest{Context: req.GetContext()}
	validatePriceListReply, err := grpcx.NewClient(s.conn, pricingv1.NewPriceListValidationServiceClient).ValidatePriceList(ctx, validatePriceListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/ValidatePriceList")
	}
	return &pricingv1.ScorePriceQuoteResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: validatePriceListReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PriceQuote: &pricingv1.PriceQuote{
			PriceQuoteId: req.GetPriceQuoteId(),
			Total:        req.GetPriceQuote().GetTotal(),
		},
	}, nil
}

// LintPriceQuotes lints a price quote.
func (s *PriceQuoteValidationServer) LintPriceQuotes(ctx context.Context, req *pricingv1.LintPriceQuotesRequest) (*pricingv1.LintPriceQuotesResponse, error) {
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "lint price quotes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	source := req.GetPriceQuote().GetAudit().ProtoReflect()
	etag := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("etag"))).String()
	explainPriceListDecisionReply, err := s.priceListValidationClient.ExplainPriceListDecision(ctx, &pricingv1.ExplainPriceListDecisionRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPriceQuote().GetAudit().GetCreatedAt(),
				Etag:      etag,
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/ExplainPriceListDecision")
	}
	for line := range iterx.Each(req.GetPriceQuote().GetLines(), func(candidate *pricingv1.PriceQuoteLine) bool { return candidate != nil }) {
		if _, err := s.priceListValidationClient.LintPriceLists(ctx, &pricingv1.LintPriceListsRequest{
			Context: req.GetContext(),
			PriceList: &pricingv1.PriceList{
				Action: &pricingv1.PriceList_Discount{Discount: &pricingv1.PriceListDiscountAction{Amount: line.GetTax().GetAmount()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/LintPriceLists")
		}
	}
	return &pricingv1.LintPriceQuotesResponse{
		Outcome: &pricingv1.LintPriceQuotesResponse_Outcome{
			Status: explainPriceListDecisionReply.GetResult().GetStatus(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			PriceQuoteId: req.GetPriceQuoteId(),
			Metadata:     req.GetPriceQuote().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPriceQuoteRules returns a price quote.
func (s *PriceQuoteValidationServer) GetPriceQuoteRules(ctx context.Context, req *pricingv1.GetPriceQuoteRulesRequest) (*pricingv1.GetPriceQuoteRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_quote", req.GetPriceQuoteId(), "get_price_quote_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get price quote rules", "tenant", req.GetContext().GetTenantId())
	return &pricingv1.GetPriceQuoteRulesResponse{
		PriceQuote: &pricingv1.PriceQuote{PriceQuoteId: req.GetPriceQuoteId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}
