package cart

import (
	"context"
	"fmt"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// CartRuleValidationServer implements `acme.cart.v1.CartRuleValidationService`.
// Checks cart rules against the rules before anything acts on them.
type CartRuleValidationServer struct {
	cartv1.UnsafeCartRuleValidationServiceServer
	store                      *Store
	log                        *slog.Logger
	returnLabelValidationPort  returnLabelValidationPort
	conn                       *grpc.ClientConn
	parcelCalculationClient    shippingv1.ParcelCalculationServiceClient
	clients                    *registry.Registry
	cartValidationClient       cartv1.CartValidationServiceClient
	loyaltyMemberClient        loyaltyv1.LoyaltyMemberServiceClient
	watchlistCalculationClient fraudv1.WatchlistCalculationServiceClient
	cartValidationPort         cartValidationPort
	bus                        *eventbus.Bus
}

// NewCartRuleValidationServer wires a CartRuleValidationServer.
func NewCartRuleValidationServer(deps platform.Deps) *CartRuleValidationServer {
	return &CartRuleValidationServer{
		store:                      NewStore(deps.DB),
		log:                        deps.Logger.With("service", "acme.cart.v1.CartRuleValidationService"),
		returnLabelValidationPort:  newReturnLabelValidationPort(deps),
		conn:                       deps.Conn,
		parcelCalculationClient:    shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		clients:                    deps.Clients,
		cartValidationClient:       cartv1.NewCartValidationServiceClient(deps.Conn),
		loyaltyMemberClient:        loyaltyv1.NewLoyaltyMemberServiceClient(deps.Conn),
		watchlistCalculationClient: fraudv1.NewWatchlistCalculationServiceClient(deps.Conn),
		cartValidationPort:         newCartValidationPort(deps),
		bus:                        deps.Bus,
	}
}

// ValidateCartRule validates a cart rule.
func (s *CartRuleValidationServer) ValidateCartRule(ctx context.Context, req *cartv1.ValidateCartRuleRequest) (*cartv1.ValidateCartRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "validate cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	validateReturnLabelRequest := &returnsv1.ValidateReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Lines: []*returnsv1.ReturnLabelLine{&returnsv1.ReturnLabelLine{Amount: req.GetCartRule().GetDiscount().GetAmount()}},
		},
	}
	validateReturnLabelReply, err := s.returnLabelValidationPort.ValidateReturnLabel(ctx, validateReturnLabelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelValidationService/ValidateReturnLabel")
	}
	previewCartRequest := &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Lines: []*cartv1.CartLine{&cartv1.CartLine{
				Tax: &cartv1.CartLineTax{Amount: req.GetCartRule().GetDiscount().GetAmount()},
			}},
			Counterparty: &cartv1.SavedListRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()},
			Audit:        &commonv1.AuditInfo{Etag: req.GetCartRule().GetAudit().GetEtag()},
		},
		Details: &cartv1.PreviewCartRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	previewCartReply := new(cartv1.PreviewCartResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", cartCartValidationServiceName, "PreviewCart"), previewCartRequest, previewCartReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	return &cartv1.ValidateCartRuleResponse{
		Result: &commonv1.OperationResult{
			Details:       validateReturnLabelReply.GetResult().GetDetails(),
			CorrelationId: previewCartReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Effective:  req.GetCartRule().GetEffective(),
		},
	}, nil
}

// PreviewCartRule previews a cart rule.
func (s *CartRuleValidationServer) PreviewCartRule(ctx context.Context, req *cartv1.PreviewCartRuleRequest) (*cartv1.PreviewCartRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "preview cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	simulateParcelRequest := buildSimulateParcelRequestForPreviewCartRule(req)
	simulateParcelReply, err := s.parcelCalculationClient.SimulateParcel(ctx, simulateParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/SimulateParcel")
	}
	return &cartv1.PreviewCartRuleResponse{
		CartRule: &cartv1.CartRule{
			Description: simulateParcelReply.GetParcel().GetComponents()["primary"].GetDescription(),
			CartRuleId:  req.GetCartRuleId(),
			Name:        req.GetCartRule().GetName(),
		},
		Outcome: &cartv1.PreviewCartRuleResponse_Outcome{Message: simulateParcelReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckCartRuleEligibility checks a cart rule.
func (s *CartRuleValidationServer) CheckCartRuleEligibility(ctx context.Context, req *cartv1.CheckCartRuleEligibilityRequest) (*cartv1.CheckCartRuleEligibilityResponse, error) {
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "check cart rule eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	cartValidation, err := registry.Lookup[cartv1.CartValidationServiceClient](s.clients, "acme.cart.v1.CartValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	lintCartsRequest := &cartv1.LintCartsRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Audit:        &commonv1.AuditInfo{CreatedBy: req.GetCartRule().GetAudit().GetCreatedBy()},
			Counterparty: &cartv1.SavedListRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()},
		},
		Details: &cartv1.LintCartsRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	lintCartsReply, err := cartValidation.LintCarts(ctx, lintCartsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/LintCarts")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := cartRuleValidationServerNormalizeCartSteps[mode]
	normalizeCartRequest := &cartv1.NormalizeCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Settlement: &cartv1.Cart_Bank{Bank: &cartv1.CartBankSettlement{Bic: lintCartsReply.GetCart().GetBank().GetBic()}},
		},
	}
	normalizeCartReply, err := run(s, ctx, normalizeCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/NormalizeCart")
	}
	return &cartv1.CheckCartRuleEligibilityResponse{
		Outcome: &cartv1.CheckCartRuleEligibilityResponse_Outcome{ReferenceId: lintCartsReply.GetOutcome().GetReferenceId()},
		Result: &commonv1.OperationResult{
			CorrelationId: normalizeCartReply.GetResult().GetCorrelationId(),
			Details:       normalizeCartReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Version:    req.GetCartRule().GetVersion(),
		},
	}, nil
}

// NormalizeCartRule normalizes a cart rule.
func (s *CartRuleValidationServer) NormalizeCartRule(ctx context.Context, req *cartv1.NormalizeCartRuleRequest) (*cartv1.NormalizeCartRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "normalize cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	validateCartRequest := &cartv1.ValidateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetCartRule().GetAudit().GetCreatedAt()},
		},
	}
	validateCartReply := new(cartv1.ValidateCartResponse)
	if err := grpc.Invoke(ctx, "/acme.cart.v1.CartValidationService/ValidateCart", validateCartRequest, validateCartReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/ValidateCart")
	}
	updateLoyaltyMemberRequest := buildUpdateLoyaltyMemberRequestForNormalizeCartRule(req)
	updateLoyaltyMemberReply, err := s.loyaltyMemberClient.UpdateLoyaltyMember(ctx, updateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberService/UpdateLoyaltyMember")
	}
	return &cartv1.NormalizeCartRuleResponse{
		Outcome: &cartv1.NormalizeCartRuleResponse_Outcome{NextSteps: validateCartReply.GetOutcome().GetNextSteps()},
		Result: &commonv1.OperationResult{
			CorrelationId: updateLoyaltyMemberReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Effective:  req.GetCartRule().GetEffective(),
		},
	}, nil
}

// ExplainCartRuleDecision explains a cart rule.
func (s *CartRuleValidationServer) ExplainCartRuleDecision(ctx context.Context, req *cartv1.ExplainCartRuleDecisionRequest) (*cartv1.ExplainCartRuleDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "explain cart rule decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	estimateWatchlistRequest := buildEstimateWatchlistRequestForExplainCartRuleDecision(req)
	estimateWatchlistReply, err := s.watchlistCalculationClient.EstimateWatchlist(ctx, estimateWatchlistRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/EstimateWatchlist")
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	previewCartRequest := &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetCartRule().GetAudit().GetEtag(),
				CreatedBy: estimateWatchlistReply.GetWatchlist().GetAudit().GetCreatedBy(),
			},
			Counterparty: &cartv1.SavedListRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()},
		},
		Details: &cartv1.PreviewCartRequest_Details{
			Contact: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
		},
	}
	previewCartReply, err := s.cartValidationPort.PreviewCart(ctx, previewCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	normalizeCartRequest := &cartv1.NormalizeCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: estimateWatchlistReply.GetWatchlist().GetLocation().GetCountryCode(),
			},
			Lines: []*cartv1.CartLine{&cartv1.CartLine{Amount: req.GetCartRule().GetDiscount().GetAmount()}},
		},
	}
	normalizeCartReply := new(cartv1.NormalizeCartResponse)
	if err := grpcx.Invoke(ctx, s.conn, cartv1.CartValidationService_NormalizeCart_FullMethodName, normalizeCartRequest, normalizeCartReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/NormalizeCart")
	}
	return &cartv1.ExplainCartRuleDecisionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateWatchlistReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimateWatchlistReply.GetWatchlist().GetAudit().GetCreatedAt(),
				UpdatedAt: previewCartReply.GetCart().GetAudit().GetUpdatedAt(),
			},
			CartRuleId: req.GetCartRuleId(),
			Version:    req.GetCartRule().GetVersion(),
		},
		Outcome: &cartv1.ExplainCartRuleDecisionResponse_Outcome{
			ReferenceId: previewCartReply.GetOutcome().GetReferenceId(),
			Status:      normalizeCartReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// ScoreCartRule scores a cart rule.
func (s *CartRuleValidationServer) ScoreCartRule(ctx context.Context, req *cartv1.ScoreCartRuleRequest) (*cartv1.ScoreCartRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "score cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	var holder struct{ Locality string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Locality"); err != nil {
			s.log.WarnContext(ctx, "copy locality", "error", err)
		}
	}
	normalizeCartRequest := newNormalizeCartRequest(withNormalizeCartContext(req.GetContext()), withNormalizeCartCart(&cartv1.Cart{
		BillingAddress: &commonv1.PostalAddress{Locality: holder.Locality},
		Audit:          &commonv1.AuditInfo{UpdatedBy: req.GetCartRule().GetAudit().GetUpdatedBy()},
		Lines:          []*cartv1.CartLine{&cartv1.CartLine{Amount: req.GetCartRule().GetDiscount().GetAmount()}},
		Counterparty:   &cartv1.SavedListRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()},
	}))
	normalizeCartReply, err := s.cartValidationClient.NormalizeCart(ctx, normalizeCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/NormalizeCart")
	}
	return &cartv1.ScoreCartRuleResponse{
		Result: &commonv1.OperationResult{
			Details: normalizeCartReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Name:       req.GetCartRule().GetName(),
		},
	}, nil
}

// LintCartRules lints a cart rule.
func (s *CartRuleValidationServer) LintCartRules(ctx context.Context, req *cartv1.LintCartRulesRequest) (*cartv1.LintCartRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintCartRules", "error", err)
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "lint cart rules", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded cartv1.LintCartRulesRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	lintInspectionsRequest := &returnsv1.LintInspectionsRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetCartRule().GetAudit().GetUpdatedBy(),
				Etag:      req.GetCartRule().GetAudit().GetEtag(),
			},
			Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()}},
		},
		Details: &returnsv1.LintInspectionsRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: (&decoded).GetContact().GetPhone()},
		},
	}
	lintInspectionsReply := new(returnsv1.LintInspectionsResponse)
	if err := s.conn.Invoke(ctx, returnsInspectionValidationServiceLintInspectionsMethod, lintInspectionsRequest, lintInspectionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionValidationService/LintInspections")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "cart.lint_cart_rules.summarize_stock_reservations",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"reason":            req.GetCartRule().GetBlock().GetReason(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &cartv1.LintCartRulesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: lintInspectionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				DisplayName: lintInspectionsReply.GetInspection().GetAssignee().GetDisplayName(),
			},
			CartRuleId: req.GetCartRuleId(),
			Priority:   req.GetCartRule().GetPriority(),
		},
	}, nil
}

// cartRuleValidationServerNormalizeCartSteps are the ways to run acme.cart.v1.CartValidationService/NormalizeCart, by
// name.
var cartRuleValidationServerNormalizeCartSteps = map[string]func(*CartRuleValidationServer, context.Context, *cartv1.NormalizeCartRequest) (*cartv1.NormalizeCartResponse, error){
	"default": (*CartRuleValidationServer).normalizeCartStep,
	"audited": (*CartRuleValidationServer).normalizeCartAuditedStep,
}

func (s *CartRuleValidationServer) normalizeCartStep(ctx context.Context, request *cartv1.NormalizeCartRequest) (*cartv1.NormalizeCartResponse, error) {
	return s.cartValidationClient.NormalizeCart(ctx, request)
}

func (s *CartRuleValidationServer) normalizeCartAuditedStep(ctx context.Context, request *cartv1.NormalizeCartRequest) (*cartv1.NormalizeCartResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.cart.v1.CartValidationService/NormalizeCart")
	return s.cartValidationClient.NormalizeCart(ctx, request)
}
