package cart

import (
	"context"
	"fmt"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CartQueryServer implements `acme.cart.v1.CartQueryService`.
// Answers read-only questions about carts, for screens and reports.
type CartQueryServer struct {
	cartv1.UnimplementedCartQueryServiceServer
	store                     *Store
	log                       *slog.Logger
	cartValidationClient      cartv1.CartValidationServiceClient
	tenantExportClient        identityv1.TenantExportServiceClient
	clients                   *registry.Registry
	conn                      *grpc.ClientConn
	sessionValidationClient   identityv1.SessionValidationServiceClient
	cartRuleCalculationClient cartv1.CartRuleCalculationServiceClient
	cartCalculationClient     cartv1.CartCalculationServiceClient
}

// NewCartQueryServer wires a CartQueryServer.
func NewCartQueryServer(deps platform.Deps) *CartQueryServer {
	return &CartQueryServer{
		store:                     NewStore(deps.DB),
		log:                       deps.Logger.With("service", "acme.cart.v1.CartQueryService"),
		cartValidationClient:      cartv1.NewCartValidationServiceClient(deps.Conn),
		tenantExportClient:        identityv1.NewTenantExportServiceClient(deps.Conn),
		clients:                   deps.Clients,
		conn:                      deps.Conn,
		sessionValidationClient:   identityv1.NewSessionValidationServiceClient(deps.Conn),
		cartRuleCalculationClient: cartv1.NewCartRuleCalculationServiceClient(deps.Conn),
		cartCalculationClient:     cartv1.NewCartCalculationServiceClient(deps.Conn),
	}
}

// LookupCart returns a cart.
func (s *CartQueryServer) LookupCart(ctx context.Context, req *cartv1.LookupCartRequest) (*cartv1.LookupCartResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupCart", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart", req.GetCartId(), "lookup_cart"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup cart", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "cart_id", req.GetCartId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	cloned2 := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	lintCartsRequest := &cartv1.LintCartsRequest{
		Context: req.GetContext(),
		CartId:  tenancy.Value(ctx, "cart_id"),
		Details: &cartv1.LintCartsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  cloned.GetLocale(),
		},
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{UpdatedAt: cloned2.GetDeadline()},
		},
	}
	lintCartsReply, err := grpcx.Call(ctx, s.cartValidationClient.LintCarts, lintCartsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/LintCarts")
	}
	return &cartv1.LookupCartResponse{
		Cart:   &cartv1.Cart{CartId: lintCartsReply.GetCart().GetCartId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseCarts lists carts a page at a time.
func (s *CartQueryServer) BrowseCarts(ctx context.Context, req *cartv1.BrowseCartsRequest) (*cartv1.BrowseCartsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse carts", "tenant", req.GetContext().GetTenantId())
	cancelTenantExportReply, err := s.tenantExportClient.CancelTenantExport(ctx, &identityv1.CancelTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.CancelTenantExportRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			NewOwner:      &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/CancelTenantExport")
	}
	cartRuleValidation, err := registry.Lookup[cartv1.CartRuleValidationServiceClient](s.clients, "acme.cart.v1.CartRuleValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	validateCartRuleRequest := &cartv1.ValidateCartRuleRequest{
		Context: req.GetContext(),
		Details: &cartv1.ValidateCartRuleRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: cancelTenantExportReply.GetTenant().GetManager().GetEmail(),
			},
		},
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cancelTenantExportReply.GetTenant().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: cancelTenantExportReply.GetTenant().GetManager().GetDisplayName(),
				UserId:      cancelTenantExportReply.GetTenant().GetManager().GetUserId(),
			},
		},
	}
	validateCartRuleReply, err := cartRuleValidation.ValidateCartRule(ctx, validateCartRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/ValidateCartRule")
	}
	return &cartv1.BrowseCartsResponse{
		Carts: []*cartv1.Cart{&cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				Locality: cancelTenantExportReply.GetTenant().GetLocation().GetLocality(),
			},
			Lines: []*cartv1.CartLine{&cartv1.CartLine{
				Attributes: cancelTenantExportReply.GetTenant().GetAttributes(),
			}},
		}},
		Result: &commonv1.OperationResult{Status: validateCartRuleReply.GetResult().GetStatus()},
	}, nil
}

// ResolveCarts returns several carts at once.
func (s *CartQueryServer) ResolveCarts(ctx context.Context, req *cartv1.ResolveCartsRequest) (*cartv1.ResolveCartsResponse, error) {
	s.log.InfoContext(ctx, "resolve carts", "tenant", req.GetContext().GetTenantId())
	return &cartv1.ResolveCartsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportCarts streams every matching cart.
func (s *CartQueryServer) ExportCarts(req *cartv1.ExportCartsRequest, stream grpc.ServerStreamingServer[cartv1.ExportCartsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExportCarts", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "export carts", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	calculateCartRequest := &cartv1.CalculateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
			Audit:          &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
			Metadata:       labels,
		},
	}
	calculateCartReply := new(cartv1.CalculateCartResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", cartCartCalculationServiceName, "CalculateCart"), calculateCartRequest, calculateCartReply); err != nil {
		return grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/CalculateCart")
	}
	if err := stream.Send(&cartv1.ExportCartsResponse{
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateCartReply.GetCart().GetAudit().GetUpdatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: calculateCartReply.GetResult().GetStatus()},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeCarts summarizes a cart.
func (s *CartQueryServer) SummarizeCarts(ctx context.Context, req *cartv1.SummarizeCartsRequest) (*cartv1.SummarizeCartsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart", req.GetCartId(), "summarize_carts"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize carts", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	previewSessionRequest := &identityv1.PreviewSessionRequest{}
	previewSessionRequest.Context = req.GetContext()
	previewSessionRequest.Annotations = annotations
	previewSessionRequest.Session = &identityv1.Session{
		Requester: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
		Assignee:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
	}
	previewSessionReply, err := s.sessionValidationClient.PreviewSession(ctx, previewSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/PreviewSession")
	}
	validateCartRequest := &cartv1.ValidateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{
				DisplayName: previewSessionReply.GetSession().GetAssignee().GetDisplayName(),
			},
		},
		Details: &cartv1.ValidateCartRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: previewSessionReply.GetSession().GetRequester().GetEmail(),
				Name:  previewSessionReply.GetSession().GetRequester().GetName(),
			},
		},
	}
	validateCartReply, err := cartv1.CartValidationServiceClient.ValidateCart(s.cartValidationClient, ctx, validateCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/ValidateCart")
	}
	_ = validateCartReply
	return &cartv1.SummarizeCartsResponse{
		Result: &commonv1.OperationResult{
			Message: previewSessionReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestCarts lists carts a page at a time.
func (s *CartQueryServer) SuggestCarts(ctx context.Context, req *cartv1.SuggestCartsRequest) (*cartv1.SuggestCartsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestCarts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest carts", "tenant", req.GetContext().GetTenantId())
	previewCartRequest := buildPreviewCartRequestForSuggestCarts(req)
	previewCartReply, err := s.cartValidationClient.PreviewCart(ctx, previewCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	quoteCartRuleRequest := &cartv1.QuoteCartRuleRequest{
		Context: req.GetContext(),
		Details: &cartv1.QuoteCartRuleRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: previewCartReply.GetCart().GetBillingAddress().GetRecipient(),
			},
		},
	}
	quoteCartRuleReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*cartv1.QuoteCartRuleResponse, error) {
		return s.cartRuleCalculationClient.QuoteCartRule(ctx, quoteCartRuleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/QuoteCartRule")
	}
	return &cartv1.SuggestCartsResponse{
		Carts: []*cartv1.Cart{&cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{
				SavedListId: previewCartReply.GetCart().GetCounterparty().GetSavedListId(),
			},
		}},
		Result: &commonv1.OperationResult{
			Details: quoteCartRuleReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// PeekCart returns a cart.
func (s *CartQueryServer) PeekCart(ctx context.Context, req *cartv1.PeekCartRequest) (*cartv1.PeekCartResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart", req.GetCartId(), "peek_cart"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek cart", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	quoteCartRequest := &cartv1.QuoteCartRequest{
		Context: req.GetContext(),
		CartId:  req.GetCartId(),
		Cart: &cartv1.Cart{
			DueAt: req.GetContext().GetDeadline(),
			Lines: []*cartv1.CartLine{&cartv1.CartLine{Attributes: baggage}},
		},
	}
	quoteCartReply, err := grpcx.Call(ctx, s.cartCalculationClient.QuoteCart, quoteCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/QuoteCart")
	}
	return &cartv1.PeekCartResponse{
		Cart: &cartv1.Cart{
			Metadata: quoteCartReply.GetCart().GetMetadata(),
			CartId:   req.GetCartId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
