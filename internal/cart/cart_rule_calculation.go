package cart

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CartRuleCalculationServer implements `acme.cart.v1.CartRuleCalculationService`.
// Computes the figures of a cart rule, such as totals, taxes, and estimates.
type CartRuleCalculationServer struct {
	cartv1.UnimplementedCartRuleCalculationServiceServer
	store                        *Store
	log                          *slog.Logger
	cartValidationPort           cartValidationPort
	accessPolicyValidationClient authorizationv1.AccessPolicyValidationServiceClient
	cartValidationClient         cartv1.CartValidationServiceClient
	meteredCartValidation        meteredCartValidation
	conn                         *grpc.ClientConn
	clients                      *registry.Registry
	productReviewClient          reviewsv1.ProductReviewServiceClient
	searchQueryHistoryClient     searchv1.SearchQueryHistoryServiceClient
	onboardingPlanQueryOnce      sync.Once
	onboardingPlanQueryCached    onboardingv1.OnboardingPlanQueryServiceClient
}

// NewCartRuleCalculationServer wires a CartRuleCalculationServer.
func NewCartRuleCalculationServer(deps platform.Deps) *CartRuleCalculationServer {
	return &CartRuleCalculationServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.cart.v1.CartRuleCalculationService"),
		cartValidationPort:           newCartValidationPort(deps),
		accessPolicyValidationClient: authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		cartValidationClient:         cartv1.NewCartValidationServiceClient(deps.Conn),
		meteredCartValidation:        meteredCartValidation{CartValidationServiceClient: cartv1.NewCartValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                         deps.Conn,
		clients:                      deps.Clients,
		productReviewClient:          reviewsv1.NewProductReviewServiceClient(deps.Conn),
		searchQueryHistoryClient:     searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
	}
}

// CalculateCartRule calculates a cart rule.
func (s *CartRuleCalculationServer) CalculateCartRule(ctx context.Context, req *cartv1.CalculateCartRuleRequest) (*cartv1.CalculateCartRuleResponse, error) {
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "calculate cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	source := req.GetDetails().GetDestination().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	ctx = tenancy.With(ctx, "country_code", req.GetDetails().GetDestination().GetCountryCode())
	previewCartRequest := &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				Region:      region,
				PostalCode:  req.GetDetails().GetDestination().GetPostalCode(),
				CountryCode: tenancy.Value(ctx, "country_code"),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetCartRule().GetAudit().GetEtag()},
		},
	}
	previewCartReply, err := s.cartValidationPort.PreviewCart(ctx, previewCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	normalizeAccessPolicyRequest := &authorizationv1.NormalizeAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Action: &authorizationv1.AccessPolicy_Route{Route: &authorizationv1.AccessPolicyRouteAction{Weight: req.GetCartRule().GetRoute().GetWeight()}},
		},
	}
	normalizeAccessPolicyReply, err := authorizationv1.AccessPolicyValidationServiceClient.NormalizeAccessPolicy(s.accessPolicyValidationClient, ctx, normalizeAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/NormalizeAccessPolicy")
	}
	return &cartv1.CalculateCartRuleResponse{
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				DisplayName: previewCartReply.GetCart().GetCounterparty().GetDisplayName(),
				UserId:      normalizeAccessPolicyReply.GetAccessPolicy().GetOwner().GetUserId(),
			},
			CartRuleId: req.GetCartRuleId(),
			Parameters: req.GetCartRule().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			Details: previewCartReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// EstimateCartRule estimates a cart rule.
func (s *CartRuleCalculationServer) EstimateCartRule(ctx context.Context, req *cartv1.EstimateCartRuleRequest) (*cartv1.EstimateCartRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "estimate cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	checkCartEligibilityRequest := buildCheckCartEligibilityRequestForEstimateCartRule(req)
	checkCartEligibilityReply, err := s.cartValidationClient.CheckCartEligibility(ctx, checkCartEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/CheckCartEligibility")
	}
	scoreCartRequest := &cartv1.ScoreCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Lines: []*cartv1.CartLine{&cartv1.CartLine{
				Tax: &cartv1.CartLineTax{Amount: req.GetCartRule().GetDiscount().GetAmount()},
			}},
			Counterparty: &cartv1.SavedListRef{
				DisplayName: checkCartEligibilityReply.GetCart().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkCartEligibilityReply.GetCart().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &cartv1.ScoreCartRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: checkCartEligibilityReply.GetCart().GetBillingAddress().GetLines(),
			},
		},
	}
	scoreCartReply, err := s.meteredCartValidation.ScoreCart(ctx, scoreCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/ScoreCart")
	}
	return &cartv1.EstimateCartRuleResponse{
		Result: &commonv1.OperationResult{
			Status:  checkCartEligibilityReply.GetResult().GetStatus(),
			Message: checkCartEligibilityReply.GetResult().GetMessage(),
		},
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreCartReply.GetCart().GetAudit().GetCreatedBy(),
			},
			CartRuleId: req.GetCartRuleId(),
		},
		Breakdown: &cartv1.EstimateCartRuleResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// QuoteCartRule quotes a cart rule.
func (s *CartRuleCalculationServer) QuoteCartRule(ctx context.Context, req *cartv1.QuoteCartRuleRequest) (*cartv1.QuoteCartRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteCartRule", "error", err)
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "quote cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	var holder struct{ PostalCode string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PostalCode"); err != nil {
			s.log.WarnContext(ctx, "copy postal_code", "error", err)
		}
	}
	validateCartRequest := &cartv1.ValidateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: holder.PostalCode,
				Lines:      req.GetDetails().GetDestination().GetLines(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetCartRule().GetAudit().GetEtag()},
		},
	}
	validateCartReply, err := grpcx.NewClient(s.conn, cartv1.NewCartValidationServiceClient).ValidateCart(ctx, validateCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/ValidateCart")
	}
	checkCartEligibilityRequest := &cartv1.CheckCartEligibilityRequest{
		Context: req.GetContext(),
		Details: &cartv1.CheckCartEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetCartRule().GetOwner().GetEmail()},
		},
	}
	checkCartEligibilityReply, err := s.meteredCartValidation.CheckCartEligibility(ctx, checkCartEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/CheckCartEligibility")
	}
	cartCalculation, err := registry.Lookup[cartv1.CartCalculationServiceClient](s.clients, "acme.cart.v1.CartCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	calculateCartRequest := &cartv1.CalculateCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Lines: []*cartv1.CartLine{&cartv1.CartLine{
				Tax: &cartv1.CartLineTax{Amount: req.GetCartRule().GetDiscount().GetAmount()},
			}},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
			Settlement:     &cartv1.Cart_Card{Card: &cartv1.CartCardSettlement{Last4: validateCartReply.GetCart().GetCard().GetLast4()}},
		},
	}
	calculateCartReply, err := cartCalculation.CalculateCart(ctx, calculateCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/CalculateCart")
	}
	watchProductReviewsStream, err := s.productReviewClient.WatchProductReviews(ctx, &reviewsv1.WatchProductReviewsRequest{
		Context: req.GetContext(),
		Filter: &reviewsv1.WatchProductReviewsRequest_Filter{
			Region:       validateCartReply.GetCart().GetBillingAddress().GetRegion(),
			CreatedAfter: checkCartEligibilityReply.GetCart().GetAudit().GetUpdatedAt(),
		},
		Subscriber: &commonv1.ContactInfo{Email: req.GetCartRule().GetOwner().GetEmail()},
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
	return &cartv1.QuoteCartRuleResponse{
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validateCartReply.GetCart().GetAudit().GetUpdatedAt(),
				UpdatedBy: validateCartReply.GetCart().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: calculateCartReply.GetCart().GetCounterparty().GetDisplayName(),
			},
			CartRuleId: req.GetCartRuleId(),
		},
		Breakdown: &cartv1.QuoteCartRuleResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareCartRuleOptions compares a cart rule.
func (s *CartRuleCalculationServer) CompareCartRuleOptions(ctx context.Context, req *cartv1.CompareCartRuleOptionsRequest) (*cartv1.CompareCartRuleOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "compare cart rule options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	scoreCartRequest := &cartv1.ScoreCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Audit:          &commonv1.AuditInfo{UpdatedAt: req.GetCartRule().GetAudit().GetUpdatedAt()},
			BillingAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
			Lines:          []*cartv1.CartLine{&cartv1.CartLine{Amount: req.GetCartRule().GetDiscount().GetAmount()}},
		},
	}
	scoreCartReply, err := grpcx.Call(ctx, s.cartValidationClient.ScoreCart, scoreCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/ScoreCart")
	}
	var holder struct{ Version int64 }
	if record := req.GetCartRule(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Version"); err != nil {
			s.log.WarnContext(ctx, "copy version", "error", err)
		}
	}
	var reason string
	switch choice := req.GetCartRule().GetAction().(type) {
	case *cartv1.CartRule_Route:
		reason = choice.Route.GetTarget()
	case *cartv1.CartRule_Block:
		reason = choice.Block.GetReason()
	}
	recordSearchQueryVersionRequest := &searchv1.RecordSearchQueryVersionRequest{}
	recordSearchQueryVersionRequest.Context = req.GetContext()
	recordSearchQueryVersionRequest.SearchQuery = &searchv1.SearchQuery{
		Subject: &searchv1.SearchIndexRef{
			DisplayName: scoreCartReply.GetCart().GetCounterparty().GetDisplayName(),
		},
		CorrelationId: scoreCartReply.GetResult().GetCorrelationId(),
	}
	recordSearchQueryVersionRequest.Details = &searchv1.RecordSearchQueryVersionRequest_Details{Version: holder.Version}
	recordSearchQueryVersionRequest.Reason = reason
	recordSearchQueryVersionReply, err := s.searchQueryHistoryClient.RecordSearchQueryVersion(ctx, recordSearchQueryVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RecordSearchQueryVersion")
	}
	return &cartv1.CompareCartRuleOptionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreCartReply.GetResult().GetReferenceIds(),
			Message:      recordSearchQueryVersionReply.GetResult().GetMessage(),
			Details:      recordSearchQueryVersionReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Name:       req.GetCartRule().GetName(),
		},
	}, nil
}

// SimulateCartRule simulates a cart rule.
func (s *CartRuleCalculationServer) SimulateCartRule(ctx context.Context, req *cartv1.SimulateCartRuleRequest) (*cartv1.SimulateCartRuleResponse, error) {
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "simulate cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	previewCartRequest := &cartv1.PreviewCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Lines: []*cartv1.CartLine{&cartv1.CartLine{Amount: req.GetCartRule().GetDiscount().GetAmount()}},
		},
	}
	send := s.previewCartSender(previewCartRequest)
	previewCartReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/PreviewCart")
	}
	sellerListingValidation, ok := s.clients.Get("acme.sellers.v1.SellerListingValidationService").(sellersv1.SellerListingValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerListingValidationService")
	}
	ctx = tenancy.With(ctx, "etag", req.GetCartRule().GetAudit().GetEtag())
	scoreSellerListingRequest := &sellersv1.ScoreSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Recipient:   previewCartReply.GetCart().GetBillingAddress().GetRecipient(),
				CountryCode: previewCartReply.GetCart().GetBillingAddress().GetCountryCode(),
			}},
			Audit: &commonv1.AuditInfo{Etag: tenancy.Value(ctx, "etag")},
		},
		Details: &sellersv1.ScoreSellerListingRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: previewCartReply.GetCart().GetBillingAddress().GetRegion(),
			},
		},
	}
	scoreSellerListingReply, err := sellerListingValidation.ScoreSellerListing(ctx, scoreSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/ScoreSellerListing")
	}
	return &cartv1.SimulateCartRuleResponse{
		Result: &commonv1.OperationResult{
			Status:       previewCartReply.GetResult().GetStatus(),
			Message:      previewCartReply.GetResult().GetMessage(),
			ReferenceIds: scoreSellerListingReply.GetResult().GetReferenceIds(),
		},
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreSellerListingReply.GetSellerListing().GetAudit().GetUpdatedAt(),
			},
			CartRuleId: req.GetCartRuleId(),
			Parameters: req.GetCartRule().GetParameters(),
		},
	}, nil
}

// ExplainCartRuleCalculation explains a cart rule.
func (s *CartRuleCalculationServer) ExplainCartRuleCalculation(ctx context.Context, req *cartv1.ExplainCartRuleCalculationRequest) (*cartv1.ExplainCartRuleCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainCartRuleCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "explain cart rule calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	findOnboardingPlansRequest := &onboardingv1.FindOnboardingPlansRequest{
		Context: req.GetContext(),
		Filter: &onboardingv1.FindOnboardingPlansRequest_Filter{
			CreatedAfter: req.GetCartRule().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetCartRule().GetCartRuleId(),
			Labels:       req.GetAnnotations(),
		},
	}
	findOnboardingPlansReply, err := s.onboardingPlanQueryLazy().FindOnboardingPlans(ctx, findOnboardingPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.onboarding.v1.OnboardingPlanQueryService/FindOnboardingPlans")
	}
	lintCartsRequest := &cartv1.LintCartsRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Lines: []*cartv1.CartLine{&cartv1.CartLine{
				Tax: &cartv1.CartLineTax{Amount: req.GetCartRule().GetDiscount().GetAmount()},
			}},
			Counterparty: &cartv1.SavedListRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()},
		},
		Details: &cartv1.LintCartsRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	var lintCartsReply *cartv1.LintCartsResponse
	if err := pipeline.Run(ctx, "acme.cart.v1.CartValidationService/LintCarts", func(ctx context.Context) error {
		answer, err := s.cartValidationClient.LintCarts(ctx, lintCartsRequest)
		lintCartsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/LintCarts")
	}
	return &cartv1.ExplainCartRuleCalculationResponse{
		Result:  &commonv1.OperationResult{Status: findOnboardingPlansReply.GetResult().GetStatus()},
		Outcome: &cartv1.ExplainCartRuleCalculationResponse_Outcome{NextSteps: lintCartsReply.GetOutcome().GetNextSteps()},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Name:       req.GetCartRule().GetName(),
		},
	}, nil
}

// previewCartSender returns a function that sends one request to acme.cart.v1.CartValidationService/PreviewCart.
func (s *CartRuleCalculationServer) previewCartSender(request *cartv1.PreviewCartRequest) func(context.Context) (*cartv1.PreviewCartResponse, error) {
	return func(ctx context.Context) (*cartv1.PreviewCartResponse, error) {
		return s.cartValidationClient.PreviewCart(ctx, request)
	}
}

// onboardingPlanQueryLazy dials acme.onboarding.v1.OnboardingPlanQueryService the first time a handler needs it.
func (s *CartRuleCalculationServer) onboardingPlanQueryLazy() onboardingv1.OnboardingPlanQueryServiceClient {
	s.onboardingPlanQueryOnce.Do(func() { s.onboardingPlanQueryCached = onboardingv1.NewOnboardingPlanQueryServiceClient(s.conn) })
	return s.onboardingPlanQueryCached
}
