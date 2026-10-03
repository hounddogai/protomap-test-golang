package cart

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	boxofficev1 "github.com/hounddogai/protomap-test-golang/gen/acme/boxoffice/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	maintenancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// CartCalculationServer implements `acme.cart.v1.CartCalculationService`.
// Computes the figures of a cart, such as totals, taxes, and estimates.
type CartCalculationServer struct {
	cartv1.UnimplementedCartCalculationServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	scoreOrderFn                    func(context.Context, *ordersv1.ScoreOrderRequest, ...grpc.CallOption) (*ordersv1.ScoreOrderResponse, error)
	workOrderCalculationClient      maintenancev1.WorkOrderCalculationServiceClient
	productReviewValidationClient   reviewsv1.ProductReviewValidationServiceClient
	refundQueryClient               paymentsv1.RefundQueryServiceClient
	refundQueryClientFallback       paymentsv1.RefundQueryServiceClient
	shippingQuoteQueryClient        checkoutv1.ShippingQuoteQueryServiceClient
	orderValidationClient           ordersv1.OrderValidationServiceClient
	carrierRateCalculationClient    shippingv1.CarrierRateCalculationServiceClient
	previewSellerListingFn          func(context.Context, *sellersv1.PreviewSellerListingRequest, ...grpc.CallOption) (*sellersv1.PreviewSellerListingResponse, error)
	riskAssessmentValidationClient  fraudv1.RiskAssessmentValidationServiceClient
	orderValidationClientFallback   ordersv1.OrderValidationServiceClient
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
	boxOfficeSaleCalculationClient  boxofficev1.BoxOfficeSaleCalculationServiceClient
	sellerListingValidationClient   sellersv1.SellerListingValidationServiceClient
	sellerScorecardValidationClient sellersv1.SellerScorecardValidationServiceClient
	previewSessionFn                func(context.Context, *identityv1.PreviewSessionRequest, ...grpc.CallOption) (*identityv1.PreviewSessionResponse, error)
}

// NewCartCalculationServer wires a CartCalculationServer.
func NewCartCalculationServer(deps platform.Deps) *CartCalculationServer {
	orderValidation := ordersv1.NewOrderValidationServiceClient(deps.Conn)
	sellerListingValidation := sellersv1.NewSellerListingValidationServiceClient(deps.Conn)
	sessionValidation := identityv1.NewSessionValidationServiceClient(deps.Conn)
	return &CartCalculationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.cart.v1.CartCalculationService"),
		conn:                            deps.Conn,
		scoreOrderFn:                    orderValidation.ScoreOrder,
		workOrderCalculationClient:      maintenancev1.NewWorkOrderCalculationServiceClient(deps.Conn),
		productReviewValidationClient:   reviewsv1.NewProductReviewValidationServiceClient(deps.Conn),
		refundQueryClient:               paymentsv1.NewRefundQueryServiceClient(deps.Conn),
		refundQueryClientFallback:       paymentsv1.NewRefundQueryServiceClient(deps.Conn),
		shippingQuoteQueryClient:        checkoutv1.NewShippingQuoteQueryServiceClient(deps.Conn),
		orderValidationClient:           ordersv1.NewOrderValidationServiceClient(deps.Conn),
		carrierRateCalculationClient:    shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn),
		previewSellerListingFn:          sellerListingValidation.PreviewSellerListing,
		riskAssessmentValidationClient:  fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn),
		orderValidationClientFallback:   ordersv1.NewOrderValidationServiceClient(deps.Conn),
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		boxOfficeSaleCalculationClient:  boxofficev1.NewBoxOfficeSaleCalculationServiceClient(deps.Conn),
		sellerListingValidationClient:   sellersv1.NewSellerListingValidationServiceClient(deps.Conn),
		sellerScorecardValidationClient: sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		previewSessionFn:                sessionValidation.PreviewSession,
	}
}

// CalculateCart calculates a cart.
func (s *CartCalculationServer) CalculateCart(ctx context.Context, req *cartv1.CalculateCartRequest) (*cartv1.CalculateCartResponse, error) {
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "calculate cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	checkManifestEligibilityRequest := &shippingv1.CheckManifestEligibilityRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			BillingAddress: &commonv1.PostalAddress{Recipient: req.GetCart().GetBillingAddress().GetRecipient()},
		},
	}
	checkManifestEligibilityReply := new(shippingv1.CheckManifestEligibilityResponse)
	if err := s.conn.Invoke(ctx, shippingManifestValidationServiceCheckManifestEligibilityMethod, checkManifestEligibilityRequest, checkManifestEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestValidationService/CheckManifestEligibility")
	}
	ctx = tenancy.With(ctx, "country_code", req.GetDetails().GetDestination().GetCountryCode())
	scoreOrderRequest := &ordersv1.ScoreOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
			Audit:          &commonv1.AuditInfo{UpdatedBy: req.GetCart().GetAudit().GetUpdatedBy()},
			Settlement: &ordersv1.Order_Bank{Bank: &ordersv1.OrderBankSettlement{
				Iban: checkManifestEligibilityReply.GetManifest().GetBank().GetIban(),
			}},
		},
		Details: &ordersv1.ScoreOrderRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: checkManifestEligibilityReply.GetManifest().GetBillingAddress().GetLocality(),
			},
		},
	}
	scoreOrderReply, err := s.scoreOrderFn(ctx, scoreOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/ScoreOrder")
	}
	return &cartv1.CalculateCartResponse{
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{
				Number: checkManifestEligibilityReply.GetManifest().GetNumber(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Region: scoreOrderReply.GetOrder().GetBillingAddress().GetRegion(),
			},
			CartId: req.GetCartId(),
			Total:  req.GetCart().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			Details: checkManifestEligibilityReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// EstimateCart estimates a cart.
func (s *CartCalculationServer) EstimateCart(ctx context.Context, req *cartv1.EstimateCartRequest) (*cartv1.EstimateCartResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "estimate cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetCart())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendApiCredentialTestNoticeRequest := &identityv1.SendApiCredentialTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendApiCredentialTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Payload:   packed,
		},
		Reason: req.GetReason(),
	}
	sendApiCredentialTestNoticeReply := new(identityv1.SendApiCredentialTestNoticeResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.ApiCredentialNotificationService_SendApiCredentialTestNotice_FullMethodName, sendApiCredentialTestNoticeRequest, sendApiCredentialTestNoticeReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice")
	}
	estimateWorkOrderRequest := buildEstimateWorkOrderRequestForEstimateCart(req)
	estimateWorkOrderReply, err := s.workOrderCalculationClient.EstimateWorkOrder(ctx, estimateWorkOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.maintenance.v1.WorkOrderCalculationService/EstimateWorkOrder")
	}
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	explainProductReviewDecisionRequest := &reviewsv1.ExplainProductReviewDecisionRequest{}
	explainProductReviewDecisionRequest.Context = req.GetContext()
	explainProductReviewDecisionRequest.ProductReview = &reviewsv1.ProductReview{
		BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		Audit: &commonv1.AuditInfo{
			CreatedAt: sendApiCredentialTestNoticeReply.GetApiCredential().GetAudit().GetCreatedAt(),
			UpdatedAt: estimateWorkOrderReply.GetWorkOrder().GetAudit().GetUpdatedAt(),
		},
	}
	explainProductReviewDecisionReply, err := s.productReviewValidationClient.ExplainProductReviewDecision(ctx, explainProductReviewDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewValidationService/ExplainProductReviewDecision")
	}
	routes := map[string]func(context.Context, *paymentsv1.FindRefundsRequest, ...grpc.CallOption) (*paymentsv1.FindRefundsResponse, error){
		"primary":  s.refundQueryClient.FindRefunds,
		"fallback": s.refundQueryClientFallback.FindRefunds,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	findRefundsRequest := &paymentsv1.FindRefundsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.FindRefundsRequest_Filter{
			Region:       explainProductReviewDecisionReply.GetProductReview().GetBillingAddress().GetRegion(),
			Labels:       sendApiCredentialTestNoticeReply.GetApiCredential().GetLabels(),
			CreatedAfter: sendApiCredentialTestNoticeReply.GetApiCredential().GetAudit().GetCreatedAt(),
		},
	}
	findRefundsReply, err := route(ctx, findRefundsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundQueryService/FindRefunds")
	}
	_ = findRefundsReply
	return &cartv1.EstimateCartResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: sendApiCredentialTestNoticeReply.GetResult().GetReferenceIds(),
			Details:      estimateWorkOrderReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Cart: &cartv1.Cart{
			Total: estimateWorkOrderReply.GetBreakdown().GetTotal(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainProductReviewDecisionReply.GetProductReview().GetAudit().GetCreatedBy(),
			},
			CartId: req.GetCartId(),
			Notes:  req.GetCart().GetNotes(),
		},
	}, nil
}

// QuoteCart quotes a cart.
func (s *CartCalculationServer) QuoteCart(ctx context.Context, req *cartv1.QuoteCartRequest) (*cartv1.QuoteCartResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteCart", "error", err)
		return nil, err
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "quote cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetCart().GetBillingAddress().GetRegion())
	metadata2 := make(map[string]string, len(req.GetCart().GetMetadata()))
	for key, value := range req.GetCart().GetMetadata() {
		metadata2[key] = value
	}
	exportShippingQuotesStream, err := s.shippingQuoteQueryClient.ExportShippingQuotes(ctx, &checkoutv1.ExportShippingQuotesRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.ExportShippingQuotesRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetCart().GetDueAt(),
			Labels:       metadata2,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/ExportShippingQuotes")
	}
	for {
		received, err := exportShippingQuotesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/ExportShippingQuotes")
		}
		_ = received
	}
	checkOrderEligibilityReply, err := s.orderValidationClient.CheckOrderEligibility(ctx, &ordersv1.CheckOrderEligibilityRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetCart().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/CheckOrderEligibility")
	}
	for line := range iterx.Each(req.GetCart().GetLines(), func(candidate *cartv1.CartLine) bool { return candidate != nil }) {
		if _, err := s.carrierRateCalculationClient.RecalculateCarrierRates(ctx, &shippingv1.RecalculateCarrierRatesRequest{
			Context:     req.GetContext(),
			CarrierRate: &shippingv1.CarrierRate{Description: line.GetDescription()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/RecalculateCarrierRates")
		}
	}
	suggestSubscriptionPlansRequest := &subscriptionsv1.SuggestSubscriptionPlansRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.SuggestSubscriptionPlansRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetCart().GetAudit().GetCreatedAt(),
			OwnerId:      checkOrderEligibilityReply.GetOrder().GetCounterparty().GetCustomerId(),
			Labels:       req.GetCart().GetMetadata(),
		},
	}
	suggestSubscriptionPlansReply := new(subscriptionsv1.SuggestSubscriptionPlansResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.subscriptions.v1.SubscriptionPlanQueryService/SuggestSubscriptionPlans", suggestSubscriptionPlansRequest, suggestSubscriptionPlansReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/SuggestSubscriptionPlans")
	}
	return &cartv1.QuoteCartResponse{
		Cart: &cartv1.Cart{
			Total: checkOrderEligibilityReply.GetOrder().GetTotal(),
			BillingAddress: &commonv1.PostalAddress{
				Recipient: checkOrderEligibilityReply.GetOrder().GetBillingAddress().GetRecipient(),
			},
			CartId: req.GetCartId(),
			DueAt:  req.GetCart().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Status:        suggestSubscriptionPlansReply.GetResult().GetStatus(),
			CorrelationId: suggestSubscriptionPlansReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// CompareCartOptions compares a cart.
func (s *CartCalculationServer) CompareCartOptions(ctx context.Context, req *cartv1.CompareCartOptionsRequest) (*cartv1.CompareCartOptionsResponse, error) {
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "compare cart options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	previewSellerListingRequest := &sellersv1.PreviewSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Lines:     req.GetDetails().GetDestination().GetLines(),
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
			}},
		},
	}
	previewSellerListingReply, err := s.previewSellerListingFn(ctx, previewSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/PreviewSellerListing")
	}
	encoded, err := protojson.Marshal(req.GetCart())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded cartv1.Cart
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	checkOrderEligibilityRequest := &ordersv1.CheckOrderEligibilityRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: previewSellerListingReply.GetSellerListing().GetAddress().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: (&decoded).GetAudit().GetCreatedBy()},
		},
	}
	checkOrderEligibilityReply := new(ordersv1.CheckOrderEligibilityResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", ordersOrderValidationServiceName, "CheckOrderEligibility"), checkOrderEligibilityRequest, checkOrderEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/CheckOrderEligibility")
	}
	normalizeOrderRequest := &ordersv1.NormalizeOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Audit:      &commonv1.AuditInfo{Etag: req.GetCart().GetAudit().GetEtag()},
			Settlement: &ordersv1.Order_Bank{Bank: &ordersv1.OrderBankSettlement{Bic: req.GetCart().GetBank().GetBic()}},
		},
	}
	normalizeOrderReply, err := grpcx.Call(ctx, s.orderValidationClient.NormalizeOrder, normalizeOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/NormalizeOrder")
	}
	return &cartv1.CompareCartOptionsResponse{
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				Lines: previewSellerListingReply.GetSellerListing().GetAddress().GetLines(),
			},
			Metadata: normalizeOrderReply.GetOrder().GetMetadata(),
			CartId:   req.GetCartId(),
			IssuedAt: req.GetCart().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			Status:        previewSellerListingReply.GetResult().GetStatus(),
			CorrelationId: checkOrderEligibilityReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// RecalculateCarts recalculates a cart.
func (s *CartCalculationServer) RecalculateCarts(ctx context.Context, req *cartv1.RecalculateCartsRequest) (*cartv1.RecalculateCartsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateCarts", "error", err)
		return nil, err
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "recalculate carts", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	suggestRefundsRequest := &paymentsv1.SuggestRefundsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.SuggestRefundsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetCart().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetCart().GetCounterparty().GetSavedListId(),
		},
	}
	suggestRefundsReply := new(paymentsv1.SuggestRefundsResponse)
	if err := s.conn.Invoke(ctx, paymentsRefundQueryServiceSuggestRefundsMethod, suggestRefundsRequest, suggestRefundsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundQueryService/SuggestRefunds")
	}
	step := &cartCalculationServerRecalculateCartsCheckRiskAssessmentEligibilityStep{request: &fraudv1.CheckRiskAssessmentEligibilityRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetCart().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetCart().GetAudit().GetEtag(),
				CreatedBy: req.GetCart().GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetCart().GetAudit().GetUpdatedAt(),
			},
		},
	}}
	if err := s.runRecalculateCartsCheckRiskAssessmentEligibility(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/CheckRiskAssessmentEligibility")
	}
	checkRiskAssessmentEligibilityReply := step.reply
	return &cartv1.RecalculateCartsResponse{
		Result: &commonv1.OperationResult{
			Message: suggestRefundsReply.GetResult().GetMessage(),
			Details: checkRiskAssessmentEligibilityReply.GetResult().GetDetails(),
			Status:  checkRiskAssessmentEligibilityReply.GetResult().GetStatus(),
		},
		Cart: &cartv1.Cart{
			CartId: req.GetCartId(),
			Status: req.GetCart().GetStatus(),
		},
	}, nil
}

// SimulateCart simulates a cart.
func (s *CartCalculationServer) SimulateCart(ctx context.Context, req *cartv1.SimulateCartRequest) (*cartv1.SimulateCartResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "simulate cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *ordersv1.ExplainOrderDecisionRequest, ...grpc.CallOption) (*ordersv1.ExplainOrderDecisionResponse, error){
		"primary":  s.orderValidationClient.ExplainOrderDecision,
		"fallback": s.orderValidationClientFallback.ExplainOrderDecision,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	var network string
	switch choice := req.GetCart().GetSettlement().(type) {
	case *cartv1.Cart_Card:
		network = choice.Card.GetNetwork()
	case *cartv1.Cart_Bank:
		network = choice.Bank.GetIban()
	case *cartv1.Cart_VoucherCode:
		network = choice.VoucherCode
	}
	ctx = tenancy.With(ctx, "last4", req.GetCart().GetCard().GetLast4())
	explainOrderDecisionRequest := &ordersv1.ExplainOrderDecisionRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
			Audit:          &commonv1.AuditInfo{CreatedBy: req.GetCart().GetAudit().GetCreatedBy()},
			Settlement: &ordersv1.Order_Card{Card: &ordersv1.OrderCardSettlement{
				Network: network,
				Last4:   tenancy.Value(ctx, "last4"),
			}},
		},
	}
	explainOrderDecisionReply, err := route(ctx, explainOrderDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/ExplainOrderDecision")
	}
	packed, err := anypb.New(req.GetCart())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeApiCredentialUpdatesRequest := &identityv1.SubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: explainOrderDecisionReply.GetOrder().GetCounterparty().GetEmail(),
			},
			Payload: packed,
		},
	}
	subscribeApiCredentialUpdatesReply, err := grpcx.Call(ctx, s.apiCredentialNotificationClient.SubscribeApiCredentialUpdates, subscribeApiCredentialUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SubscribeApiCredentialUpdates")
	}
	estimateBoxOfficeSaleRequest := newEstimateBoxOfficeSaleRequest(withEstimateBoxOfficeSaleContext(req.GetContext()), withEstimateBoxOfficeSaleBoxOfficeSale(&boxofficev1.BoxOfficeSale{
		Settlement: &boxofficev1.BoxOfficeSale_Bank{Bank: &boxofficev1.BoxOfficeSaleBankSettlement{
			AccountHolder: explainOrderDecisionReply.GetOrder().GetBank().GetAccountHolder(),
			Iban:          subscribeApiCredentialUpdatesReply.GetApiCredential().GetBank().GetIban(),
		}},
		BillingAddress: &commonv1.PostalAddress{
			PostalCode: explainOrderDecisionReply.GetOrder().GetBillingAddress().GetPostalCode(),
		},
	}))
	estimateBoxOfficeSaleReply, err := s.boxOfficeSaleCalculationClient.EstimateBoxOfficeSale(ctx, estimateBoxOfficeSaleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.boxoffice.v1.BoxOfficeSaleCalculationService/EstimateBoxOfficeSale")
	}
	validateSellerListingRequest := &sellersv1.ValidateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				Etag:      explainOrderDecisionReply.GetOrder().GetAudit().GetEtag(),
				CreatedAt: estimateBoxOfficeSaleReply.GetBoxOfficeSale().GetAudit().GetCreatedAt(),
			},
			Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
				Recipient: estimateBoxOfficeSaleReply.GetBoxOfficeSale().GetBillingAddress().GetRecipient(),
			}},
			Owner: &identityv1.UserRef{
				DisplayName: explainOrderDecisionReply.GetOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
	type validateSellerListingResult struct {
		reply *sellersv1.ValidateSellerListingResponse
		err   error
	}
	validateSellerListingResults := make(chan validateSellerListingResult, 1)
	go func() {
		reply, err := s.sellerListingValidationClient.ValidateSellerListing(ctx, validateSellerListingRequest)
		validateSellerListingResults <- validateSellerListingResult{reply, err}
	}()
	var validateSellerListingReply *sellersv1.ValidateSellerListingResponse
	select {
	case result := <-validateSellerListingResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerListingValidationService/ValidateSellerListing")
		}
		validateSellerListingReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &cartv1.SimulateCartResponse{
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: explainOrderDecisionReply.GetOrder().GetBillingAddress().GetRecipient(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: subscribeApiCredentialUpdatesReply.GetApiCredential().GetAudit().GetCreatedAt(),
				UpdatedBy: validateSellerListingReply.GetSellerListing().GetAudit().GetUpdatedBy(),
			},
			Notes: estimateBoxOfficeSaleReply.GetBoxOfficeSale().GetNotes(),
			Counterparty: &cartv1.SavedListRef{
				DisplayName: validateSellerListingReply.GetSellerListing().GetOwner().GetDisplayName(),
			},
			CartId:   req.GetCartId(),
			IssuedAt: req.GetCart().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: explainOrderDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ExplainCartCalculation explains a cart.
func (s *CartCalculationServer) ExplainCartCalculation(ctx context.Context, req *cartv1.ExplainCartCalculationRequest) (*cartv1.ExplainCartCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "explain cart calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	scoreSellerScorecardRequest := newScoreSellerScorecardRequest(withScoreSellerScorecardContext(req.GetContext()), withScoreSellerScorecardDetails(&sellersv1.ScoreSellerScorecardRequest_Details{
		Destination: &commonv1.PostalAddress{
			PostalCode: req.GetCart().GetBillingAddress().GetPostalCode(),
		},
	}))
	scoreSellerScorecardReply, err := s.sellerScorecardValidationClient.ScoreSellerScorecard(ctx, scoreSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ScoreSellerScorecard")
	}
	previewSessionRequest := &identityv1.PreviewSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{
				Etag:      scoreSellerScorecardReply.GetSellerScorecard().GetAudit().GetEtag(),
				CreatedAt: req.GetCart().GetAudit().GetCreatedAt(),
			},
		},
	}
	previewSessionReply, err := s.previewSessionFn(ctx, previewSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/PreviewSession")
	}
	return &cartv1.ExplainCartCalculationResponse{
		Cart: &cartv1.Cart{
			Number:       scoreSellerScorecardReply.GetSellerScorecard().GetNumber(),
			Total:        scoreSellerScorecardReply.GetBreakdown().GetTotal(),
			Audit:        &commonv1.AuditInfo{Etag: previewSessionReply.GetSession().GetAudit().GetEtag()},
			CartId:       req.GetCartId(),
			Counterparty: req.GetCart().GetCounterparty(),
		},
		Outcome: &cartv1.ExplainCartCalculationResponse_Outcome{NextSteps: previewSessionReply.GetOutcome().GetNextSteps()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetCartRates returns a cart.
func (s *CartCalculationServer) GetCartRates(ctx context.Context, req *cartv1.GetCartRatesRequest) (*cartv1.GetCartRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart", req.GetCartId(), "get_cart_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get cart rates", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	checkSellerListingEligibilityRequest := &sellersv1.CheckSellerListingEligibilityRequest{}
	checkSellerListingEligibilityRequest.Context = req.GetContext()
	checkSellerListingEligibilityRequest.SellerListing = &sellersv1.SellerListing{
		Owner: &identityv1.UserRef{UserId: cachedContext.GetActor().GetUserId()},
	}
	checkSellerListingEligibilityRequest.Details = &sellersv1.CheckSellerListingEligibilityRequest_Details{
		Contact: &commonv1.ContactInfo{Email: email},
		Locale:  req.GetContext().GetLocale(),
	}
	checkSellerListingEligibilityReply, err := s.sellerListingValidationClient.CheckSellerListingEligibility(ctx, checkSellerListingEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/CheckSellerListingEligibility")
	}
	return &cartv1.GetCartRatesResponse{
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{
				DisplayName: checkSellerListingEligibilityReply.GetSellerListing().GetOwner().GetDisplayName(),
			},
			CartId: req.GetCartId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// cartCalculationServerRecalculateCartsCheckRiskAssessmentEligibilityStep carries the request of one step of
// RecalculateCarts and, once it ran, its reply.
type cartCalculationServerRecalculateCartsCheckRiskAssessmentEligibilityStep struct {
	request *fraudv1.CheckRiskAssessmentEligibilityRequest
	reply   *fraudv1.CheckRiskAssessmentEligibilityResponse
}

func (s *CartCalculationServer) runRecalculateCartsCheckRiskAssessmentEligibility(ctx context.Context, step *cartCalculationServerRecalculateCartsCheckRiskAssessmentEligibilityStep) error {
	reply, err := s.riskAssessmentValidationClient.CheckRiskAssessmentEligibility(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
