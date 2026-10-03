package pricing

import (
	"context"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PriceListApprovalServer implements `acme.pricing.v1.PriceListApprovalService`.
// Routes price lists through approval, with escalation and delegation.
type PriceListApprovalServer struct {
	*priceListApprovalHandlers
}

// priceListApprovalHandlers holds the handlers of PriceListApprovalServer, with what they use.
type priceListApprovalHandlers struct {
	pricingv1.UnimplementedPriceListApprovalServiceServer
	store                                 *Store
	log                                   *slog.Logger
	lookupPriceListFn                     func(context.Context, *pricingv1.LookupPriceListRequest, ...grpc.CallOption) (*pricingv1.LookupPriceListResponse, error)
	currencyRateValidationClient          pricingv1.CurrencyRateValidationServiceClient
	clients                               *registry.Registry
	savedListApprovalClient               cartv1.SavedListApprovalServiceClient
	conn                                  *grpc.ClientConn
	priceQuoteValidationClient            pricingv1.PriceQuoteValidationServiceClient
	priceQuoteValidationClientFallback    pricingv1.PriceQuoteValidationServiceClient
	sellerListingWorkflowClient           sellersv1.SellerListingWorkflowServiceClient
	stockMovementApprovalClient           inventoryv1.StockMovementApprovalServiceClient
	priceOverrideValidationClient         pricingv1.PriceOverrideValidationServiceClient
	priceOverrideValidationClientFallback pricingv1.PriceOverrideValidationServiceClient
}

// NewPriceListApprovalServer wires a PriceListApprovalServer.
func NewPriceListApprovalServer(deps platform.Deps) *PriceListApprovalServer {
	priceListQuery := pricingv1.NewPriceListQueryServiceClient(deps.Conn)
	return &PriceListApprovalServer{priceListApprovalHandlers: &priceListApprovalHandlers{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.pricing.v1.PriceListApprovalService"),
		lookupPriceListFn:                     priceListQuery.LookupPriceList,
		currencyRateValidationClient:          pricingv1.NewCurrencyRateValidationServiceClient(deps.Conn),
		clients:                               deps.Clients,
		savedListApprovalClient:               cartv1.NewSavedListApprovalServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		priceQuoteValidationClient:            pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		priceQuoteValidationClientFallback:    pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		sellerListingWorkflowClient:           sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		stockMovementApprovalClient:           inventoryv1.NewStockMovementApprovalServiceClient(deps.Conn),
		priceOverrideValidationClient:         pricingv1.NewPriceOverrideValidationServiceClient(deps.Conn),
		priceOverrideValidationClientFallback: pricingv1.NewPriceOverrideValidationServiceClient(deps.Conn),
	}}
}

// SubmitPriceList submits a price list.
func (s *priceListApprovalHandlers) SubmitPriceList(ctx context.Context, req *pricingv1.SubmitPriceListRequest) (*pricingv1.SubmitPriceListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitPriceList", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "submit price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	lookupPriceListRequest := &pricingv1.LookupPriceListRequest{
		Context:     req.GetContext(),
		PriceListId: req.GetPriceList().GetPriceListId(),
	}
	lookupPriceListReply, err := s.lookupPriceListFn(ctx, lookupPriceListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListQueryService/LookupPriceList")
	}
	step := &priceListApprovalHandlersSubmitPriceListValidateCurrencyRateStep{request: &pricingv1.ValidateCurrencyRateRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{DisplayName: req.GetPriceList().GetOwner().GetDisplayName()},
		},
	}}
	if err := s.runSubmitPriceListValidateCurrencyRate(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/ValidateCurrencyRate")
	}
	validateCurrencyRateReply := step.reply
	return &pricingv1.SubmitPriceListResponse{
		Result: &commonv1.OperationResult{
			Message:       lookupPriceListReply.GetResult().GetMessage(),
			CorrelationId: validateCurrencyRateReply.GetCurrencyRate().GetCorrelationId(),
			Status:        "ok",
		},
		PriceList: &pricingv1.PriceList{
			Name:        lookupPriceListReply.GetPriceList().GetName(),
			PriceListId: req.GetPriceListId(),
			Status:      req.GetPriceList().GetStatus(),
		},
	}, nil
}

// ApprovePriceList approves a price list.
func (s *priceListApprovalHandlers) ApprovePriceList(ctx context.Context, req *pricingv1.ApprovePriceListRequest) (*pricingv1.ApprovePriceListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApprovePriceList", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "approve price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	checkoutSessionApproval, err := registry.Lookup[checkoutv1.CheckoutSessionApprovalServiceClient](s.clients, "acme.checkout.v1.CheckoutSessionApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloned := proto.Clone(req.GetPriceList()).(*pricingv1.PriceList)
	escalateCheckoutSessionRequest := &checkoutv1.EscalateCheckoutSessionRequest{
		Context: req.GetContext(),
		CheckoutSession: &checkoutv1.CheckoutSession{
			Lines: []*checkoutv1.CheckoutSessionLine{&checkoutv1.CheckoutSessionLine{
				Tax: &checkoutv1.CheckoutSessionLineTax{Amount: req.GetPriceList().GetDiscount().GetAmount()},
			}},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetPriceList().GetAudit().GetCreatedAt()},
			Counterparty: &checkoutv1.ShippingQuoteRef{DisplayName: cloned.GetOwner().GetDisplayName()},
		},
		Details: &checkoutv1.EscalateCheckoutSessionRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	escalateCheckoutSessionReply, err := checkoutSessionApproval.EscalateCheckoutSession(ctx, escalateCheckoutSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/EscalateCheckoutSession")
	}
	return &pricingv1.ApprovePriceListResponse{
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: escalateCheckoutSessionReply.GetCheckoutSession().GetAudit().GetUpdatedBy(),
			},
			PriceListId: req.GetPriceListId(),
			Version:     req.GetPriceList().GetVersion(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: escalateCheckoutSessionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// EscalatePriceList escalates a price list.
func (s *priceListApprovalHandlers) EscalatePriceList(ctx context.Context, req *pricingv1.EscalatePriceListRequest) (*pricingv1.EscalatePriceListResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalatePriceList", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "escalate price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	detailsCache := cache.New[string, *pricingv1.EscalatePriceListRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	approveSavedListRequest := &cartv1.ApproveSavedListRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Lines: []*cartv1.SavedListLine{&cartv1.SavedListLine{
				Tax: &cartv1.SavedListLineTax{Amount: req.GetPriceList().GetDiscount().GetAmount()},
			}},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetPriceList().GetAudit().GetCreatedBy()},
		},
		Details: &cartv1.ApproveSavedListRequest_Details{
			Approver: &identityv1.UserRef{DisplayName: cachedDetails.GetApprover().GetDisplayName()},
		},
	}
	approveSavedListReply, err := grpcx.Call(ctx, s.savedListApprovalClient.ApproveSavedList, approveSavedListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListApprovalService/ApproveSavedList")
	}
	ctx = tenancy.With(ctx, "email", req.GetPriceList().GetOwner().GetEmail())
	packed, err := anypb.New(req.GetPriceList())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	checkCurrencyRateEligibilityRequest := &pricingv1.CheckCurrencyRateEligibilityRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: approveSavedListReply.GetSavedList().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	checkCurrencyRateEligibilityReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*pricingv1.CheckCurrencyRateEligibilityResponse, error) {
		return s.currencyRateValidationClient.CheckCurrencyRateEligibility(ctx, checkCurrencyRateEligibilityRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/CheckCurrencyRateEligibility")
	}
	return &pricingv1.EscalatePriceListResponse{
		Result: &commonv1.OperationResult{
			Status:       approveSavedListReply.GetOutcome().GetStatus(),
			ReferenceIds: checkCurrencyRateEligibilityReply.GetResult().GetReferenceIds(),
			Details:      checkCurrencyRateEligibilityReply.GetResult().GetDetails(),
		},
		PriceList: &pricingv1.PriceList{
			PriceListId: req.GetPriceListId(),
			Status:      req.GetPriceList().GetStatus(),
		},
	}, nil
}

// ListPendingPriceLists lists price lists a page at a time.
func (s *priceListApprovalHandlers) ListPendingPriceLists(ctx context.Context, req *pricingv1.ListPendingPriceListsRequest) (*pricingv1.ListPendingPriceListsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingPriceLists", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending price lists", "tenant", req.GetContext().GetTenantId())
	simulateHouseholdRequest := &customersv1.SimulateHouseholdRequest{
		Context: req.GetContext(),
		Details: &customersv1.SimulateHouseholdRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	simulateHouseholdReply, err := grpcx.NewClient(s.conn, customersv1.NewHouseholdCalculationServiceClient).SimulateHousehold(ctx, simulateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/SimulateHousehold")
	}
	synonymSync, err := registry.Lookup[searchv1.SynonymSyncServiceClient](s.clients, "acme.search.v1.SynonymSyncService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushSynonymRequest := &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: simulateHouseholdReply.GetHousehold().GetAudit().GetUpdatedBy(),
				Etag:      simulateHouseholdReply.GetHousehold().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				Email:       simulateHouseholdReply.GetHousehold().GetManager().GetEmail(),
				DisplayName: simulateHouseholdReply.GetHousehold().GetManager().GetDisplayName(),
			},
		},
		Details: &searchv1.PushSynonymRequest_Details{Payload: snapshot},
	}
	pushSynonymReply, err := synonymSync.PushSynonym(ctx, pushSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	return &pricingv1.ListPendingPriceListsResponse{
		PriceLists: []*pricingv1.PriceList{&pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				Email: simulateHouseholdReply.GetHousehold().GetManager().GetEmail(),
			},
		}},
		Result: &commonv1.OperationResult{
			ReferenceIds: pushSynonymReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// DelegatePriceListApproval delegates a price list.
func (s *priceListApprovalHandlers) DelegatePriceListApproval(ctx context.Context, req *pricingv1.DelegatePriceListApprovalRequest) (*pricingv1.DelegatePriceListApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "delegate price list approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetPriceList())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullPaymentInstrumentsRequest := &paymentsv1.PullPaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{Email: req.GetPriceList().GetOwner().GetEmail()},
			Audit:  &commonv1.AuditInfo{CreatedAt: req.GetPriceList().GetAudit().GetCreatedAt()},
		},
		Details: &paymentsv1.PullPaymentInstrumentsRequest_Details{Payload: snapshot},
	}
	pullPaymentInstrumentsReply := new(paymentsv1.PullPaymentInstrumentsResponse)
	if err := s.conn.Invoke(ctx, paymentsPaymentInstrumentSyncServicePullPaymentInstrumentsMethod, pullPaymentInstrumentsRequest, pullPaymentInstrumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/PullPaymentInstruments")
	}
	return &pricingv1.DelegatePriceListApprovalResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: pullPaymentInstrumentsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PriceList: &pricingv1.PriceList{PriceListId: req.GetPriceListId()},
		Outcome:   &pricingv1.DelegatePriceListApprovalResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RecallPriceList recalls a price list.
func (s *priceListApprovalHandlers) RecallPriceList(ctx context.Context, req *pricingv1.RecallPriceListRequest) (*pricingv1.RecallPriceListResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "recall price list", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *pricingv1.NormalizePriceQuoteRequest, ...grpc.CallOption) (*pricingv1.NormalizePriceQuoteResponse, error){
		"primary":  s.priceQuoteValidationClient.NormalizePriceQuote,
		"fallback": s.priceQuoteValidationClientFallback.NormalizePriceQuote,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	normalizePriceQuoteRequest := &pricingv1.NormalizePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{Amount: req.GetPriceList().GetDiscount().GetAmount()}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetPriceList().GetAudit().GetUpdatedBy(),
				Etag:      req.GetPriceList().GetAudit().GetEtag(),
			},
		},
	}
	normalizePriceQuoteReply, err := route(ctx, normalizePriceQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/NormalizePriceQuote")
	}
	advanceSellerListingWorkflowRequest := &sellersv1.AdvanceSellerListingWorkflowRequest{}
	advanceSellerListingWorkflowRequest.Context = req.GetContext()
	advanceSellerListingWorkflowRequest.SellerListing = &sellersv1.SellerListing{
		Owner: &identityv1.UserRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
		Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
			Lines: normalizePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetLines(),
		}},
	}
	advanceSellerListingWorkflowReply, err := s.sellerListingWorkflowClient.AdvanceSellerListingWorkflow(ctx, advanceSellerListingWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/AdvanceSellerListingWorkflow")
	}
	packed, err := anypb.New(req.GetPriceList())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	submitStockMovementRequest := &inventoryv1.SubmitStockMovementRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.SubmitStockMovementRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: normalizePriceQuoteReply.GetPriceQuote().GetCounterparty().GetDisplayName(),
			},
		},
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{Email: req.GetDetails().GetApprover().GetEmail()},
			Detail:  packed,
		},
	}
	var submitStockMovementReply *inventoryv1.SubmitStockMovementResponse
	if err := pipeline.Run(ctx, "acme.inventory.v1.StockMovementApprovalService/SubmitStockMovement", func(ctx context.Context) error {
		answer, err := s.stockMovementApprovalClient.SubmitStockMovement(ctx, submitStockMovementRequest)
		submitStockMovementReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementApprovalService/SubmitStockMovement")
	}
	return &pricingv1.RecallPriceListResponse{
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizePriceQuoteReply.GetPriceQuote().GetAudit().GetUpdatedBy(),
			},
			Description: advanceSellerListingWorkflowReply.GetSellerListing().GetComponents()["primary"].GetDescription(),
			PriceListId: req.GetPriceListId(),
			Version:     req.GetPriceList().GetVersion(),
		},
		Outcome: &pricingv1.RecallPriceListResponse_Outcome{Message: submitStockMovementReply.GetOutcome().GetMessage()},
		Result: &commonv1.OperationResult{
			ReferenceIds: submitStockMovementReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// GetPriceListApprovalStatus returns a price list.
func (s *priceListApprovalHandlers) GetPriceListApprovalStatus(ctx context.Context, req *pricingv1.GetPriceListApprovalStatusRequest) (*pricingv1.GetPriceListApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_list", req.GetPriceListId(), "get_price_list_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get price list approval status", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	advanceSellerListingWorkflowReply, err := s.sellerListingWorkflowClient.AdvanceSellerListingWorkflow(ctx, &sellersv1.AdvanceSellerListingWorkflowRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{
				Email:  tenancy.Value(ctx, "email"),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/AdvanceSellerListingWorkflow")
	}
	return &pricingv1.GetPriceListApprovalStatusResponse{
		PriceList: &pricingv1.PriceList{
			Audit: &commonv1.AuditInfo{
				CreatedBy: advanceSellerListingWorkflowReply.GetSellerListing().GetAudit().GetCreatedBy(),
			},
			PriceListId: req.GetPriceListId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentPriceListApproval comments a price list.
func (s *priceListApprovalHandlers) CommentPriceListApproval(ctx context.Context, req *pricingv1.CommentPriceListApprovalRequest) (*pricingv1.CommentPriceListApprovalResponse, error) {
	if err := s.store.SavePriceList(ctx, req.GetContext().GetTenantId(), req.GetPriceList()); err != nil {
		return nil, grpcx.Wrap(err, "save price list")
	}
	s.log.InfoContext(ctx, "comment price list approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceList().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *pricingv1.ValidatePriceOverrideRequest, ...grpc.CallOption) (*pricingv1.ValidatePriceOverrideResponse, error){
		"primary":  s.priceOverrideValidationClient.ValidatePriceOverride,
		"fallback": s.priceOverrideValidationClientFallback.ValidatePriceOverride,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetDetails()).(*pricingv1.CommentPriceListApprovalRequest_Details)
	validatePriceOverrideRequest := &pricingv1.ValidatePriceOverrideRequest{
		Context: req.GetContext(),
		Details: &pricingv1.ValidatePriceOverrideRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned.GetApprover().GetEmail()},
		},
	}
	validatePriceOverrideReply, err := route(ctx, validatePriceOverrideRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideValidationService/ValidatePriceOverride")
	}
	packed, err := anypb.New(req.GetPriceList())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintCurrencyRatesReply, err := s.currencyRateValidationClient.LintCurrencyRates(ctx, &pricingv1.LintCurrencyRatesRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &pricingv1.LintCurrencyRatesRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: validatePriceOverrideReply.GetPriceOverride().GetOwner().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/LintCurrencyRates")
	}
	return &pricingv1.CommentPriceListApprovalResponse{
		Outcome: &pricingv1.CommentPriceListApprovalResponse_Outcome{
			Message: validatePriceOverrideReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: lintCurrencyRatesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PriceList: &pricingv1.PriceList{
			PriceListId: req.GetPriceListId(),
			Effective:   req.GetPriceList().GetEffective(),
		},
	}, nil
}

// priceListApprovalHandlersSubmitPriceListValidateCurrencyRateStep carries the request of one step of SubmitPriceList
// and, once it ran, its reply.
type priceListApprovalHandlersSubmitPriceListValidateCurrencyRateStep struct {
	request *pricingv1.ValidateCurrencyRateRequest
	reply   *pricingv1.ValidateCurrencyRateResponse
}

func (s *priceListApprovalHandlers) runSubmitPriceListValidateCurrencyRate(ctx context.Context, step *priceListApprovalHandlersSubmitPriceListValidateCurrencyRateStep) error {
	reply, err := s.currencyRateValidationClient.ValidateCurrencyRate(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
