package pricing

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	performancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PriceOverrideLifecycleServer implements `acme.pricing.v1.PriceOverrideLifecycleService`.
// Moves a price override between the states of its lifecycle.
type PriceOverrideLifecycleServer struct {
	pricingv1.UnimplementedPriceOverrideLifecycleServiceServer
	store                            *Store
	log                              *slog.Logger
	quoteCustomerSegmentFn           func(context.Context, *customersv1.QuoteCustomerSegmentRequest, ...grpc.CallOption) (*customersv1.QuoteCustomerSegmentResponse, error)
	escalateOrderNoteFn              func(context.Context, *ordersv1.EscalateOrderNoteRequest, ...grpc.CallOption) (*ordersv1.EscalateOrderNoteResponse, error)
	householdCalculationClient       customersv1.HouseholdCalculationServiceClient
	priceOverrideCalculationClient   pricingv1.PriceOverrideCalculationServiceClient
	meteredPriceOverrideValidation   meteredPriceOverrideValidation
	conn                             *grpc.ClientConn
	priceListClient                  pricingv1.PriceListServiceClient
	listApiCredentialSubscriptionsFn func(context.Context, *identityv1.ListApiCredentialSubscriptionsRequest, ...grpc.CallOption) (*identityv1.ListApiCredentialSubscriptionsResponse, error)
	meteredPriceOverrideCalculation  meteredPriceOverrideCalculation
	reviewClient                     performancev1.ReviewServiceClient
	sellerApprovalClient             sellersv1.SellerApprovalServiceClient
}

// NewPriceOverrideLifecycleServer wires a PriceOverrideLifecycleServer.
func NewPriceOverrideLifecycleServer(deps platform.Deps) *PriceOverrideLifecycleServer {
	customerSegmentCalculation := customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn)
	orderNoteApproval := ordersv1.NewOrderNoteApprovalServiceClient(deps.Conn)
	apiCredentialNotification := identityv1.NewApiCredentialNotificationServiceClient(deps.Conn)
	return &PriceOverrideLifecycleServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.pricing.v1.PriceOverrideLifecycleService"),
		quoteCustomerSegmentFn:           customerSegmentCalculation.QuoteCustomerSegment,
		escalateOrderNoteFn:              orderNoteApproval.EscalateOrderNote,
		householdCalculationClient:       customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		priceOverrideCalculationClient:   pricingv1.NewPriceOverrideCalculationServiceClient(deps.Conn),
		meteredPriceOverrideValidation:   meteredPriceOverrideValidation{PriceOverrideValidationServiceClient: pricingv1.NewPriceOverrideValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		conn:                             deps.Conn,
		priceListClient:                  pricingv1.NewPriceListServiceClient(deps.Conn),
		listApiCredentialSubscriptionsFn: apiCredentialNotification.ListApiCredentialSubscriptions,
		meteredPriceOverrideCalculation:  meteredPriceOverrideCalculation{PriceOverrideCalculationServiceClient: pricingv1.NewPriceOverrideCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		reviewClient:                     performancev1.NewReviewServiceClient(deps.Conn),
		sellerApprovalClient:             sellersv1.NewSellerApprovalServiceClient(deps.Conn),
	}
}

// ActivatePriceOverride activates a price override.
func (s *PriceOverrideLifecycleServer) ActivatePriceOverride(ctx context.Context, req *pricingv1.ActivatePriceOverrideRequest) (*pricingv1.ActivatePriceOverrideResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_override", req.GetPriceOverrideId(), "activate_price_override"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate price override", "tenant", req.GetContext().GetTenantId())
	estimatePreferenceRequest := &customersv1.EstimatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNewOwner().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{
				Reason:      req.GetReason(),
				NotifyEmail: req.GetDetails().GetNotifyContact().GetEmail(),
			}},
		},
	}
	estimatePreferenceReply, err := estimatePreferenceCall(ctx, estimatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/EstimatePreference")
	}
	quoteCustomerSegmentRequest := &customersv1.QuoteCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				UserId: estimatePreferenceReply.GetPreference().GetOwner().GetUserId(),
				Email:  estimatePreferenceReply.GetPreference().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimatePreferenceReply.GetPreference().GetAudit().GetCreatedAt(),
				CreatedBy: estimatePreferenceReply.GetPreference().GetAudit().GetCreatedBy(),
			},
		},
	}
	quoteCustomerSegmentReply, err := s.quoteCustomerSegmentFn(ctx, quoteCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/QuoteCustomerSegment")
	}
	return &pricingv1.ActivatePriceOverrideResponse{
		Result: &commonv1.OperationResult{Status: estimatePreferenceReply.GetResult().GetStatus()},
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimatePreferenceReply.GetPreference().GetAudit().GetCreatedAt(),
			},
			Description:     quoteCustomerSegmentReply.GetCustomerSegment().GetDescription(),
			PriceOverrideId: req.GetPriceOverrideId(),
		},
		Outcome: &pricingv1.ActivatePriceOverrideResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendPriceOverride suspends a price override.
func (s *PriceOverrideLifecycleServer) SuspendPriceOverride(ctx context.Context, req *pricingv1.SuspendPriceOverrideRequest) (*pricingv1.SuspendPriceOverrideResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_override", req.GetPriceOverrideId(), "suspend_price_override"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend price override", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	escalateOrderNoteRequest := &ordersv1.EscalateOrderNoteRequest{
		Context: req.GetContext(),
		Details: &ordersv1.EscalateOrderNoteRequest_Details{
			Approver:    &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	escalateOrderNoteReply, err := s.escalateOrderNoteFn(ctx, escalateOrderNoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteApprovalService/EscalateOrderNote")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded pricingv1.SuspendPriceOverrideRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	calculateHouseholdRequest := &customersv1.CalculateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{Email: (&decoded).GetNotifyContact().GetEmail()},
				}},
			}},
		},
	}
	var calculateHouseholdReply *customersv1.CalculateHouseholdResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.HouseholdCalculationService/CalculateHousehold", func(ctx context.Context) error {
		answer, err := s.householdCalculationClient.CalculateHousehold(ctx, calculateHouseholdRequest)
		calculateHouseholdReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/CalculateHousehold")
	}
	return &pricingv1.SuspendPriceOverrideResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: escalateOrderNoteReply.GetOrderNote().GetCorrelationId(),
			ReferenceIds:  escalateOrderNoteReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: calculateHouseholdReply.GetHousehold().GetAudit().GetUpdatedAt(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
		},
		Outcome: &pricingv1.SuspendPriceOverrideResponse_Outcome{
			Message:     calculateHouseholdReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// RenewPriceOverride renews a price override.
func (s *PriceOverrideLifecycleServer) RenewPriceOverride(ctx context.Context, req *pricingv1.RenewPriceOverrideRequest) (*pricingv1.RenewPriceOverrideResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_override", req.GetPriceOverrideId(), "renew_price_override"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew price override", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *pricingv1.RenewPriceOverrideRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	comparePriceOverrideOptionsRequest := &pricingv1.ComparePriceOverrideOptionsRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNewOwner().GetEmail(),
				DisplayName: cachedDetails.GetNewOwner().GetDisplayName(),
			},
			Action: &pricingv1.PriceOverride_Block{Block: &pricingv1.PriceOverrideBlockAction{
				NotifyEmail: req.GetDetails().GetNotifyContact().GetEmail(),
				Reason:      req.GetReason(),
			}},
		},
	}
	comparePriceOverrideOptionsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*pricingv1.ComparePriceOverrideOptionsResponse, error) {
		return s.priceOverrideCalculationClient.ComparePriceOverrideOptions(ctx, comparePriceOverrideOptionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/ComparePriceOverrideOptions")
	}
	previewPriceOverrideRequest := &pricingv1.PreviewPriceOverrideRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Action: &pricingv1.PriceOverride_Route{Route: &pricingv1.PriceOverrideRouteAction{
				Target: comparePriceOverrideOptionsReply.GetPriceOverride().GetRoute().GetTarget(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: comparePriceOverrideOptionsReply.GetPriceOverride().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email: comparePriceOverrideOptionsReply.GetPriceOverride().GetOwner().GetEmail(),
			},
		},
	}
	previewPriceOverrideReply, err := s.meteredPriceOverrideValidation.PreviewPriceOverride(ctx, previewPriceOverrideRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideValidationService/PreviewPriceOverride")
	}
	return &pricingv1.RenewPriceOverrideResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: comparePriceOverrideOptionsReply.GetResult().GetReferenceIds(),
			Details:      previewPriceOverrideReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{
			Parameters:      comparePriceOverrideOptionsReply.GetPriceOverride().GetParameters(),
			PriceOverrideId: req.GetPriceOverrideId(),
		},
		Outcome: &pricingv1.RenewPriceOverrideResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpirePriceOverride expires a price override.
func (s *PriceOverrideLifecycleServer) ExpirePriceOverride(ctx context.Context, req *pricingv1.ExpirePriceOverrideRequest) (*pricingv1.ExpirePriceOverrideResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_override", req.GetPriceOverrideId(), "expire_price_override"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire price override", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*pricingv1.ExpirePriceOverrideRequest_Details)
	rejectCarrierRateRequest := &shippingv1.RejectCarrierRateRequest{
		Context: req.GetContext(),
		Details: &shippingv1.RejectCarrierRateRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNewOwner().GetEmail(),
				UserId: cloned.GetNewOwner().GetUserId(),
			},
		},
	}
	rejectCarrierRateReply := new(shippingv1.RejectCarrierRateResponse)
	if err := s.conn.Invoke(ctx, shippingCarrierRateApprovalServiceRejectCarrierRateMethod, rejectCarrierRateRequest, rejectCarrierRateReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateApprovalService/RejectCarrierRate")
	}
	clonePriceListRequest := &pricingv1.ClonePriceListRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
			Action: &pricingv1.PriceList_Block{Block: &pricingv1.PriceListBlockAction{
				Reason: rejectCarrierRateReply.GetCarrierRate().GetBlock().GetReason(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: rejectCarrierRateReply.GetCarrierRate().GetAudit().GetUpdatedAt(),
				Etag:      rejectCarrierRateReply.GetCarrierRate().GetAudit().GetEtag(),
			},
		},
	}
	type clonePriceListResult struct {
		reply *pricingv1.ClonePriceListResponse
		err   error
	}
	clonePriceListResults := make(chan clonePriceListResult, 1)
	go func() {
		reply, err := s.priceListClient.ClonePriceList(ctx, clonePriceListRequest)
		clonePriceListResults <- clonePriceListResult{reply, err}
	}()
	var clonePriceListReply *pricingv1.ClonePriceListResponse
	select {
	case result := <-clonePriceListResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.pricing.v1.PriceListService/ClonePriceList")
		}
		clonePriceListReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &pricingv1.ExpirePriceOverrideResponse{
		Outcome: &pricingv1.ExpirePriceOverrideResponse_Outcome{
			Status:      rejectCarrierRateReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{
				UserId: clonePriceListReply.GetPriceList().GetOwner().GetUserId(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPriceOverrideLifecycle returns a price override.
func (s *PriceOverrideLifecycleServer) GetPriceOverrideLifecycle(ctx context.Context, req *pricingv1.GetPriceOverrideLifecycleRequest) (*pricingv1.GetPriceOverrideLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_override", req.GetPriceOverrideId(), "get_price_override_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get price override lifecycle", "tenant", req.GetContext().GetTenantId())
	quotePriceQuoteRequest := &pricingv1.QuotePriceQuoteRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit:    &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Metadata: req.GetContext().GetBaggage(),
		},
	}
	quotePriceQuoteReply := new(pricingv1.QuotePriceQuoteResponse)
	if err := s.conn.Invoke(ctx, pricingPriceQuoteCalculationServiceQuotePriceQuoteMethod, quotePriceQuoteRequest, quotePriceQuoteReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/QuotePriceQuote")
	}
	listApiCredentialSubscriptionsRequest := &identityv1.ListApiCredentialSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListApiCredentialSubscriptionsRequest_Filter{
			Region:       quotePriceQuoteReply.GetPriceQuote().GetBillingAddress().GetRegion(),
			CreatedAfter: quotePriceQuoteReply.GetPriceQuote().GetAudit().GetCreatedAt(),
			Labels:       quotePriceQuoteReply.GetPriceQuote().GetMetadata(),
			OwnerId:      quotePriceQuoteReply.GetResult().GetCorrelationId(),
		},
	}
	listApiCredentialSubscriptionsReply, err := s.listApiCredentialSubscriptionsFn(ctx, listApiCredentialSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/ListApiCredentialSubscriptions")
	}
	return &pricingv1.GetPriceOverrideLifecycleResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: quotePriceQuoteReply.GetResult().GetReferenceIds(),
			Details:      listApiCredentialSubscriptionsReply.GetResult().GetDetails(),
			Message:      listApiCredentialSubscriptionsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{PriceOverrideId: req.GetPriceOverrideId()},
	}, nil
}

// SchedulePriceOverrideExpiry schedules a price override.
func (s *PriceOverrideLifecycleServer) SchedulePriceOverrideExpiry(ctx context.Context, req *pricingv1.SchedulePriceOverrideExpiryRequest) (*pricingv1.SchedulePriceOverrideExpiryResponse, error) {
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "schedule price override expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	simulatePriceOverrideRequest := &pricingv1.SimulatePriceOverrideRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Action: &pricingv1.PriceOverride_Route{Route: &pricingv1.PriceOverrideRouteAction{Weight: req.GetPriceOverride().GetRoute().GetWeight()}},
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNotify().GetEmail(),
				DisplayName: req.GetPriceOverride().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPriceOverride().GetAudit().GetUpdatedAt()},
		},
	}
	simulatePriceOverrideReply, err := s.meteredPriceOverrideCalculation.SimulatePriceOverride(ctx, simulatePriceOverrideRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/SimulatePriceOverride")
	}
	listReviewsRequest := &performancev1.ListReviewsRequest{
		Context: req.GetContext(),
		Filter: &performancev1.ListReviewsRequest_Filter{
			CreatedAfter: req.GetPriceOverride().GetAudit().GetCreatedAt(),
			Labels:       req.GetPriceOverride().GetParameters(),
			OwnerId:      req.GetPriceOverride().GetPriceOverrideId(),
		},
	}
	listReviewsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*performancev1.ListReviewsResponse, error) {
		return s.reviewClient.ListReviews(ctx, listReviewsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.performance.v1.ReviewService/ListReviews")
	}
	recallSellerRequest := &sellersv1.RecallSellerRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RecallSellerRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: simulatePriceOverrideReply.GetPriceOverride().GetOwner().GetUserId(),
			},
		},
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetPriceOverride().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetPriceOverride().GetAudit().GetUpdatedAt(),
				CreatedAt: simulatePriceOverrideReply.GetPriceOverride().GetAudit().GetCreatedAt(),
			},
		},
	}
	recallSellerReply, err := grpcx.Call(ctx, s.sellerApprovalClient.RecallSeller, recallSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/RecallSeller")
	}
	return &pricingv1.SchedulePriceOverrideExpiryResponse{
		Result: &commonv1.OperationResult{
			Message:       simulatePriceOverrideReply.GetResult().GetMessage(),
			Status:        simulatePriceOverrideReply.GetResult().GetStatus(),
			CorrelationId: listReviewsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  listReviewsReply.GetResult().GetReferenceIds(),
		},
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{
				UserId: recallSellerReply.GetSeller().GetRelationshipManager().GetUserId(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Priority:        req.GetPriceOverride().GetPriority(),
		},
	}, nil
}
