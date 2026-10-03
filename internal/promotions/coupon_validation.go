package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	anomaliesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/anomalies/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	devicesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/devices/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	seatingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/seating/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CouponValidationServer implements `acme.promotions.v1.CouponValidationService`.
// Checks coupons against the rules before anything acts on them.
type CouponValidationServer struct {
	promotionsv1.UnimplementedCouponValidationServiceServer
	store                          *Store
	log                            *slog.Logger
	orderAmendmentValidationClient ordersv1.OrderAmendmentValidationServiceClient
	promotionValidationClient      promotionsv1.PromotionValidationServiceClient
	mediaAssetClient               mediav1.MediaAssetServiceClient
	managedDeviceQueryClient       devicesv1.ManagedDeviceQueryServiceClient
	shipmentQueryClient            shippingv1.ShipmentQueryServiceClient
	documentQueryClient            documentsv1.DocumentQueryServiceClient
	seatMapQueryClient             seatingv1.SeatMapQueryServiceClient
	pointsLedgerCalculationClient  loyaltyv1.PointsLedgerCalculationServiceClient
	metricSeriesQueryClient        metricsv1.MetricSeriesQueryServiceClient
	loyaltyMemberValidationClient  loyaltyv1.LoyaltyMemberValidationServiceClient
	packingSlipQueryClient         fulfillmentv1.PackingSlipQueryServiceClient
	conn                           *grpc.ClientConn
	anomalyQueryClient             anomaliesv1.AnomalyQueryServiceClient
	savedListCalculationOnce       sync.Once
	savedListCalculationCached     cartv1.SavedListCalculationServiceClient
	promotionCalculationClient     promotionsv1.PromotionCalculationServiceClient
}

// NewCouponValidationServer wires a CouponValidationServer.
func NewCouponValidationServer(deps platform.Deps) *CouponValidationServer {
	return &CouponValidationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.promotions.v1.CouponValidationService"),
		orderAmendmentValidationClient: ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn),
		promotionValidationClient:      promotionsv1.NewPromotionValidationServiceClient(deps.Conn),
		mediaAssetClient:               mediav1.NewMediaAssetServiceClient(deps.Conn),
		managedDeviceQueryClient:       devicesv1.NewManagedDeviceQueryServiceClient(deps.Conn),
		shipmentQueryClient:            shippingv1.NewShipmentQueryServiceClient(deps.Conn),
		documentQueryClient:            documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		seatMapQueryClient:             seatingv1.NewSeatMapQueryServiceClient(deps.Conn),
		pointsLedgerCalculationClient:  loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		metricSeriesQueryClient:        metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		loyaltyMemberValidationClient:  loyaltyv1.NewLoyaltyMemberValidationServiceClient(deps.Conn),
		packingSlipQueryClient:         fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn),
		conn:                           deps.Conn,
		anomalyQueryClient:             anomaliesv1.NewAnomalyQueryServiceClient(deps.Conn),
		promotionCalculationClient:     promotionsv1.NewPromotionCalculationServiceClient(deps.Conn),
	}
}

// ValidateCoupon validates a coupon.
func (s *CouponValidationServer) ValidateCoupon(ctx context.Context, req *promotionsv1.ValidateCouponRequest) (*promotionsv1.ValidateCouponResponse, error) {
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "validate coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	var accountHolder string
	switch choice := req.GetCoupon().GetFunding().(type) {
	case *promotionsv1.Coupon_Card:
		accountHolder = choice.Card.GetHolderName()
	case *promotionsv1.Coupon_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *promotionsv1.Coupon_ExternalAccountId:
		accountHolder = choice.ExternalAccountId
	}
	previewOrderAmendmentRequest := &ordersv1.PreviewOrderAmendmentRequest{}
	previewOrderAmendmentRequest.Context = req.GetContext()
	previewOrderAmendmentRequest.OrderAmendment = &ordersv1.OrderAmendment{
		Settlement: &ordersv1.OrderAmendment_Bank{Bank: &ordersv1.OrderAmendmentBankSettlement{AccountHolder: accountHolder}},
	}
	previewOrderAmendmentReply, err := s.orderAmendmentValidationClient.PreviewOrderAmendment(ctx, previewOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/PreviewOrderAmendment")
	}
	for limit := range iterx.Each(req.GetCoupon().GetLimits(), func(candidate *promotionsv1.CouponLimit) bool { return candidate != nil }) {
		if _, err := s.promotionValidationClient.ScorePromotion(ctx, &promotionsv1.ScorePromotionRequest{
			Context: req.GetContext(),
			Promotion: &promotionsv1.Promotion{
				Action: &promotionsv1.Promotion_Discount{Discount: &promotionsv1.PromotionDiscountAction{Amount: limit.GetAmount()}},
				Name:   limit.GetName(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/ScorePromotion")
		}
	}
	labels := make(map[string]string, len(req.GetCoupon().GetLabels()))
	for key, value := range req.GetCoupon().GetLabels() {
		labels[key] = value
	}
	searchMediaAssetsRequest := &mediav1.SearchMediaAssetsRequest{
		Context: req.GetContext(),
		Filter: &mediav1.SearchMediaAssetsRequest_Filter{
			Region: previewOrderAmendmentReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
			Labels: labels,
		},
	}
	searchMediaAssetsReply, err := mediav1.MediaAssetServiceClient.SearchMediaAssets(s.mediaAssetClient, ctx, searchMediaAssetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetService/SearchMediaAssets")
	}
	exportManagedDevicesStream, err := s.managedDeviceQueryClient.ExportManagedDevices(ctx, &devicesv1.ExportManagedDevicesRequest{
		Context: req.GetContext(),
		Filter: &devicesv1.ExportManagedDevicesRequest_Filter{
			Region:  previewOrderAmendmentReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
			Labels:  req.GetCoupon().GetLabels(),
			OwnerId: previewOrderAmendmentReply.GetOrderAmendment().GetCounterparty().GetBackorderId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.devices.v1.ManagedDeviceQueryService/ExportManagedDevices")
	}
	for {
		received, err := exportManagedDevicesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.devices.v1.ManagedDeviceQueryService/ExportManagedDevices")
		}
		_ = received
	}
	_ = searchMediaAssetsReply
	return &promotionsv1.ValidateCouponResponse{
		Result: &commonv1.OperationResult{Status: previewOrderAmendmentReply.GetOutcome().GetStatus()},
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Contact:  req.GetCoupon().GetContact(),
		},
	}, nil
}

// PreviewCoupon previews a coupon.
func (s *CouponValidationServer) PreviewCoupon(ctx context.Context, req *promotionsv1.PreviewCouponRequest) (*promotionsv1.PreviewCouponResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "preview coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "coupon_id", req.GetCoupon().GetCouponId())
	exportShipmentsStream, err := s.shipmentQueryClient.ExportShipments(ctx, &shippingv1.ExportShipmentsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ExportShipmentsRequest_Filter{
			Labels:       req.GetCoupon().GetLabels(),
			CreatedAfter: req.GetCoupon().GetAudit().GetCreatedAt(),
			OwnerId:      tenancy.Value(ctx, "coupon_id"),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentQueryService/ExportShipments")
	}
	for {
		received, err := exportShipmentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentQueryService/ExportShipments")
		}
		_ = received
	}
	return &promotionsv1.PreviewCouponResponse{
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Labels:   req.GetCoupon().GetLabels(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckCouponEligibility checks a coupon.
func (s *CouponValidationServer) CheckCouponEligibility(ctx context.Context, req *promotionsv1.CheckCouponEligibilityRequest) (*promotionsv1.CheckCouponEligibilityResponse, error) {
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "check coupon eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	lintPromotionsReply, err := s.promotionValidationClient.LintPromotions(ctx, &promotionsv1.LintPromotionsRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetCoupon().GetAudit().GetUpdatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/LintPromotions")
	}
	normalizePromotionRequest := buildNormalizePromotionRequestForCheckCouponEligibility(req, lintPromotionsReply)
	normalizePromotionReply, err := s.promotionValidationClient.NormalizePromotion(ctx, normalizePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/NormalizePromotion")
	}
	return &promotionsv1.CheckCouponEligibilityResponse{
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintPromotionsReply.GetPromotion().GetAudit().GetCreatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Email: lintPromotionsReply.GetPromotion().GetOwner().GetEmail(),
			},
			CouponId: req.GetCouponId(),
			Holder:   req.GetCoupon().GetHolder(),
		},
		Outcome: &promotionsv1.CheckCouponEligibilityResponse_Outcome{Message: normalizePromotionReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeCoupon normalizes a coupon.
func (s *CouponValidationServer) NormalizeCoupon(ctx context.Context, req *promotionsv1.NormalizeCouponRequest) (*promotionsv1.NormalizeCouponResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "normalize coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	exportDocumentsStream, err := s.documentQueryClient.ExportDocuments(ctx, &documentsv1.ExportDocumentsRequest{
		Context: req.GetContext(),
		Filter:  &documentsv1.ExportDocumentsRequest_Filter{Labels: req.GetCoupon().GetLabels()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ExportDocuments")
	}
	for {
		received, err := exportDocumentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ExportDocuments")
		}
		_ = received
	}
	return &promotionsv1.NormalizeCouponResponse{
		Coupon: &promotionsv1.Coupon{
			CouponId:     req.GetCouponId(),
			CurrencyCode: req.GetCoupon().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainCouponDecision explains a coupon.
func (s *CouponValidationServer) ExplainCouponDecision(ctx context.Context, req *promotionsv1.ExplainCouponDecisionRequest) (*promotionsv1.ExplainCouponDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "explain coupon decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	step := &couponValidationServerExplainCouponDecisionResolveSeatMapsStep{request: &seatingv1.ResolveSeatMapsRequest{Context: req.GetContext()}}
	if err := s.runExplainCouponDecisionResolveSeatMaps(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.seating.v1.SeatMapQueryService/ResolveSeatMaps")
	}
	resolveSeatMapsReply := step.reply
	for limit := range iterx.Each(req.GetCoupon().GetLimits(), func(candidate *promotionsv1.CouponLimit) bool { return candidate != nil }) {
		if _, err := s.pointsLedgerCalculationClient.CalculatePointsLedger(ctx, &loyaltyv1.CalculatePointsLedgerRequest{
			Context: req.GetContext(),
			PointsLedger: &loyaltyv1.PointsLedger{
				Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{
					Name:   limit.GetName(),
					Period: limit.GetPeriod(),
					Amount: limit.GetAmount(),
				}},
				Funding: &loyaltyv1.PointsLedger_Bank{Bank: &loyaltyv1.PointsLedgerBankFunding{Bic: req.GetCoupon().GetBank().GetBic()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/CalculatePointsLedger")
		}
	}
	summarizeMetricSeriesesRequest := buildSummarizeMetricSeriesesRequestForExplainCouponDecision(req)
	summarizeMetricSeriesesReply, err := s.metricSeriesQueryClient.SummarizeMetricSerieses(ctx, summarizeMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := couponValidationServerExplainLoyaltyMemberDecisionSteps[mode]
	explainLoyaltyMemberDecisionRequest := &loyaltyv1.ExplainLoyaltyMemberDecisionRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Phone: req.GetCoupon().GetContact().GetPhone()},
		},
		Details: &loyaltyv1.ExplainLoyaltyMemberDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetCoupon().GetContact().GetPreferredChannel(),
			},
		},
	}
	explainLoyaltyMemberDecisionReply, err := run(s, ctx, explainLoyaltyMemberDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/ExplainLoyaltyMemberDecision")
	}
	return &promotionsv1.ExplainCouponDecisionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: resolveSeatMapsReply.GetResult().GetReferenceIds(),
			Message:      summarizeMetricSeriesesReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Outcome: &promotionsv1.ExplainCouponDecisionResponse_Outcome{
			Message: summarizeMetricSeriesesReply.GetResult().GetMessage(),
			Status:  explainLoyaltyMemberDecisionReply.GetResult().GetStatus(),
		},
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Balance:  req.GetCoupon().GetBalance(),
		},
	}, nil
}

// ScoreCoupon scores a coupon.
func (s *CouponValidationServer) ScoreCoupon(ctx context.Context, req *promotionsv1.ScoreCouponRequest) (*promotionsv1.ScoreCouponResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "score coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	resolvePackingSlipsRequest := &fulfillmentv1.ResolvePackingSlipsRequest{Context: req.GetContext()}
	type resolvePackingSlipsResult struct {
		reply *fulfillmentv1.ResolvePackingSlipsResponse
		err   error
	}
	resolvePackingSlipsResults := make(chan resolvePackingSlipsResult, 1)
	go func() {
		reply, err := s.packingSlipQueryClient.ResolvePackingSlips(ctx, resolvePackingSlipsRequest)
		resolvePackingSlipsResults <- resolvePackingSlipsResult{reply, err}
	}()
	var resolvePackingSlipsReply *fulfillmentv1.ResolvePackingSlipsResponse
	select {
	case result := <-resolvePackingSlipsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fulfillment.v1.PackingSlipQueryService/ResolvePackingSlips")
		}
		resolvePackingSlipsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	detailsCache := cache.New[string, *promotionsv1.ScoreCouponRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	calculateCouponRequest := &promotionsv1.CalculateCouponRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.CalculateCouponRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: cachedDetails.GetDestination().GetRecipient()},
		},
	}
	calculateCouponReply, err := grpcx.NewClient(s.conn, promotionsv1.NewCouponCalculationServiceClient).CalculateCoupon(ctx, calculateCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/CalculateCoupon")
	}
	step := &couponValidationServerScoreCouponCountAnomaliesStep{request: &anomaliesv1.CountAnomaliesRequest{
		Context: req.GetContext(),
		Filter: &anomaliesv1.CountAnomaliesRequest_Filter{
			Region: req.GetDetails().GetDestination().GetRegion(),
			Labels: calculateCouponReply.GetCoupon().GetLabels(),
		},
	}}
	if err := s.runScoreCouponCountAnomalies(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.anomalies.v1.AnomalyQueryService/CountAnomalies")
	}
	countAnomaliesReply := step.reply
	return &promotionsv1.ScoreCouponResponse{
		Result: &commonv1.OperationResult{
			Status:       resolvePackingSlipsReply.GetResult().GetStatus(),
			Details:      resolvePackingSlipsReply.GetResult().GetDetails(),
			ReferenceIds: countAnomaliesReply.GetResult().GetReferenceIds(),
		},
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateCouponReply.GetCoupon().GetAudit().GetCreatedAt(),
			},
			CouponId:  req.GetCouponId(),
			Available: req.GetCoupon().GetAvailable(),
		},
	}, nil
}

// LintCoupons lints a coupon.
func (s *CouponValidationServer) LintCoupons(ctx context.Context, req *promotionsv1.LintCouponsRequest) (*promotionsv1.LintCouponsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "lint coupons", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	quoteSavedListRequest := &cartv1.QuoteSavedListRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Settlement: &cartv1.SavedList_Card{Card: &cartv1.SavedListCardSettlement{
				Token:   req.GetCoupon().GetCard().GetToken(),
				Network: req.GetCoupon().GetCard().GetNetwork(),
			}},
		},
	}
	quoteSavedListReply, err := s.savedListCalculationLazy().QuoteSavedList(ctx, quoteSavedListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListCalculationService/QuoteSavedList")
	}
	summarizeMetricSeriesesRequest := &metricsv1.SummarizeMetricSeriesesRequest{}
	summarizeMetricSeriesesRequest.Context = req.GetContext()
	summarizeMetricSeriesesRequest.Reason = req.GetReason()
	summarizeMetricSeriesesRequest.Annotations = req.GetAnnotations()
	summarizeMetricSeriesesRequest.Details = &metricsv1.SummarizeMetricSeriesesRequest_Details{Filters: req.GetCoupon().GetLabels()}
	summarizeMetricSeriesesReply, err := s.metricSeriesQueryClient.SummarizeMetricSerieses(ctx, summarizeMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses")
	}
	return &promotionsv1.LintCouponsResponse{
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				Version: quoteSavedListReply.GetSavedList().GetCounterparty().GetVersion(),
			},
			CouponId: req.GetCouponId(),
			Status:   req.GetCoupon().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: summarizeMetricSeriesesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetCouponRules returns a coupon.
func (s *CouponValidationServer) GetCouponRules(ctx context.Context, req *promotionsv1.GetCouponRulesRequest) (*promotionsv1.GetCouponRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "get_coupon_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get coupon rules", "tenant", req.GetContext().GetTenantId())
	calculatePromotionRequest := &promotionsv1.CalculatePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	var calculatePromotionReply *promotionsv1.CalculatePromotionResponse
	if err := pipeline.Run(ctx, "acme.promotions.v1.PromotionCalculationService/CalculatePromotion", func(ctx context.Context) error {
		answer, err := s.promotionCalculationClient.CalculatePromotion(ctx, calculatePromotionRequest)
		calculatePromotionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/CalculatePromotion")
	}
	return &promotionsv1.GetCouponRulesResponse{
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				Etag: calculatePromotionReply.GetPromotion().GetAudit().GetEtag(),
			},
			CouponId: req.GetCouponId(),
		},
		Result: &commonv1.OperationResult{Status: calculatePromotionReply.GetResult().GetStatus()},
	}, nil
}

// couponValidationServerExplainCouponDecisionResolveSeatMapsStep carries the request of one step of
// ExplainCouponDecision and, once it ran, its reply.
type couponValidationServerExplainCouponDecisionResolveSeatMapsStep struct {
	request *seatingv1.ResolveSeatMapsRequest
	reply   *seatingv1.ResolveSeatMapsResponse
}

func (s *CouponValidationServer) runExplainCouponDecisionResolveSeatMaps(ctx context.Context, step *couponValidationServerExplainCouponDecisionResolveSeatMapsStep) error {
	reply, err := s.seatMapQueryClient.ResolveSeatMaps(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// couponValidationServerExplainLoyaltyMemberDecisionSteps are the ways to run
// acme.loyalty.v1.LoyaltyMemberValidationService/ExplainLoyaltyMemberDecision, by name.
var couponValidationServerExplainLoyaltyMemberDecisionSteps = map[string]func(*CouponValidationServer, context.Context, *loyaltyv1.ExplainLoyaltyMemberDecisionRequest) (*loyaltyv1.ExplainLoyaltyMemberDecisionResponse, error){
	"default": (*CouponValidationServer).explainLoyaltyMemberDecisionStep,
	"audited": (*CouponValidationServer).explainLoyaltyMemberDecisionAuditedStep,
}

func (s *CouponValidationServer) explainLoyaltyMemberDecisionStep(ctx context.Context, request *loyaltyv1.ExplainLoyaltyMemberDecisionRequest) (*loyaltyv1.ExplainLoyaltyMemberDecisionResponse, error) {
	return s.loyaltyMemberValidationClient.ExplainLoyaltyMemberDecision(ctx, request)
}

func (s *CouponValidationServer) explainLoyaltyMemberDecisionAuditedStep(ctx context.Context, request *loyaltyv1.ExplainLoyaltyMemberDecisionRequest) (*loyaltyv1.ExplainLoyaltyMemberDecisionResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.loyalty.v1.LoyaltyMemberValidationService/ExplainLoyaltyMemberDecision")
	return s.loyaltyMemberValidationClient.ExplainLoyaltyMemberDecision(ctx, request)
}

// couponValidationServerScoreCouponCountAnomaliesStep carries the request of one step of ScoreCoupon and, once it ran,
// its reply.
type couponValidationServerScoreCouponCountAnomaliesStep struct {
	request *anomaliesv1.CountAnomaliesRequest
	reply   *anomaliesv1.CountAnomaliesResponse
}

func (s *CouponValidationServer) runScoreCouponCountAnomalies(ctx context.Context, step *couponValidationServerScoreCouponCountAnomaliesStep) error {
	reply, err := s.anomalyQueryClient.CountAnomalies(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// savedListCalculationLazy dials acme.cart.v1.SavedListCalculationService the first time a handler needs it.
func (s *CouponValidationServer) savedListCalculationLazy() cartv1.SavedListCalculationServiceClient {
	s.savedListCalculationOnce.Do(func() { s.savedListCalculationCached = cartv1.NewSavedListCalculationServiceClient(s.conn) })
	return s.savedListCalculationCached
}
