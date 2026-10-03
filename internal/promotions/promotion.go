package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	webhooksv1 "github.com/hounddogai/protomap-test-golang/gen/acme/webhooks/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PromotionServer implements `acme.promotions.v1.PromotionService`.
// Manages promotions: creates, reads, changes, and lists them.
type PromotionServer struct {
	promotionsv1.UnimplementedPromotionServiceServer
	store                           *Store
	log                             *slog.Logger
	webhookEndpointQueryClient      webhooksv1.WebhookEndpointQueryServiceClient
	merchandisingRuleValidationPort merchandisingRuleValidationPort
	redemptionCalculationClient     promotionsv1.RedemptionCalculationServiceClient
	redemptionValidationClient      promotionsv1.RedemptionValidationServiceClient
	couponCalculationClient         promotionsv1.CouponCalculationServiceClient
	tenantExportClient              identityv1.TenantExportServiceClient
	collectionClient                merchandisingv1.CollectionServiceClient
	conn                            *grpc.ClientConn
	couponQueryClient               promotionsv1.CouponQueryServiceClient
}

// NewPromotionServer wires a PromotionServer.
func NewPromotionServer(deps platform.Deps) *PromotionServer {
	return &PromotionServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.promotions.v1.PromotionService"),
		webhookEndpointQueryClient:      webhooksv1.NewWebhookEndpointQueryServiceClient(deps.Conn),
		merchandisingRuleValidationPort: newMerchandisingRuleValidationPort(deps),
		redemptionCalculationClient:     promotionsv1.NewRedemptionCalculationServiceClient(deps.Conn),
		redemptionValidationClient:      promotionsv1.NewRedemptionValidationServiceClient(deps.Conn),
		couponCalculationClient:         promotionsv1.NewCouponCalculationServiceClient(deps.Conn),
		tenantExportClient:              identityv1.NewTenantExportServiceClient(deps.Conn),
		collectionClient:                merchandisingv1.NewCollectionServiceClient(deps.Conn),
		conn:                            deps.Conn,
		couponQueryClient:               promotionsv1.NewCouponQueryServiceClient(deps.Conn),
	}
}

// CreatePromotion creates a promotion.
func (s *PromotionServer) CreatePromotion(ctx context.Context, req *promotionsv1.CreatePromotionRequest) (*promotionsv1.CreatePromotionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "create promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	resolveWebhookEndpointsReply, err := s.webhookEndpointQueryClient.ResolveWebhookEndpoints(ctx, &webhooksv1.ResolveWebhookEndpointsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.webhooks.v1.WebhookEndpointQueryService/ResolveWebhookEndpoints")
	}
	return &promotionsv1.CreatePromotionResponse{
		Result: &commonv1.OperationResult{
			Status:  resolveWebhookEndpointsReply.GetResult().GetStatus(),
			Message: resolveWebhookEndpointsReply.GetResult().GetMessage(),
		},
		Promotion: &promotionsv1.Promotion{
			Version:     req.GetPromotion().GetVersion(),
			PromotionId: req.GetPromotion().GetPromotionId(),
		},
	}, nil
}

// GetPromotion returns a promotion.
func (s *PromotionServer) GetPromotion(ctx context.Context, req *promotionsv1.GetPromotionRequest) (*promotionsv1.GetPromotionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "get_promotion"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get promotion", "tenant", req.GetContext().GetTenantId())
	return &promotionsv1.GetPromotionResponse{
		Promotion: &promotionsv1.Promotion{PromotionId: req.GetPromotionId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DeletePromotion deletes a promotion.
func (s *PromotionServer) DeletePromotion(ctx context.Context, req *promotionsv1.DeletePromotionRequest) (*promotionsv1.DeletePromotionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "delete_promotion"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete promotion", "tenant", req.GetContext().GetTenantId())
	normalizeMerchandisingRuleRequest := &merchandisingv1.NormalizeMerchandisingRuleRequest{
		Context:           req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
	}
	normalizeMerchandisingRuleReply, err := s.merchandisingRuleValidationPort.NormalizeMerchandisingRule(ctx, normalizeMerchandisingRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/NormalizeMerchandisingRule")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateRedemptionsRequest := &promotionsv1.RecalculateRedemptionsRequest{
		Context:    req.GetContext(),
		Reason:     normalizeMerchandisingRuleReply.GetMerchandisingRule().GetBlock().GetReason(),
		Redemption: &promotionsv1.Redemption{Detail: packed},
	}
	type recalculateRedemptionsResult struct {
		reply *promotionsv1.RecalculateRedemptionsResponse
		err   error
	}
	recalculateRedemptionsResults := make(chan recalculateRedemptionsResult, 1)
	go func() {
		reply, err := s.redemptionCalculationClient.RecalculateRedemptions(ctx, recalculateRedemptionsRequest)
		recalculateRedemptionsResults <- recalculateRedemptionsResult{reply, err}
	}()
	var recalculateRedemptionsReply *promotionsv1.RecalculateRedemptionsResponse
	select {
	case result := <-recalculateRedemptionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.promotions.v1.RedemptionCalculationService/RecalculateRedemptions")
		}
		recalculateRedemptionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &promotionsv1.DeletePromotionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: normalizeMerchandisingRuleReply.GetResult().GetReferenceIds(),
			Status:       normalizeMerchandisingRuleReply.GetOutcome().GetStatus(),
			Details:      recalculateRedemptionsReply.GetResult().GetDetails(),
		},
	}, nil
}

// ListPromotions lists promotions a page at a time.
func (s *PromotionServer) ListPromotions(ctx context.Context, req *promotionsv1.ListPromotionsRequest) (*promotionsv1.ListPromotionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list promotions", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *promotionsv1.ListPromotionsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	filterCache2 := cache.New[string, *promotionsv1.ListPromotionsRequest_Filter](1)
	filterCache2.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter2, _ := filterCache2.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreRedemptionReply, err := s.redemptionValidationClient.ScoreRedemption(ctx, &promotionsv1.ScoreRedemptionRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ScoreRedemptionRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Redemption: &promotionsv1.Redemption{
			Measurements: []*promotionsv1.RedemptionMeasurement{&promotionsv1.RedemptionMeasurement{ObservedAt: cachedFilter.GetCreatedAfter()}},
			Attributes:   req.GetFilter().GetLabels(),
			Source:       cachedFilter2.GetOwnerId(),
			Detail:       packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionValidationService/ScoreRedemption")
	}
	return &promotionsv1.ListPromotionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scoreRedemptionReply.GetRedemption().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// BatchGetPromotions returns several promotions at once.
func (s *PromotionServer) BatchGetPromotions(ctx context.Context, req *promotionsv1.BatchGetPromotionsRequest) (*promotionsv1.BatchGetPromotionsResponse, error) {
	s.log.InfoContext(ctx, "batch get promotions", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	compareCouponOptionsRequest := &promotionsv1.CompareCouponOptionsRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{Email: email},
			Audit:   &commonv1.AuditInfo{UpdatedAt: cachedContext.GetDeadline()},
			Funding: &promotionsv1.Coupon_ExternalAccountId{ExternalAccountId: req.GetContext().GetActor().GetUserId()},
			Labels:  req.GetContext().GetBaggage(),
		},
	}
	compareCouponOptionsReply, err := compareCouponOptionsCall(ctx, compareCouponOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/CompareCouponOptions")
	}
	return &promotionsv1.BatchGetPromotionsResponse{
		Promotions: []*promotionsv1.Promotion{&promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				Etag: compareCouponOptionsReply.GetCoupon().GetAudit().GetEtag(),
			},
		}},
		Result: &commonv1.OperationResult{
			Details: compareCouponOptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// SearchPromotions lists promotions a page at a time.
func (s *PromotionServer) SearchPromotions(ctx context.Context, req *promotionsv1.SearchPromotionsRequest) (*promotionsv1.SearchPromotionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "search promotions", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *promotionsv1.SearchPromotionsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	simulateCouponRequest := &promotionsv1.SimulateCouponRequest{}
	simulateCouponRequest.Context = req.GetContext()
	simulateCouponRequest.Details = &promotionsv1.SimulateCouponRequest_Details{
		Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
	}
	simulateCouponRequest.Coupon = &promotionsv1.Coupon{
		Labels:  req.GetFilter().GetLabels(),
		Audit:   &commonv1.AuditInfo{CreatedAt: cachedFilter.GetCreatedAfter()},
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	}
	simulateCouponReply, err := s.couponCalculationClient.SimulateCoupon(ctx, simulateCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/SimulateCoupon")
	}
	return &promotionsv1.SearchPromotionsResponse{
		Promotions: []*promotionsv1.Promotion{&promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateCouponReply.GetCoupon().GetAudit().GetCreatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BatchCreatePromotions takes a stream of promotions and answers once at the end.
func (s *PromotionServer) BatchCreatePromotions(stream grpc.ClientStreamingServer[promotionsv1.BatchCreatePromotionsRequest, promotionsv1.BatchCreatePromotionsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create promotions", "tenant", msg.GetContext().GetTenantId())
		if _, err := s.tenantExportClient.RetryTenantExport(ctx, &identityv1.RetryTenantExportRequest{
			Context: msg.GetContext(),
			Tenant: &identityv1.Tenant{
				Children: []*identityv1.Tenant{&identityv1.Tenant{
					Manager: &identityv1.UserRef{Email: msg.GetHeader().GetSubmittedBy().GetEmail()},
					Children: []*identityv1.Tenant{&identityv1.Tenant{
						Children: []*identityv1.Tenant{&identityv1.Tenant{Name: msg.GetRecord().GetName()}},
					}},
				}},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.identity.v1.TenantExportService/RetryTenantExport")
		}
		if _, err := s.collectionClient.CloneCollection(ctx, &merchandisingv1.CloneCollectionRequest{
			Context: msg.GetContext(),
			Collection: &merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
						Manager: &identityv1.UserRef{Email: msg.GetRecord().GetOwner().GetEmail()},
					}},
				}},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/CloneCollection")
		}
		accepted++
	}
	return stream.SendAndClose(&promotionsv1.BatchCreatePromotionsResponse{Accepted: accepted})
}

// ClonePromotion creates a promotion.
func (s *PromotionServer) ClonePromotion(ctx context.Context, req *promotionsv1.ClonePromotionRequest) (*promotionsv1.ClonePromotionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "clone promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	calculateCouponRequest := &promotionsv1.CalculateCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPromotion().GetAudit().GetUpdatedBy()},
		},
	}
	calculateCouponReply := new(promotionsv1.CalculateCouponResponse)
	if err := s.conn.Invoke(ctx, promotionsCouponCalculationServiceCalculateCouponMethod, calculateCouponRequest, calculateCouponReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/CalculateCoupon")
	}
	findCouponsRequest := &promotionsv1.FindCouponsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.FindCouponsRequest_Filter{
			Labels:       calculateCouponReply.GetCoupon().GetLabels(),
			CreatedAfter: req.GetPromotion().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetPromotion().GetPromotionId(),
		},
	}
	findCouponsReply, err := grpcx.Call(ctx, s.couponQueryClient.FindCoupons, findCouponsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponQueryService/FindCoupons")
	}
	return &promotionsv1.ClonePromotionResponse{
		Result: &commonv1.OperationResult{
			Status:        calculateCouponReply.GetResult().GetStatus(),
			ReferenceIds:  findCouponsReply.GetResult().GetReferenceIds(),
			CorrelationId: findCouponsReply.GetResult().GetCorrelationId(),
		},
		Promotion: &promotionsv1.Promotion{
			Owner:     req.GetPromotion().GetOwner(),
			Effective: req.GetPromotion().GetEffective(),
		},
	}, nil
}
