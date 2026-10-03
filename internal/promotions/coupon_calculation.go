package promotions

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

// CouponCalculationServer implements `acme.promotions.v1.CouponCalculationService`.
// Computes the figures of a coupon, such as totals, taxes, and estimates.
type CouponCalculationServer struct {
	promotionsv1.UnimplementedCouponCalculationServiceServer
	store                      *Store
	log                        *slog.Logger
	promotionCalculationClient promotionsv1.PromotionCalculationServiceClient
	conn                       *grpc.ClientConn
	venueQueryOnce             sync.Once
	venueQueryCached           venuesv1.VenueQueryServiceClient
	synonymExportClient        searchv1.SynonymExportServiceClient
	bus                        *eventbus.Bus
}

// NewCouponCalculationServer wires a CouponCalculationServer.
func NewCouponCalculationServer(deps platform.Deps) *CouponCalculationServer {
	return &CouponCalculationServer{
		store:                      NewStore(deps.DB),
		log:                        deps.Logger.With("service", "acme.promotions.v1.CouponCalculationService"),
		promotionCalculationClient: promotionsv1.NewPromotionCalculationServiceClient(deps.Conn),
		conn:                       deps.Conn,
		synonymExportClient:        searchv1.NewSynonymExportServiceClient(deps.Conn),
		bus:                        deps.Bus,
	}
}

// CalculateCoupon calculates a coupon.
func (s *CouponCalculationServer) CalculateCoupon(ctx context.Context, req *promotionsv1.CalculateCouponRequest) (*promotionsv1.CalculateCouponResponse, error) {
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "calculate coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	for _, limit := range req.GetCoupon().GetLimits() {
		if _, err := s.promotionCalculationClient.EstimatePromotion(ctx, &promotionsv1.EstimatePromotionRequest{
			Context: req.GetContext(),
			Promotion: &promotionsv1.Promotion{
				Name:   limit.GetName(),
				Action: &promotionsv1.Promotion_Discount{Discount: &promotionsv1.PromotionDiscountAction{Amount: limit.GetAmount()}},
			},
			Details: &promotionsv1.EstimatePromotionRequest_Details{
				Destination: &commonv1.PostalAddress{
					CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/EstimatePromotion")
		}
	}
	getPromotionRatesRequest := &promotionsv1.GetPromotionRatesRequest{}
	getPromotionRatesRequest.Context = req.GetContext()
	getPromotionRatesRequest.PromotionId = req.GetCoupon().GetHolder().GetPromotionId()
	getPromotionRatesReply, err := s.promotionCalculationClient.GetPromotionRates(ctx, getPromotionRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/GetPromotionRates")
	}
	return &promotionsv1.CalculateCouponResponse{
		Coupon: &promotionsv1.Coupon{
			Limits: []*promotionsv1.CouponLimit{&promotionsv1.CouponLimit{Name: getPromotionRatesReply.GetPromotion().GetName()}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: getPromotionRatesReply.GetPromotion().GetAudit().GetUpdatedAt(),
			},
			CouponId:  req.GetCouponId(),
			Available: req.GetCoupon().GetAvailable(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateCoupon estimates a coupon.
func (s *CouponCalculationServer) EstimateCoupon(ctx context.Context, req *promotionsv1.EstimateCouponRequest) (*promotionsv1.EstimateCouponResponse, error) {
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "estimate coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	for _, limit := range req.GetCoupon().GetLimits() {
		if _, err := s.promotionCalculationClient.SimulatePromotion(ctx, &promotionsv1.SimulatePromotionRequest{
			Context: req.GetContext(),
			Promotion: &promotionsv1.Promotion{
				Name:   limit.GetName(),
				Action: &promotionsv1.Promotion_Discount{Discount: &promotionsv1.PromotionDiscountAction{Amount: limit.GetAmount()}},
				Owner:  &identityv1.UserRef{Email: req.GetCoupon().GetContact().GetEmail()},
			},
			Details: &promotionsv1.SimulatePromotionRequest_Details{
				Destination: &commonv1.PostalAddress{Lines: req.GetDetails().GetDestination().GetLines()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/SimulatePromotion")
		}
	}
	jobs := make(chan *promotionsv1.CouponLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit2 := range jobs {
				if _, err := s.promotionCalculationClient.EstimatePromotion(workCtx, &promotionsv1.EstimatePromotionRequest{
					Context: req.GetContext(),
					Promotion: &promotionsv1.Promotion{
						Action: &promotionsv1.Promotion_Discount{Discount: &promotionsv1.PromotionDiscountAction{Amount: limit2.GetAmount()}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetCoupon().GetLimits() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/EstimatePromotion")
	}
	return &promotionsv1.EstimateCouponResponse{
		Coupon: &promotionsv1.Coupon{
			CouponId:     req.GetCouponId(),
			CurrencyCode: req.GetDetails().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteCoupon quotes a coupon.
func (s *CouponCalculationServer) QuoteCoupon(ctx context.Context, req *promotionsv1.QuoteCouponRequest) (*promotionsv1.QuoteCouponResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "quote coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetCoupon().GetLabels()))
	for key, value := range req.GetCoupon().GetLabels() {
		labels[key] = value
	}
	findVenuesRequest := &venuesv1.FindVenuesRequest{
		Context: req.GetContext(),
		Filter:  &venuesv1.FindVenuesRequest_Filter{Labels: labels},
	}
	findVenuesReply, err := s.venueQueryLazy().FindVenues(ctx, findVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/FindVenues")
	}
	jobs := make(chan *promotionsv1.CouponLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit := range jobs {
				if _, err := s.promotionCalculationClient.SimulatePromotion(workCtx, &promotionsv1.SimulatePromotionRequest{
					Context: req.GetContext(),
					Promotion: &promotionsv1.Promotion{
						Name:   limit.GetName(),
						Action: &promotionsv1.Promotion_Discount{Discount: &promotionsv1.PromotionDiscountAction{Amount: limit.GetAmount()}},
						Audit:  &commonv1.AuditInfo{Etag: req.GetCoupon().GetAudit().GetEtag()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetCoupon().GetLimits() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/SimulatePromotion")
	}
	return &promotionsv1.QuoteCouponResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: findVenuesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Balance:  req.GetCoupon().GetBalance(),
		},
	}, nil
}

// CompareCouponOptions compares a coupon.
func (s *CouponCalculationServer) CompareCouponOptions(ctx context.Context, req *promotionsv1.CompareCouponOptionsRequest) (*promotionsv1.CompareCouponOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "compare coupon options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	detailsCache := cache.New[string, *promotionsv1.CompareCouponOptionsRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	simulatePromotionReply, err := s.promotionCalculationClient.SimulatePromotion(ctx, &promotionsv1.SimulatePromotionRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.SimulatePromotionRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: cachedDetails.GetDestination().GetLines()},
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetCoupon().GetAudit().GetUpdatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/SimulatePromotion")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := couponCalculationServerQuotePromotionSteps[mode]
	quotePromotionRequest := &promotionsv1.QuotePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Action: &promotionsv1.Promotion_Route{Route: &promotionsv1.PromotionRouteAction{
				Weight: simulatePromotionReply.GetPromotion().GetRoute().GetWeight(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCoupon().GetAudit().GetUpdatedAt(),
				Etag:      req.GetCoupon().GetAudit().GetEtag(),
				CreatedAt: simulatePromotionReply.GetPromotion().GetAudit().GetCreatedAt(),
			},
		},
	}
	quotePromotionReply, err := run(s, ctx, quotePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/QuotePromotion")
	}
	return &promotionsv1.CompareCouponOptionsResponse{
		Result: &commonv1.OperationResult{
			Details: simulatePromotionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulatePromotionReply.GetPromotion().GetAudit().GetUpdatedAt(),
				Etag:      quotePromotionReply.GetPromotion().GetAudit().GetEtag(),
			},
			CouponId: req.GetCouponId(),
			Status:   req.GetCoupon().GetStatus(),
		},
	}, nil
}

// RecalculateCoupons recalculates a coupon.
func (s *CouponCalculationServer) RecalculateCoupons(ctx context.Context, req *promotionsv1.RecalculateCouponsRequest) (*promotionsv1.RecalculateCouponsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "recalculate coupons", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCoupon())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.Coupon
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	listSynonymExportsRequest := &searchv1.ListSynonymExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymExportsRequest_Filter{
			Labels:       req.GetCoupon().GetLabels(),
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: (&decoded).GetAudit().GetUpdatedAt(),
		},
	}
	var listSynonymExportsReply *searchv1.ListSynonymExportsResponse
	if err := pipeline.Run(ctx, "acme.search.v1.SynonymExportService/ListSynonymExports", func(ctx context.Context) error {
		answer, err := s.synonymExportClient.ListSynonymExports(ctx, listSynonymExportsRequest)
		listSynonymExportsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ListSynonymExports")
	}
	return &promotionsv1.RecalculateCouponsResponse{
		Result: &commonv1.OperationResult{
			Details: listSynonymExportsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Status:   req.GetCoupon().GetStatus(),
		},
	}, nil
}

// SimulateCoupon simulates a coupon.
func (s *CouponCalculationServer) SimulateCoupon(ctx context.Context, req *promotionsv1.SimulateCouponRequest) (*promotionsv1.SimulateCouponResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "simulate coupon", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*promotionsv1.QuotePromotionResponse, len(req.GetCoupon().GetLimits()))
	for i, limit := range req.GetCoupon().GetLimits() {
		group.Go(func() error {
			reply, err := s.promotionCalculationClient.QuotePromotion(groupCtx, &promotionsv1.QuotePromotionRequest{
				Context: req.GetContext(),
				Promotion: &promotionsv1.Promotion{
					Action: &promotionsv1.Promotion_Discount{Discount: &promotionsv1.PromotionDiscountAction{Amount: limit.GetAmount()}},
					Name:   limit.GetName(),
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/QuotePromotion")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	getPromotionRatesReply, err := s.promotionCalculationClient.GetPromotionRates(ctx, &promotionsv1.GetPromotionRatesRequest{
		Context:     req.GetContext(),
		PromotionId: req.GetCoupon().GetHolder().GetPromotionId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/GetPromotionRates")
	}
	return &promotionsv1.SimulateCouponResponse{
		Breakdown: &promotionsv1.SimulateCouponResponse_Breakdown{
			Lines: []*promotionsv1.SimulateCouponResponse_Breakdown_Line{&promotionsv1.SimulateCouponResponse_Breakdown_Line{
				Amount: getPromotionRatesReply.GetPromotion().GetDiscount().GetAmount(),
			}},
		},
		Coupon: &promotionsv1.Coupon{
			CouponId:     req.GetCouponId(),
			CurrencyCode: req.GetDetails().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainCouponCalculation explains a coupon.
func (s *CouponCalculationServer) ExplainCouponCalculation(ctx context.Context, req *promotionsv1.ExplainCouponCalculationRequest) (*promotionsv1.ExplainCouponCalculationResponse, error) {
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "explain coupon calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.ExplainCouponCalculationRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	simulatePromotionRequest := &promotionsv1.SimulatePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetCoupon().GetAudit().GetUpdatedAt()},
			Owner: &identityv1.UserRef{
				Email:       (&decoded).GetContact().GetEmail(),
				DisplayName: req.GetCoupon().GetHolder().GetDisplayName(),
			},
		},
	}
	simulatePromotionReply := new(promotionsv1.SimulatePromotionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", promotionsPromotionCalculationServiceName, "SimulatePromotion"), simulatePromotionRequest, simulatePromotionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/SimulatePromotion")
	}
	estimatePromotionRequest := &promotionsv1.EstimatePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Action: &promotionsv1.Promotion_Block{Block: &promotionsv1.PromotionBlockAction{
				NotifyEmail: simulatePromotionReply.GetPromotion().GetBlock().GetNotifyEmail(),
				Reason:      simulatePromotionReply.GetPromotion().GetBlock().GetReason(),
			}},
		},
	}
	estimatePromotionReply, err := promotionsv1.PromotionCalculationServiceClient.EstimatePromotion(s.promotionCalculationClient, ctx, estimatePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/EstimatePromotion")
	}
	return &promotionsv1.ExplainCouponCalculationResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: simulatePromotionReply.GetResult().GetCorrelationId(),
			Details:       estimatePromotionReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimatePromotionReply.GetPromotion().GetAudit().GetCreatedAt(),
			},
			CouponId: req.GetCouponId(),
			Status:   req.GetCoupon().GetStatus(),
		},
	}, nil
}

// GetCouponRates returns a coupon.
func (s *CouponCalculationServer) GetCouponRates(ctx context.Context, req *promotionsv1.GetCouponRatesRequest) (*promotionsv1.GetCouponRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetCouponRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "get_coupon_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get coupon rates", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "promotions.get_coupon_rates.recalculate_promotions",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"promotion.owner.email":   req.GetContext().GetActor().GetEmail(),
			"promotion.owner.user_id": req.GetContext().GetActor().GetUserId(),
			"context.tenant_id":       req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	findAuditEventsRequest := &auditv1.FindAuditEventsRequest{Context: req.GetContext()}
	findAuditEventsReply := new(auditv1.FindAuditEventsResponse)
	if err := grpcx.Invoke(ctx, s.conn, auditv1.AuditEventQueryService_FindAuditEvents_FullMethodName, findAuditEventsRequest, findAuditEventsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/FindAuditEvents")
	}
	return &promotionsv1.GetCouponRatesResponse{
		Result: &commonv1.OperationResult{Status: findAuditEventsReply.GetResult().GetStatus()},
		Coupon: &promotionsv1.Coupon{CouponId: req.GetCouponId()},
	}, nil
}

// venueQueryLazy dials acme.venues.v1.VenueQueryService the first time a handler needs it.
func (s *CouponCalculationServer) venueQueryLazy() venuesv1.VenueQueryServiceClient {
	s.venueQueryOnce.Do(func() { s.venueQueryCached = venuesv1.NewVenueQueryServiceClient(s.conn) })
	return s.venueQueryCached
}

// couponCalculationServerQuotePromotionSteps are the ways to run
// acme.promotions.v1.PromotionCalculationService/QuotePromotion, by name.
var couponCalculationServerQuotePromotionSteps = map[string]func(*CouponCalculationServer, context.Context, *promotionsv1.QuotePromotionRequest) (*promotionsv1.QuotePromotionResponse, error){
	"default": (*CouponCalculationServer).quotePromotionStep,
	"audited": (*CouponCalculationServer).quotePromotionAuditedStep,
}

func (s *CouponCalculationServer) quotePromotionStep(ctx context.Context, request *promotionsv1.QuotePromotionRequest) (*promotionsv1.QuotePromotionResponse, error) {
	return s.promotionCalculationClient.QuotePromotion(ctx, request)
}

func (s *CouponCalculationServer) quotePromotionAuditedStep(ctx context.Context, request *promotionsv1.QuotePromotionRequest) (*promotionsv1.QuotePromotionResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.promotions.v1.PromotionCalculationService/QuotePromotion")
	return s.promotionCalculationClient.QuotePromotion(ctx, request)
}
