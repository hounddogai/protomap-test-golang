package pricing

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CurrencyRateCalculationServer implements `acme.pricing.v1.CurrencyRateCalculationService`.
// Computes the figures of a currency rate, such as totals, taxes, and estimates.
type CurrencyRateCalculationServer struct {
	pricingv1.UnimplementedCurrencyRateCalculationServiceServer
	store                         *Store
	log                           *slog.Logger
	priceListValidationClient     pricingv1.PriceListValidationServiceClient
	conn                          *grpc.ClientConn
	venueClient                   venuesv1.VenueServiceClient
	clients                       *registry.Registry
	jobOpeningClient              recruitingv1.JobOpeningServiceClient
	promotionQueryClient          promotionsv1.PromotionQueryServiceClient
	priceQuoteValidationClient    pricingv1.PriceQuoteValidationServiceClient
	priceOverrideCalculationPort  priceOverrideCalculationPort
	trackingEventValidationClient shippingv1.TrackingEventValidationServiceClient
}

// NewCurrencyRateCalculationServer wires a CurrencyRateCalculationServer.
func NewCurrencyRateCalculationServer(deps platform.Deps) *CurrencyRateCalculationServer {
	return &CurrencyRateCalculationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.pricing.v1.CurrencyRateCalculationService"),
		priceListValidationClient:     pricingv1.NewPriceListValidationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		venueClient:                   venuesv1.NewVenueServiceClient(deps.Conn),
		clients:                       deps.Clients,
		jobOpeningClient:              recruitingv1.NewJobOpeningServiceClient(deps.Conn),
		promotionQueryClient:          promotionsv1.NewPromotionQueryServiceClient(deps.Conn),
		priceQuoteValidationClient:    pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		priceOverrideCalculationPort:  newPriceOverrideCalculationPort(deps),
		trackingEventValidationClient: shippingv1.NewTrackingEventValidationServiceClient(deps.Conn),
	}
}

// CalculateCurrencyRate calculates a currency rate.
func (s *CurrencyRateCalculationServer) CalculateCurrencyRate(ctx context.Context, req *pricingv1.CalculateCurrencyRateRequest) (*pricingv1.CalculateCurrencyRateResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "calculate currency rate", "tenant", req.GetContext().GetTenantId())
	checkPriceListEligibilityRequest := &pricingv1.CheckPriceListEligibilityRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCurrencyRate().GetSubject().GetDisplayName(),
				Email:       req.GetCurrencyRate().GetSubject().GetEmail(),
			},
			Action: &pricingv1.PriceList_Block{Block: &pricingv1.PriceListBlockAction{Reason: req.GetReason()}},
		},
	}
	var checkPriceListEligibilityReply *pricingv1.CheckPriceListEligibilityResponse
	if err := pipeline.Run(ctx, "acme.pricing.v1.PriceListValidationService/CheckPriceListEligibility", func(ctx context.Context) error {
		answer, err := s.priceListValidationClient.CheckPriceListEligibility(ctx, checkPriceListEligibilityRequest)
		checkPriceListEligibilityReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/CheckPriceListEligibility")
	}
	return &pricingv1.CalculateCurrencyRateResponse{
		Breakdown: &pricingv1.CalculateCurrencyRateResponse_Breakdown{
			Lines: []*pricingv1.CalculateCurrencyRateResponse_Breakdown_Line{&pricingv1.CalculateCurrencyRateResponse_Breakdown_Line{
				Amount: checkPriceListEligibilityReply.GetPriceList().GetDiscount().GetAmount(),
			}},
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Subject:        req.GetCurrencyRate().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateCurrencyRate estimates a currency rate.
func (s *CurrencyRateCalculationServer) EstimateCurrencyRate(ctx context.Context, req *pricingv1.EstimateCurrencyRateRequest) (*pricingv1.EstimateCurrencyRateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateCurrencyRate", "error", err)
		return nil, err
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "estimate currency rate", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	lintPriceListsRequest := &pricingv1.LintPriceListsRequest{
		Context: req.GetContext(),
		Details: &pricingv1.LintPriceListsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetCurrencyRate().GetSubject().GetEmail()},
		},
		Annotations: annotations,
	}
	lintPriceListsReply := new(pricingv1.LintPriceListsResponse)
	if err := grpcx.Invoke(ctx, s.conn, pricingv1.PriceListValidationService_LintPriceLists_FullMethodName, lintPriceListsRequest, lintPriceListsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/LintPriceLists")
	}
	return &pricingv1.EstimateCurrencyRateResponse{
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				Email: lintPriceListsReply.GetPriceList().GetOwner().GetEmail(),
			},
			CurrencyRateId: req.GetCurrencyRateId(),
		},
		Breakdown: &pricingv1.EstimateCurrencyRateResponse_Breakdown{
			Lines: []*pricingv1.EstimateCurrencyRateResponse_Breakdown_Line{&pricingv1.EstimateCurrencyRateResponse_Breakdown_Line{
				Amount: lintPriceListsReply.GetPriceList().GetDiscount().GetAmount(),
			}},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetCurrencyRate().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// QuoteCurrencyRate quotes a currency rate.
func (s *CurrencyRateCalculationServer) QuoteCurrencyRate(ctx context.Context, req *pricingv1.QuoteCurrencyRateRequest) (*pricingv1.QuoteCurrencyRateResponse, error) {
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "quote currency rate", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*pricingv1.NormalizePriceListResponse, len(req.GetCurrencyRate().GetMeasurements()))
	for i, measurement := range req.GetCurrencyRate().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.priceListValidationClient.NormalizePriceList(groupCtx, &pricingv1.NormalizePriceListRequest{
				Context: req.GetContext(),
				PriceList: &pricingv1.PriceList{
					Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
					Owner: &identityv1.UserRef{
						DisplayName: req.GetCurrencyRate().GetSubject().GetDisplayName(),
						Email:       req.GetCurrencyRate().GetSubject().GetEmail(),
					},
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
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/NormalizePriceList")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkPriceListEligibilityRequest := &pricingv1.CheckPriceListEligibilityRequest{
		Context:     req.GetContext(),
		PriceListId: envelope.GetDisplayName(),
	}
	checkPriceListEligibilityReply, err := grpcx.Call(ctx, s.priceListValidationClient.CheckPriceListEligibility, checkPriceListEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/CheckPriceListEligibility")
	}
	var envelope2 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validatePriceListRequest := &pricingv1.ValidatePriceListRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				DisplayName: checkPriceListEligibilityReply.GetPriceList().GetOwner().GetDisplayName(),
			},
		},
		PriceListId: envelope2.GetDisplayName(),
	}
	var validatePriceListReply *pricingv1.ValidatePriceListResponse
	if err := pipeline.Run(ctx, "acme.pricing.v1.PriceListValidationService/ValidatePriceList", func(ctx context.Context) error {
		answer, err := s.priceListValidationClient.ValidatePriceList(ctx, validatePriceListRequest)
		validatePriceListReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/ValidatePriceList")
	}
	return &pricingv1.QuoteCurrencyRateResponse{
		Result: &commonv1.OperationResult{
			Message:      checkPriceListEligibilityReply.GetOutcome().GetMessage(),
			ReferenceIds: validatePriceListReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Kind:           req.GetCurrencyRate().GetKind(),
		},
	}, nil
}

// CompareCurrencyRateOptions compares a currency rate.
func (s *CurrencyRateCalculationServer) CompareCurrencyRateOptions(ctx context.Context, req *pricingv1.CompareCurrencyRateOptionsRequest) (*pricingv1.CompareCurrencyRateOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "compare currency rate options", "tenant", req.GetContext().GetTenantId())
	jobs := make(chan *pricingv1.CurrencyRateMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.venueClient.CloneVenue(workCtx, &venuesv1.CloneVenueRequest{
					Context: req.GetContext(),
					Venue: &venuesv1.Venue{
						Dimensions: &venuesv1.VenueDimensions{Unit: measurement.GetUnit()},
						Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{
							PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
						}},
						Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetCurrencyRate().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CloneVenue")
	}
	priceQuoteCalculation, err := registry.Lookup[pricingv1.PriceQuoteCalculationServiceClient](s.clients, "acme.pricing.v1.PriceQuoteCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	comparePriceQuoteOptionsRequest := &pricingv1.ComparePriceQuoteOptionsRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{Locality: req.GetDetails().GetDestination().GetLocality()},
		},
		PriceQuoteId: envelope.GetDisplayName(),
	}
	comparePriceQuoteOptionsReply, err := priceQuoteCalculation.ComparePriceQuoteOptions(ctx, comparePriceQuoteOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteCalculationService/ComparePriceQuoteOptions")
	}
	batchGetJobOpeningsRequest := &recruitingv1.BatchGetJobOpeningsRequest{Context: req.GetContext()}
	send := s.batchGetJobOpeningsSender(batchGetJobOpeningsRequest)
	batchGetJobOpeningsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningService/BatchGetJobOpenings")
	}
	return &pricingv1.CompareCurrencyRateOptionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: comparePriceQuoteOptionsReply.GetResult().GetReferenceIds(),
			Message:      batchGetJobOpeningsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Subject:        req.GetCurrencyRate().GetSubject(),
		},
	}, nil
}

// RecalculateCurrencyRates recalculates a currency rate.
func (s *CurrencyRateCalculationServer) RecalculateCurrencyRates(ctx context.Context, req *pricingv1.RecalculateCurrencyRatesRequest) (*pricingv1.RecalculateCurrencyRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateCurrencyRates", "error", err)
		return nil, err
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "recalculate currency rates", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	var holder struct{ CustomerId string }
	if record := req.GetCurrencyRate().GetSubject(); record != nil {
		if err := mapper.CopyFields(&holder, record, "CustomerId"); err != nil {
			s.log.WarnContext(ctx, "copy customer_id", "error", err)
		}
	}
	suggestMetricSeriesesRequest := &metricsv1.SuggestMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.SuggestMetricSeriesesRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetCurrencyRate().GetOccurredAt(),
			OwnerId:      holder.CustomerId,
		},
	}
	suggestMetricSeriesesReply, err := suggestMetricSeriesesCall(ctx, suggestMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SuggestMetricSerieses")
	}
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	searchLoyaltyMembersRequest := &loyaltyv1.SearchLoyaltyMembersRequest{
		Context: req.GetContext(),
		Filter:  &loyaltyv1.SearchLoyaltyMembersRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	searchLoyaltyMembersReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewLoyaltyMemberServiceClient).SearchLoyaltyMembers(ctx, searchLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberService/SearchLoyaltyMembers")
	}
	return &pricingv1.RecalculateCurrencyRatesResponse{
		Result: &commonv1.OperationResult{
			Status:       suggestMetricSeriesesReply.GetResult().GetStatus(),
			Message:      suggestMetricSeriesesReply.GetResult().GetMessage(),
			ReferenceIds: searchLoyaltyMembersReply.GetResult().GetReferenceIds(),
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			CurrencyRateId: req.GetCurrencyRateId(),
			Source:         req.GetCurrencyRate().GetSource(),
		},
	}, nil
}

// SimulateCurrencyRate simulates a currency rate.
func (s *CurrencyRateCalculationServer) SimulateCurrencyRate(ctx context.Context, req *pricingv1.SimulateCurrencyRateRequest) (*pricingv1.SimulateCurrencyRateResponse, error) {
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "simulate currency rate", "tenant", req.GetContext().GetTenantId())
	resolvePromotionsReply, err := s.promotionQueryClient.ResolvePromotions(ctx, &promotionsv1.ResolvePromotionsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionQueryService/ResolvePromotions")
	}
	normalizePriceListRequest := &pricingv1.NormalizePriceListRequest{
		Context: req.GetContext(),
		PriceList: &pricingv1.PriceList{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCurrencyRate().GetSubject().GetDisplayName(),
			},
		},
	}
	normalizePriceListReply := new(pricingv1.NormalizePriceListResponse)
	if err := s.conn.Invoke(ctx, pricingPriceListValidationServiceNormalizePriceListMethod, normalizePriceListRequest, normalizePriceListReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/NormalizePriceList")
	}
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintPriceQuotesReply, err := s.priceQuoteValidationClient.LintPriceQuotes(ctx, &pricingv1.LintPriceQuotesRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizePriceListReply.GetPriceList().GetAudit().GetUpdatedAt(),
			},
		},
		PriceQuoteId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/LintPriceQuotes")
	}
	return &pricingv1.SimulateCurrencyRateResponse{
		CurrencyRate: &pricingv1.CurrencyRate{
			CorrelationId:  resolvePromotionsReply.GetResult().GetCorrelationId(),
			CurrencyRateId: req.GetCurrencyRateId(),
			Attributes:     req.GetCurrencyRate().GetAttributes(),
		},
		Breakdown: &pricingv1.SimulateCurrencyRateResponse_Breakdown{
			Lines: []*pricingv1.SimulateCurrencyRateResponse_Breakdown_Line{&pricingv1.SimulateCurrencyRateResponse_Breakdown_Line{
				Amount: normalizePriceListReply.GetPriceList().GetDiscount().GetAmount(),
			}},
		},
		Result: &commonv1.OperationResult{Status: lintPriceQuotesReply.GetResult().GetStatus()},
	}, nil
}

// ExplainCurrencyRateCalculation explains a currency rate.
func (s *CurrencyRateCalculationServer) ExplainCurrencyRateCalculation(ctx context.Context, req *pricingv1.ExplainCurrencyRateCalculationRequest) (*pricingv1.ExplainCurrencyRateCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainCurrencyRateCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveCurrencyRate(ctx, req.GetContext().GetTenantId(), req.GetCurrencyRate()); err != nil {
		return nil, grpcx.Wrap(err, "save currency rate")
	}
	s.log.InfoContext(ctx, "explain currency rate calculation", "tenant", req.GetContext().GetTenantId())
	source := req.GetCurrencyRate().GetSubject().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	var envelope pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkPriceListEligibilityReply, err := s.priceListValidationClient.CheckPriceListEligibility(ctx, &pricingv1.CheckPriceListEligibilityRequest{
		Context:     req.GetContext(),
		PriceList:   &pricingv1.PriceList{Owner: &identityv1.UserRef{Email: email}},
		PriceListId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/CheckPriceListEligibility")
	}
	validatePriceListRequest := buildValidatePriceListRequestForExplainCurrencyRateCalculation(req, checkPriceListEligibilityReply)
	validatePriceListReply, err := s.priceListValidationClient.ValidatePriceList(ctx, validatePriceListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListValidationService/ValidatePriceList")
	}
	var envelope2 pricingv1.PriceListRef
	if err := req.GetCurrencyRate().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	calculatePriceOverrideRequest := &pricingv1.CalculatePriceOverrideRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Action: &pricingv1.PriceOverride_Block{Block: &pricingv1.PriceOverrideBlockAction{
				NotifyEmail: validatePriceListReply.GetPriceList().GetBlock().GetNotifyEmail(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkPriceListEligibilityReply.GetPriceList().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId: checkPriceListEligibilityReply.GetPriceList().GetOwner().GetUserId(),
				Email:  validatePriceListReply.GetPriceList().GetOwner().GetEmail(),
			},
		},
		PriceOverrideId: envelope2.GetDisplayName(),
	}
	calculatePriceOverrideReply, err := s.priceOverrideCalculationPort.CalculatePriceOverride(ctx, calculatePriceOverrideRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/CalculatePriceOverride")
	}
	for measurement := range iterx.Each(req.GetCurrencyRate().GetMeasurements(), func(candidate *pricingv1.CurrencyRateMeasurement) bool { return candidate != nil }) {
		if _, err := s.trackingEventValidationClient.ScoreTrackingEvent(ctx, &shippingv1.ScoreTrackingEventRequest{
			Context: req.GetContext(),
			TrackingEvent: &shippingv1.TrackingEvent{
				Measurements: []*shippingv1.TrackingEventMeasurement{&shippingv1.TrackingEventMeasurement{
					Metric:     measurement.GetMetric(),
					ObservedAt: measurement.GetObservedAt(),
					Value:      measurement.GetValue(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventValidationService/ScoreTrackingEvent")
		}
	}
	return &pricingv1.ExplainCurrencyRateCalculationResponse{
		Outcome: &pricingv1.ExplainCurrencyRateCalculationResponse_Outcome{
			ReferenceId: checkPriceListEligibilityReply.GetOutcome().GetReferenceId(),
			EffectiveAt: validatePriceListReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Details: calculatePriceOverrideReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				DisplayName: calculatePriceOverrideReply.GetPriceOverride().GetOwner().GetDisplayName(),
			},
			CurrencyRateId: req.GetCurrencyRateId(),
			Sequence:       req.GetCurrencyRate().GetSequence(),
		},
	}, nil
}

// GetCurrencyRateRates returns a currency rate.
func (s *CurrencyRateCalculationServer) GetCurrencyRateRates(ctx context.Context, req *pricingv1.GetCurrencyRateRatesRequest) (*pricingv1.GetCurrencyRateRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "currency_rate", req.GetCurrencyRateId(), "get_currency_rate_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get currency rate rates", "tenant", req.GetContext().GetTenantId())
	return &pricingv1.GetCurrencyRateRatesResponse{
		CurrencyRate: &pricingv1.CurrencyRate{CurrencyRateId: req.GetCurrencyRateId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// batchGetJobOpeningsSender returns a function that sends one request to
// acme.recruiting.v1.JobOpeningService/BatchGetJobOpenings.
func (s *CurrencyRateCalculationServer) batchGetJobOpeningsSender(request *recruitingv1.BatchGetJobOpeningsRequest) func(context.Context) (*recruitingv1.BatchGetJobOpeningsResponse, error) {
	return func(ctx context.Context) (*recruitingv1.BatchGetJobOpeningsResponse, error) {
		return s.jobOpeningClient.BatchGetJobOpenings(ctx, request)
	}
}
