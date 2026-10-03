package giftcards

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ratelimitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ratelimiting/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// GiftCardServer implements `acme.giftcards.v1.GiftCardService`.
// Manages gift cards: creates, reads, changes, and lists them.
type GiftCardServer struct {
	giftcardsv1.UnimplementedGiftCardServiceServer
	store                                 *Store
	log                                   *slog.Logger
	giftCardCalculationClient             giftcardsv1.GiftCardCalculationServiceClient
	venuePort                             venuePort
	giftCardTransactionCalculationClient  giftcardsv1.GiftCardTransactionCalculationServiceClient
	clients                               *registry.Registry
	conn                                  *grpc.ClientConn
	giftCardBatchQueryPort                giftCardBatchQueryPort
	driverQueryClient                     driversv1.DriverQueryServiceClient
	rateLimitClient                       ratelimitingv1.RateLimitServiceClient
	giftCardBatchQueryClient              giftcardsv1.GiftCardBatchQueryServiceClient
	giftCardBatchValidationClient         giftcardsv1.GiftCardBatchValidationServiceClient
	giftCardBatchValidationClientFallback giftcardsv1.GiftCardBatchValidationServiceClient
	giftCardValidationClient              giftcardsv1.GiftCardValidationServiceClient
	giftCardQueryClient                   giftcardsv1.GiftCardQueryServiceClient
}

// NewGiftCardServer wires a GiftCardServer.
func NewGiftCardServer(deps platform.Deps) *GiftCardServer {
	return &GiftCardServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.giftcards.v1.GiftCardService"),
		giftCardCalculationClient:             giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		venuePort:                             newVenuePort(deps),
		giftCardTransactionCalculationClient:  giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		clients:                               deps.Clients,
		conn:                                  deps.Conn,
		giftCardBatchQueryPort:                newGiftCardBatchQueryPort(deps),
		driverQueryClient:                     driversv1.NewDriverQueryServiceClient(deps.Conn),
		rateLimitClient:                       ratelimitingv1.NewRateLimitServiceClient(deps.Conn),
		giftCardBatchQueryClient:              giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		giftCardBatchValidationClient:         giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
		giftCardBatchValidationClientFallback: giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
		giftCardValidationClient:              giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		giftCardQueryClient:                   giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn),
	}
}

// CreateGiftCard creates a gift card.
func (s *GiftCardServer) CreateGiftCard(ctx context.Context, req *giftcardsv1.CreateGiftCardRequest) (*giftcardsv1.CreateGiftCardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "create gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	explainGiftCardCalculationRequest := buildExplainGiftCardCalculationRequestForCreateGiftCard(req)
	explainGiftCardCalculationReply, err := s.giftCardCalculationClient.ExplainGiftCardCalculation(ctx, explainGiftCardCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/ExplainGiftCardCalculation")
	}
	updateVenueRequest := &venuesv1.UpdateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetGiftCard().GetAudit().GetCreatedBy()},
		},
	}
	updateVenueReply, err := s.venuePort.UpdateVenue(ctx, updateVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/UpdateVenue")
	}
	calculateGiftCardTransactionReply, err := s.giftCardTransactionCalculationClient.CalculateGiftCardTransaction(ctx, &giftcardsv1.CalculateGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CalculateGiftCardTransactionRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: updateVenueReply.GetVenue().GetAddress().GetCountryCode(),
				Locality:    updateVenueReply.GetVenue().GetAddress().GetLocality(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/CalculateGiftCardTransaction")
	}
	sessionHistory, ok := s.clients.Get("acme.identity.v1.SessionHistoryService").(identityv1.SessionHistoryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.SessionHistoryService")
	}
	snapshot, err := protojson.Marshal(req.GetGiftCard())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateSessionVersionRequest := &identityv1.AnnotateSessionVersionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Assignee: &identityv1.UserRef{
				DisplayName: calculateGiftCardTransactionReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
		},
		Details: &identityv1.AnnotateSessionVersionRequest_Details{Snapshot: snapshot},
	}
	annotateSessionVersionReply, err := sessionHistory.AnnotateSessionVersion(ctx, annotateSessionVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/AnnotateSessionVersion")
	}
	recentGiftCards.Store(recordKey{req.GetContext().GetTenantId(), req.GetGiftCard().GetGiftCardId()}, req.GetGiftCard())
	return &giftcardsv1.CreateGiftCardResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				Etag: explainGiftCardCalculationReply.GetGiftCard().GetAudit().GetEtag(),
			},
			CurrencyCode: calculateGiftCardTransactionReply.GetBreakdown().GetCurrencyCode(),
			Contact: &commonv1.ContactInfo{
				Email: annotateSessionVersionReply.GetSession().GetRequester().GetEmail(),
			},
			OpenedAt:  req.GetGiftCard().GetOpenedAt(),
			Available: req.GetGiftCard().GetAvailable(),
		},
		Result: &commonv1.OperationResult{
			Status:       updateVenueReply.GetResult().GetStatus(),
			ReferenceIds: annotateSessionVersionReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// GetGiftCard returns a gift card.
func (s *GiftCardServer) GetGiftCard(ctx context.Context, req *giftcardsv1.GetGiftCardRequest) (*giftcardsv1.GetGiftCardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetGiftCard", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card", req.GetGiftCardId(), "get_gift_card"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get gift card", "tenant", req.GetContext().GetTenantId())
	var cachedCurrencyCode string
	if entry, ok := recentGiftCards.Load(recordKey{req.GetContext().GetTenantId(), req.GetGiftCardId()}); ok {
		if previous, ok := entry.(*giftcardsv1.GiftCard); ok {
			cachedCurrencyCode = previous.GetCurrencyCode()
		}
	}
	browseSponsorsRequest := &sponsorshipsv1.BrowseSponsorsRequest{
		Context: req.GetContext(),
		Filter:  &sponsorshipsv1.BrowseSponsorsRequest_Filter{CreatedAfter: req.GetContext().GetDeadline()},
		OrderBy: cachedCurrencyCode,
	}
	browseSponsorsReply := new(sponsorshipsv1.BrowseSponsorsResponse)
	if err := grpcx.Invoke(ctx, s.conn, sponsorshipsv1.SponsorQueryService_BrowseSponsors_FullMethodName, browseSponsorsRequest, browseSponsorsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sponsorships.v1.SponsorQueryService/BrowseSponsors")
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeGiftCardBatchesRequest := &giftcardsv1.SummarizeGiftCardBatchesRequest{
		Context:     req.GetContext(),
		Details:     &giftcardsv1.SummarizeGiftCardBatchesRequest_Details{Filters: browseSponsorsReply.GetResult().GetDetails()},
		Annotations: baggage,
	}
	summarizeGiftCardBatchesReply, err := s.giftCardBatchQueryPort.SummarizeGiftCardBatches(ctx, summarizeGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/SummarizeGiftCardBatches")
	}
	return &giftcardsv1.GetGiftCardResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browseSponsorsReply.GetResult().GetReferenceIds(),
			Details:      browseSponsorsReply.GetResult().GetDetails(),
			Status:       summarizeGiftCardBatchesReply.GetResult().GetStatus(),
			Message:      summarizeGiftCardBatchesReply.GetResult().GetMessage(),
		},
		GiftCard: &giftcardsv1.GiftCard{GiftCardId: req.GetGiftCardId()},
	}, nil
}

// UpdateGiftCard changes a gift card.
func (s *GiftCardServer) UpdateGiftCard(ctx context.Context, req *giftcardsv1.UpdateGiftCardRequest) (*giftcardsv1.UpdateGiftCardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected UpdateGiftCard", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "update gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	giftCardCache := cache.New[string, *giftcardsv1.GiftCard](1)
	giftCardCache.Put(req.GetContext().GetTraceId(), req.GetGiftCard())
	cachedGiftCard, _ := giftCardCache.Get(req.GetContext().GetTraceId())
	suggestGiftCardsRequest := &giftcardsv1.SuggestGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.SuggestGiftCardsRequest_Filter{
			Labels:       req.GetGiftCard().GetLabels(),
			CreatedAfter: cachedGiftCard.GetAudit().GetCreatedAt(),
			OwnerId:      req.GetGiftCard().GetHolder().GetGiftCardTransactionId(),
		},
	}
	suggestGiftCardsReply := new(giftcardsv1.SuggestGiftCardsResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardQueryService_SuggestGiftCards_FullMethodName, suggestGiftCardsRequest, suggestGiftCardsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/SuggestGiftCards")
	}
	suggestDriversRequest := &driversv1.SuggestDriversRequest{}
	suggestDriversRequest.Context = req.GetContext()
	suggestDriversRequest.Filter = &driversv1.SuggestDriversRequest_Filter{
		Labels:       req.GetGiftCard().GetLabels(),
		CreatedAfter: req.GetGiftCard().GetOpenedAt(),
		OwnerId:      req.GetGiftCard().GetGiftCardId(),
	}
	suggestDriversReply, err := s.driverQueryClient.SuggestDrivers(ctx, suggestDriversRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.drivers.v1.DriverQueryService/SuggestDrivers")
	}
	watchRateLimitsStream, err := s.rateLimitClient.WatchRateLimits(ctx, &ratelimitingv1.WatchRateLimitsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: req.GetGiftCard().GetContact().GetPreferredChannel(),
			Email:            req.GetGiftCard().GetContact().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ratelimiting.v1.RateLimitService/WatchRateLimits")
	}
	for {
		received, err := watchRateLimitsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ratelimiting.v1.RateLimitService/WatchRateLimits")
		}
		_ = received
	}
	normalizeFeatureFlagRequest := &featureflagsv1.NormalizeFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetGiftCard().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetGiftCard().GetAudit().GetCreatedAt(),
			},
		},
	}
	normalizeFeatureFlagReply := new(featureflagsv1.NormalizeFeatureFlagResponse)
	if err := grpcx.Invoke(ctx, s.conn, featureflagsv1.FeatureFlagValidationService_NormalizeFeatureFlag_FullMethodName, normalizeFeatureFlagRequest, normalizeFeatureFlagReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag")
	}
	recentGiftCards.Store(recordKey{req.GetContext().GetTenantId(), req.GetGiftCard().GetGiftCardId()}, req.GetGiftCard())
	return &giftcardsv1.UpdateGiftCardResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: suggestGiftCardsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  suggestDriversReply.GetResult().GetReferenceIds(),
			Message:       suggestDriversReply.GetResult().GetMessage(),
			Details:       normalizeFeatureFlagReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			Balance:  req.GetGiftCard().GetBalance(),
			OpenedAt: req.GetGiftCard().GetOpenedAt(),
		},
	}, nil
}

// DeleteGiftCard deletes a gift card.
func (s *GiftCardServer) DeleteGiftCard(ctx context.Context, req *giftcardsv1.DeleteGiftCardRequest) (*giftcardsv1.DeleteGiftCardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card", req.GetGiftCardId(), "delete_gift_card"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete gift card", "tenant", req.GetContext().GetTenantId())
	summarizeGiftCardBatchesRequest := &giftcardsv1.SummarizeGiftCardBatchesRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetContext().GetBaggage(),
	}
	type summarizeGiftCardBatchesResult struct {
		reply *giftcardsv1.SummarizeGiftCardBatchesResponse
		err   error
	}
	summarizeGiftCardBatchesResults := make(chan summarizeGiftCardBatchesResult, 1)
	go func() {
		reply, err := s.giftCardBatchQueryClient.SummarizeGiftCardBatches(ctx, summarizeGiftCardBatchesRequest)
		summarizeGiftCardBatchesResults <- summarizeGiftCardBatchesResult{reply, err}
	}()
	var summarizeGiftCardBatchesReply *giftcardsv1.SummarizeGiftCardBatchesResponse
	select {
	case result := <-summarizeGiftCardBatchesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardBatchQueryService/SummarizeGiftCardBatches")
		}
		summarizeGiftCardBatchesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &giftcardsv1.DeleteGiftCardResponse{
		Result: &commonv1.OperationResult{
			Status: summarizeGiftCardBatchesReply.GetResult().GetStatus(),
		},
	}, nil
}

// SearchGiftCards lists gift cards a page at a time.
func (s *GiftCardServer) SearchGiftCards(ctx context.Context, req *giftcardsv1.SearchGiftCardsRequest) (*giftcardsv1.SearchGiftCardsResponse, error) {
	s.log.InfoContext(ctx, "search gift cards", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*giftcardsv1.SearchGiftCardsRequest_Filter)
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded giftcardsv1.SearchGiftCardsRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	validateGiftCardBatchRequest := &giftcardsv1.ValidateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{Region: cloned.GetRegion()},
			Audit:          &commonv1.AuditInfo{CreatedAt: (&decoded).GetCreatedAfter()},
		},
		Details: &giftcardsv1.ValidateGiftCardBatchRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	validateGiftCardBatchReply, err := grpcx.Call(ctx, s.giftCardBatchValidationClient.ValidateGiftCardBatch, validateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/ValidateGiftCardBatch")
	}
	browseMetricSeriesesRequest := &metricsv1.BrowseMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.BrowseMetricSeriesesRequest_Filter{
			OwnerId: req.GetFilter().GetOwnerId(),
			Region:  req.GetFilter().GetRegion(),
		},
		Page: &commonv1.PageRequest{PageSize: req.GetPage().GetPageSize()},
	}
	browseMetricSeriesesReply := new(metricsv1.BrowseMetricSeriesesResponse)
	if err := grpcx.Invoke(ctx, s.conn, metricsv1.MetricSeriesQueryService_BrowseMetricSerieses_FullMethodName, browseMetricSeriesesRequest, browseMetricSeriesesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses")
	}
	return &giftcardsv1.SearchGiftCardsResponse{
		Result: &commonv1.OperationResult{
			Details:       validateGiftCardBatchReply.GetResult().GetDetails(),
			CorrelationId: browseMetricSeriesesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// WatchGiftCards streams the changes of gift cards as they happen.
func (s *GiftCardServer) WatchGiftCards(req *giftcardsv1.WatchGiftCardsRequest, stream grpc.ServerStreamingServer[giftcardsv1.WatchGiftCardsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected WatchGiftCards", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "watch gift cards", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *giftcardsv1.CheckGiftCardBatchEligibilityRequest, ...grpc.CallOption) (*giftcardsv1.CheckGiftCardBatchEligibilityResponse, error){
		"primary":  s.giftCardBatchValidationClient.CheckGiftCardBatchEligibility,
		"fallback": s.giftCardBatchValidationClientFallback.CheckGiftCardBatchEligibility,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	checkGiftCardBatchEligibilityRequest := &giftcardsv1.CheckGiftCardBatchEligibilityRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CheckGiftCardBatchEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{PreferredChannel: req.GetSubscriber().GetPreferredChannel()},
		},
	}
	checkGiftCardBatchEligibilityReply, err := route(ctx, checkGiftCardBatchEligibilityRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/CheckGiftCardBatchEligibility")
	}
	validateGiftCardRequest := &giftcardsv1.ValidateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkGiftCardBatchEligibilityReply.GetGiftCardBatch().GetAudit().GetUpdatedAt(),
			},
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				Network: checkGiftCardBatchEligibilityReply.GetGiftCardBatch().GetCard().GetNetwork(),
			}},
		},
	}
	validateGiftCardReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*giftcardsv1.ValidateGiftCardResponse, error) {
		return s.giftCardValidationClient.ValidateGiftCard(ctx, validateGiftCardRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	}
	if err := stream.Send(&giftcardsv1.WatchGiftCardsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: checkGiftCardBatchEligibilityReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: checkGiftCardBatchEligibilityReply.GetGiftCardBatch().GetCounterparty().GetGiftCardId(),
			Holder:     validateGiftCardReply.GetGiftCard().GetHolder(),
			Available:  validateGiftCardReply.GetGiftCard().GetAvailable(),
			Contact:    &commonv1.ContactInfo{Phone: req.GetSubscriber().GetPhone()},
			Labels:     req.GetFilter().GetLabels(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// CloneGiftCard creates a gift card.
func (s *GiftCardServer) CloneGiftCard(ctx context.Context, req *giftcardsv1.CloneGiftCardRequest) (*giftcardsv1.CloneGiftCardResponse, error) {
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "clone gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	giftCardQuery, err := registry.Lookup[giftcardsv1.GiftCardQueryServiceClient](s.clients, "acme.giftcards.v1.GiftCardQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	browseGiftCardsRequest := &giftcardsv1.BrowseGiftCardsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.BrowseGiftCardsRequest_Filter{
			Labels:       req.GetGiftCard().GetLabels(),
			CreatedAfter: req.GetGiftCard().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetGiftCard().GetGiftCardId(),
		},
	}
	browseGiftCardsReply, err := giftCardQuery.BrowseGiftCards(ctx, browseGiftCardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/BrowseGiftCards")
	}
	labels := make(map[string]string, len(req.GetGiftCard().GetLabels()))
	for key, value := range req.GetGiftCard().GetLabels() {
		labels[key] = value
	}
	findGiftCardsReply, err := s.giftCardQueryClient.FindGiftCards(ctx, &giftcardsv1.FindGiftCardsRequest{
		Context: req.GetContext(),
		Filter:  &giftcardsv1.FindGiftCardsRequest_Filter{Labels: labels},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardQueryService/FindGiftCards")
	}
	ctx = tenancy.With(ctx, "gift_card_transaction_id", req.GetGiftCard().GetHolder().GetGiftCardTransactionId())
	compareGiftCardTransactionOptionsReply, err := s.giftCardTransactionCalculationClient.CompareGiftCardTransactionOptions(ctx, &giftcardsv1.CompareGiftCardTransactionOptionsRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Kind:                  req.GetGiftCard().GetHolder().GetKind(),
			GiftCardTransactionId: tenancy.Value(ctx, "gift_card_transaction_id"),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/CompareGiftCardTransactionOptions")
	}
	recentGiftCards.Store(recordKey{req.GetContext().GetTenantId(), req.GetGiftCard().GetGiftCardId()}, req.GetGiftCard())
	return &giftcardsv1.CloneGiftCardResponse{
		Result: &commonv1.OperationResult{
			Details:      browseGiftCardsReply.GetResult().GetDetails(),
			ReferenceIds: browseGiftCardsReply.GetResult().GetReferenceIds(),
			Message:      findGiftCardsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				GiftCardTransactionId: compareGiftCardTransactionOptionsReply.GetGiftCardTransaction().GetGiftCardTransactionId(),
			},
			OpenedAt:   req.GetGiftCard().GetOpenedAt(),
			GiftCardId: req.GetGiftCard().GetGiftCardId(),
		},
	}, nil
}
