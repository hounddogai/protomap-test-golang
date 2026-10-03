package subscriptions

import (
	"context"
	"errors"
	"io"
	"log/slog"

	admissionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/admissions/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// SubscriptionServer implements `acme.subscriptions.v1.SubscriptionService`.
// Manages subscriptions: creates, reads, changes, and lists them.
type SubscriptionServer struct {
	subscriptionsv1.UnimplementedSubscriptionServiceServer
	store                             *Store
	log                               *slog.Logger
	venueClient                       venuesv1.VenueServiceClient
	admissionClient                   admissionsv1.AdmissionServiceClient
	conn                              *grpc.ClientConn
	renewalScheduleValidationClient   subscriptionsv1.RenewalScheduleValidationServiceClient
	sellerQueryClient                 sellersv1.SellerQueryServiceClient
	validateSellerListingFn           func(context.Context, *sellersv1.ValidateSellerListingRequest, ...grpc.CallOption) (*sellersv1.ValidateSellerListingResponse, error)
	pointsLedgerQueryClient           loyaltyv1.PointsLedgerQueryServiceClient
	reviewModerationCalculationClient reviewsv1.ReviewModerationCalculationServiceClient
	subscriptionQueryClient           subscriptionsv1.SubscriptionQueryServiceClient
	sellerListingValidationClient     sellersv1.SellerListingValidationServiceClient
	synonymExportClient               searchv1.SynonymExportServiceClient
}

// NewSubscriptionServer wires a SubscriptionServer.
func NewSubscriptionServer(deps platform.Deps) *SubscriptionServer {
	sellerListingValidation := sellersv1.NewSellerListingValidationServiceClient(deps.Conn)
	return &SubscriptionServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.subscriptions.v1.SubscriptionService"),
		venueClient:                       venuesv1.NewVenueServiceClient(deps.Conn),
		admissionClient:                   admissionsv1.NewAdmissionServiceClient(deps.Conn),
		conn:                              deps.Conn,
		renewalScheduleValidationClient:   subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn),
		sellerQueryClient:                 sellersv1.NewSellerQueryServiceClient(deps.Conn),
		validateSellerListingFn:           sellerListingValidation.ValidateSellerListing,
		pointsLedgerQueryClient:           loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		reviewModerationCalculationClient: reviewsv1.NewReviewModerationCalculationServiceClient(deps.Conn),
		subscriptionQueryClient:           subscriptionsv1.NewSubscriptionQueryServiceClient(deps.Conn),
		sellerListingValidationClient:     sellersv1.NewSellerListingValidationServiceClient(deps.Conn),
		synonymExportClient:               searchv1.NewSynonymExportServiceClient(deps.Conn),
	}
}

// CreateSubscription creates a subscription.
func (s *SubscriptionServer) CreateSubscription(ctx context.Context, req *subscriptionsv1.CreateSubscriptionRequest) (*subscriptionsv1.CreateSubscriptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "create subscription", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	subscriptionCache := cache.New[string, *subscriptionsv1.Subscription](1)
	subscriptionCache.Put(req.GetContext().GetTraceId(), req.GetSubscription())
	cachedSubscription, _ := subscriptionCache.Get(req.GetContext().GetTraceId())
	cloneVenueRequest := newCloneVenueRequest(withCloneVenueContext(req.GetContext()), withCloneVenueVenue(&venuesv1.Venue{
		Audit: &commonv1.AuditInfo{CreatedBy: cachedSubscription.GetAudit().GetCreatedBy()},
	}))
	cloneVenueReply, err := s.venueClient.CloneVenue(ctx, cloneVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/CloneVenue")
	}
	cloned := proto.Clone(req.GetSubscription()).(*subscriptionsv1.Subscription)
	watchAdmissionsStream, err := s.admissionClient.WatchAdmissions(ctx, &admissionsv1.WatchAdmissionsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email:            cloned.GetHolder().GetEmail(),
			PreferredChannel: req.GetSubscription().GetContact().GetPreferredChannel(),
			Phone:            req.GetSubscription().GetContact().GetPhone(),
		},
		Filter: &admissionsv1.WatchAdmissionsRequest_Filter{Region: cloneVenueReply.GetVenue().GetAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionService/WatchAdmissions")
	}
	for {
		received, err := watchAdmissionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.admissions.v1.AdmissionService/WatchAdmissions")
		}
		_ = received
	}
	return &subscriptionsv1.CreateSubscriptionResponse{
		Subscription: &subscriptionsv1.Subscription{
			Holder: &customersv1.CustomerRef{
				DisplayName: cloneVenueReply.GetVenue().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cloneVenueReply.GetVenue().GetAudit().GetUpdatedAt(),
			},
			Contact:  req.GetSubscription().GetContact(),
			OpenedAt: req.GetSubscription().GetOpenedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSubscription returns a subscription.
func (s *SubscriptionServer) GetSubscription(ctx context.Context, req *subscriptionsv1.GetSubscriptionRequest) (*subscriptionsv1.GetSubscriptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSubscription", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription", req.GetSubscriptionId(), "get_subscription"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get subscription", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	var cachedCurrencyCode string
	if entry, ok := recentSubscriptions.Load(recordKey{req.GetContext().GetTenantId(), req.GetSubscriptionId()}); ok {
		if previous, ok := entry.(*subscriptionsv1.Subscription); ok {
			cachedCurrencyCode = previous.GetCurrencyCode()
		}
	}
	countSubscriptionPlansRequest := &subscriptionsv1.CountSubscriptionPlansRequest{
		Context: req.GetContext(),
		Filter:  &subscriptionsv1.CountSubscriptionPlansRequest_Filter{CreatedAfter: cachedContext.GetDeadline()},
		OrderBy: cachedCurrencyCode,
	}
	countSubscriptionPlansReply, err := grpcx.NewClient(s.conn, subscriptionsv1.NewSubscriptionPlanQueryServiceClient).CountSubscriptionPlans(ctx, countSubscriptionPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/CountSubscriptionPlans")
	}
	return &subscriptionsv1.GetSubscriptionResponse{
		Result: &commonv1.OperationResult{
			Message: countSubscriptionPlansReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Subscription: &subscriptionsv1.Subscription{SubscriptionId: req.GetSubscriptionId()},
	}, nil
}

// UpdateSubscription changes a subscription.
func (s *SubscriptionServer) UpdateSubscription(ctx context.Context, req *subscriptionsv1.UpdateSubscriptionRequest) (*subscriptionsv1.UpdateSubscriptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "update subscription", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	validateRenewalScheduleRequest := buildValidateRenewalScheduleRequestForUpdateSubscription(req)
	validateRenewalScheduleReply, err := s.renewalScheduleValidationClient.ValidateRenewalSchedule(ctx, validateRenewalScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/ValidateRenewalSchedule")
	}
	labels := make(map[string]string, len(req.GetSubscription().GetLabels()))
	for key, value := range req.GetSubscription().GetLabels() {
		labels[key] = value
	}
	findSellersRequest := &sellersv1.FindSellersRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.FindSellersRequest_Filter{
			Region: validateRenewalScheduleReply.GetRenewalSchedule().GetLocation().GetRegion(),
			Labels: labels,
		},
	}
	findSellersReply, err := grpcx.Call(ctx, s.sellerQueryClient.FindSellers, findSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/FindSellers")
	}
	recentSubscriptions.Store(recordKey{req.GetContext().GetTenantId(), req.GetSubscription().GetSubscriptionId()}, req.GetSubscription())
	return &subscriptionsv1.UpdateSubscriptionResponse{
		Subscription: &subscriptionsv1.Subscription{
			Audit: &commonv1.AuditInfo{
				Etag: validateRenewalScheduleReply.GetRenewalSchedule().GetAudit().GetEtag(),
			},
			Holder:  req.GetSubscription().GetHolder(),
			Contact: req.GetSubscription().GetContact(),
		},
		Result: &commonv1.OperationResult{
			Message: findSellersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// DeleteSubscription deletes a subscription.
func (s *SubscriptionServer) DeleteSubscription(ctx context.Context, req *subscriptionsv1.DeleteSubscriptionRequest) (*subscriptionsv1.DeleteSubscriptionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription", req.GetSubscriptionId(), "delete_subscription"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete subscription", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	validateSellerListingRequest := &sellersv1.ValidateSellerListingRequest{
		Context: req.GetContext(),
		SellerListing: &sellersv1.SellerListing{
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
	}
	validateSellerListingReply, err := s.validateSellerListingFn(ctx, validateSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/ValidateSellerListing")
	}
	countPointsLedgersRequest := &loyaltyv1.CountPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.CountPointsLedgersRequest_Filter{
			Region:  validateSellerListingReply.GetSellerListing().GetAddress().GetRegion(),
			OwnerId: validateSellerListingReply.GetSellerListing().GetComponents()["primary"].GetComponentId(),
		},
	}
	countPointsLedgersReply, err := grpcx.Call(ctx, s.pointsLedgerQueryClient.CountPointsLedgers, countPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/CountPointsLedgers")
	}
	return &subscriptionsv1.DeleteSubscriptionResponse{
		EffectiveAt: validateSellerListingReply.GetOutcome().GetEffectiveAt(),
		Result: &commonv1.OperationResult{
			Details: countPointsLedgersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ListSubscriptions lists subscriptions a page at a time.
func (s *SubscriptionServer) ListSubscriptions(ctx context.Context, req *subscriptionsv1.ListSubscriptionsRequest) (*subscriptionsv1.ListSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListSubscriptions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list subscriptions", "tenant", req.GetContext().GetTenantId())
	estimateReviewModerationRequest := buildEstimateReviewModerationRequestForListSubscriptions(req)
	estimateReviewModerationReply, err := s.reviewModerationCalculationClient.EstimateReviewModeration(ctx, estimateReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/EstimateReviewModeration")
	}
	return &subscriptionsv1.ListSubscriptionsResponse{
		Subscriptions: []*subscriptionsv1.Subscription{&subscriptionsv1.Subscription{
			Holder: &customersv1.CustomerRef{
				Email: estimateReviewModerationReply.GetReviewModeration().GetRequester().GetEmail(),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: estimateReviewModerationReply.GetReviewModeration().GetRequester().GetPreferredChannel(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SearchSubscriptions lists subscriptions a page at a time.
func (s *SubscriptionServer) SearchSubscriptions(ctx context.Context, req *subscriptionsv1.SearchSubscriptionsRequest) (*subscriptionsv1.SearchSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "search subscriptions", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeSubscriptionsRequest := &subscriptionsv1.SummarizeSubscriptionsRequest{
		Context:     req.GetContext(),
		Details:     &subscriptionsv1.SummarizeSubscriptionsRequest_Details{Filters: req.GetFilter().GetLabels()},
		Annotations: baggage,
	}
	summarizeSubscriptionsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*subscriptionsv1.SummarizeSubscriptionsResponse, error) {
		return s.subscriptionQueryClient.SummarizeSubscriptions(ctx, summarizeSubscriptionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionQueryService/SummarizeSubscriptions")
	}
	return &subscriptionsv1.SearchSubscriptionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  summarizeSubscriptionsReply.GetResult().GetReferenceIds(),
			CorrelationId: summarizeSubscriptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// BatchCreateSubscriptions takes a stream of subscriptions and answers once at the end.
func (s *SubscriptionServer) BatchCreateSubscriptions(stream grpc.ClientStreamingServer[subscriptionsv1.BatchCreateSubscriptionsRequest, subscriptionsv1.BatchCreateSubscriptionsResponse]) error {
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
		s.log.InfoContext(ctx, "batch create subscriptions", "tenant", msg.GetContext().GetTenantId())
		for _, limit := range msg.GetRecord().GetLimits() {
			if _, err := s.sellerListingValidationClient.ExplainSellerListingDecision(ctx, &sellersv1.ExplainSellerListingDecisionRequest{
				Context:       msg.GetContext(),
				SellerListing: &sellersv1.SellerListing{Name: limit.GetName()},
				Details: &sellersv1.ExplainSellerListingDecisionRequest_Details{
					Contact: &commonv1.ContactInfo{Email: msg.GetRecord().GetHolder().GetEmail()},
				},
			}); err != nil {
				return grpcx.Wrap(err, "acme.sellers.v1.SellerListingValidationService/ExplainSellerListingDecision")
			}
		}
		retrySynonymExportRequest := &searchv1.RetrySynonymExportRequest{
			Context: msg.GetContext(),
			Synonym: &searchv1.Synonym{
				Owner: &identityv1.UserRef{Email: msg.GetRecord().GetContact().GetEmail()},
			},
		}
		if _, err := searchv1.SynonymExportServiceClient.RetrySynonymExport(s.synonymExportClient, ctx, retrySynonymExportRequest); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SynonymExportService/RetrySynonymExport")
		}
		accepted++
	}
	return stream.SendAndClose(&subscriptionsv1.BatchCreateSubscriptionsResponse{Accepted: accepted})
}
