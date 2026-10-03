package subscriptions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	analyticsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/analytics/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
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

// SubscriptionPlanQueryServer implements `acme.subscriptions.v1.SubscriptionPlanQueryService`.
// Answers read-only questions about subscription plans, for screens and reports.
type SubscriptionPlanQueryServer struct {
	subscriptionsv1.UnsafeSubscriptionPlanQueryServiceServer
	store                                *Store
	log                                  *slog.Logger
	subscriptionQueryClient              subscriptionsv1.SubscriptionQueryServiceClient
	analyticsEventClient                 analyticsv1.AnalyticsEventServiceClient
	clients                              *registry.Registry
	conn                                 *grpc.ClientConn
	subscriptionPlanValidationOnce       sync.Once
	subscriptionPlanValidationCached     subscriptionsv1.SubscriptionPlanValidationServiceClient
	tierValidationClient                 loyaltyv1.TierValidationServiceClient
	renewalScheduleValidationClient      subscriptionsv1.RenewalScheduleValidationServiceClient
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	validateUsageRecordFn                func(context.Context, *subscriptionsv1.ValidateUsageRecordRequest, ...grpc.CallOption) (*subscriptionsv1.ValidateUsageRecordResponse, error)
	apiCredentialNotificationClient      identityv1.ApiCredentialNotificationServiceClient
	subscriptionPlanValidationClient     subscriptionsv1.SubscriptionPlanValidationServiceClient
}

// NewSubscriptionPlanQueryServer wires a SubscriptionPlanQueryServer.
func NewSubscriptionPlanQueryServer(deps platform.Deps) *SubscriptionPlanQueryServer {
	usageRecordValidation := subscriptionsv1.NewUsageRecordValidationServiceClient(deps.Conn)
	return &SubscriptionPlanQueryServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.subscriptions.v1.SubscriptionPlanQueryService"),
		subscriptionQueryClient:              subscriptionsv1.NewSubscriptionQueryServiceClient(deps.Conn),
		analyticsEventClient:                 analyticsv1.NewAnalyticsEventServiceClient(deps.Conn),
		clients:                              deps.Clients,
		conn:                                 deps.Conn,
		tierValidationClient:                 loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		renewalScheduleValidationClient:      subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		validateUsageRecordFn:                usageRecordValidation.ValidateUsageRecord,
		apiCredentialNotificationClient:      identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		subscriptionPlanValidationClient:     subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn),
	}
}

// LookupSubscriptionPlan returns a subscription plan.
func (s *SubscriptionPlanQueryServer) LookupSubscriptionPlan(ctx context.Context, req *subscriptionsv1.LookupSubscriptionPlanRequest) (*subscriptionsv1.LookupSubscriptionPlanResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription_plan", req.GetSubscriptionPlanId(), "lookup_subscription_plan"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup subscription plan", "tenant", req.GetContext().GetTenantId())
	step := &subscriptionPlanQueryServerLookupSubscriptionPlanSuggestSubscriptionsStep{request: &subscriptionsv1.SuggestSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.SuggestSubscriptionsRequest_Filter{
			OwnerId:      req.GetSubscriptionPlanId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}}
	if err := s.runLookupSubscriptionPlanSuggestSubscriptions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionQueryService/SuggestSubscriptions")
	}
	suggestSubscriptionsReply := step.reply
	watchAnalyticsEventsStream, err := s.analyticsEventClient.WatchAnalyticsEvents(ctx, &analyticsv1.WatchAnalyticsEventsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter: &analyticsv1.WatchAnalyticsEventsRequest_Filter{
			Labels:  suggestSubscriptionsReply.GetResult().GetDetails(),
			OwnerId: req.GetSubscriptionPlanId(),
		},
		Since: req.GetContext().GetDeadline(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.analytics.v1.AnalyticsEventService/WatchAnalyticsEvents")
	}
	for {
		received, err := watchAnalyticsEventsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.analytics.v1.AnalyticsEventService/WatchAnalyticsEvents")
		}
		_ = received
	}
	return &subscriptionsv1.LookupSubscriptionPlanResponse{
		Result: &commonv1.OperationResult{
			Details: suggestSubscriptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{SubscriptionPlanId: req.GetSubscriptionPlanId()},
	}, nil
}

// BrowseSubscriptionPlans lists subscription plans a page at a time.
func (s *SubscriptionPlanQueryServer) BrowseSubscriptionPlans(ctx context.Context, req *subscriptionsv1.BrowseSubscriptionPlansRequest) (*subscriptionsv1.BrowseSubscriptionPlansResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse subscription plans", "tenant", req.GetContext().GetTenantId())
	subscriptionQuery, err := registry.Lookup[subscriptionsv1.SubscriptionQueryServiceClient](s.clients, "acme.subscriptions.v1.SubscriptionQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resolveSubscriptionsRequest := &subscriptionsv1.ResolveSubscriptionsRequest{Context: req.GetContext()}
	resolveSubscriptionsReply, err := subscriptionQuery.ResolveSubscriptions(ctx, resolveSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionQueryService/ResolveSubscriptions")
	}
	return &subscriptionsv1.BrowseSubscriptionPlansResponse{
		Result: &commonv1.OperationResult{Status: resolveSubscriptionsReply.GetResult().GetStatus()},
	}, nil
}

// FindSubscriptionPlans lists subscription plans a page at a time.
func (s *SubscriptionPlanQueryServer) FindSubscriptionPlans(ctx context.Context, req *subscriptionsv1.FindSubscriptionPlansRequest) (*subscriptionsv1.FindSubscriptionPlansResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find subscription plans", "tenant", req.GetContext().GetTenantId())
	return &subscriptionsv1.FindSubscriptionPlansResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountSubscriptionPlans counts subscription plans by status.
func (s *SubscriptionPlanQueryServer) CountSubscriptionPlans(ctx context.Context, req *subscriptionsv1.CountSubscriptionPlansRequest) (*subscriptionsv1.CountSubscriptionPlansResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count subscription plans", "tenant", req.GetContext().GetTenantId())
	return &subscriptionsv1.CountSubscriptionPlansResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolveSubscriptionPlans returns several subscription plans at once.
func (s *SubscriptionPlanQueryServer) ResolveSubscriptionPlans(ctx context.Context, req *subscriptionsv1.ResolveSubscriptionPlansRequest) (*subscriptionsv1.ResolveSubscriptionPlansResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve subscription plans", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	normalizeVenueRequest := &venuesv1.NormalizeVenueRequest{
		Context: req.GetContext(),
		Details: &venuesv1.NormalizeVenueRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  (&decoded).GetLocale(),
		},
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
	}
	normalizeVenueReply, err := grpcx.NewClient(s.conn, venuesv1.NewVenueValidationServiceClient).NormalizeVenue(ctx, normalizeVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/NormalizeVenue")
	}
	validateUsageRecordRequest := &subscriptionsv1.ValidateUsageRecordRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Measurements: []*subscriptionsv1.UsageRecordMeasurement{&subscriptionsv1.UsageRecordMeasurement{
				Unit: normalizeVenueReply.GetVenue().GetDimensions().GetUnit(),
			}},
			CorrelationId: normalizeVenueReply.GetResult().GetCorrelationId(),
		},
	}
	validateUsageRecordReply, err := validateUsageRecordCall(ctx, validateUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/ValidateUsageRecord")
	}
	return &subscriptionsv1.ResolveSubscriptionPlansResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  normalizeVenueReply.GetResult().GetReferenceIds(),
			CorrelationId: normalizeVenueReply.GetResult().GetCorrelationId(),
			Status:        validateUsageRecordReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// ExportSubscriptionPlans streams every matching subscription plan.
func (s *SubscriptionPlanQueryServer) ExportSubscriptionPlans(req *subscriptionsv1.ExportSubscriptionPlansRequest, stream grpc.ServerStreamingServer[subscriptionsv1.ExportSubscriptionPlansResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export subscription plans", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*subscriptionsv1.ExportSubscriptionPlansRequest_Filter)
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	explainUsageRecordCalculationRequest := &subscriptionsv1.ExplainUsageRecordCalculationRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.ExplainUsageRecordCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{
			Measurements: []*subscriptionsv1.UsageRecordMeasurement{&subscriptionsv1.UsageRecordMeasurement{ObservedAt: cloned.GetCreatedAfter()}},
			Detail:       packed,
		},
	}
	explainUsageRecordCalculationReply, err := grpcx.NewClient(s.conn, subscriptionsv1.NewUsageRecordCalculationServiceClient).ExplainUsageRecordCalculation(ctx, explainUsageRecordCalculationRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordCalculationService/ExplainUsageRecordCalculation")
	}
	lintSubscriptionPlansRequest := &subscriptionsv1.LintSubscriptionPlansRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.LintSubscriptionPlansRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: explainUsageRecordCalculationReply.GetUsageRecord().GetSubject().GetEmail(),
			},
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				DisplayName: explainUsageRecordCalculationReply.GetUsageRecord().GetSubject().GetDisplayName(),
				Email:       req.GetContext().GetActor().GetEmail(),
				UserId:      req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	lintSubscriptionPlansReply, err := s.subscriptionPlanValidationLazy().LintSubscriptionPlans(ctx, lintSubscriptionPlansRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/LintSubscriptionPlans")
	}
	filterCache := cache.New[string, *subscriptionsv1.ExportSubscriptionPlansRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	browseSubscriptionsRequest := &subscriptionsv1.BrowseSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.BrowseSubscriptionsRequest_Filter{
			Query:        req.GetFilter().GetQuery(),
			Region:       req.GetFilter().GetRegion(),
			CreatedAfter: cachedFilter.GetCreatedAfter(),
		},
	}
	browseSubscriptionsReply, err := subscriptionsv1.SubscriptionQueryServiceClient.BrowseSubscriptions(s.subscriptionQueryClient, ctx, browseSubscriptionsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionQueryService/BrowseSubscriptions")
	}
	checkTierEligibilityRequest := buildCheckTierEligibilityRequestForExportSubscriptionPlans(req, lintSubscriptionPlansReply)
	checkTierEligibilityReply, err := s.tierValidationClient.CheckTierEligibility(ctx, checkTierEligibilityRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/CheckTierEligibility")
	}
	if err := stream.Send(&subscriptionsv1.ExportSubscriptionPlansResponse{
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				DisplayName: explainUsageRecordCalculationReply.GetUsageRecord().GetSubject().GetDisplayName(),
			},
			Parameters: checkTierEligibilityReply.GetTier().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: explainUsageRecordCalculationReply.GetUsageRecord().GetCorrelationId(),
			Details:       browseSubscriptionsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeSubscriptionPlans summarizes a subscription plan.
func (s *SubscriptionPlanQueryServer) SummarizeSubscriptionPlans(ctx context.Context, req *subscriptionsv1.SummarizeSubscriptionPlansRequest) (*subscriptionsv1.SummarizeSubscriptionPlansResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription_plan", req.GetSubscriptionPlanId(), "summarize_subscription_plans"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize subscription plans", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	explainSellerDecisionRequest := &sellersv1.ExplainSellerDecisionRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{Email: cachedContext.GetActor().GetEmail()},
		},
	}
	explainSellerDecisionReply, err := explainSellerDecisionCall(ctx, explainSellerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/ExplainSellerDecision")
	}
	checkSubscriptionPlanEligibilityRequest := &subscriptionsv1.CheckSubscriptionPlanEligibilityRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.CheckSubscriptionPlanEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             explainSellerDecisionReply.GetSeller().GetContact().GetName(),
				PreferredChannel: explainSellerDecisionReply.GetSeller().GetContact().GetPreferredChannel(),
				Phone:            explainSellerDecisionReply.GetSeller().GetContact().GetPhone(),
			},
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				Etag: explainSellerDecisionReply.GetSeller().GetAudit().GetEtag(),
			},
		},
	}
	checkSubscriptionPlanEligibilityReply, err := checkSubscriptionPlanEligibilityCall(ctx, checkSubscriptionPlanEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/CheckSubscriptionPlanEligibility")
	}
	checkRenewalScheduleEligibilityRequest := &subscriptionsv1.CheckRenewalScheduleEligibilityRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: explainSellerDecisionReply.GetSeller().GetContact().GetPreferredChannel(),
				},
			}},
			Location: &commonv1.PostalAddress{
				CountryCode: explainSellerDecisionReply.GetSeller().GetPrimaryAddress().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainSellerDecisionReply.GetSeller().GetAudit().GetUpdatedBy(),
			},
		},
	}
	var checkRenewalScheduleEligibilityReply *subscriptionsv1.CheckRenewalScheduleEligibilityResponse
	if err := pipeline.Run(ctx, "acme.subscriptions.v1.RenewalScheduleValidationService/CheckRenewalScheduleEligibility", func(ctx context.Context) error {
		answer, err := s.renewalScheduleValidationClient.CheckRenewalScheduleEligibility(ctx, checkRenewalScheduleEligibilityRequest)
		checkRenewalScheduleEligibilityReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/CheckRenewalScheduleEligibility")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	quoteGiftCardTransactionRequest := &giftcardsv1.QuoteGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.QuoteGiftCardTransactionRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:      checkRenewalScheduleEligibilityReply.GetRenewalSchedule().GetLocation().GetLines(),
				PostalCode: checkRenewalScheduleEligibilityReply.GetRenewalSchedule().GetLocation().GetPostalCode(),
			},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	quoteGiftCardTransactionReply, err := giftcardsv1.GiftCardTransactionCalculationServiceClient.QuoteGiftCardTransaction(s.giftCardTransactionCalculationClient, ctx, quoteGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/QuoteGiftCardTransaction")
	}
	_ = quoteGiftCardTransactionReply
	return &subscriptionsv1.SummarizeSubscriptionPlansResponse{
		Result: &commonv1.OperationResult{
			Details:       explainSellerDecisionReply.GetResult().GetDetails(),
			CorrelationId: checkSubscriptionPlanEligibilityReply.GetResult().GetCorrelationId(),
			Status:        checkSubscriptionPlanEligibilityReply.GetOutcome().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestSubscriptionPlans lists subscription plans a page at a time.
func (s *SubscriptionPlanQueryServer) SuggestSubscriptionPlans(ctx context.Context, req *subscriptionsv1.SuggestSubscriptionPlansRequest) (*subscriptionsv1.SuggestSubscriptionPlansResponse, error) {
	s.log.InfoContext(ctx, "suggest subscription plans", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateUsageRecordRequest := &subscriptionsv1.ValidateUsageRecordRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Detail:  packed,
		},
	}
	validateUsageRecordReply, err := s.validateUsageRecordFn(ctx, validateUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/ValidateUsageRecord")
	}
	packed2, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateGiftCardTransactionsReply, err := s.giftCardTransactionCalculationClient.RecalculateGiftCardTransactions(ctx, &giftcardsv1.RecalculateGiftCardTransactionsRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: validateUsageRecordReply.GetUsageRecord().GetSubject().GetDisplayName(),
			},
			CorrelationId: validateUsageRecordReply.GetUsageRecord().GetCorrelationId(),
			Detail:        packed2,
		},
		Details: &giftcardsv1.RecalculateGiftCardTransactionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/RecalculateGiftCardTransactions")
	}
	return &subscriptionsv1.SuggestSubscriptionPlansResponse{
		Result: &commonv1.OperationResult{
			Message:       validateUsageRecordReply.GetOutcome().GetMessage(),
			CorrelationId: recalculateGiftCardTransactionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SubscriptionPlans: []*subscriptionsv1.SubscriptionPlan{&subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				DisplayName: validateUsageRecordReply.GetUsageRecord().GetSubject().GetDisplayName(),
			},
		}},
	}, nil
}

// PeekSubscriptionPlan returns a subscription plan.
func (s *SubscriptionPlanQueryServer) PeekSubscriptionPlan(ctx context.Context, req *subscriptionsv1.PeekSubscriptionPlanRequest) (*subscriptionsv1.PeekSubscriptionPlanResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription_plan", req.GetSubscriptionPlanId(), "peek_subscription_plan"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek subscription plan", "tenant", req.GetContext().GetTenantId())
	streamApiCredentialNoticesStream, err := s.apiCredentialNotificationClient.StreamApiCredentialNotices(ctx, &identityv1.StreamApiCredentialNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter:     &identityv1.StreamApiCredentialNoticesRequest_Filter{OwnerId: req.GetSubscriptionPlanId()},
		Since:      req.GetContext().GetDeadline(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/StreamApiCredentialNotices")
	}
	for {
		received, err := streamApiCredentialNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/StreamApiCredentialNotices")
		}
		_ = received
	}
	normalizeSubscriptionPlanReply, err := s.subscriptionPlanValidationClient.NormalizeSubscriptionPlan(ctx, &subscriptionsv1.NormalizeSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Owner:              &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/NormalizeSubscriptionPlan")
	}
	return &subscriptionsv1.PeekSubscriptionPlanResponse{
		Result: &commonv1.OperationResult{
			Details: normalizeSubscriptionPlanReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				DisplayName: normalizeSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetDisplayName(),
			},
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
		},
	}, nil
}

// subscriptionPlanQueryServerLookupSubscriptionPlanSuggestSubscriptionsStep carries the request of one step of
// LookupSubscriptionPlan and, once it ran, its reply.
type subscriptionPlanQueryServerLookupSubscriptionPlanSuggestSubscriptionsStep struct {
	request *subscriptionsv1.SuggestSubscriptionsRequest
	reply   *subscriptionsv1.SuggestSubscriptionsResponse
}

func (s *SubscriptionPlanQueryServer) runLookupSubscriptionPlanSuggestSubscriptions(ctx context.Context, step *subscriptionPlanQueryServerLookupSubscriptionPlanSuggestSubscriptionsStep) error {
	reply, err := s.subscriptionQueryClient.SuggestSubscriptions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// subscriptionPlanValidationLazy dials acme.subscriptions.v1.SubscriptionPlanValidationService the first time a handler
// needs it.
func (s *SubscriptionPlanQueryServer) subscriptionPlanValidationLazy() subscriptionsv1.SubscriptionPlanValidationServiceClient {
	s.subscriptionPlanValidationOnce.Do(func() {
		s.subscriptionPlanValidationCached = subscriptionsv1.NewSubscriptionPlanValidationServiceClient(s.conn)
	})
	return s.subscriptionPlanValidationCached
}
