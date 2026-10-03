package storefront

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// StorefrontSessionQueryServer implements `acme.storefront.v1.StorefrontSessionQueryService`.
// Answers read-only questions about storefront sessions, for screens and reports.
type StorefrontSessionQueryServer struct {
	storefrontv1.UnimplementedStorefrontSessionQueryServiceServer
	store                                       *Store
	log                                         *slog.Logger
	customerLifecycleClient                     customersv1.CustomerLifecycleServiceClient
	shopperViewWorkflowClient                   storefrontv1.ShopperViewWorkflowServiceClient
	metricSeriesQueryClient                     metricsv1.MetricSeriesQueryServiceClient
	conn                                        *grpc.ClientConn
	consentRecordSyncOnce                       sync.Once
	consentRecordSyncCached                     customersv1.ConsentRecordSyncServiceClient
	shopperViewIntegrationClient                storefrontv1.ShopperViewIntegrationServiceClient
	storefrontSessionNotificationClient         storefrontv1.StorefrontSessionNotificationServiceClient
	shopperViewNotificationClient               storefrontv1.ShopperViewNotificationServiceClient
	landingPageWorkflowClient                   storefrontv1.LandingPageWorkflowServiceClient
	partnerWebhookQueryClient                   partnerapiv1.PartnerWebhookQueryServiceClient
	storefrontSessionNotificationClientFallback storefrontv1.StorefrontSessionNotificationServiceClient
	searchQueryValidationClient                 searchv1.SearchQueryValidationServiceClient
}

// NewStorefrontSessionQueryServer wires a StorefrontSessionQueryServer.
func NewStorefrontSessionQueryServer(deps platform.Deps) *StorefrontSessionQueryServer {
	return &StorefrontSessionQueryServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.storefront.v1.StorefrontSessionQueryService"),
		customerLifecycleClient:             customersv1.NewCustomerLifecycleServiceClient(deps.Conn),
		shopperViewWorkflowClient:           storefrontv1.NewShopperViewWorkflowServiceClient(deps.Conn),
		metricSeriesQueryClient:             metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		conn:                                deps.Conn,
		shopperViewIntegrationClient:        storefrontv1.NewShopperViewIntegrationServiceClient(deps.Conn),
		storefrontSessionNotificationClient: storefrontv1.NewStorefrontSessionNotificationServiceClient(deps.Conn),
		shopperViewNotificationClient:       storefrontv1.NewShopperViewNotificationServiceClient(deps.Conn),
		landingPageWorkflowClient:           storefrontv1.NewLandingPageWorkflowServiceClient(deps.Conn),
		partnerWebhookQueryClient:           partnerapiv1.NewPartnerWebhookQueryServiceClient(deps.Conn),
		storefrontSessionNotificationClientFallback: storefrontv1.NewStorefrontSessionNotificationServiceClient(deps.Conn),
		searchQueryValidationClient:                 searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
	}
}

// LookupStorefrontSession returns a storefront session.
func (s *StorefrontSessionQueryServer) LookupStorefrontSession(ctx context.Context, req *storefrontv1.LookupStorefrontSessionRequest) (*storefrontv1.LookupStorefrontSessionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "lookup_storefront_session"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup storefront session", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	scheduleCustomerExpiryRequest := &customersv1.ScheduleCustomerExpiryRequest{}
	scheduleCustomerExpiryRequest.Context = req.GetContext()
	scheduleCustomerExpiryRequest.Customer = &customersv1.Customer{
		RelationshipManager: &identityv1.UserRef{
			Email:  req.GetContext().GetActor().GetEmail(),
			UserId: cachedContext.GetActor().GetUserId(),
		},
	}
	scheduleCustomerExpiryReply, err := s.customerLifecycleClient.ScheduleCustomerExpiry(ctx, scheduleCustomerExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerLifecycleService/ScheduleCustomerExpiry")
	}
	advanceShopperViewWorkflowRequest := &storefrontv1.AdvanceShopperViewWorkflowRequest{
		Context: req.GetContext(),
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				DisplayName: scheduleCustomerExpiryReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			Locator: &storefrontv1.ShopperView_Address{Address: &commonv1.PostalAddress{
				CountryCode: scheduleCustomerExpiryReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			}},
		},
	}
	send := s.advanceShopperViewWorkflowSender(advanceShopperViewWorkflowRequest)
	advanceShopperViewWorkflowReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/AdvanceShopperViewWorkflow")
	}
	_ = advanceShopperViewWorkflowReply
	return &storefrontv1.LookupStorefrontSessionResponse{
		Result: &commonv1.OperationResult{Status: scheduleCustomerExpiryReply.GetResult().GetStatus()},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: scheduleCustomerExpiryReply.GetCustomer().GetContact().GetPreferredChannel(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
		},
	}, nil
}

// BrowseStorefrontSessions lists storefront sessions a page at a time.
func (s *StorefrontSessionQueryServer) BrowseStorefrontSessions(ctx context.Context, req *storefrontv1.BrowseStorefrontSessionsRequest) (*storefrontv1.BrowseStorefrontSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse storefront sessions", "tenant", req.GetContext().GetTenantId())
	filterCache := cache.New[string, *storefrontv1.BrowseStorefrontSessionsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	listCustomerSegmentImportsRequest := &customersv1.ListCustomerSegmentImportsRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
		Filter: &customersv1.ListCustomerSegmentImportsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			OwnerId:      req.GetFilter().GetOwnerId(),
			Query:        cachedFilter.GetQuery(),
		},
	}
	listCustomerSegmentImportsReply, err := listCustomerSegmentImportsCall(ctx, listCustomerSegmentImportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/ListCustomerSegmentImports")
	}
	step := &storefrontSessionQueryServerBrowseStorefrontSessionsSummarizeMetricSeriesesStep{request: &metricsv1.SummarizeMetricSeriesesRequest{
		Context:     req.GetContext(),
		Annotations: listCustomerSegmentImportsReply.GetResult().GetDetails(),
	}}
	if err := s.runBrowseStorefrontSessionsSummarizeMetricSerieses(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses")
	}
	summarizeMetricSeriesesReply := step.reply
	return &storefrontv1.BrowseStorefrontSessionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listCustomerSegmentImportsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  summarizeMetricSeriesesReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		PageInfo: &commonv1.PageInfo{
			TotalSize: listCustomerSegmentImportsReply.GetPageInfo().GetTotalSize(),
		},
	}, nil
}

// CountStorefrontSessions counts storefront sessions by status.
func (s *StorefrontSessionQueryServer) CountStorefrontSessions(ctx context.Context, req *storefrontv1.CountStorefrontSessionsRequest) (*storefrontv1.CountStorefrontSessionsResponse, error) {
	s.log.InfoContext(ctx, "count storefront sessions", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullConsentRecordsRequest := &customersv1.PullConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Source:       req.GetFilter().GetOwnerId(),
			Detail:       packed,
		},
		Details: &customersv1.PullConsentRecordsRequest_Details{Payload: snapshot},
	}
	pullConsentRecordsReply, err := s.consentRecordSyncLazy().PullConsentRecords(ctx, pullConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/PullConsentRecords")
	}
	step := &storefrontSessionQueryServerCountStorefrontSessionsTestShopperViewConnectionStep{request: &storefrontv1.TestShopperViewConnectionRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.TestShopperViewConnectionRequest_Details{
			MessageId:  pullConsentRecordsReply.GetOutcome().GetMessage(),
			ReceivedAt: pullConsentRecordsReply.GetOutcome().GetEffectiveAt(),
		},
	}}
	if err := s.runCountStorefrontSessionsTestShopperViewConnection(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/TestShopperViewConnection")
	}
	testShopperViewConnectionReply := step.reply
	return &storefrontv1.CountStorefrontSessionsResponse{
		Result: &commonv1.OperationResult{
			Status:       pullConsentRecordsReply.GetOutcome().GetStatus(),
			ReferenceIds: pullConsentRecordsReply.GetResult().GetReferenceIds(),
			Message:      testShopperViewConnectionReply.GetOutcome().GetMessage(),
			Details:      testShopperViewConnectionReply.GetResult().GetDetails(),
		},
	}, nil
}

// ResolveStorefrontSessions returns several storefront sessions at once.
func (s *StorefrontSessionQueryServer) ResolveStorefrontSessions(ctx context.Context, req *storefrontv1.ResolveStorefrontSessionsRequest) (*storefrontv1.ResolveStorefrontSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResolveStorefrontSessions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve storefront sessions", "tenant", req.GetContext().GetTenantId())
	subscribeStorefrontSessionUpdatesRequest := buildSubscribeStorefrontSessionUpdatesRequestForResolveStorefrontSessions(req)
	subscribeStorefrontSessionUpdatesReply, err := s.storefrontSessionNotificationClient.SubscribeStorefrontSessionUpdates(ctx, subscribeStorefrontSessionUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionNotificationService/SubscribeStorefrontSessionUpdates")
	}
	unsubscribeShopperViewUpdatesRequest := buildUnsubscribeShopperViewUpdatesRequestForResolveStorefrontSessions(req, subscribeStorefrontSessionUpdatesReply)
	unsubscribeShopperViewUpdatesReply, err := s.shopperViewNotificationClient.UnsubscribeShopperViewUpdates(ctx, unsubscribeShopperViewUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewNotificationService/UnsubscribeShopperViewUpdates")
	}
	return &storefrontv1.ResolveStorefrontSessionsResponse{
		StorefrontSessions: []*storefrontv1.StorefrontSession{&storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: subscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetRequester().GetPreferredChannel(),
			},
			Comments: []*storefrontv1.StorefrontSessionComment{&storefrontv1.StorefrontSessionComment{
				CreatedAt: unsubscribeShopperViewUpdatesReply.GetShopperView().GetAudit().GetCreatedAt(),
			}},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportStorefrontSessions streams every matching storefront session.
func (s *StorefrontSessionQueryServer) ExportStorefrontSessions(req *storefrontv1.ExportStorefrontSessionsRequest, stream grpc.ServerStreamingServer[storefrontv1.ExportStorefrontSessionsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export storefront sessions", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	listLandingPageWorkflowsRequest := &storefrontv1.ListLandingPageWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.ListLandingPageWorkflowsRequest_Filter{
			Labels:       labels,
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Query:        req.GetFilter().GetQuery(),
			Region:       req.GetFilter().GetRegion(),
		},
	}
	listLandingPageWorkflowsReply, err := storefrontv1.LandingPageWorkflowServiceClient.ListLandingPageWorkflows(s.landingPageWorkflowClient, ctx, listLandingPageWorkflowsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.LandingPageWorkflowService/ListLandingPageWorkflows")
	}
	labels2 := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key2, value2 := range req.GetFilter().GetLabels() {
		labels2[key2] = value2
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	unsubscribeShopperViewUpdatesRequest := &storefrontv1.UnsubscribeShopperViewUpdatesRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.UnsubscribeShopperViewUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: labels2,
			Payload:   packed,
		},
		Annotations: listLandingPageWorkflowsReply.GetResult().GetDetails(),
	}
	unsubscribeShopperViewUpdatesReply, err := storefrontv1.ShopperViewNotificationServiceClient.UnsubscribeShopperViewUpdates(s.shopperViewNotificationClient, ctx, unsubscribeShopperViewUpdatesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.ShopperViewNotificationService/UnsubscribeShopperViewUpdates")
	}
	if err := stream.Send(&storefrontv1.ExportStorefrontSessionsResponse{
		Result: &commonv1.OperationResult{
			Message: listLandingPageWorkflowsReply.GetResult().GetMessage(),
			Status:  listLandingPageWorkflowsReply.GetResult().GetStatus(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: unsubscribeShopperViewUpdatesReply.GetShopperView().GetAudit().GetUpdatedAt(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeStorefrontSessions summarizes a storefront session.
func (s *StorefrontSessionQueryServer) SummarizeStorefrontSessions(ctx context.Context, req *storefrontv1.SummarizeStorefrontSessionsRequest) (*storefrontv1.SummarizeStorefrontSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "summarize_storefront_sessions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize storefront sessions", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	countFeatureFlagsRequest := &featureflagsv1.CountFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.CountFeatureFlagsRequest_Filter{
			Labels:       filters,
			OwnerId:      req.GetStorefrontSessionId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	countFeatureFlagsReply, err := grpcx.NewClient(s.conn, featureflagsv1.NewFeatureFlagQueryServiceClient).CountFeatureFlags(ctx, countFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	resumeLandingPageWorkflowRequest := &storefrontv1.ResumeLandingPageWorkflowRequest{
		Context: req.GetContext(),
		Reason:  tenancy.Value(ctx, "reason"),
		Details: &storefrontv1.ResumeLandingPageWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations: req.GetAnnotations(),
	}
	resumeLandingPageWorkflowReply, err := resumeLandingPageWorkflowCall(ctx, resumeLandingPageWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageWorkflowService/ResumeLandingPageWorkflow")
	}
	exportPartnerWebhooksStream, err := s.partnerWebhookQueryClient.ExportPartnerWebhooks(ctx, &partnerapiv1.ExportPartnerWebhooksRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ExportPartnerWebhooksRequest_Filter{
			Region:       resumeLandingPageWorkflowReply.GetLandingPage().GetBillingAddress().GetRegion(),
			CreatedAfter: resumeLandingPageWorkflowReply.GetLandingPage().GetDueAt(),
			OwnerId:      resumeLandingPageWorkflowReply.GetLandingPage().GetCounterparty().GetCustomerId(),
		},
		Format: req.GetDetails().GetFormat(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookQueryService/ExportPartnerWebhooks")
	}
	for {
		received, err := exportPartnerWebhooksStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookQueryService/ExportPartnerWebhooks")
		}
		_ = received
	}
	return &storefrontv1.SummarizeStorefrontSessionsResponse{
		Result: &commonv1.OperationResult{
			Details: countFeatureFlagsReply.GetResult().GetDetails(),
			Status:  countFeatureFlagsReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestStorefrontSessions lists storefront sessions a page at a time.
func (s *StorefrontSessionQueryServer) SuggestStorefrontSessions(ctx context.Context, req *storefrontv1.SuggestStorefrontSessionsRequest) (*storefrontv1.SuggestStorefrontSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest storefront sessions", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *storefrontv1.UnsubscribeStorefrontSessionUpdatesRequest, ...grpc.CallOption) (*storefrontv1.UnsubscribeStorefrontSessionUpdatesResponse, error){
		"primary":  s.storefrontSessionNotificationClient.UnsubscribeStorefrontSessionUpdates,
		"fallback": s.storefrontSessionNotificationClientFallback.UnsubscribeStorefrontSessionUpdates,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeStorefrontSessionUpdatesRequest := &storefrontv1.UnsubscribeStorefrontSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.UnsubscribeStorefrontSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Payload:   packed,
		},
		Annotations: req.GetFilter().GetLabels(),
	}
	unsubscribeStorefrontSessionUpdatesReply, err := route(ctx, unsubscribeStorefrontSessionUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionNotificationService/UnsubscribeStorefrontSessionUpdates")
	}
	submitConsentRecordRequest := &customersv1.SubmitConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: unsubscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetCustomer().GetDisplayName(),
			},
		},
		Details: &customersv1.SubmitConsentRecordRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       unsubscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetAssignee().GetEmail(),
				UserId:      unsubscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetAssignee().GetUserId(),
				DisplayName: unsubscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetAssignee().GetDisplayName(),
			},
		},
	}
	submitConsentRecordReply, err := submitConsentRecordCall(ctx, submitConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordApprovalService/SubmitConsentRecord")
	}
	return &storefrontv1.SuggestStorefrontSessionsResponse{
		StorefrontSessions: []*storefrontv1.StorefrontSession{&storefrontv1.StorefrontSession{
			Priority: unsubscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetPriority(),
			Assignee: &identityv1.UserRef{
				DisplayName: submitConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: submitConsentRecordReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PeekStorefrontSession returns a storefront session.
func (s *StorefrontSessionQueryServer) PeekStorefrontSession(ctx context.Context, req *storefrontv1.PeekStorefrontSessionRequest) (*storefrontv1.PeekStorefrontSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "peek_storefront_session"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek storefront session", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreSearchQueryRequest := &searchv1.ScoreSearchQueryRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			CorrelationId: req.GetStorefrontSessionId(),
			Measurements:  []*searchv1.SearchQueryMeasurement{&searchv1.SearchQueryMeasurement{ObservedAt: req.GetContext().GetDeadline()}},
			Detail:        packed,
		},
		Annotations: baggage,
	}
	scoreSearchQueryReply, err := grpcx.Call(ctx, s.searchQueryValidationClient.ScoreSearchQuery, scoreSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ScoreSearchQuery")
	}
	return &storefrontv1.PeekStorefrontSessionResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Assignee: &identityv1.UserRef{
				DisplayName: scoreSearchQueryReply.GetSearchQuery().GetSubject().GetDisplayName(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// advanceShopperViewWorkflowSender returns a function that sends one request to
// acme.storefront.v1.ShopperViewWorkflowService/AdvanceShopperViewWorkflow.
func (s *StorefrontSessionQueryServer) advanceShopperViewWorkflowSender(request *storefrontv1.AdvanceShopperViewWorkflowRequest) func(context.Context) (*storefrontv1.AdvanceShopperViewWorkflowResponse, error) {
	return func(ctx context.Context) (*storefrontv1.AdvanceShopperViewWorkflowResponse, error) {
		return s.shopperViewWorkflowClient.AdvanceShopperViewWorkflow(ctx, request)
	}
}

// storefrontSessionQueryServerBrowseStorefrontSessionsSummarizeMetricSeriesesStep carries the request of one step of
// BrowseStorefrontSessions and, once it ran, its reply.
type storefrontSessionQueryServerBrowseStorefrontSessionsSummarizeMetricSeriesesStep struct {
	request *metricsv1.SummarizeMetricSeriesesRequest
	reply   *metricsv1.SummarizeMetricSeriesesResponse
}

func (s *StorefrontSessionQueryServer) runBrowseStorefrontSessionsSummarizeMetricSerieses(ctx context.Context, step *storefrontSessionQueryServerBrowseStorefrontSessionsSummarizeMetricSeriesesStep) error {
	reply, err := s.metricSeriesQueryClient.SummarizeMetricSerieses(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// consentRecordSyncLazy dials acme.customers.v1.ConsentRecordSyncService the first time a handler needs it.
func (s *StorefrontSessionQueryServer) consentRecordSyncLazy() customersv1.ConsentRecordSyncServiceClient {
	s.consentRecordSyncOnce.Do(func() { s.consentRecordSyncCached = customersv1.NewConsentRecordSyncServiceClient(s.conn) })
	return s.consentRecordSyncCached
}

// storefrontSessionQueryServerCountStorefrontSessionsTestShopperViewConnectionStep carries the request of one step of
// CountStorefrontSessions and, once it ran, its reply.
type storefrontSessionQueryServerCountStorefrontSessionsTestShopperViewConnectionStep struct {
	request *storefrontv1.TestShopperViewConnectionRequest
	reply   *storefrontv1.TestShopperViewConnectionResponse
}

func (s *StorefrontSessionQueryServer) runCountStorefrontSessionsTestShopperViewConnection(ctx context.Context, step *storefrontSessionQueryServerCountStorefrontSessionsTestShopperViewConnectionStep) error {
	reply, err := s.shopperViewIntegrationClient.TestShopperViewConnection(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
