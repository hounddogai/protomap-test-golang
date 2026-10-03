package storefront

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// ShopperViewServer implements `acme.storefront.v1.ShopperViewService`.
// Manages shopper views: creates, reads, changes, and lists them.
type ShopperViewServer struct {
	storefrontv1.UnimplementedShopperViewServiceServer
	store                         *Store
	log                           *slog.Logger
	consentRecordWorkflowPort     consentRecordWorkflowPort
	landingPageNotificationClient storefrontv1.LandingPageNotificationServiceClient
	landingPageQueryClient        storefrontv1.LandingPageQueryServiceClient
	conn                          *grpc.ClientConn
	partnerOrderIntegrationClient partnerapiv1.PartnerOrderIntegrationServiceClient
	featureFlagValidationClient   featureflagsv1.FeatureFlagValidationServiceClient
	searchQueryValidationClient   searchv1.SearchQueryValidationServiceClient
	clients                       *registry.Registry
	sessionValidationClient       identityv1.SessionValidationServiceClient
}

// NewShopperViewServer wires a ShopperViewServer.
func NewShopperViewServer(deps platform.Deps) *ShopperViewServer {
	return &ShopperViewServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.storefront.v1.ShopperViewService"),
		consentRecordWorkflowPort:     newConsentRecordWorkflowPort(deps),
		landingPageNotificationClient: storefrontv1.NewLandingPageNotificationServiceClient(deps.Conn),
		landingPageQueryClient:        storefrontv1.NewLandingPageQueryServiceClient(deps.Conn),
		conn:                          deps.Conn,
		partnerOrderIntegrationClient: partnerapiv1.NewPartnerOrderIntegrationServiceClient(deps.Conn),
		featureFlagValidationClient:   featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		searchQueryValidationClient:   searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
		clients:                       deps.Clients,
		sessionValidationClient:       identityv1.NewSessionValidationServiceClient(deps.Conn),
	}
}

// CreateShopperView creates a shopper view.
func (s *ShopperViewServer) CreateShopperView(ctx context.Context, req *storefrontv1.CreateShopperViewRequest) (*storefrontv1.CreateShopperViewResponse, error) {
	if err := s.store.SaveShopperView(ctx, req.GetContext().GetTenantId(), req.GetShopperView()); err != nil {
		return nil, grpcx.Wrap(err, "save shopper view")
	}
	s.log.InfoContext(ctx, "create shopper view", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShopperView().GetAudit().GetEtag())
	advanceConsentRecordWorkflowRequest := &customersv1.AdvanceConsentRecordWorkflowRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{Unit: req.GetShopperView().GetDimensions().GetUnit()}},
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetShopperView().GetOwner().GetDisplayName(),
			},
		},
	}
	advanceConsentRecordWorkflowReply, err := s.consentRecordWorkflowPort.AdvanceConsentRecordWorkflow(ctx, advanceConsentRecordWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordWorkflowService/AdvanceConsentRecordWorkflow")
	}
	var region string
	switch choice := req.GetShopperView().GetLocator().(type) {
	case *storefrontv1.ShopperView_Address:
		region = choice.Address.GetRegion()
	case *storefrontv1.ShopperView_FacilityCode:
		region = choice.FacilityCode
	}
	listLandingPageSubscriptionsReply, err := s.landingPageNotificationClient.ListLandingPageSubscriptions(ctx, &storefrontv1.ListLandingPageSubscriptionsRequest{
		Context: req.GetContext(),
		Filter:  &storefrontv1.ListLandingPageSubscriptionsRequest_Filter{Region: region},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageNotificationService/ListLandingPageSubscriptions")
	}
	summarizeLandingPagesRequest := buildSummarizeLandingPagesRequestForCreateShopperView(req, listLandingPageSubscriptionsReply)
	summarizeLandingPagesReply, err := s.landingPageQueryClient.SummarizeLandingPages(ctx, summarizeLandingPagesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageQueryService/SummarizeLandingPages")
	}
	recentShopperViews.Store(recordKey{req.GetContext().GetTenantId(), req.GetShopperView().GetShopperViewId()}, req.GetShopperView())
	return &storefrontv1.CreateShopperViewResponse{
		Result: &commonv1.OperationResult{
			Message:       advanceConsentRecordWorkflowReply.GetResult().GetMessage(),
			CorrelationId: listLandingPageSubscriptionsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  summarizeLandingPagesReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		ShopperView: &storefrontv1.ShopperView{
			Name:   req.GetShopperView().GetName(),
			Status: req.GetShopperView().GetStatus(),
		},
	}, nil
}

// GetShopperView returns a shopper view.
func (s *ShopperViewServer) GetShopperView(ctx context.Context, req *storefrontv1.GetShopperViewRequest) (*storefrontv1.GetShopperViewResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "get_shopper_view"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get shopper view", "tenant", req.GetContext().GetTenantId())
	var cachedCode string
	if entry, ok := recentShopperViews.Load(recordKey{req.GetContext().GetTenantId(), req.GetShopperViewId()}); ok {
		if previous, ok := entry.(*storefrontv1.ShopperView); ok {
			cachedCode = previous.GetCode()
		}
	}
	retryShopperViewWorkflowRequest := &storefrontv1.RetryShopperViewWorkflowRequest{
		Context:       req.GetContext(),
		ShopperViewId: req.GetShopperViewId(),
		Reason:        cachedCode,
	}
	retryShopperViewWorkflowReply := new(storefrontv1.RetryShopperViewWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, storefrontv1.ShopperViewWorkflowService_RetryShopperViewWorkflow_FullMethodName, retryShopperViewWorkflowRequest, retryShopperViewWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/RetryShopperViewWorkflow")
	}
	return &storefrontv1.GetShopperViewResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: retryShopperViewWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		ShopperView: &storefrontv1.ShopperView{
			Status:        retryShopperViewWorkflowReply.GetShopperView().GetStatus(),
			ShopperViewId: req.GetShopperViewId(),
		},
	}, nil
}

// ListShopperViews lists shopper views a page at a time.
func (s *ShopperViewServer) ListShopperViews(ctx context.Context, req *storefrontv1.ListShopperViewsRequest) (*storefrontv1.ListShopperViewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list shopper views", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded storefrontv1.ListShopperViewsRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	connectPartnerOrderSourceReply, err := s.partnerOrderIntegrationClient.ConnectPartnerOrderSource(ctx, &partnerapiv1.ConnectPartnerOrderSourceRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ConnectPartnerOrderSourceRequest_Details{
			ReceivedAt: req.GetFilter().GetCreatedAfter(),
			MessageId:  (&decoded).GetOwnerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/ConnectPartnerOrderSource")
	}
	return &storefrontv1.ListShopperViewsResponse{
		Result: &commonv1.OperationResult{
			Details: connectPartnerOrderSourceReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ShopperViews: []*storefrontv1.ShopperView{&storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				CreatedBy: connectPartnerOrderSourceReply.GetPartnerOrder().GetAudit().GetCreatedBy(),
			},
		}},
	}, nil
}

// SearchShopperViews lists shopper views a page at a time.
func (s *ShopperViewServer) SearchShopperViews(ctx context.Context, req *storefrontv1.SearchShopperViewsRequest) (*storefrontv1.SearchShopperViewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SearchShopperViews", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "search shopper views", "tenant", req.GetContext().GetTenantId())
	resolvePartnerIntegrationsRequest := &partnerapiv1.ResolvePartnerIntegrationsRequest{Context: req.GetContext()}
	resolvePartnerIntegrationsReply, err := grpcx.NewClient(s.conn, partnerapiv1.NewPartnerIntegrationQueryServiceClient).ResolvePartnerIntegrations(ctx, resolvePartnerIntegrationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationQueryService/ResolvePartnerIntegrations")
	}
	return &storefrontv1.SearchShopperViewsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: resolvePartnerIntegrationsReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// WatchShopperViews streams the changes of shopper views as they happen.
func (s *ShopperViewServer) WatchShopperViews(req *storefrontv1.WatchShopperViewsRequest, stream grpc.ServerStreamingServer[storefrontv1.WatchShopperViewsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "watch shopper views", "tenant", req.GetContext().GetTenantId())
	scoreFeatureFlagRequest := &featureflagsv1.ScoreFeatureFlagRequest{
		Context: req.GetContext(),
		Details: &featureflagsv1.ScoreFeatureFlagRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	type scoreFeatureFlagResult struct {
		reply *featureflagsv1.ScoreFeatureFlagResponse
		err   error
	}
	scoreFeatureFlagResults := make(chan scoreFeatureFlagResult, 1)
	go func() {
		reply, err := s.featureFlagValidationClient.ScoreFeatureFlag(ctx, scoreFeatureFlagRequest)
		scoreFeatureFlagResults <- scoreFeatureFlagResult{reply, err}
	}()
	var scoreFeatureFlagReply *featureflagsv1.ScoreFeatureFlagResponse
	select {
	case result := <-scoreFeatureFlagResults:
		if result.err != nil {
			return grpcx.Wrap(result.err, "acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag")
		}
		scoreFeatureFlagReply = result.reply
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	subscriberCache := cache.New[string, *commonv1.ContactInfo](1)
	subscriberCache.Put(req.GetContext().GetTraceId(), req.GetSubscriber())
	cachedSubscriber, _ := subscriberCache.Get(req.GetContext().GetTraceId())
	lintSearchQueriesRequest := &searchv1.LintSearchQueriesRequest{}
	lintSearchQueriesRequest.Context = req.GetContext()
	lintSearchQueriesRequest.Details = &searchv1.LintSearchQueriesRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: scoreFeatureFlagReply.GetFeatureFlag().GetOwner().GetEmail(),
			Phone: cachedSubscriber.GetPhone(),
		},
	}
	lintSearchQueriesRequest.SearchQuery = &searchv1.SearchQuery{
		Subject: &searchv1.SearchIndexRef{
			DisplayName: scoreFeatureFlagReply.GetFeatureFlag().GetOwner().GetDisplayName(),
		},
	}
	lintSearchQueriesReply, err := s.searchQueryValidationClient.LintSearchQueries(ctx, lintSearchQueriesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/LintSearchQueries")
	}
	if err := stream.Send(&storefrontv1.WatchShopperViewsResponse{
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				DisplayName: scoreFeatureFlagReply.GetFeatureFlag().GetOwner().GetDisplayName(),
				Email:       req.GetSubscriber().GetEmail(),
			},
			Code: lintSearchQueriesReply.GetSearchQuery().GetSubject().GetCode(),
		},
		Result: &commonv1.OperationResult{Status: lintSearchQueriesReply.GetResult().GetStatus()},
	}); err != nil {
		return err
	}
	return nil
}

// CloneShopperView creates a shopper view.
func (s *ShopperViewServer) CloneShopperView(ctx context.Context, req *storefrontv1.CloneShopperViewRequest) (*storefrontv1.CloneShopperViewResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CloneShopperView", "error", err)
		return nil, err
	}
	if err := s.store.SaveShopperView(ctx, req.GetContext().GetTenantId(), req.GetShopperView()); err != nil {
		return nil, grpcx.Wrap(err, "save shopper view")
	}
	s.log.InfoContext(ctx, "clone shopper view", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShopperView().GetAudit().GetEtag())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	schedulePartnerIntegrationReportRequest := &partnerapiv1.SchedulePartnerIntegrationReportRequest{
		Context:     req.GetContext(),
		Details:     &partnerapiv1.SchedulePartnerIntegrationReportRequest_Details{Filters: req.GetShopperView().GetSpecs()},
		Annotations: baggage,
	}
	schedulePartnerIntegrationReportReply := new(partnerapiv1.SchedulePartnerIntegrationReportResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.partnerapi.v1.PartnerIntegrationReportingService/SchedulePartnerIntegrationReport", schedulePartnerIntegrationReportRequest, schedulePartnerIntegrationReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/SchedulePartnerIntegrationReport")
	}
	customerSegmentSync, err := registry.Lookup[customersv1.CustomerSegmentSyncServiceClient](s.clients, "acme.customers.v1.CustomerSegmentSyncService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	listCustomerSegmentConflictsRequest := &customersv1.ListCustomerSegmentConflictsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListCustomerSegmentConflictsRequest_Filter{
			Region:       req.GetShopperView().GetAddress().GetRegion(),
			CreatedAfter: req.GetShopperView().GetAudit().GetCreatedAt(),
		},
	}
	listCustomerSegmentConflictsReply, err := customerSegmentSync.ListCustomerSegmentConflicts(ctx, listCustomerSegmentConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/ListCustomerSegmentConflicts")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := shopperViewServerLintSessionsSteps[mode]
	lintSessionsRequest := &identityv1.LintSessionsRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetShopperView().GetAudit().GetUpdatedBy()},
		},
	}
	lintSessionsReply, err := run(s, ctx, lintSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/LintSessions")
	}
	return &storefrontv1.CloneShopperViewResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: schedulePartnerIntegrationReportReply.GetResult().GetCorrelationId(),
			ReferenceIds:  listCustomerSegmentConflictsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				DisplayName: lintSessionsReply.GetSession().GetCustomer().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintSessionsReply.GetSession().GetAudit().GetCreatedBy(),
			},
			Capacity:      req.GetShopperView().GetCapacity(),
			ShopperViewId: req.GetShopperView().GetShopperViewId(),
		},
	}, nil
}

// shopperViewServerLintSessionsSteps are the ways to run acme.identity.v1.SessionValidationService/LintSessions, by
// name.
var shopperViewServerLintSessionsSteps = map[string]func(*ShopperViewServer, context.Context, *identityv1.LintSessionsRequest) (*identityv1.LintSessionsResponse, error){
	"default": (*ShopperViewServer).lintSessionsStep,
	"audited": (*ShopperViewServer).lintSessionsAuditedStep,
}

func (s *ShopperViewServer) lintSessionsStep(ctx context.Context, request *identityv1.LintSessionsRequest) (*identityv1.LintSessionsResponse, error) {
	return s.sessionValidationClient.LintSessions(ctx, request)
}

func (s *ShopperViewServer) lintSessionsAuditedStep(ctx context.Context, request *identityv1.LintSessionsRequest) (*identityv1.LintSessionsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.SessionValidationService/LintSessions")
	return s.sessionValidationClient.LintSessions(ctx, request)
}
