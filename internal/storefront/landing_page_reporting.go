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
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	webbffv1 "github.com/hounddogai/protomap-test-golang/gen/acme/webbff/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// LandingPageReportingServer implements `acme.storefront.v1.LandingPageReportingService`.
// Builds and schedules reports about landing pages.
type LandingPageReportingServer struct {
	storefrontv1.UnimplementedLandingPageReportingServiceServer
	store                             *Store
	log                               *slog.Logger
	unsubscribeApiCredentialUpdatesFn func(context.Context, *identityv1.UnsubscribeApiCredentialUpdatesRequest, ...grpc.CallOption) (*identityv1.UnsubscribeApiCredentialUpdatesResponse, error)
	storefrontSessionWorkflowClient   storefrontv1.StorefrontSessionWorkflowServiceClient
	partnerOrderIntegrationClient     partnerapiv1.PartnerOrderIntegrationServiceClient
	pageCompositionClient             webbffv1.PageCompositionServiceClient
	customerSegmentLifecycleClient    customersv1.CustomerSegmentLifecycleServiceClient
	clients                           *registry.Registry
	consentRecordSyncClient           customersv1.ConsentRecordSyncServiceClient
	searchIndexNotificationClient     searchv1.SearchIndexNotificationServiceClient
	featureFlagValidationClient       featureflagsv1.FeatureFlagValidationServiceClient
	partnerWebhookReportingClient     partnerapiv1.PartnerWebhookReportingServiceClient
	conn                              *grpc.ClientConn
	partnerOrderOnce                  sync.Once
	partnerOrderCached                partnerapiv1.PartnerOrderServiceClient
	meteredPartnerWebhookValidation   meteredPartnerWebhookValidation
}

// NewLandingPageReportingServer wires a LandingPageReportingServer.
func NewLandingPageReportingServer(deps platform.Deps) *LandingPageReportingServer {
	apiCredentialNotification := identityv1.NewApiCredentialNotificationServiceClient(deps.Conn)
	return &LandingPageReportingServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.storefront.v1.LandingPageReportingService"),
		unsubscribeApiCredentialUpdatesFn: apiCredentialNotification.UnsubscribeApiCredentialUpdates,
		storefrontSessionWorkflowClient:   storefrontv1.NewStorefrontSessionWorkflowServiceClient(deps.Conn),
		partnerOrderIntegrationClient:     partnerapiv1.NewPartnerOrderIntegrationServiceClient(deps.Conn),
		pageCompositionClient:             webbffv1.NewPageCompositionServiceClient(deps.Conn),
		customerSegmentLifecycleClient:    customersv1.NewCustomerSegmentLifecycleServiceClient(deps.Conn),
		clients:                           deps.Clients,
		consentRecordSyncClient:           customersv1.NewConsentRecordSyncServiceClient(deps.Conn),
		searchIndexNotificationClient:     searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		featureFlagValidationClient:       featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		partnerWebhookReportingClient:     partnerapiv1.NewPartnerWebhookReportingServiceClient(deps.Conn),
		conn:                              deps.Conn,
		meteredPartnerWebhookValidation:   meteredPartnerWebhookValidation{PartnerWebhookValidationServiceClient: partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// GenerateLandingPageReport generates a landing page.
func (s *LandingPageReportingServer) GenerateLandingPageReport(ctx context.Context, req *storefrontv1.GenerateLandingPageReportRequest) (*storefrontv1.GenerateLandingPageReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "generate_landing_page_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate landing page report", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	unsubscribeApiCredentialUpdatesRequest := &identityv1.UnsubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
			Variables: filters,
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	unsubscribeApiCredentialUpdatesReply, err := s.unsubscribeApiCredentialUpdatesFn(ctx, unsubscribeApiCredentialUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/UnsubscribeApiCredentialUpdates")
	}
	pauseStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowClient.PauseStorefrontSessionWorkflow(ctx, &storefrontv1.PauseStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetAudit().GetUpdatedBy(),
				Etag:      unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetAudit().GetEtag(),
			},
		},
		Details: &storefrontv1.PauseStorefrontSessionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetContact().GetPreferredChannel(),
				Phone:            unsubscribeApiCredentialUpdatesReply.GetApiCredential().GetContact().GetPhone(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/PauseStorefrontSessionWorkflow")
	}
	retryStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowClient.RetryStorefrontSessionWorkflow(ctx, &storefrontv1.RetryStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				Email: pauseStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetEmail(),
			},
			Origin: &storefrontv1.StorefrontSession_Channel{Channel: &storefrontv1.StorefrontSessionChannelOrigin{
				Address: pauseStorefrontSessionWorkflowReply.GetStorefrontSession().GetChannel().GetAddress(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/RetryStorefrontSessionWorkflow")
	}
	return &storefrontv1.GenerateLandingPageReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: unsubscribeApiCredentialUpdatesReply.GetResult().GetCorrelationId(),
			Details:       unsubscribeApiCredentialUpdatesReply.GetResult().GetDetails(),
			Status:        pauseStorefrontSessionWorkflowReply.GetOutcome().GetStatus(),
			ReferenceIds:  retryStorefrontSessionWorkflowReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchLandingPageReport returns a landing page.
func (s *LandingPageReportingServer) FetchLandingPageReport(ctx context.Context, req *storefrontv1.FetchLandingPageReportRequest) (*storefrontv1.FetchLandingPageReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "fetch_landing_page_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch landing page report", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cancelStorefrontSessionWorkflowRequest := &storefrontv1.CancelStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Assignee: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: (&decoded).GetActor().GetUserId(),
			},
			Origin: &storefrontv1.StorefrontSession_ExternalReference{ExternalReference: req.GetLandingPageId()},
		},
	}
	cancelStorefrontSessionWorkflowReply, err := storefrontv1.StorefrontSessionWorkflowServiceClient.CancelStorefrontSessionWorkflow(s.storefrontSessionWorkflowClient, ctx, cancelStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/CancelStorefrontSessionWorkflow")
	}
	return &storefrontv1.FetchLandingPageReportResponse{
		LandingPage: &storefrontv1.LandingPage{
			Audit: &commonv1.AuditInfo{
				Etag: cancelStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetEtag(),
			},
			LandingPageId: req.GetLandingPageId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleLandingPageReport schedules a landing page.
func (s *LandingPageReportingServer) ScheduleLandingPageReport(ctx context.Context, req *storefrontv1.ScheduleLandingPageReportRequest) (*storefrontv1.ScheduleLandingPageReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "schedule_landing_page_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule landing page report", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	connectPartnerOrderSourceRequest := &partnerapiv1.ConnectPartnerOrderSourceRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details: &partnerapiv1.ConnectPartnerOrderSourceRequest_Details{
			MessageId:  req.GetLandingPageId(),
			ReceivedAt: req.GetContext().GetDeadline(),
			Envelope:   packed,
		},
	}
	connectPartnerOrderSourceReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*partnerapiv1.ConnectPartnerOrderSourceResponse, error) {
		return s.partnerOrderIntegrationClient.ConnectPartnerOrderSource(ctx, connectPartnerOrderSourceRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/ConnectPartnerOrderSource")
	}
	advanceStorefrontSessionWorkflowRequest := &storefrontv1.AdvanceStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: connectPartnerOrderSourceReply.GetPartnerOrder().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: connectPartnerOrderSourceReply.GetPartnerOrder().GetCounterparty().GetEmail(),
			},
			Origin: &storefrontv1.StorefrontSession_Customer{Customer: &storefrontv1.ShopperViewRef{
				DisplayName: connectPartnerOrderSourceReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			}},
		},
	}
	advanceStorefrontSessionWorkflowReply, err := storefrontv1.StorefrontSessionWorkflowServiceClient.AdvanceStorefrontSessionWorkflow(s.storefrontSessionWorkflowClient, ctx, advanceStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/AdvanceStorefrontSessionWorkflow")
	}
	searchPageCompositionsRequest := &webbffv1.SearchPageCompositionsRequest{
		Context: req.GetContext(),
		Filter: &webbffv1.SearchPageCompositionsRequest_Filter{
			Region:       connectPartnerOrderSourceReply.GetPartnerOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: advanceStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetUpdatedAt(),
		},
	}
	searchPageCompositionsReply, err := webbffv1.PageCompositionServiceClient.SearchPageCompositions(s.pageCompositionClient, ctx, searchPageCompositionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.webbff.v1.PageCompositionService/SearchPageCompositions")
	}
	return &storefrontv1.ScheduleLandingPageReportResponse{
		Result: &commonv1.OperationResult{
			Message:      connectPartnerOrderSourceReply.GetResult().GetMessage(),
			Status:       advanceStorefrontSessionWorkflowReply.GetResult().GetStatus(),
			Details:      searchPageCompositionsReply.GetResult().GetDetails(),
			ReferenceIds: searchPageCompositionsReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamLandingPageMetrics streams the changes of landing pages as they happen.
func (s *LandingPageReportingServer) StreamLandingPageMetrics(req *storefrontv1.StreamLandingPageMetricsRequest, stream grpc.ServerStreamingServer[storefrontv1.StreamLandingPageMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream landing page metrics", "tenant", req.GetContext().GetTenantId())
	source := req.GetSubscriber().ProtoReflect()
	preferredChannel := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("preferred_channel"))).String()
	scheduleCustomerSegmentExpiryRequest := &customersv1.ScheduleCustomerSegmentExpiryRequest{
		Context: req.GetContext(),
		Details: &customersv1.ScheduleCustomerSegmentExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email:            req.GetSubscriber().GetEmail(),
				PreferredChannel: preferredChannel,
				Phone:            req.GetSubscriber().GetPhone(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	scheduleCustomerSegmentExpiryReply, err := grpcx.Call(ctx, s.customerSegmentLifecycleClient.ScheduleCustomerSegmentExpiry, scheduleCustomerSegmentExpiryRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentLifecycleService/ScheduleCustomerSegmentExpiry")
	}
	storefrontSessionWorkflow, ok := s.clients.Get("acme.storefront.v1.StorefrontSessionWorkflowService").(storefrontv1.StorefrontSessionWorkflowServiceClient)
	if !ok {
		return status.Errorf(codes.Internal, "no client of %s", "acme.storefront.v1.StorefrontSessionWorkflowService")
	}
	subscriberCache := cache.New[string, *commonv1.ContactInfo](1)
	subscriberCache.Put(req.GetContext().GetTraceId(), req.GetSubscriber())
	cachedSubscriber, _ := subscriberCache.Get(req.GetContext().GetTraceId())
	signalStorefrontSessionWorkflowRequest := &storefrontv1.SignalStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Comments: []*storefrontv1.StorefrontSessionComment{&storefrontv1.StorefrontSessionComment{
				CreatedAt: scheduleCustomerSegmentExpiryReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			}},
			Requester: &commonv1.ContactInfo{Name: cachedSubscriber.GetName()},
		},
		Details: &storefrontv1.SignalStorefrontSessionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: scheduleCustomerSegmentExpiryReply.GetCustomerSegment().GetOwner().GetEmail(),
				Phone: req.GetSubscriber().GetPhone(),
			},
		},
	}
	signalStorefrontSessionWorkflowReply, err := storefrontSessionWorkflow.SignalStorefrontSessionWorkflow(ctx, signalStorefrontSessionWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/SignalStorefrontSessionWorkflow")
	}
	synonymNotification, ok := s.clients.Get("acme.search.v1.SynonymNotificationService").(searchv1.SynonymNotificationServiceClient)
	if !ok {
		return status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SynonymNotificationService")
	}
	listSynonymSubscriptionsRequest := &searchv1.ListSynonymSubscriptionsRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.ListSynonymSubscriptionsRequest_Filter{Labels: req.GetFilter().GetLabels()},
	}
	listSynonymSubscriptionsReply, err := synonymNotification.ListSynonymSubscriptions(ctx, listSynonymSubscriptionsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/ListSynonymSubscriptions")
	}
	streamConsentRecordChangesStream, err := s.consentRecordSyncClient.StreamConsentRecordChanges(ctx)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/StreamConsentRecordChanges")
	}
	if err := streamConsentRecordChangesStream.Send(&customersv1.StreamConsentRecordChangesRequest{
		Context: req.GetContext(),
		Frame: &customersv1.StreamConsentRecordChangesRequest_Change{Change: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: signalStorefrontSessionWorkflowReply.GetStorefrontSession().GetCustomer().GetDisplayName(),
			},
			CorrelationId: signalStorefrontSessionWorkflowReply.GetResult().GetCorrelationId(),
			Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{
				ObservedAt: scheduleCustomerSegmentExpiryReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			}},
		}},
	}); err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/StreamConsentRecordChanges")
	}
	if err := streamConsentRecordChangesStream.CloseSend(); err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/StreamConsentRecordChanges")
	}
	acknowledged, err := streamConsentRecordChangesStream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/StreamConsentRecordChanges")
	}
	_ = acknowledged
	if err := stream.Send(&storefrontv1.StreamLandingPageMetricsResponse{
		LandingPage: &storefrontv1.LandingPage{
			Lines: []*storefrontv1.LandingPageLine{&storefrontv1.LandingPageLine{
				Description: scheduleCustomerSegmentExpiryReply.GetCustomerSegment().GetDescription(),
			}},
			DueAt: signalStorefrontSessionWorkflowReply.GetStorefrontSession().GetSla().GetDueAt(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: signalStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetUpdatedAt(),
			},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			Status: listSynonymSubscriptionsReply.GetResult().GetStatus(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListLandingPageReports lists landing pages a page at a time.
func (s *LandingPageReportingServer) ListLandingPageReports(ctx context.Context, req *storefrontv1.ListLandingPageReportsRequest) (*storefrontv1.ListLandingPageReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListLandingPageReports", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list landing page reports", "tenant", req.GetContext().GetTenantId())
	return &storefrontv1.ListLandingPageReportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ShareLandingPageReport shares a landing page.
func (s *LandingPageReportingServer) ShareLandingPageReport(ctx context.Context, req *storefrontv1.ShareLandingPageReportRequest) (*storefrontv1.ShareLandingPageReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "share_landing_page_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share landing page report", "tenant", req.GetContext().GetTenantId())
	listSearchIndexSubscriptionsRequest := buildListSearchIndexSubscriptionsRequestForShareLandingPageReport(req)
	listSearchIndexSubscriptionsReply, err := s.searchIndexNotificationClient.ListSearchIndexSubscriptions(ctx, listSearchIndexSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions")
	}
	scoreFeatureFlagRequest := &featureflagsv1.ScoreFeatureFlagRequest{}
	scoreFeatureFlagRequest.Context = req.GetContext()
	scoreFeatureFlagRequest.FeatureFlag = &featureflagsv1.FeatureFlag{
		Owner: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
	}
	scoreFeatureFlagReply, err := s.featureFlagValidationClient.ScoreFeatureFlag(ctx, scoreFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag")
	}
	return &storefrontv1.ShareLandingPageReportResponse{
		Outcome: &storefrontv1.ShareLandingPageReportResponse_Outcome{
			Status: listSearchIndexSubscriptionsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: listSearchIndexSubscriptionsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		LandingPage: &storefrontv1.LandingPage{
			Total:         scoreFeatureFlagReply.GetBreakdown().GetTotal(),
			LandingPageId: req.GetLandingPageId(),
			Counterparty:  &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// CountLandingPageTotals counts landing pages by status.
func (s *LandingPageReportingServer) CountLandingPageTotals(ctx context.Context, req *storefrontv1.CountLandingPageTotalsRequest) (*storefrontv1.CountLandingPageTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountLandingPageTotals", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count landing page totals", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sharePartnerWebhookReportRequest := &partnerapiv1.SharePartnerWebhookReportRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SharePartnerWebhookReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: holder.Email},
			Payload:   packed,
		},
		Annotations: req.GetFilter().GetLabels(),
	}
	sharePartnerWebhookReportReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*partnerapiv1.SharePartnerWebhookReportResponse, error) {
		return s.partnerWebhookReportingClient.SharePartnerWebhookReport(ctx, sharePartnerWebhookReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookReportingService/SharePartnerWebhookReport")
	}
	return &storefrontv1.CountLandingPageTotalsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: sharePartnerWebhookReportReply.GetResult().GetCorrelationId(),
			Details:       sharePartnerWebhookReportReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}, nil
}

// DeleteLandingPageReport deletes a landing page.
func (s *LandingPageReportingServer) DeleteLandingPageReport(ctx context.Context, req *storefrontv1.DeleteLandingPageReportRequest) (*storefrontv1.DeleteLandingPageReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "delete_landing_page_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete landing page report", "tenant", req.GetContext().GetTenantId())
	signalStorefrontSessionWorkflowRequest := &storefrontv1.SignalStorefrontSessionWorkflowRequest{
		Context:           req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
		Details: &storefrontv1.SignalStorefrontSessionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Reason: req.GetReason(),
	}
	signalStorefrontSessionWorkflowReply, err := grpcx.Call(ctx, s.storefrontSessionWorkflowClient.SignalStorefrontSessionWorkflow, signalStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/SignalStorefrontSessionWorkflow")
	}
	createPartnerOrderRequest := &partnerapiv1.CreatePartnerOrderRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: signalStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetUpdatedBy(),
			},
		},
	}
	createPartnerOrderReply, err := s.partnerOrderLazy().CreatePartnerOrder(ctx, createPartnerOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderService/CreatePartnerOrder")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validatePartnerWebhookRequest := &partnerapiv1.ValidatePartnerWebhookRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: createPartnerOrderReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	validatePartnerWebhookReply, err := s.meteredPartnerWebhookValidation.ValidatePartnerWebhook(ctx, validatePartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ValidatePartnerWebhook")
	}
	startStorefrontSessionWorkflowRequest := &storefrontv1.StartStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				Email: signalStorefrontSessionWorkflowReply.GetStorefrontSession().GetAssignee().GetEmail(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: createPartnerOrderReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
		},
	}
	startStorefrontSessionWorkflowReply := new(storefrontv1.StartStorefrontSessionWorkflowResponse)
	if err := s.conn.Invoke(ctx, storefrontStorefrontSessionWorkflowServiceStartStorefrontSessionWorkflowMethod, startStorefrontSessionWorkflowRequest, startStorefrontSessionWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/StartStorefrontSessionWorkflow")
	}
	return &storefrontv1.DeleteLandingPageReportResponse{
		Result: &commonv1.OperationResult{
			Message:      signalStorefrontSessionWorkflowReply.GetOutcome().GetMessage(),
			Status:       validatePartnerWebhookReply.GetOutcome().GetStatus(),
			Details:      startStorefrontSessionWorkflowReply.GetResult().GetDetails(),
			ReferenceIds: startStorefrontSessionWorkflowReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// partnerOrderLazy dials acme.partnerapi.v1.PartnerOrderService the first time a handler needs it.
func (s *LandingPageReportingServer) partnerOrderLazy() partnerapiv1.PartnerOrderServiceClient {
	s.partnerOrderOnce.Do(func() { s.partnerOrderCached = partnerapiv1.NewPartnerOrderServiceClient(s.conn) })
	return s.partnerOrderCached
}
