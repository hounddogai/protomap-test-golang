package storefront

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShopperViewNotificationServer implements `acme.storefront.v1.ShopperViewNotificationService`.
// Tells the people who follow a shopper view what changed.
type ShopperViewNotificationServer struct {
	storefrontv1.UnimplementedShopperViewNotificationServiceServer
	store                               *Store
	log                                 *slog.Logger
	conn                                *grpc.ClientConn
	customerSegmentImportClient         customersv1.CustomerSegmentImportServiceClient
	storefrontSessionNotificationClient storefrontv1.StorefrontSessionNotificationServiceClient
	clients                             *registry.Registry
	landingPageWorkflowClient           storefrontv1.LandingPageWorkflowServiceClient
	searchQueryExportPort               searchQueryExportPort
	consentRecordWorkflowClient         customersv1.ConsentRecordWorkflowServiceClient
	tenantAdminClient                   identityv1.TenantAdminServiceClient
	notificationQueryClient             notificationsv1.NotificationQueryServiceClient
	customerWorkflowClient              customersv1.CustomerWorkflowServiceClient
	replayShopperViewEventsFn           func(context.Context, *storefrontv1.ReplayShopperViewEventsRequest, ...grpc.CallOption) (*storefrontv1.ReplayShopperViewEventsResponse, error)
}

// NewShopperViewNotificationServer wires a ShopperViewNotificationServer.
func NewShopperViewNotificationServer(deps platform.Deps) ShopperViewNotificationServer {
	shopperViewIntegration := storefrontv1.NewShopperViewIntegrationServiceClient(deps.Conn)
	return ShopperViewNotificationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.storefront.v1.ShopperViewNotificationService"),
		conn:                                deps.Conn,
		customerSegmentImportClient:         customersv1.NewCustomerSegmentImportServiceClient(deps.Conn),
		storefrontSessionNotificationClient: storefrontv1.NewStorefrontSessionNotificationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		landingPageWorkflowClient:           storefrontv1.NewLandingPageWorkflowServiceClient(deps.Conn),
		searchQueryExportPort:               newSearchQueryExportPort(deps),
		consentRecordWorkflowClient:         customersv1.NewConsentRecordWorkflowServiceClient(deps.Conn),
		tenantAdminClient:                   identityv1.NewTenantAdminServiceClient(deps.Conn),
		notificationQueryClient:             notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		customerWorkflowClient:              customersv1.NewCustomerWorkflowServiceClient(deps.Conn),
		replayShopperViewEventsFn:           shopperViewIntegration.ReplayShopperViewEvents,
	}
}

// SubscribeShopperViewUpdates subscribes a shopper view.
func (s ShopperViewNotificationServer) SubscribeShopperViewUpdates(ctx context.Context, req *storefrontv1.SubscribeShopperViewUpdatesRequest) (*storefrontv1.SubscribeShopperViewUpdatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "subscribe_shopper_view_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe shopper view updates", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	variables := make(map[string]string, len(req.GetDetails().GetVariables()))
	for key2, value2 := range req.GetDetails().GetVariables() {
		variables[key2] = value2
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	generateHouseholdReportRequest := &customersv1.GenerateHouseholdReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &customersv1.GenerateHouseholdReportRequest_Details{Filters: variables},
		HouseholdId: envelope.GetDisplayName(),
	}
	generateHouseholdReportReply := new(customersv1.GenerateHouseholdReportResponse)
	if err := grpc.Invoke(ctx, "/acme.customers.v1.HouseholdReportingService/GenerateHouseholdReport", generateHouseholdReportRequest, generateHouseholdReportReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdReportingService/GenerateHouseholdReport")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	mapCustomerSegmentImportColumnsReply, err := s.customerSegmentImportClient.MapCustomerSegmentImportColumns(ctx, &customersv1.MapCustomerSegmentImportColumnsRequest{
		Context: req.GetContext(),
		Details: &customersv1.MapCustomerSegmentImportColumnsRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
				Name:             req.GetDetails().GetRecipient().GetName(),
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		CustomerSegmentId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/MapCustomerSegmentImportColumns")
	}
	return &storefrontv1.SubscribeShopperViewUpdatesResponse{
		Outcome: &storefrontv1.SubscribeShopperViewUpdatesResponse_Outcome{
			Status: generateHouseholdReportReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: generateHouseholdReportReply.GetResult().GetCorrelationId(),
			Message:       mapCustomerSegmentImportColumnsReply.GetResult().GetMessage(),
			Status:        mapCustomerSegmentImportColumnsReply.GetResult().GetStatus(),
		},
		ShopperView: &storefrontv1.ShopperView{
			ShopperViewId: req.GetShopperViewId(),
			Owner:         &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// UnsubscribeShopperViewUpdates unsubscribes a shopper view.
func (s ShopperViewNotificationServer) UnsubscribeShopperViewUpdates(ctx context.Context, req *storefrontv1.UnsubscribeShopperViewUpdatesRequest) (*storefrontv1.UnsubscribeShopperViewUpdatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "unsubscribe_shopper_view_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe shopper view updates", "tenant", req.GetContext().GetTenantId())
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	streamStorefrontSessionNoticesStream, err := s.storefrontSessionNotificationClient.StreamStorefrontSessionNotices(ctx, &storefrontv1.StreamStorefrontSessionNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetRecipient().GetEmail()},
		Filter:     &storefrontv1.StreamStorefrontSessionNoticesRequest_Filter{Query: envelope.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionNotificationService/StreamStorefrontSessionNotices")
	}
	for {
		received, err := streamStorefrontSessionNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionNotificationService/StreamStorefrontSessionNotices")
		}
		_ = received
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	testStorefrontSessionConnectionRequest := &storefrontv1.TestStorefrontSessionConnectionRequest{
		Context:             req.GetContext(),
		Annotations:         req.GetAnnotations(),
		StorefrontSessionId: envelope2.GetDisplayName(),
	}
	testStorefrontSessionConnectionReply := new(storefrontv1.TestStorefrontSessionConnectionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", storefrontStorefrontSessionIntegrationServiceName, "TestStorefrontSessionConnection"), testStorefrontSessionConnectionRequest, testStorefrontSessionConnectionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/TestStorefrontSessionConnection")
	}
	return &storefrontv1.UnsubscribeShopperViewUpdatesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: testStorefrontSessionConnectionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ShopperView: &storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				CreatedBy: testStorefrontSessionConnectionReply.GetStorefrontSession().GetAudit().GetCreatedBy(),
			},
			ShopperViewId: req.GetShopperViewId(),
			Owner:         &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// NotifyShopperViewWatchers notifys a shopper view.
func (s ShopperViewNotificationServer) NotifyShopperViewWatchers(ctx context.Context, req *storefrontv1.NotifyShopperViewWatchersRequest) (*storefrontv1.NotifyShopperViewWatchersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NotifyShopperViewWatchers", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "notify_shopper_view_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify shopper view watchers", "tenant", req.GetContext().GetTenantId())
	storefrontSessionNotification, ok := s.clients.Get("acme.storefront.v1.StorefrontSessionNotificationService").(storefrontv1.StorefrontSessionNotificationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.storefront.v1.StorefrontSessionNotificationService")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeStorefrontSessionUpdatesRequest := &storefrontv1.SubscribeStorefrontSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.SubscribeStorefrontSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: req.GetDetails().GetRecipient().GetEmail(),
				Phone: req.GetDetails().GetRecipient().GetPhone(),
				Name:  req.GetDetails().GetRecipient().GetName(),
			},
			Channel: req.GetDetails().GetChannel(),
			Payload: packed,
		},
	}
	subscribeStorefrontSessionUpdatesReply, err := storefrontSessionNotification.SubscribeStorefrontSessionUpdates(ctx, subscribeStorefrontSessionUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionNotificationService/SubscribeStorefrontSessionUpdates")
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cancelPartnerOrderWorkflowRequest := &partnerapiv1.CancelPartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.CancelPartnerOrderWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: subscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetRequester().GetEmail(),
				Phone: req.GetDetails().GetRecipient().GetPhone(),
			},
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				Email: subscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetAssignee().GetEmail(),
			},
		},
		PartnerOrderId: envelope.GetDisplayName(),
	}
	cancelPartnerOrderWorkflowReply := new(partnerapiv1.CancelPartnerOrderWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, partnerapiv1.PartnerOrderWorkflowService_CancelPartnerOrderWorkflow_FullMethodName, cancelPartnerOrderWorkflowRequest, cancelPartnerOrderWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/CancelPartnerOrderWorkflow")
	}
	return &storefrontv1.NotifyShopperViewWatchersResponse{
		ShopperView: &storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				CreatedAt: subscribeStorefrontSessionUpdatesReply.GetStorefrontSession().GetAudit().GetCreatedAt(),
			},
			ShopperViewId: req.GetShopperViewId(),
			Owner:         &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Outcome: &storefrontv1.NotifyShopperViewWatchersResponse_Outcome{
			EffectiveAt: cancelPartnerOrderWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListShopperViewSubscriptions lists shopper views a page at a time.
func (s ShopperViewNotificationServer) ListShopperViewSubscriptions(ctx context.Context, req *storefrontv1.ListShopperViewSubscriptionsRequest) (*storefrontv1.ListShopperViewSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list shopper view subscriptions", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifyStorefrontSessionWatchersReply, err := s.storefrontSessionNotificationClient.NotifyStorefrontSessionWatchers(ctx, &storefrontv1.NotifyStorefrontSessionWatchersRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.NotifyStorefrontSessionWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Payload:   packed,
		},
		Annotations: labels,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionNotificationService/NotifyStorefrontSessionWatchers")
	}
	return &storefrontv1.ListShopperViewSubscriptionsResponse{
		ShopperViews: []*storefrontv1.ShopperView{&storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				DisplayName: notifyStorefrontSessionWatchersReply.GetStorefrontSession().GetAssignee().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SendShopperViewTestNotice sends a shopper view.
func (s ShopperViewNotificationServer) SendShopperViewTestNotice(ctx context.Context, req *storefrontv1.SendShopperViewTestNoticeRequest) (*storefrontv1.SendShopperViewTestNoticeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "send_shopper_view_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send shopper view test notice", "tenant", req.GetContext().GetTenantId())
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pauseLandingPageWorkflowReply, err := s.landingPageWorkflowClient.PauseLandingPageWorkflow(ctx, &storefrontv1.PauseLandingPageWorkflowRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.PauseLandingPageWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetRecipient().GetPhone(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
				Name:             req.GetDetails().GetRecipient().GetName(),
			},
		},
		LandingPage: &storefrontv1.LandingPage{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		LandingPageId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageWorkflowService/PauseLandingPageWorkflow")
	}
	listSearchQueryExportsRequest := &searchv1.ListSearchQueryExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQueryExportsRequest_Filter{
			Region:       pauseLandingPageWorkflowReply.GetLandingPage().GetBillingAddress().GetRegion(),
			CreatedAfter: pauseLandingPageWorkflowReply.GetLandingPage().GetAudit().GetCreatedAt(),
		},
	}
	listSearchQueryExportsReply, err := s.searchQueryExportPort.ListSearchQueryExports(ctx, listSearchQueryExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ListSearchQueryExports")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryPartnerIntegrationWorkflowRequest := &partnerapiv1.RetryPartnerIntegrationWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.RetryPartnerIntegrationWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Funding: &partnerapiv1.PartnerIntegration_Bank{Bank: &partnerapiv1.PartnerIntegrationBankFunding{
				Bic: pauseLandingPageWorkflowReply.GetLandingPage().GetBank().GetBic(),
			}},
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetRecipient().GetName()},
		},
		PartnerIntegrationId: envelope2.GetDisplayName(),
	}
	retryPartnerIntegrationWorkflowReply := new(partnerapiv1.RetryPartnerIntegrationWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", partnerapiPartnerIntegrationWorkflowServiceName, "RetryPartnerIntegrationWorkflow"), retryPartnerIntegrationWorkflowRequest, retryPartnerIntegrationWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/RetryPartnerIntegrationWorkflow")
	}
	return &storefrontv1.SendShopperViewTestNoticeResponse{
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				DisplayName: pauseLandingPageWorkflowReply.GetLandingPage().GetCounterparty().GetDisplayName(),
				Email:       req.GetDetails().GetRecipient().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: retryPartnerIntegrationWorkflowReply.GetPartnerIntegration().GetAudit().GetUpdatedBy(),
			},
			ShopperViewId: req.GetShopperViewId(),
		},
		Result: &commonv1.OperationResult{
			Details:      listSearchQueryExportsReply.GetResult().GetDetails(),
			ReferenceIds: listSearchQueryExportsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// GetShopperViewPreferences returns a shopper view.
func (s ShopperViewNotificationServer) GetShopperViewPreferences(ctx context.Context, req *storefrontv1.GetShopperViewPreferencesRequest) (*storefrontv1.GetShopperViewPreferencesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetShopperViewPreferences", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "get_shopper_view_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get shopper view preferences", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	ctx = tenancy.With(ctx, "trace_id", req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	cancelConsentRecordWorkflowReply, err := s.consentRecordWorkflowClient.CancelConsentRecordWorkflow(ctx, &customersv1.CancelConsentRecordWorkflowRequest{
		Context: req.GetContext(),
		Details: &customersv1.CancelConsentRecordWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId: req.GetShopperViewId(),
			Measurements:  []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: (&decoded).GetDeadline()}},
			Source:        tenancy.Value(ctx, "trace_id"),
			Detail:        packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordWorkflowService/CancelConsentRecordWorkflow")
	}
	return &storefrontv1.GetShopperViewPreferencesResponse{
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				DisplayName: cancelConsentRecordWorkflowReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			ShopperViewId: req.GetShopperViewId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamShopperViewNotices streams the changes of shopper views as they happen.
func (s ShopperViewNotificationServer) StreamShopperViewNotices(req *storefrontv1.StreamShopperViewNoticesRequest, stream grpc.ServerStreamingServer[storefrontv1.StreamShopperViewNoticesResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream shopper view notices", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetSubscriber()).(*commonv1.ContactInfo)
	cloned2 := proto.Clone(req.GetSubscriber()).(*commonv1.ContactInfo)
	source := req.GetSubscriber().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	archiveTenantReply, err := s.tenantAdminClient.ArchiveTenant(ctx, &identityv1.ArchiveTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.ArchiveTenantRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Phone:            cloned.GetPhone(),
				PreferredChannel: cloned2.GetPreferredChannel(),
				Email:            email,
				Name:             req.GetSubscriber().GetName(),
			},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ArchiveTenant")
	}
	cloned3 := proto.Clone(req.GetFilter()).(*storefrontv1.StreamShopperViewNoticesRequest_Filter)
	step := &shopperViewNotificationServerStreamShopperViewNoticesFindNotificationsStep{request: &notificationsv1.FindNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.FindNotificationsRequest_Filter{
			Region:       archiveTenantReply.GetTenant().GetLocation().GetRegion(),
			OwnerId:      req.GetFilter().GetOwnerId(),
			Query:        req.GetFilter().GetQuery(),
			CreatedAfter: cloned3.GetCreatedAfter(),
		},
	}}
	if err := s.runStreamShopperViewNoticesFindNotifications(ctx, step); err != nil {
		return grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/FindNotifications")
	}
	findNotificationsReply := step.reply
	retryCustomerWorkflowRequest := &customersv1.RetryCustomerWorkflowRequest{}
	retryCustomerWorkflowRequest.Context = req.GetContext()
	retryCustomerWorkflowRequest.Customer = &customersv1.Customer{
		Audit: &commonv1.AuditInfo{
			CreatedAt: archiveTenantReply.GetTenant().GetAudit().GetCreatedAt(),
		},
		PrimaryAddress: &commonv1.PostalAddress{
			CountryCode: archiveTenantReply.GetTenant().GetLocation().GetCountryCode(),
		},
	}
	retryCustomerWorkflowReply, err := s.customerWorkflowClient.RetryCustomerWorkflow(ctx, retryCustomerWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerWorkflowService/RetryCustomerWorkflow")
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	replayShopperViewEventsRequest := &storefrontv1.ReplayShopperViewEventsRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.ReplayShopperViewEventsRequest_Details{
			MessageId: archiveTenantReply.GetOutcome().GetMessage(),
			Envelope:  packed,
		},
	}
	replayShopperViewEventsReply, err := s.replayShopperViewEventsFn(ctx, replayShopperViewEventsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/ReplayShopperViewEvents")
	}
	if err := stream.Send(&storefrontv1.StreamShopperViewNoticesResponse{
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				Email:       archiveTenantReply.GetTenant().GetManager().GetEmail(),
				UserId:      archiveTenantReply.GetTenant().GetManager().GetUserId(),
				DisplayName: retryCustomerWorkflowReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: replayShopperViewEventsReply.GetShopperView().GetAudit().GetCreatedBy(),
			},
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: findNotificationsReply.GetResult().GetReferenceIds(),
			Message:      replayShopperViewEventsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// shopperViewNotificationServerStreamShopperViewNoticesFindNotificationsStep carries the request of one step of
// StreamShopperViewNotices and, once it ran, its reply.
type shopperViewNotificationServerStreamShopperViewNoticesFindNotificationsStep struct {
	request *notificationsv1.FindNotificationsRequest
	reply   *notificationsv1.FindNotificationsResponse
}

func (s ShopperViewNotificationServer) runStreamShopperViewNoticesFindNotifications(ctx context.Context, step *shopperViewNotificationServerStreamShopperViewNoticesFindNotificationsStep) error {
	reply, err := s.notificationQueryClient.FindNotifications(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
