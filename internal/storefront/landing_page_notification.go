package storefront

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
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
	"google.golang.org/protobuf/types/known/anypb"
)

// LandingPageNotificationServer implements `acme.storefront.v1.LandingPageNotificationService`.
// Tells the people who follow a landing page what changed.
type LandingPageNotificationServer struct {
	storefrontv1.UnimplementedLandingPageNotificationServiceServer
	store                               *Store
	log                                 *slog.Logger
	partnerIntegrationWorkflowClient    partnerapiv1.PartnerIntegrationWorkflowServiceClient
	conn                                *grpc.ClientConn
	partnerIntegrationIntegrationClient partnerapiv1.PartnerIntegrationIntegrationServiceClient
	householdApprovalPort               householdApprovalPort
	landingPageIntegrationClient        storefrontv1.LandingPageIntegrationServiceClient
	featureFlagClient                   featureflagsv1.FeatureFlagServiceClient
	partnerIntegrationReportingClient   partnerapiv1.PartnerIntegrationReportingServiceClient
	partnerWebhookWorkflowClient        partnerapiv1.PartnerWebhookWorkflowServiceClient
	storefrontSessionIntegrationClient  storefrontv1.StorefrontSessionIntegrationServiceClient
	clients                             *registry.Registry
	partnerOrderApprovalClient          partnerapiv1.PartnerOrderApprovalServiceClient
	scheduleLandingPageReportFn         func(context.Context, *storefrontv1.ScheduleLandingPageReportRequest, ...grpc.CallOption) (*storefrontv1.ScheduleLandingPageReportResponse, error)
	shopperViewIntegrationClient        storefrontv1.ShopperViewIntegrationServiceClient
	synonymSyncClient                   searchv1.SynonymSyncServiceClient
	shopperViewWorkflowClient           storefrontv1.ShopperViewWorkflowServiceClient
}

// NewLandingPageNotificationServer wires a LandingPageNotificationServer.
func NewLandingPageNotificationServer(deps platform.Deps) *LandingPageNotificationServer {
	landingPageReporting := storefrontv1.NewLandingPageReportingServiceClient(deps.Conn)
	return &LandingPageNotificationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.storefront.v1.LandingPageNotificationService"),
		partnerIntegrationWorkflowClient:    partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn),
		conn:                                deps.Conn,
		partnerIntegrationIntegrationClient: partnerapiv1.NewPartnerIntegrationIntegrationServiceClient(deps.Conn),
		householdApprovalPort:               newHouseholdApprovalPort(deps),
		landingPageIntegrationClient:        storefrontv1.NewLandingPageIntegrationServiceClient(deps.Conn),
		featureFlagClient:                   featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		partnerIntegrationReportingClient:   partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn),
		partnerWebhookWorkflowClient:        partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn),
		storefrontSessionIntegrationClient:  storefrontv1.NewStorefrontSessionIntegrationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		partnerOrderApprovalClient:          partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn),
		scheduleLandingPageReportFn:         landingPageReporting.ScheduleLandingPageReport,
		shopperViewIntegrationClient:        storefrontv1.NewShopperViewIntegrationServiceClient(deps.Conn),
		synonymSyncClient:                   searchv1.NewSynonymSyncServiceClient(deps.Conn),
		shopperViewWorkflowClient:           storefrontv1.NewShopperViewWorkflowServiceClient(deps.Conn),
	}
}

// SubscribeLandingPageUpdates subscribes a landing page.
func (s *LandingPageNotificationServer) SubscribeLandingPageUpdates(ctx context.Context, req *storefrontv1.SubscribeLandingPageUpdatesRequest) (*storefrontv1.SubscribeLandingPageUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubscribeLandingPageUpdates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "subscribe_landing_page_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe landing page updates", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded storefrontv1.SubscribeLandingPageUpdatesRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetRecipient().GetPreferredChannel())
	cancelPartnerIntegrationWorkflowRequest := newCancelPartnerIntegrationWorkflowRequest(withCancelPartnerIntegrationWorkflowContext(req.GetContext()), withCancelPartnerIntegrationWorkflowPartnerIntegration(&partnerapiv1.PartnerIntegration{
		Contact: &commonv1.ContactInfo{Phone: (&decoded).GetRecipient().GetPhone()},
	}), withCancelPartnerIntegrationWorkflowDetails(&partnerapiv1.CancelPartnerIntegrationWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
	}))
	cancelPartnerIntegrationWorkflowReply, err := s.partnerIntegrationWorkflowClient.CancelPartnerIntegrationWorkflow(ctx, cancelPartnerIntegrationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/CancelPartnerIntegrationWorkflow")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	acknowledgeStorefrontSessionEventRequest := &storefrontv1.AcknowledgeStorefrontSessionEventRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &storefrontv1.AcknowledgeStorefrontSessionEventRequest_Details{Envelope: packed},
	}
	acknowledgeStorefrontSessionEventReply := new(storefrontv1.AcknowledgeStorefrontSessionEventResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", storefrontStorefrontSessionIntegrationServiceName, "AcknowledgeStorefrontSessionEvent"), acknowledgeStorefrontSessionEventRequest, acknowledgeStorefrontSessionEventReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/AcknowledgeStorefrontSessionEvent")
	}
	relayPartnerIntegrationMessagesStream, err := s.partnerIntegrationIntegrationClient.RelayPartnerIntegrationMessages(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/RelayPartnerIntegrationMessages")
	}
	if err := relayPartnerIntegrationMessagesStream.Send(&partnerapiv1.RelayPartnerIntegrationMessagesRequest{
		Context: req.GetContext(),
		Frame: &partnerapiv1.RelayPartnerIntegrationMessagesRequest_Change{Change: &partnerapiv1.PartnerIntegration{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: cancelPartnerIntegrationWorkflowReply.GetPartnerIntegration().GetContact().GetPreferredChannel(),
				Email:            acknowledgeStorefrontSessionEventReply.GetStorefrontSession().GetRequester().GetEmail(),
			},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/RelayPartnerIntegrationMessages")
	}
	if err := relayPartnerIntegrationMessagesStream.CloseSend(); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/RelayPartnerIntegrationMessages")
	}
	acknowledged, err := relayPartnerIntegrationMessagesStream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/RelayPartnerIntegrationMessages")
	}
	_ = acknowledged
	return &storefrontv1.SubscribeLandingPageUpdatesResponse{
		Outcome: &storefrontv1.SubscribeLandingPageUpdatesResponse_Outcome{
			Message: cancelPartnerIntegrationWorkflowReply.GetResult().GetMessage(),
		},
		LandingPage: &storefrontv1.LandingPage{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cancelPartnerIntegrationWorkflowReply.GetPartnerIntegration().GetAudit().GetCreatedBy(),
				Etag:      acknowledgeStorefrontSessionEventReply.GetStorefrontSession().GetAudit().GetEtag(),
			},
			LandingPageId: req.GetLandingPageId(),
			Counterparty:  &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnsubscribeLandingPageUpdates unsubscribes a landing page.
func (s *LandingPageNotificationServer) UnsubscribeLandingPageUpdates(ctx context.Context, req *storefrontv1.UnsubscribeLandingPageUpdatesRequest) (*storefrontv1.UnsubscribeLandingPageUpdatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "unsubscribe_landing_page_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe landing page updates", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	rejectHouseholdRequest := &customersv1.RejectHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
				}},
			}},
		},
	}
	rejectHouseholdReply, err := s.householdApprovalPort.RejectHousehold(ctx, rejectHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdApprovalService/RejectHousehold")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	step := &landingPageNotificationServerUnsubscribeLandingPageUpdatesTestLandingPageConnectionStep{request: &storefrontv1.TestLandingPageConnectionRequest{
		Context:       req.GetContext(),
		LandingPageId: req.GetLandingPageId(),
		Annotations:   annotations,
	}}
	if err := s.runUnsubscribeLandingPageUpdatesTestLandingPageConnection(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageIntegrationService/TestLandingPageConnection")
	}
	testLandingPageConnectionReply := step.reply
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := landingPageNotificationServerListFeatureFlagsSteps[mode]
	listFeatureFlagsRequest := &featureflagsv1.ListFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.ListFeatureFlagsRequest_Filter{
			Region:       rejectHouseholdReply.GetHousehold().GetLocation().GetRegion(),
			CreatedAfter: rejectHouseholdReply.GetHousehold().GetAudit().GetCreatedAt(),
			OwnerId:      rejectHouseholdReply.GetResult().GetCorrelationId(),
			Labels:       rejectHouseholdReply.GetResult().GetDetails(),
		},
	}
	listFeatureFlagsReply, err := run(s, ctx, listFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/ListFeatureFlags")
	}
	countPartnerIntegrationTotalsRequest := &partnerapiv1.CountPartnerIntegrationTotalsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.CountPartnerIntegrationTotalsRequest_Filter{
			Region:       rejectHouseholdReply.GetHousehold().GetLocation().GetRegion(),
			CreatedAfter: testLandingPageConnectionReply.GetLandingPage().GetAudit().GetUpdatedAt(),
			OwnerId:      rejectHouseholdReply.GetOutcome().GetReferenceId(),
			Labels:       listFeatureFlagsReply.GetResult().GetDetails(),
		},
	}
	type countPartnerIntegrationTotalsResult struct {
		reply *partnerapiv1.CountPartnerIntegrationTotalsResponse
		err   error
	}
	countPartnerIntegrationTotalsResults := make(chan countPartnerIntegrationTotalsResult, 1)
	go func() {
		reply, err := s.partnerIntegrationReportingClient.CountPartnerIntegrationTotals(ctx, countPartnerIntegrationTotalsRequest)
		countPartnerIntegrationTotalsResults <- countPartnerIntegrationTotalsResult{reply, err}
	}()
	var countPartnerIntegrationTotalsReply *partnerapiv1.CountPartnerIntegrationTotalsResponse
	select {
	case result := <-countPartnerIntegrationTotalsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerIntegrationReportingService/CountPartnerIntegrationTotals")
		}
		countPartnerIntegrationTotalsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &storefrontv1.UnsubscribeLandingPageUpdatesResponse{
		Outcome: &storefrontv1.UnsubscribeLandingPageUpdatesResponse_Outcome{
			Message: rejectHouseholdReply.GetResult().GetMessage(),
			Status:  listFeatureFlagsReply.GetResult().GetStatus(),
		},
		LandingPage: &storefrontv1.LandingPage{
			Audit: &commonv1.AuditInfo{
				CreatedAt: rejectHouseholdReply.GetHousehold().GetAudit().GetCreatedAt(),
			},
			LandingPageId: req.GetLandingPageId(),
			Counterparty:  &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{
			Status:       listFeatureFlagsReply.GetResult().GetStatus(),
			ReferenceIds: countPartnerIntegrationTotalsReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// NotifyLandingPageWatchers notifys a landing page.
func (s *LandingPageNotificationServer) NotifyLandingPageWatchers(ctx context.Context, req *storefrontv1.NotifyLandingPageWatchersRequest) (*storefrontv1.NotifyLandingPageWatchersResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "notify_landing_page_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify landing page watchers", "tenant", req.GetContext().GetTenantId())
	advancePartnerWebhookWorkflowRequest := buildAdvancePartnerWebhookWorkflowRequestForNotifyLandingPageWatchers(req)
	advancePartnerWebhookWorkflowReply, err := s.partnerWebhookWorkflowClient.AdvancePartnerWebhookWorkflow(ctx, advancePartnerWebhookWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/AdvancePartnerWebhookWorkflow")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := landingPageNotificationServerListStorefrontSessionSourcesSteps[mode]
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listStorefrontSessionSourcesRequest := &storefrontv1.ListStorefrontSessionSourcesRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.ListStorefrontSessionSourcesRequest_Filter{
			CreatedAfter: advancePartnerWebhookWorkflowReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      advancePartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetPartnerIntegrationId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listStorefrontSessionSourcesReply, err := run(s, ctx, listStorefrontSessionSourcesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/ListStorefrontSessionSources")
	}
	storefrontSessionIntegration, err := registry.Lookup[storefrontv1.StorefrontSessionIntegrationServiceClient](s.clients, "acme.storefront.v1.StorefrontSessionIntegrationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	disconnectStorefrontSessionSourceRequest := &storefrontv1.DisconnectStorefrontSessionSourceRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details: &storefrontv1.DisconnectStorefrontSessionSourceRequest_Details{
			MessageId: advancePartnerWebhookWorkflowReply.GetResult().GetMessage(),
		},
	}
	disconnectStorefrontSessionSourceReply, err := storefrontSessionIntegration.DisconnectStorefrontSessionSource(ctx, disconnectStorefrontSessionSourceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/DisconnectStorefrontSessionSource")
	}
	return &storefrontv1.NotifyLandingPageWatchersResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: advancePartnerWebhookWorkflowReply.GetResult().GetReferenceIds(),
			Status:       listStorefrontSessionSourcesReply.GetResult().GetStatus(),
		},
		Outcome: &storefrontv1.NotifyLandingPageWatchersResponse_Outcome{
			Status: advancePartnerWebhookWorkflowReply.GetOutcome().GetStatus(),
		},
		LandingPage: &storefrontv1.LandingPage{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: disconnectStorefrontSessionSourceReply.GetStorefrontSession().GetAudit().GetUpdatedAt(),
			},
			LandingPageId: req.GetLandingPageId(),
			Counterparty:  &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// ListLandingPageSubscriptions lists landing pages a page at a time.
func (s *LandingPageNotificationServer) ListLandingPageSubscriptions(ctx context.Context, req *storefrontv1.ListLandingPageSubscriptionsRequest) (*storefrontv1.ListLandingPageSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListLandingPageSubscriptions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list landing page subscriptions", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.PageRequest
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	findAuditEventsRequest := &auditv1.FindAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.FindAuditEventsRequest_Filter{
			OwnerId: req.GetFilter().GetOwnerId(),
			Labels:  labels,
		},
		Page: &commonv1.PageRequest{
			PageSize:  (&decoded).GetPageSize(),
			PageToken: req.GetPage().GetPageToken(),
		},
	}
	findAuditEventsReply, err := findAuditEventsCall(ctx, findAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/FindAuditEvents")
	}
	labels2 := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key2, value2 := range req.GetFilter().GetLabels() {
		labels2[key2] = value2
	}
	schedulePreferenceExpiryRequest := &customersv1.SchedulePreferenceExpiryRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &customersv1.SchedulePreferenceExpiryRequest_Details{Parameters: labels2},
	}
	schedulePreferenceExpiryReply, err := schedulePreferenceExpiryCall(ctx, schedulePreferenceExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceLifecycleService/SchedulePreferenceExpiry")
	}
	return &storefrontv1.ListLandingPageSubscriptionsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: findAuditEventsReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{
			Message: schedulePreferenceExpiryReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		LandingPages: []*storefrontv1.LandingPage{&storefrontv1.LandingPage{
			Lines: []*storefrontv1.LandingPageLine{&storefrontv1.LandingPageLine{
				Amount: schedulePreferenceExpiryReply.GetPreference().GetDiscount().GetAmount(),
			}},
		}},
	}, nil
}

// SendLandingPageTestNotice sends a landing page.
func (s *LandingPageNotificationServer) SendLandingPageTestNotice(ctx context.Context, req *storefrontv1.SendLandingPageTestNoticeRequest) (*storefrontv1.SendLandingPageTestNoticeResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "send_landing_page_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send landing page test notice", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryPartnerWebhookWorkflowRequest := &partnerapiv1.RetryPartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.RetryPartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetRecipient().GetEmail(),
				Name:             req.GetDetails().GetRecipient().GetName(),
				Phone:            req.GetDetails().GetRecipient().GetPhone(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
		PartnerWebhook:   &partnerapiv1.PartnerWebhook{Detail: packed},
		PartnerWebhookId: envelope.GetDisplayName(),
	}
	retryPartnerWebhookWorkflowReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*partnerapiv1.RetryPartnerWebhookWorkflowResponse, error) {
		return s.partnerWebhookWorkflowClient.RetryPartnerWebhookWorkflow(ctx, retryPartnerWebhookWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/RetryPartnerWebhookWorkflow")
	}
	submitPartnerOrderRequest := &partnerapiv1.SubmitPartnerOrderRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SubmitPartnerOrderRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{
				DisplayName: retryPartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
			Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{
				Attributes: retryPartnerWebhookWorkflowReply.GetPartnerWebhook().GetAttributes(),
			}},
		},
	}
	submitPartnerOrderReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*partnerapiv1.SubmitPartnerOrderResponse, error) {
		return s.partnerOrderApprovalClient.SubmitPartnerOrder(ctx, submitPartnerOrderRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderApprovalService/SubmitPartnerOrder")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scheduleLandingPageReportRequest := &storefrontv1.ScheduleLandingPageReportRequest{
		Context:       req.GetContext(),
		Annotations:   req.GetAnnotations(),
		LandingPageId: req.GetLandingPageId(),
		Details:       &storefrontv1.ScheduleLandingPageReportRequest_Details{Format: envelope2.GetDisplayName()},
	}
	scheduleLandingPageReportReply, err := s.scheduleLandingPageReportFn(ctx, scheduleLandingPageReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/ScheduleLandingPageReport")
	}
	return &storefrontv1.SendLandingPageTestNoticeResponse{
		Outcome: &storefrontv1.SendLandingPageTestNoticeResponse_Outcome{
			Message: retryPartnerWebhookWorkflowReply.GetResult().GetMessage(),
			Status:  scheduleLandingPageReportReply.GetResult().GetStatus(),
		},
		LandingPage: &storefrontv1.LandingPage{
			Audit: &commonv1.AuditInfo{
				CreatedBy: submitPartnerOrderReply.GetPartnerOrder().GetAudit().GetCreatedBy(),
			},
			Notes:         submitPartnerOrderReply.GetPartnerOrder().GetNotes(),
			LandingPageId: req.GetLandingPageId(),
			Counterparty:  &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{
			Details: scheduleLandingPageReportReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// GetLandingPagePreferences returns a landing page.
func (s *LandingPageNotificationServer) GetLandingPagePreferences(ctx context.Context, req *storefrontv1.GetLandingPagePreferencesRequest) (*storefrontv1.GetLandingPagePreferencesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "get_landing_page_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get landing page preferences", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	ingestShopperViewEventsStream, err := s.shopperViewIntegrationClient.IngestShopperViewEvents(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/IngestShopperViewEvents")
	}
	if err := ingestShopperViewEventsStream.Send(&storefrontv1.IngestShopperViewEventsRequest{
		Context: req.GetContext(),
		Chunk: &storefrontv1.IngestShopperViewEventsRequest_Record{Record: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				Email:  tenancy.Value(ctx, "email"),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit:      &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Components: map[string]*storefrontv1.ShopperViewComponent{"primary": &storefrontv1.ShopperViewComponent{ComponentId: req.GetLandingPageId()}},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/IngestShopperViewEvents")
	}
	if _, err := ingestShopperViewEventsStream.CloseAndRecv(); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/IngestShopperViewEvents")
	}
	storefrontSessionWorkflow, err := registry.Lookup[storefrontv1.StorefrontSessionWorkflowServiceClient](s.clients, "acme.storefront.v1.StorefrontSessionWorkflowService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	resumeStorefrontSessionWorkflowRequest := &storefrontv1.ResumeStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Assignee:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit:     &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	}
	resumeStorefrontSessionWorkflowReply, err := storefrontSessionWorkflow.ResumeStorefrontSessionWorkflow(ctx, resumeStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/ResumeStorefrontSessionWorkflow")
	}
	return &storefrontv1.GetLandingPagePreferencesResponse{
		Result: &commonv1.OperationResult{
			Details: resumeStorefrontSessionWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		LandingPage: &storefrontv1.LandingPage{LandingPageId: req.GetLandingPageId()},
	}, nil
}

// StreamLandingPageNotices streams the changes of landing pages as they happen.
func (s *LandingPageNotificationServer) StreamLandingPageNotices(req *storefrontv1.StreamLandingPageNoticesRequest, stream grpc.ServerStreamingServer[storefrontv1.StreamLandingPageNoticesResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream landing page notices", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded storefrontv1.StreamLandingPageNoticesRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	snapshot, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode snapshot")
	}
	pushPreferenceRequest := &customersv1.PushPreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{NotifyEmail: req.GetSubscriber().GetEmail()}},
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: (&decoded).GetCreatedAfter()},
		},
		Details: &customersv1.PushPreferenceRequest_Details{Payload: snapshot},
	}
	pushPreferenceReply, err := pushPreferenceCall(ctx, pushPreferenceRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.PreferenceSyncService/PushPreference")
	}
	pushSynonymRequest := &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Action: &searchv1.Synonym_Discount{Discount: &searchv1.SynonymDiscountAction{
				Percent: pushPreferenceReply.GetPreference().GetDiscount().GetPercent(),
			}},
		},
	}
	pushSynonymReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*searchv1.PushSynonymResponse, error) {
		return s.synonymSyncClient.PushSynonym(ctx, pushSynonymRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	resumeShopperViewWorkflowRequest := buildResumeShopperViewWorkflowRequestForStreamLandingPageNotices(req, pushPreferenceReply, pushSynonymReply)
	resumeShopperViewWorkflowReply, err := s.shopperViewWorkflowClient.ResumeShopperViewWorkflow(ctx, resumeShopperViewWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/ResumeShopperViewWorkflow")
	}
	if err := stream.Send(&storefrontv1.StreamLandingPageNoticesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: pushPreferenceReply.GetResult().GetCorrelationId(),
			Status:        pushPreferenceReply.GetResult().GetStatus(),
		},
		LandingPage: &storefrontv1.LandingPage{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: pushSynonymReply.GetSynonym().GetOwner().GetDisplayName(),
				Email:       req.GetSubscriber().GetEmail(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Lines:  resumeShopperViewWorkflowReply.GetShopperView().GetAddress().GetLines(),
				Region: req.GetFilter().GetRegion(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: resumeShopperViewWorkflowReply.GetShopperView().GetAudit().GetCreatedBy(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// landingPageNotificationServerUnsubscribeLandingPageUpdatesTestLandingPageConnectionStep carries the request of one
// step of UnsubscribeLandingPageUpdates and, once it ran, its reply.
type landingPageNotificationServerUnsubscribeLandingPageUpdatesTestLandingPageConnectionStep struct {
	request *storefrontv1.TestLandingPageConnectionRequest
	reply   *storefrontv1.TestLandingPageConnectionResponse
}

func (s *LandingPageNotificationServer) runUnsubscribeLandingPageUpdatesTestLandingPageConnection(ctx context.Context, step *landingPageNotificationServerUnsubscribeLandingPageUpdatesTestLandingPageConnectionStep) error {
	reply, err := s.landingPageIntegrationClient.TestLandingPageConnection(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// landingPageNotificationServerListFeatureFlagsSteps are the ways to run
// acme.featureflags.v1.FeatureFlagService/ListFeatureFlags, by name.
var landingPageNotificationServerListFeatureFlagsSteps = map[string]func(*LandingPageNotificationServer, context.Context, *featureflagsv1.ListFeatureFlagsRequest) (*featureflagsv1.ListFeatureFlagsResponse, error){
	"default": (*LandingPageNotificationServer).listFeatureFlagsStep,
	"audited": (*LandingPageNotificationServer).listFeatureFlagsAuditedStep,
}

func (s *LandingPageNotificationServer) listFeatureFlagsStep(ctx context.Context, request *featureflagsv1.ListFeatureFlagsRequest) (*featureflagsv1.ListFeatureFlagsResponse, error) {
	return s.featureFlagClient.ListFeatureFlags(ctx, request)
}

func (s *LandingPageNotificationServer) listFeatureFlagsAuditedStep(ctx context.Context, request *featureflagsv1.ListFeatureFlagsRequest) (*featureflagsv1.ListFeatureFlagsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.featureflags.v1.FeatureFlagService/ListFeatureFlags")
	return s.featureFlagClient.ListFeatureFlags(ctx, request)
}

// landingPageNotificationServerListStorefrontSessionSourcesSteps are the ways to run
// acme.storefront.v1.StorefrontSessionIntegrationService/ListStorefrontSessionSources, by name.
var landingPageNotificationServerListStorefrontSessionSourcesSteps = map[string]func(*LandingPageNotificationServer, context.Context, *storefrontv1.ListStorefrontSessionSourcesRequest) (*storefrontv1.ListStorefrontSessionSourcesResponse, error){
	"default": (*LandingPageNotificationServer).listStorefrontSessionSourcesStep,
	"audited": (*LandingPageNotificationServer).listStorefrontSessionSourcesAuditedStep,
}

func (s *LandingPageNotificationServer) listStorefrontSessionSourcesStep(ctx context.Context, request *storefrontv1.ListStorefrontSessionSourcesRequest) (*storefrontv1.ListStorefrontSessionSourcesResponse, error) {
	return s.storefrontSessionIntegrationClient.ListStorefrontSessionSources(ctx, request)
}

func (s *LandingPageNotificationServer) listStorefrontSessionSourcesAuditedStep(ctx context.Context, request *storefrontv1.ListStorefrontSessionSourcesRequest) (*storefrontv1.ListStorefrontSessionSourcesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.storefront.v1.StorefrontSessionIntegrationService/ListStorefrontSessionSources")
	return s.storefrontSessionIntegrationClient.ListStorefrontSessionSources(ctx, request)
}
