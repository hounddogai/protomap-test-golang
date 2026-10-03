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
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	patientappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patientapp/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
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

// StorefrontSessionNotificationServer implements `acme.storefront.v1.StorefrontSessionNotificationService`.
// Tells the people who follow a storefront session what changed.
type StorefrontSessionNotificationServer struct {
	*storefrontSessionNotificationHandlers
}

// storefrontSessionNotificationHandlers holds the handlers of StorefrontSessionNotificationServer, with what they use.
type storefrontSessionNotificationHandlers struct {
	storefrontv1.UnimplementedStorefrontSessionNotificationServiceServer
	store                             *Store
	log                               *slog.Logger
	partnerWebhookValidationClient    partnerapiv1.PartnerWebhookValidationServiceClient
	shopperViewIntegrationClient      storefrontv1.ShopperViewIntegrationServiceClient
	partnerWebhookValidationPort      partnerWebhookValidationPort
	auditEventClient                  auditv1.AuditEventServiceClient
	preferenceReportingClient         customersv1.PreferenceReportingServiceClient
	partnerOrderClient                partnerapiv1.PartnerOrderServiceClient
	sessionQueryPort                  sessionQueryPort
	landingPageWorkflowClient         storefrontv1.LandingPageWorkflowServiceClient
	meteredPartnerWebhookNotification meteredPartnerWebhookNotification
	patientHomeClient                 patientappv1.PatientHomeServiceClient
	storefrontSessionWorkflowPort     storefrontSessionWorkflowPort
	landingPageReportingClient        storefrontv1.LandingPageReportingServiceClient
	clients                           *registry.Registry
	conn                              *grpc.ClientConn
}

// NewStorefrontSessionNotificationServer wires a StorefrontSessionNotificationServer.
func NewStorefrontSessionNotificationServer(deps platform.Deps) *StorefrontSessionNotificationServer {
	return &StorefrontSessionNotificationServer{storefrontSessionNotificationHandlers: &storefrontSessionNotificationHandlers{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.storefront.v1.StorefrontSessionNotificationService"),
		partnerWebhookValidationClient:    partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		shopperViewIntegrationClient:      storefrontv1.NewShopperViewIntegrationServiceClient(deps.Conn),
		partnerWebhookValidationPort:      newPartnerWebhookValidationPort(deps),
		auditEventClient:                  auditv1.NewAuditEventServiceClient(deps.Conn),
		preferenceReportingClient:         customersv1.NewPreferenceReportingServiceClient(deps.Conn),
		partnerOrderClient:                partnerapiv1.NewPartnerOrderServiceClient(deps.Conn),
		sessionQueryPort:                  newSessionQueryPort(deps),
		landingPageWorkflowClient:         storefrontv1.NewLandingPageWorkflowServiceClient(deps.Conn),
		meteredPartnerWebhookNotification: meteredPartnerWebhookNotification{PartnerWebhookNotificationServiceClient: partnerapiv1.NewPartnerWebhookNotificationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		patientHomeClient:                 patientappv1.NewPatientHomeServiceClient(deps.Conn),
		storefrontSessionWorkflowPort:     newStorefrontSessionWorkflowPort(deps),
		landingPageReportingClient:        storefrontv1.NewLandingPageReportingServiceClient(deps.Conn),
		clients:                           deps.Clients,
		conn:                              deps.Conn,
	}}
}

// SubscribeStorefrontSessionUpdates subscribes a storefront session.
func (s *storefrontSessionNotificationHandlers) SubscribeStorefrontSessionUpdates(ctx context.Context, req *storefrontv1.SubscribeStorefrontSessionUpdatesRequest) (*storefrontv1.SubscribeStorefrontSessionUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "subscribe_storefront_session_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe storefront session updates", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *storefrontv1.SubscribeStorefrontSessionUpdatesRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	advanceLandingPageWorkflowRequest := &storefrontv1.AdvanceLandingPageWorkflowRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.AdvanceLandingPageWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetRecipient().GetName(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
		LandingPage: &storefrontv1.LandingPage{
			Counterparty: &customersv1.CustomerRef{Email: cachedDetails.GetRecipient().GetEmail()},
		},
		LandingPageId: envelope.GetDisplayName(),
	}
	advanceLandingPageWorkflowReply, err := advanceLandingPageWorkflowCall(ctx, advanceLandingPageWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageWorkflowService/AdvanceLandingPageWorkflow")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainPartnerWebhookDecisionReply, err := s.partnerWebhookValidationClient.ExplainPartnerWebhookDecision(ctx, &partnerapiv1.ExplainPartnerWebhookDecisionRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ExplainPartnerWebhookDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetRecipient().GetName()},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ExplainPartnerWebhookDecision")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	relayShopperViewMessagesStream, err := s.shopperViewIntegrationClient.RelayShopperViewMessages(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/RelayShopperViewMessages")
	}
	if err := relayShopperViewMessagesStream.Send(&storefrontv1.RelayShopperViewMessagesRequest{
		Context: req.GetContext(),
		Frame: &storefrontv1.RelayShopperViewMessagesRequest_Change{Change: &storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: advanceLandingPageWorkflowReply.GetLandingPage().GetAudit().GetUpdatedAt(),
			},
			ShopperViewId: envelope2.GetDisplayName(),
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/RelayShopperViewMessages")
	}
	if err := relayShopperViewMessagesStream.CloseSend(); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/RelayShopperViewMessages")
	}
	acknowledged, err := relayShopperViewMessagesStream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/RelayShopperViewMessages")
	}
	_ = acknowledged
	return &storefrontv1.SubscribeStorefrontSessionUpdatesResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: advanceLandingPageWorkflowReply.GetLandingPage().GetAudit().GetUpdatedBy(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
		Outcome: &storefrontv1.SubscribeStorefrontSessionUpdatesResponse_Outcome{
			Message: explainPartnerWebhookDecisionReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnsubscribeStorefrontSessionUpdates unsubscribes a storefront session.
func (s *storefrontSessionNotificationHandlers) UnsubscribeStorefrontSessionUpdates(ctx context.Context, req *storefrontv1.UnsubscribeStorefrontSessionUpdatesRequest) (*storefrontv1.UnsubscribeStorefrontSessionUpdatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "unsubscribe_storefront_session_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe storefront session updates", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded storefrontv1.UnsubscribeStorefrontSessionUpdatesRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewPartnerWebhookRequest := &partnerapiv1.PreviewPartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.PreviewPartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: (&decoded).GetRecipient().GetEmail(),
				Name:  req.GetDetails().GetRecipient().GetName(),
			},
		},
		PartnerWebhook:   &partnerapiv1.PartnerWebhook{Detail: packed},
		PartnerWebhookId: envelope.GetDisplayName(),
	}
	previewPartnerWebhookReply, err := s.partnerWebhookValidationPort.PreviewPartnerWebhook(ctx, previewPartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/PreviewPartnerWebhook")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := storefrontSessionNotificationHandlersSearchAuditEventsSteps[mode]
	searchAuditEventsRequest := &auditv1.SearchAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.SearchAuditEventsRequest_Filter{
			CreatedAfter: previewPartnerWebhookReply.GetOutcome().GetEffectiveAt(),
			Labels:       previewPartnerWebhookReply.GetResult().GetDetails(),
			OwnerId:      previewPartnerWebhookReply.GetPartnerWebhook().GetPartnerWebhookId(),
		},
	}
	searchAuditEventsReply, err := run(s, ctx, searchAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/SearchAuditEvents")
	}
	return &storefrontv1.UnsubscribeStorefrontSessionUpdatesResponse{
		Outcome: &storefrontv1.UnsubscribeStorefrontSessionUpdatesResponse_Outcome{
			Status:  previewPartnerWebhookReply.GetResult().GetStatus(),
			Message: previewPartnerWebhookReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Details: searchAuditEventsReply.GetResult().GetDetails(),
			Status:  searchAuditEventsReply.GetResult().GetStatus(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
		},
	}, nil
}

// NotifyStorefrontSessionWatchers notifys a storefront session.
func (s *storefrontSessionNotificationHandlers) NotifyStorefrontSessionWatchers(ctx context.Context, req *storefrontv1.NotifyStorefrontSessionWatchersRequest) (*storefrontv1.NotifyStorefrontSessionWatchersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "notify_storefront_session_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify storefront session watchers", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *storefrontv1.NotifyStorefrontSessionWatchersRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	replayShopperViewEventsReply, err := s.shopperViewIntegrationClient.ReplayShopperViewEvents(ctx, &storefrontv1.ReplayShopperViewEventsRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details: &storefrontv1.ReplayShopperViewEventsRequest_Details{
			MessageId: cachedDetails.GetTemplateId(),
			Envelope:  packed,
		},
		ShopperViewId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/ReplayShopperViewEvents")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := storefrontSessionNotificationHandlersSchedulePreferenceReportSteps[mode]
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	schedulePreferenceReportRequest := &customersv1.SchedulePreferenceReportRequest{
		Context:      req.GetContext(),
		Annotations:  annotations,
		Reason:       req.GetReason(),
		PreferenceId: envelope2.GetDisplayName(),
	}
	schedulePreferenceReportReply, err := run(s, ctx, schedulePreferenceReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceReportingService/SchedulePreferenceReport")
	}
	batchGetPartnerOrdersRequest := &partnerapiv1.BatchGetPartnerOrdersRequest{Context: req.GetContext()}
	type batchGetPartnerOrdersResult struct {
		reply *partnerapiv1.BatchGetPartnerOrdersResponse
		err   error
	}
	batchGetPartnerOrdersResults := make(chan batchGetPartnerOrdersResult, 1)
	go func() {
		reply, err := s.partnerOrderClient.BatchGetPartnerOrders(ctx, batchGetPartnerOrdersRequest)
		batchGetPartnerOrdersResults <- batchGetPartnerOrdersResult{reply, err}
	}()
	var batchGetPartnerOrdersReply *partnerapiv1.BatchGetPartnerOrdersResponse
	select {
	case result := <-batchGetPartnerOrdersResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerOrderService/BatchGetPartnerOrders")
		}
		batchGetPartnerOrdersReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	var envelope3 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countSessionsRequest := &identityv1.CountSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.CountSessionsRequest_Filter{
			Region:       replayShopperViewEventsReply.GetShopperView().GetAddress().GetRegion(),
			CreatedAfter: replayShopperViewEventsReply.GetShopperView().GetAudit().GetUpdatedAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope3.GetDisplayName()},
	}
	countSessionsReply, err := s.sessionQueryPort.CountSessions(ctx, countSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/CountSessions")
	}
	_ = batchGetPartnerOrdersReply
	return &storefrontv1.NotifyStorefrontSessionWatchersResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				Email:            replayShopperViewEventsReply.GetShopperView().GetOwner().GetEmail(),
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  schedulePreferenceReportReply.GetResult().GetReferenceIds(),
			CorrelationId: countSessionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &storefrontv1.NotifyStorefrontSessionWatchersResponse_Outcome{
			Status:  schedulePreferenceReportReply.GetResult().GetStatus(),
			Message: countSessionsReply.GetResult().GetMessage(),
		},
	}, nil
}

// ListStorefrontSessionSubscriptions lists storefront sessions a page at a time.
func (s *storefrontSessionNotificationHandlers) ListStorefrontSessionSubscriptions(ctx context.Context, req *storefrontv1.ListStorefrontSessionSubscriptionsRequest) (*storefrontv1.ListStorefrontSessionSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list storefront session subscriptions", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	signalLandingPageWorkflowRequest := &storefrontv1.SignalLandingPageWorkflowRequest{
		Context: req.GetContext(),
		LandingPage: &storefrontv1.LandingPage{
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			Audit:          &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	signalLandingPageWorkflowReply, err := storefrontv1.LandingPageWorkflowServiceClient.SignalLandingPageWorkflow(s.landingPageWorkflowClient, ctx, signalLandingPageWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageWorkflowService/SignalLandingPageWorkflow")
	}
	unsubscribePartnerWebhookUpdatesRequest := &partnerapiv1.UnsubscribePartnerWebhookUpdatesRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.UnsubscribePartnerWebhookUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: signalLandingPageWorkflowReply.GetLandingPage().GetCounterparty().GetEmail(),
			},
		},
		Annotations: req.GetFilter().GetLabels(),
	}
	unsubscribePartnerWebhookUpdatesReply, err := s.meteredPartnerWebhookNotification.UnsubscribePartnerWebhookUpdates(ctx, unsubscribePartnerWebhookUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookNotificationService/UnsubscribePartnerWebhookUpdates")
	}
	return &storefrontv1.ListStorefrontSessionSubscriptionsResponse{
		StorefrontSessions: []*storefrontv1.StorefrontSession{&storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: signalLandingPageWorkflowReply.GetLandingPage().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{
			ReferenceIds: unsubscribePartnerWebhookUpdatesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// SendStorefrontSessionTestNotice sends a storefront session.
func (s *storefrontSessionNotificationHandlers) SendStorefrontSessionTestNotice(ctx context.Context, req *storefrontv1.SendStorefrontSessionTestNoticeRequest) (*storefrontv1.SendStorefrontSessionTestNoticeResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "send_storefront_session_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send storefront session test notice", "tenant", req.GetContext().GetTenantId())
	variables := make(map[string]string, len(req.GetDetails().GetVariables()))
	for key, value := range req.GetDetails().GetVariables() {
		variables[key] = value
	}
	cloned := proto.Clone(req.GetDetails()).(*storefrontv1.SendStorefrontSessionTestNoticeRequest_Details)
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPatientHomesRequest := newListPatientHomesRequest(withListPatientHomesContext(req.GetContext()), withListPatientHomesFilter(&patientappv1.ListPatientHomesRequest_Filter{
		Labels:       variables,
		CreatedAfter: req.GetContext().GetDeadline(),
		OwnerId:      cloned.GetTemplateId(),
	}), withListPatientHomesPage(&commonv1.PageRequest{PageToken: envelope.GetDisplayName()}))
	listPatientHomesReply, err := s.patientHomeClient.ListPatientHomes(ctx, listPatientHomesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.patientapp.v1.PatientHomeService/ListPatientHomes")
	}
	cancelStorefrontSessionWorkflowRequest := &storefrontv1.CancelStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.CancelStorefrontSessionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
				Name:             req.GetDetails().GetRecipient().GetName(),
			},
		},
	}
	cancelStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowPort.CancelStorefrontSessionWorkflow(ctx, cancelStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/CancelStorefrontSessionWorkflow")
	}
	pauseLandingPageWorkflowRequest := buildPauseLandingPageWorkflowRequestForSendStorefrontSessionTestNotice(req, cancelStorefrontSessionWorkflowReply)
	pauseLandingPageWorkflowReply, err := s.landingPageWorkflowClient.PauseLandingPageWorkflow(ctx, pauseLandingPageWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageWorkflowService/PauseLandingPageWorkflow")
	}
	return &storefrontv1.SendStorefrontSessionTestNoticeResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPatientHomesReply.GetResult().GetCorrelationId(),
			Status:        cancelStorefrontSessionWorkflowReply.GetOutcome().GetStatus(),
		},
		Outcome: &storefrontv1.SendStorefrontSessionTestNoticeResponse_Outcome{
			Status:  listPatientHomesReply.GetResult().GetStatus(),
			Message: pauseLandingPageWorkflowReply.GetOutcome().GetMessage(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Requester:           &commonv1.ContactInfo{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// GetStorefrontSessionPreferences returns a storefront session.
func (s *storefrontSessionNotificationHandlers) GetStorefrontSessionPreferences(ctx context.Context, req *storefrontv1.GetStorefrontSessionPreferencesRequest) (*storefrontv1.GetStorefrontSessionPreferencesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "get_storefront_session_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get storefront session preferences", "tenant", req.GetContext().GetTenantId())
	step := &storefrontSessionNotificationHandlersGetStorefrontSessionPreferencesCountLandingPageTotalsStep{request: &storefrontv1.CountLandingPageTotalsRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.CountLandingPageTotalsRequest_Filter{
			OwnerId:      req.GetStorefrontSessionId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}}
	if err := s.runGetStorefrontSessionPreferencesCountLandingPageTotals(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/CountLandingPageTotals")
	}
	countLandingPageTotalsReply := step.reply
	listPreferenceReportsReply, err := s.preferenceReportingClient.ListPreferenceReports(ctx, &customersv1.ListPreferenceReportsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListPreferenceReportsRequest_Filter{
			Labels: countLandingPageTotalsReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceReportingService/ListPreferenceReports")
	}
	_ = listPreferenceReportsReply
	return &storefrontv1.GetStorefrontSessionPreferencesResponse{
		Result:            &commonv1.OperationResult{Status: countLandingPageTotalsReply.GetResult().GetStatus()},
		StorefrontSession: &storefrontv1.StorefrontSession{StorefrontSessionId: req.GetStorefrontSessionId()},
	}, nil
}

// StreamStorefrontSessionNotices streams the changes of storefront sessions as they happen.
func (s *storefrontSessionNotificationHandlers) StreamStorefrontSessionNotices(req *storefrontv1.StreamStorefrontSessionNoticesRequest, stream grpc.ServerStreamingServer[storefrontv1.StreamStorefrontSessionNoticesResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "stream storefront session notices", "tenant", req.GetContext().GetTenantId())
	userHistory, err := registry.Lookup[identityv1.UserHistoryServiceClient](s.clients, "acme.identity.v1.UserHistoryService")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	pruneUserVersionsRequest := &identityv1.PruneUserVersionsRequest{
		Context: req.GetContext(),
		UserId:  userId,
	}
	pruneUserVersionsReply, err := userHistory.PruneUserVersions(ctx, pruneUserVersionsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/PruneUserVersions")
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	scheduleLandingPageReportReply, err := s.landingPageReportingClient.ScheduleLandingPageReport(ctx, &storefrontv1.ScheduleLandingPageReportRequest{
		Context:     req.GetContext(),
		Details:     &storefrontv1.ScheduleLandingPageReportRequest_Details{Filters: pruneUserVersionsReply.GetResult().GetDetails()},
		Annotations: labels,
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/ScheduleLandingPageReport")
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	replayShopperViewEventsRequest := &storefrontv1.ReplayShopperViewEventsRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.ReplayShopperViewEventsRequest_Details{
			ReceivedAt: req.GetFilter().GetCreatedAfter(),
			MessageId:  scheduleLandingPageReportReply.GetResult().GetMessage(),
			Envelope:   packed,
		},
	}
	type replayShopperViewEventsResult struct {
		reply *storefrontv1.ReplayShopperViewEventsResponse
		err   error
	}
	replayShopperViewEventsResults := make(chan replayShopperViewEventsResult, 1)
	go func() {
		reply, err := s.shopperViewIntegrationClient.ReplayShopperViewEvents(ctx, replayShopperViewEventsRequest)
		replayShopperViewEventsResults <- replayShopperViewEventsResult{reply, err}
	}()
	var replayShopperViewEventsReply *storefrontv1.ReplayShopperViewEventsResponse
	select {
	case result := <-replayShopperViewEventsResults:
		if result.err != nil {
			return grpcx.Wrap(result.err, "acme.storefront.v1.ShopperViewIntegrationService/ReplayShopperViewEvents")
		}
		replayShopperViewEventsReply = result.reply
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
	resumeLandingPageWorkflowRequest := &storefrontv1.ResumeLandingPageWorkflowRequest{
		Context: req.GetContext(),
		LandingPage: &storefrontv1.LandingPage{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: replayShopperViewEventsReply.GetShopperView().GetAddress().GetPostalCode(),
			},
		},
	}
	resumeLandingPageWorkflowReply := new(storefrontv1.ResumeLandingPageWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", storefrontLandingPageWorkflowServiceName, "ResumeLandingPageWorkflow"), resumeLandingPageWorkflowRequest, resumeLandingPageWorkflowReply); err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.LandingPageWorkflowService/ResumeLandingPageWorkflow")
	}
	if err := stream.Send(&storefrontv1.StreamStorefrontSessionNoticesResponse{
		Result: &commonv1.OperationResult{
			Status:        pruneUserVersionsReply.GetResult().GetStatus(),
			Details:       scheduleLandingPageReportReply.GetResult().GetDetails(),
			ReferenceIds:  scheduleLandingPageReportReply.GetResult().GetReferenceIds(),
			CorrelationId: replayShopperViewEventsReply.GetResult().GetCorrelationId(),
			Message:       replayShopperViewEventsReply.GetResult().GetMessage(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				Email: resumeLandingPageWorkflowReply.GetLandingPage().GetCounterparty().GetEmail(),
				Phone: req.GetSubscriber().GetPhone(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: resumeLandingPageWorkflowReply.GetLandingPage().GetAudit().GetUpdatedBy(),
			},
			Assignee: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// storefrontSessionNotificationHandlersSearchAuditEventsSteps are the ways to run
// acme.audit.v1.AuditEventService/SearchAuditEvents, by name.
var storefrontSessionNotificationHandlersSearchAuditEventsSteps = map[string]func(*storefrontSessionNotificationHandlers, context.Context, *auditv1.SearchAuditEventsRequest) (*auditv1.SearchAuditEventsResponse, error){
	"default": (*storefrontSessionNotificationHandlers).searchAuditEventsStep,
	"audited": (*storefrontSessionNotificationHandlers).searchAuditEventsAuditedStep,
}

func (s *storefrontSessionNotificationHandlers) searchAuditEventsStep(ctx context.Context, request *auditv1.SearchAuditEventsRequest) (*auditv1.SearchAuditEventsResponse, error) {
	return s.auditEventClient.SearchAuditEvents(ctx, request)
}

func (s *storefrontSessionNotificationHandlers) searchAuditEventsAuditedStep(ctx context.Context, request *auditv1.SearchAuditEventsRequest) (*auditv1.SearchAuditEventsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.audit.v1.AuditEventService/SearchAuditEvents")
	return s.auditEventClient.SearchAuditEvents(ctx, request)
}

// storefrontSessionNotificationHandlersSchedulePreferenceReportSteps are the ways to run
// acme.customers.v1.PreferenceReportingService/SchedulePreferenceReport, by name.
var storefrontSessionNotificationHandlersSchedulePreferenceReportSteps = map[string]func(*storefrontSessionNotificationHandlers, context.Context, *customersv1.SchedulePreferenceReportRequest) (*customersv1.SchedulePreferenceReportResponse, error){
	"default": (*storefrontSessionNotificationHandlers).schedulePreferenceReportStep,
	"audited": (*storefrontSessionNotificationHandlers).schedulePreferenceReportAuditedStep,
}

func (s *storefrontSessionNotificationHandlers) schedulePreferenceReportStep(ctx context.Context, request *customersv1.SchedulePreferenceReportRequest) (*customersv1.SchedulePreferenceReportResponse, error) {
	return s.preferenceReportingClient.SchedulePreferenceReport(ctx, request)
}

func (s *storefrontSessionNotificationHandlers) schedulePreferenceReportAuditedStep(ctx context.Context, request *customersv1.SchedulePreferenceReportRequest) (*customersv1.SchedulePreferenceReportResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.PreferenceReportingService/SchedulePreferenceReport")
	return s.preferenceReportingClient.SchedulePreferenceReport(ctx, request)
}

// storefrontSessionNotificationHandlersGetStorefrontSessionPreferencesCountLandingPageTotalsStep carries the request of
// one step of GetStorefrontSessionPreferences and, once it ran, its reply.
type storefrontSessionNotificationHandlersGetStorefrontSessionPreferencesCountLandingPageTotalsStep struct {
	request *storefrontv1.CountLandingPageTotalsRequest
	reply   *storefrontv1.CountLandingPageTotalsResponse
}

func (s *storefrontSessionNotificationHandlers) runGetStorefrontSessionPreferencesCountLandingPageTotals(ctx context.Context, step *storefrontSessionNotificationHandlersGetStorefrontSessionPreferencesCountLandingPageTotalsStep) error {
	reply, err := s.landingPageReportingClient.CountLandingPageTotals(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
