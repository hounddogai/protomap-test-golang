package storefront

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	datahubv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datahub/v1"
	edgegatewayv1 "github.com/hounddogai/protomap-test-golang/gen/acme/edgegateway/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	procurementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/procurement/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
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

// StorefrontSessionServer implements `acme.storefront.v1.StorefrontSessionService`.
// Manages storefront sessions: creates, reads, changes, and lists them.
type StorefrontSessionServer struct {
	storefrontv1.UnsafeStorefrontSessionServiceServer
	store                               *Store
	log                                 *slog.Logger
	landingPageWorkflowClient           storefrontv1.LandingPageWorkflowServiceClient
	searchIndexClient                   searchv1.SearchIndexServiceClient
	consentRecordSyncClient             customersv1.ConsentRecordSyncServiceClient
	consentRecordWorkflowClient         customersv1.ConsentRecordWorkflowServiceClient
	bus                                 *eventbus.Bus
	conn                                *grpc.ClientConn
	shopperViewQueryClient              storefrontv1.ShopperViewQueryServiceClient
	partnerOrderWorkflowClient          partnerapiv1.PartnerOrderWorkflowServiceClient
	sessionHistoryClient                identityv1.SessionHistoryServiceClient
	userHistoryClient                   identityv1.UserHistoryServiceClient
	shopperViewIntegrationClient        storefrontv1.ShopperViewIntegrationServiceClient
	clients                             *registry.Registry
	workspaceClient                     datahubv1.WorkspaceServiceClient
	partnerWebhookWorkflowClient        partnerapiv1.PartnerWebhookWorkflowServiceClient
	storefrontSessionQueryClient        storefrontv1.StorefrontSessionQueryServiceClient
	meteredStorefrontSessionIntegration meteredStorefrontSessionIntegration
	routeBindingClient                  edgegatewayv1.RouteBindingServiceClient
	partnerWebhookQueryClient           partnerapiv1.PartnerWebhookQueryServiceClient
	landingPageIntegrationPort          landingPageIntegrationPort
	preferenceReportingClient           customersv1.PreferenceReportingServiceClient
}

// NewStorefrontSessionServer wires a StorefrontSessionServer.
func NewStorefrontSessionServer(deps platform.Deps) *StorefrontSessionServer {
	return &StorefrontSessionServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.storefront.v1.StorefrontSessionService"),
		landingPageWorkflowClient:           storefrontv1.NewLandingPageWorkflowServiceClient(deps.Conn),
		searchIndexClient:                   searchv1.NewSearchIndexServiceClient(deps.Conn),
		consentRecordSyncClient:             customersv1.NewConsentRecordSyncServiceClient(deps.Conn),
		consentRecordWorkflowClient:         customersv1.NewConsentRecordWorkflowServiceClient(deps.Conn),
		bus:                                 deps.Bus,
		conn:                                deps.Conn,
		shopperViewQueryClient:              storefrontv1.NewShopperViewQueryServiceClient(deps.Conn),
		partnerOrderWorkflowClient:          partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn),
		sessionHistoryClient:                identityv1.NewSessionHistoryServiceClient(deps.Conn),
		userHistoryClient:                   identityv1.NewUserHistoryServiceClient(deps.Conn),
		shopperViewIntegrationClient:        storefrontv1.NewShopperViewIntegrationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		workspaceClient:                     datahubv1.NewWorkspaceServiceClient(deps.Conn),
		partnerWebhookWorkflowClient:        partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn),
		storefrontSessionQueryClient:        storefrontv1.NewStorefrontSessionQueryServiceClient(deps.Conn),
		meteredStorefrontSessionIntegration: meteredStorefrontSessionIntegration{StorefrontSessionIntegrationServiceClient: storefrontv1.NewStorefrontSessionIntegrationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		routeBindingClient:                  edgegatewayv1.NewRouteBindingServiceClient(deps.Conn),
		partnerWebhookQueryClient:           partnerapiv1.NewPartnerWebhookQueryServiceClient(deps.Conn),
		landingPageIntegrationPort:          newLandingPageIntegrationPort(deps),
		preferenceReportingClient:           customersv1.NewPreferenceReportingServiceClient(deps.Conn),
	}
}

// CreateStorefrontSession creates a storefront session.
func (s *StorefrontSessionServer) CreateStorefrontSession(ctx context.Context, req *storefrontv1.CreateStorefrontSessionRequest) (*storefrontv1.CreateStorefrontSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CreateStorefrontSession", "error", err)
		return nil, err
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "create storefront session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	storefrontSessionCache := cache.New[string, *storefrontv1.StorefrontSession](1)
	storefrontSessionCache.Put(req.GetContext().GetTraceId(), req.GetStorefrontSession())
	cachedStorefrontSession, _ := storefrontSessionCache.Get(req.GetContext().GetTraceId())
	encoded, err := protojson.Marshal(req.GetStorefrontSession())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded storefrontv1.StorefrontSession
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	resumeLandingPageWorkflowRequest := &storefrontv1.ResumeLandingPageWorkflowRequest{
		Context: req.GetContext(),
		LandingPage: &storefrontv1.LandingPage{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetStorefrontSession().GetAudit().GetUpdatedAt(),
				CreatedAt: (&decoded).GetAudit().GetCreatedAt(),
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetStorefrontSession().GetAssignee().GetDisplayName(),
				Email:       cachedStorefrontSession.GetAssignee().GetEmail(),
			},
		},
	}
	var resumeLandingPageWorkflowReply *storefrontv1.ResumeLandingPageWorkflowResponse
	if err := pipeline.Run(ctx, "acme.storefront.v1.LandingPageWorkflowService/ResumeLandingPageWorkflow", func(ctx context.Context) error {
		answer, err := s.landingPageWorkflowClient.ResumeLandingPageWorkflow(ctx, resumeLandingPageWorkflowRequest)
		resumeLandingPageWorkflowReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageWorkflowService/ResumeLandingPageWorkflow")
	}
	createSearchIndexRequest := &searchv1.CreateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: resumeLandingPageWorkflowReply.GetLandingPage().GetAudit().GetUpdatedAt(),
			},
		},
	}
	send := s.createSearchIndexSender(createSearchIndexRequest)
	createSearchIndexReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/CreateSearchIndex")
	}
	return &storefrontv1.CreateStorefrontSessionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resumeLandingPageWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: createSearchIndexReply.GetSearchIndex().GetAudit().GetUpdatedBy(),
			},
			Assignee: req.GetStorefrontSession().GetAssignee(),
			Priority: req.GetStorefrontSession().GetPriority(),
		},
	}, nil
}

// GetStorefrontSession returns a storefront session.
func (s *StorefrontSessionServer) GetStorefrontSession(ctx context.Context, req *storefrontv1.GetStorefrontSessionRequest) (*storefrontv1.GetStorefrontSessionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "get_storefront_session"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get storefront session", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var cachedSubject string
	if entry, ok := recentStorefrontSessions.Load(recordKey{req.GetContext().GetTenantId(), req.GetStorefrontSessionId()}); ok {
		if previous, ok := entry.(*storefrontv1.StorefrontSession); ok {
			cachedSubject = previous.GetSubject()
		}
	}
	pushConsentRecordRequest := &customersv1.PushConsentRecordRequest{}
	pushConsentRecordRequest.Context = req.GetContext()
	pushConsentRecordRequest.ConsentRecord = &customersv1.ConsentRecord{
		Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: (&decoded).GetDeadline()}},
		Detail:       packed,
	}
	pushConsentRecordRequest.Details = &customersv1.PushConsentRecordRequest_Details{Payload: snapshot}
	pushConsentRecordRequest.Reason = cachedSubject
	pushConsentRecordReply, err := s.consentRecordSyncClient.PushConsentRecord(ctx, pushConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/PushConsentRecord")
	}
	startConsentRecordWorkflowReply, err := s.consentRecordWorkflowClient.StartConsentRecordWorkflow(ctx, &customersv1.StartConsentRecordWorkflowRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				Path:        pushConsentRecordReply.GetConsentRecord().GetSubject().GetPath(),
				HouseholdId: pushConsentRecordReply.GetConsentRecord().GetSubject().GetHouseholdId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordWorkflowService/StartConsentRecordWorkflow")
	}
	return &storefrontv1.GetStorefrontSessionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  pushConsentRecordReply.GetResult().GetReferenceIds(),
			CorrelationId: pushConsentRecordReply.GetConsentRecord().GetCorrelationId(),
			Details:       startConsentRecordWorkflowReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		StorefrontSession: &storefrontv1.StorefrontSession{StorefrontSessionId: req.GetStorefrontSessionId()},
	}, nil
}

// UpdateStorefrontSession changes a storefront session.
func (s *StorefrontSessionServer) UpdateStorefrontSession(ctx context.Context, req *storefrontv1.UpdateStorefrontSessionRequest) (*storefrontv1.UpdateStorefrontSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "update storefront session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	disconnectPartnerWebhookSourceRequest := &partnerapiv1.DisconnectPartnerWebhookSourceRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.DisconnectPartnerWebhookSourceRequest_Details{
			ReceivedAt: req.GetStorefrontSession().GetAudit().GetUpdatedAt(),
		},
	}
	disconnectPartnerWebhookSourceReply, err := disconnectPartnerWebhookSourceCall(ctx, disconnectPartnerWebhookSourceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookIntegrationService/DisconnectPartnerWebhookSource")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "storefront.update_storefront_session.signal_shopper_view_workflow",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.notify.phone":            req.GetStorefrontSession().GetRequester().GetPhone(),
			"shopper_view.owner.display_name": req.GetStorefrontSession().GetCustomer().GetDisplayName(),
			"context.tenant_id":               req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	cloned := proto.Clone(req.GetStorefrontSession()).(*storefrontv1.StorefrontSession)
	ctx = tenancy.With(ctx, "phone", req.GetStorefrontSession().GetRequester().GetPhone())
	subscribeLandingPageUpdatesRequest := &storefrontv1.SubscribeLandingPageUpdatesRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.SubscribeLandingPageUpdatesRequest_Details{
			Channel: req.GetStorefrontSession().GetChannel().GetChannel(),
			Recipient: &commonv1.ContactInfo{
				Name:  cloned.GetRequester().GetName(),
				Phone: tenancy.Value(ctx, "phone"),
			},
		},
	}
	subscribeLandingPageUpdatesReply := new(storefrontv1.SubscribeLandingPageUpdatesResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", storefrontLandingPageNotificationServiceName, "SubscribeLandingPageUpdates"), subscribeLandingPageUpdatesRequest, subscribeLandingPageUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageNotificationService/SubscribeLandingPageUpdates")
	}
	recentStorefrontSessions.Store(recordKey{req.GetContext().GetTenantId(), req.GetStorefrontSession().GetStorefrontSessionId()}, req.GetStorefrontSession())
	return &storefrontv1.UpdateStorefrontSessionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: disconnectPartnerWebhookSourceReply.GetPartnerWebhook().GetCorrelationId(),
			Status:        "ok",
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: subscribeLandingPageUpdatesReply.GetLandingPage().GetAudit().GetUpdatedAt(),
			},
			Subject:  req.GetStorefrontSession().GetSubject(),
			Priority: req.GetStorefrontSession().GetPriority(),
		},
	}, nil
}

// DeleteStorefrontSession deletes a storefront session.
func (s *StorefrontSessionServer) DeleteStorefrontSession(ctx context.Context, req *storefrontv1.DeleteStorefrontSessionRequest) (*storefrontv1.DeleteStorefrontSessionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "delete_storefront_session"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete storefront session", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	suggestShopperViewsRequest := &storefrontv1.SuggestShopperViewsRequest{}
	suggestShopperViewsRequest.Context = req.GetContext()
	suggestShopperViewsRequest.Filter = &storefrontv1.SuggestShopperViewsRequest_Filter{
		OwnerId:      req.GetStorefrontSessionId(),
		CreatedAfter: req.GetContext().GetDeadline(),
		Labels:       baggage,
	}
	suggestShopperViewsReply, err := s.shopperViewQueryClient.SuggestShopperViews(ctx, suggestShopperViewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewQueryService/SuggestShopperViews")
	}
	updatePurchaseRequisitionRequest := &procurementv1.UpdatePurchaseRequisitionRequest{
		Context:             req.GetContext(),
		PurchaseRequisition: &procurementv1.PurchaseRequisition{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
	}
	updatePurchaseRequisitionReply := new(procurementv1.UpdatePurchaseRequisitionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", procurementPurchaseRequisitionServiceName, "UpdatePurchaseRequisition"), updatePurchaseRequisitionRequest, updatePurchaseRequisitionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.procurement.v1.PurchaseRequisitionService/UpdatePurchaseRequisition")
	}
	pausePartnerOrderWorkflowRequest := buildPausePartnerOrderWorkflowRequestForDeleteStorefrontSession(req, updatePurchaseRequisitionReply)
	pausePartnerOrderWorkflowReply, err := s.partnerOrderWorkflowClient.PausePartnerOrderWorkflow(ctx, pausePartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/PausePartnerOrderWorkflow")
	}
	return &storefrontv1.DeleteStorefrontSessionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestShopperViewsReply.GetResult().GetReferenceIds(),
			Details:      updatePurchaseRequisitionReply.GetResult().GetDetails(),
			Status:       pausePartnerOrderWorkflowReply.GetResult().GetStatus(),
		},
	}, nil
}

// ListStorefrontSessions lists storefront sessions a page at a time.
func (s *StorefrontSessionServer) ListStorefrontSessions(ctx context.Context, req *storefrontv1.ListStorefrontSessionsRequest) (*storefrontv1.ListStorefrontSessionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list storefront sessions", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	recordSessionVersionRequest := &identityv1.RecordSessionVersionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Assignee: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &identityv1.RecordSessionVersionRequest_Details{Snapshot: snapshot},
	}
	recordSessionVersionReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*identityv1.RecordSessionVersionResponse, error) {
		return s.sessionHistoryClient.RecordSessionVersion(ctx, recordSessionVersionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/RecordSessionVersion")
	}
	snapshot2, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	recordUserVersionRequest := &identityv1.RecordUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{
				UserId: recordSessionVersionReply.GetSession().GetAssignee().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recordSessionVersionReply.GetSession().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &identityv1.RecordUserVersionRequest_Details{Snapshot: snapshot2},
	}
	var recordUserVersionReply *identityv1.RecordUserVersionResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.UserHistoryService/RecordUserVersion", func(ctx context.Context) error {
		answer, err := s.userHistoryClient.RecordUserVersion(ctx, recordUserVersionRequest)
		recordUserVersionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RecordUserVersion")
	}
	return &storefrontv1.ListStorefrontSessionsResponse{
		StorefrontSessions: []*storefrontv1.StorefrontSession{&storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recordSessionVersionReply.GetSession().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: recordUserVersionReply.GetUser().GetRelationshipManager().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: recordUserVersionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// BatchGetStorefrontSessions returns several storefront sessions at once.
func (s *StorefrontSessionServer) BatchGetStorefrontSessions(ctx context.Context, req *storefrontv1.BatchGetStorefrontSessionsRequest) (*storefrontv1.BatchGetStorefrontSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get storefront sessions", "tenant", req.GetContext().GetTenantId())
	replayShopperViewEventsReply, err := s.shopperViewIntegrationClient.ReplayShopperViewEvents(ctx, &storefrontv1.ReplayShopperViewEventsRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.ReplayShopperViewEventsRequest_Details{
			ReceivedAt: req.GetContext().GetDeadline(),
			MessageId:  req.GetContext().GetTraceId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewIntegrationService/ReplayShopperViewEvents")
	}
	return &storefrontv1.BatchGetStorefrontSessionsResponse{
		StorefrontSessions: []*storefrontv1.StorefrontSession{&storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				Email: replayShopperViewEventsReply.GetShopperView().GetOwner().GetEmail(),
			},
			Assignee: &identityv1.UserRef{
				Email: replayShopperViewEventsReply.GetShopperView().GetOwner().GetEmail(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SearchStorefrontSessions lists storefront sessions a page at a time.
func (s *StorefrontSessionServer) SearchStorefrontSessions(ctx context.Context, req *storefrontv1.SearchStorefrontSessionsRequest) (*storefrontv1.SearchStorefrontSessionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "search storefront sessions", "tenant", req.GetContext().GetTenantId())
	partnerWebhookIntegration, err := registry.Lookup[partnerapiv1.PartnerWebhookIntegrationServiceClient](s.clients, "acme.partnerapi.v1.PartnerWebhookIntegrationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	disconnectPartnerWebhookSourceRequest := &partnerapiv1.DisconnectPartnerWebhookSourceRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.DisconnectPartnerWebhookSourceRequest_Details{
			ReceivedAt: req.GetFilter().GetCreatedAfter(),
			MessageId:  req.GetFilter().GetOwnerId(),
		},
		Annotations: labels,
	}
	disconnectPartnerWebhookSourceReply, err := partnerWebhookIntegration.DisconnectPartnerWebhookSource(ctx, disconnectPartnerWebhookSourceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookIntegrationService/DisconnectPartnerWebhookSource")
	}
	return &storefrontv1.SearchStorefrontSessionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: disconnectPartnerWebhookSourceReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// WatchStorefrontSessions streams the changes of storefront sessions as they happen.
func (s *StorefrontSessionServer) WatchStorefrontSessions(req *storefrontv1.WatchStorefrontSessionsRequest, stream grpc.ServerStreamingServer[storefrontv1.WatchStorefrontSessionsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected WatchStorefrontSessions", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "watch storefront sessions", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Query string }
	if record := req.GetFilter(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Query"); err != nil {
			s.log.WarnContext(ctx, "copy query", "error", err)
		}
	}
	watchWorkspacesStream, err := s.workspaceClient.WatchWorkspaces(ctx, &datahubv1.WatchWorkspacesRequest{
		Context: req.GetContext(),
		Filter: &datahubv1.WatchWorkspacesRequest_Filter{
			Region: req.GetFilter().GetRegion(),
			Query:  holder.Query,
		},
		Subscriber: &commonv1.ContactInfo{Email: req.GetSubscriber().GetEmail()},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.datahub.v1.WorkspaceService/WatchWorkspaces")
	}
	for {
		received, err := watchWorkspacesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return grpcx.Wrap(err, "acme.datahub.v1.WorkspaceService/WatchWorkspaces")
		}
		_ = received
	}
	subscriberCache := cache.New[string, *commonv1.ContactInfo](1)
	subscriberCache.Put(req.GetContext().GetTraceId(), req.GetSubscriber())
	cachedSubscriber, _ := subscriberCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	advancePartnerWebhookWorkflowRequest := &partnerapiv1.AdvancePartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.AdvancePartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone:            cachedSubscriber.GetPhone(),
				Email:            req.GetSubscriber().GetEmail(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
				Name:             req.GetSubscriber().GetName(),
			},
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	}
	advancePartnerWebhookWorkflowReply, err := grpcx.Call(ctx, s.partnerWebhookWorkflowClient.AdvancePartnerWebhookWorkflow, advancePartnerWebhookWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/AdvancePartnerWebhookWorkflow")
	}
	filterCache := cache.New[string, *storefrontv1.WatchStorefrontSessionsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	suggestStorefrontSessionsRequest := &storefrontv1.SuggestStorefrontSessionsRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.SuggestStorefrontSessionsRequest_Filter{
			Statuses: req.GetFilter().GetStatuses(),
			Query:    req.GetFilter().GetQuery(),
			Region:   cachedFilter.GetRegion(),
		},
	}
	suggestStorefrontSessionsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*storefrontv1.SuggestStorefrontSessionsResponse, error) {
		return s.storefrontSessionQueryClient.SuggestStorefrontSessions(ctx, suggestStorefrontSessionsRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionQueryService/SuggestStorefrontSessions")
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	listStorefrontSessionSourcesRequest := &storefrontv1.ListStorefrontSessionSourcesRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.ListStorefrontSessionSourcesRequest_Filter{
			Labels:  labels,
			OwnerId: req.GetFilter().GetOwnerId(),
			Query:   req.GetFilter().GetQuery(),
			Region:  req.GetFilter().GetRegion(),
		},
	}
	listStorefrontSessionSourcesReply, err := s.meteredStorefrontSessionIntegration.ListStorefrontSessionSources(ctx, listStorefrontSessionSourcesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/ListStorefrontSessionSources")
	}
	_ = listStorefrontSessionSourcesReply
	if err := stream.Send(&storefrontv1.WatchStorefrontSessionsResponse{
		Result: &commonv1.OperationResult{
			Status:  advancePartnerWebhookWorkflowReply.GetResult().GetStatus(),
			Details: suggestStorefrontSessionsReply.GetResult().GetDetails(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				Email: req.GetSubscriber().GetEmail(),
				Name:  req.GetSubscriber().GetName(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateStorefrontSessions takes a stream of storefront sessions and answers once at the end.
func (s *StorefrontSessionServer) BatchCreateStorefrontSessions(stream grpc.ClientStreamingServer[storefrontv1.BatchCreateStorefrontSessionsRequest, storefrontv1.BatchCreateStorefrontSessionsResponse]) error {
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
		s.log.InfoContext(ctx, "batch create storefront sessions", "tenant", msg.GetContext().GetTenantId())
		for _, comment := range msg.GetRecord().GetComments() {
			if _, err := s.routeBindingClient.CloneRouteBinding(ctx, &edgegatewayv1.CloneRouteBindingRequest{
				Context: msg.GetContext(),
				RouteBinding: &edgegatewayv1.RouteBinding{
					Audit: &commonv1.AuditInfo{
						CreatedAt: comment.GetCreatedAt(),
						UpdatedBy: msg.GetRecord().GetAudit().GetUpdatedBy(),
					},
					Action: &edgegatewayv1.RouteBinding_Block{Block: &edgegatewayv1.RouteBindingBlockAction{NotifyEmail: comment.GetAuthorEmail()}},
					Owner:  &identityv1.UserRef{UserId: msg.GetRecord().GetAssignee().GetUserId()},
				},
			}); err != nil {
				return grpcx.Wrap(err, "acme.edgegateway.v1.RouteBindingService/CloneRouteBinding")
			}
		}
		exportPartnerWebhooksStream, err := s.partnerWebhookQueryClient.ExportPartnerWebhooks(ctx, &partnerapiv1.ExportPartnerWebhooksRequest{
			Context: msg.GetContext(),
			Format:  msg.GetHeader().GetFormat(),
			Filter: &partnerapiv1.ExportPartnerWebhooksRequest_Filter{
				CreatedAfter: msg.GetRecord().GetAudit().GetUpdatedAt(),
				OwnerId:      msg.GetRecord().GetSla().GetPolicyId(),
			},
		})
		if err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookQueryService/ExportPartnerWebhooks")
		}
		for {
			received, err := exportPartnerWebhooksStream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookQueryService/ExportPartnerWebhooks")
			}
			_ = received
		}
		accepted++
	}
	return stream.SendAndClose(&storefrontv1.BatchCreateStorefrontSessionsResponse{Accepted: accepted})
}

// CloneStorefrontSession creates a storefront session.
func (s *StorefrontSessionServer) CloneStorefrontSession(ctx context.Context, req *storefrontv1.CloneStorefrontSessionRequest) (*storefrontv1.CloneStorefrontSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CloneStorefrontSession", "error", err)
		return nil, err
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "clone storefront session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	customFields := make(map[string]string, len(req.GetStorefrontSession().GetCustomFields()))
	for key, value := range req.GetStorefrontSession().GetCustomFields() {
		customFields[key] = value
	}
	packed, err := anypb.New(req.GetStorefrontSession())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	replayLandingPageEventsRequest := &storefrontv1.ReplayLandingPageEventsRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.ReplayLandingPageEventsRequest_Details{
			ReceivedAt: req.GetStorefrontSession().GetAudit().GetUpdatedAt(),
			MessageId:  req.GetStorefrontSession().GetAssignee().GetUserId(),
			Envelope:   packed,
		},
		Annotations: customFields,
	}
	replayLandingPageEventsReply, err := s.landingPageIntegrationPort.ReplayLandingPageEvents(ctx, replayLandingPageEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageIntegrationService/ReplayLandingPageEvents")
	}
	listPreferenceReportsRequest := &customersv1.ListPreferenceReportsRequest{}
	listPreferenceReportsRequest.Context = req.GetContext()
	listPreferenceReportsRequest.Filter = &customersv1.ListPreferenceReportsRequest_Filter{
		Region:       replayLandingPageEventsReply.GetLandingPage().GetBillingAddress().GetRegion(),
		CreatedAfter: replayLandingPageEventsReply.GetLandingPage().GetAudit().GetCreatedAt(),
		OwnerId:      replayLandingPageEventsReply.GetLandingPage().GetLandingPageId(),
	}
	listPreferenceReportsReply, err := s.preferenceReportingClient.ListPreferenceReports(ctx, listPreferenceReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceReportingService/ListPreferenceReports")
	}
	return &storefrontv1.CloneStorefrontSessionResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: replayLandingPageEventsReply.GetLandingPage().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: replayLandingPageEventsReply.GetLandingPage().GetCounterparty().GetEmail(),
			},
			Requester: req.GetStorefrontSession().GetRequester(),
			Status:    req.GetStorefrontSession().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: listPreferenceReportsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// createSearchIndexSender returns a function that sends one request to
// acme.search.v1.SearchIndexService/CreateSearchIndex.
func (s *StorefrontSessionServer) createSearchIndexSender(request *searchv1.CreateSearchIndexRequest) func(context.Context) (*searchv1.CreateSearchIndexResponse, error) {
	return func(ctx context.Context) (*searchv1.CreateSearchIndexResponse, error) {
		return s.searchIndexClient.CreateSearchIndex(ctx, request)
	}
}
