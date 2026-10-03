package storefront

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	driverappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/driverapp/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	patientappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patientapp/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// LandingPageIntegrationServer implements `acme.storefront.v1.LandingPageIntegrationService`.
// Connects external systems that send landing page events.
type LandingPageIntegrationServer struct {
	storefrontv1.UnimplementedLandingPageIntegrationServiceServer
	store                               *Store
	log                                 *slog.Logger
	clients                             *registry.Registry
	storefrontSessionNotificationClient storefrontv1.StorefrontSessionNotificationServiceClient
	landingPageReportingClient          storefrontv1.LandingPageReportingServiceClient
	storefrontSessionIntegrationClient  storefrontv1.StorefrontSessionIntegrationServiceClient
	customerWorkflowClient              customersv1.CustomerWorkflowServiceClient
	conn                                *grpc.ClientConn
	tenantNotificationOnce              sync.Once
	tenantNotificationCached            identityv1.TenantNotificationServiceClient
	featureFlagQueryClient              featureflagsv1.FeatureFlagQueryServiceClient
	searchQueryValidationClient         searchv1.SearchQueryValidationServiceClient
	userQueryClient                     identityv1.UserQueryServiceClient
	meteredDriverHomeQuery              meteredDriverHomeQuery
	meteredPatientHome                  meteredPatientHome
	partnerOrderIntegrationClient       partnerapiv1.PartnerOrderIntegrationServiceClient
	sessionQueryClient                  identityv1.SessionQueryServiceClient
}

// NewLandingPageIntegrationServer wires a LandingPageIntegrationServer.
func NewLandingPageIntegrationServer(deps platform.Deps) *LandingPageIntegrationServer {
	return &LandingPageIntegrationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.storefront.v1.LandingPageIntegrationService"),
		clients:                             deps.Clients,
		storefrontSessionNotificationClient: storefrontv1.NewStorefrontSessionNotificationServiceClient(deps.Conn),
		landingPageReportingClient:          storefrontv1.NewLandingPageReportingServiceClient(deps.Conn),
		storefrontSessionIntegrationClient:  storefrontv1.NewStorefrontSessionIntegrationServiceClient(deps.Conn),
		customerWorkflowClient:              customersv1.NewCustomerWorkflowServiceClient(deps.Conn),
		conn:                                deps.Conn,
		featureFlagQueryClient:              featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		searchQueryValidationClient:         searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
		userQueryClient:                     identityv1.NewUserQueryServiceClient(deps.Conn),
		meteredDriverHomeQuery:              meteredDriverHomeQuery{DriverHomeQueryServiceClient: driverappv1.NewDriverHomeQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		meteredPatientHome:                  meteredPatientHome{PatientHomeServiceClient: patientappv1.NewPatientHomeServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		partnerOrderIntegrationClient:       partnerapiv1.NewPartnerOrderIntegrationServiceClient(deps.Conn),
		sessionQueryClient:                  identityv1.NewSessionQueryServiceClient(deps.Conn),
	}
}

// ConnectLandingPageSource connects a landing page.
func (s *LandingPageIntegrationServer) ConnectLandingPageSource(ctx context.Context, req *storefrontv1.ConnectLandingPageSourceRequest) (*storefrontv1.ConnectLandingPageSourceResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ConnectLandingPageSource", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "connect_landing_page_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "connect landing page source", "tenant", req.GetContext().GetTenantId())
	customerSegmentApproval, ok := s.clients.Get("acme.customers.v1.CustomerSegmentApprovalService").(customersv1.CustomerSegmentApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerSegmentApprovalService")
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recallCustomerSegmentRequest := &customersv1.RecallCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetReason()}},
			Owner:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &customersv1.RecallCustomerSegmentRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations:       req.GetAnnotations(),
		CustomerSegmentId: envelope.GetDisplayName(),
	}
	recallCustomerSegmentReply, err := customerSegmentApproval.RecallCustomerSegment(ctx, recallCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentApprovalService/RecallCustomerSegment")
	}
	streamStorefrontSessionNoticesStream, err := s.storefrontSessionNotificationClient.StreamStorefrontSessionNotices(ctx, &storefrontv1.StreamStorefrontSessionNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: recallCustomerSegmentReply.GetCustomerSegment().GetBlock().GetNotifyEmail(),
		},
		Filter: &storefrontv1.StreamStorefrontSessionNoticesRequest_Filter{
			CreatedAfter: recallCustomerSegmentReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
		},
		Since: recallCustomerSegmentReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
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
	deleteLandingPageReportRequest := &storefrontv1.DeleteLandingPageReportRequest{
		Context:       req.GetContext(),
		LandingPageId: req.GetLandingPageId(),
		Reason:        recallCustomerSegmentReply.GetCustomerSegment().GetBlock().GetReason(),
		Etag:          recallCustomerSegmentReply.GetCustomerSegment().GetAudit().GetEtag(),
	}
	deleteLandingPageReportReply, err := grpcx.Call(ctx, s.landingPageReportingClient.DeleteLandingPageReport, deleteLandingPageReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/DeleteLandingPageReport")
	}
	var holder struct{ MessageId string }
	if record := req.GetDetails(); record != nil {
		if err := mapper.CopyFields(&holder, record, "MessageId"); err != nil {
			s.log.WarnContext(ctx, "copy message_id", "error", err)
		}
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	testStorefrontSessionConnectionReply, err := s.storefrontSessionIntegrationClient.TestStorefrontSessionConnection(ctx, &storefrontv1.TestStorefrontSessionConnectionRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.TestStorefrontSessionConnectionRequest_Details{
			MessageId: holder.MessageId,
			Envelope:  packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/TestStorefrontSessionConnection")
	}
	return &storefrontv1.ConnectLandingPageSourceResponse{
		LandingPage: &storefrontv1.LandingPage{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: recallCustomerSegmentReply.GetCustomerSegment().GetOwner().GetDisplayName(),
				Email:       testStorefrontSessionConnectionReply.GetStorefrontSession().GetRequester().GetEmail(),
			},
			LandingPageId: req.GetLandingPageId(),
		},
		Result: &commonv1.OperationResult{
			Details: deleteLandingPageReportReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &storefrontv1.ConnectLandingPageSourceResponse_Outcome{
			Status: deleteLandingPageReportReply.GetResult().GetStatus(),
		},
	}, nil
}

// DisconnectLandingPageSource disconnects a landing page.
func (s *LandingPageIntegrationServer) DisconnectLandingPageSource(ctx context.Context, req *storefrontv1.DisconnectLandingPageSourceRequest) (*storefrontv1.DisconnectLandingPageSourceResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "disconnect_landing_page_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "disconnect landing page source", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &landingPageIntegrationServerDisconnectLandingPageSourceStartCustomerWorkflowStep{request: &customersv1.StartCustomerWorkflowRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		CustomerId:  envelope.GetDisplayName(),
	}}
	if err := s.runDisconnectLandingPageSourceStartCustomerWorkflow(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerWorkflowService/StartCustomerWorkflow")
	}
	startCustomerWorkflowReply := step.reply
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	unsubscribeTenantUpdatesRequest := &identityv1.UnsubscribeTenantUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeTenantUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name: startCustomerWorkflowReply.GetCustomer().GetContact().GetName(),
			},
			Payload: packed,
		},
		TenantId: envelope2.GetDisplayName(),
	}
	unsubscribeTenantUpdatesReply, err := s.tenantNotificationLazy().UnsubscribeTenantUpdates(ctx, unsubscribeTenantUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/UnsubscribeTenantUpdates")
	}
	return &storefrontv1.DisconnectLandingPageSourceResponse{
		LandingPage: &storefrontv1.LandingPage{
			BillingAddress: &commonv1.PostalAddress{
				Region:     startCustomerWorkflowReply.GetCustomer().GetPrimaryAddress().GetRegion(),
				PostalCode: startCustomerWorkflowReply.GetCustomer().GetPrimaryAddress().GetPostalCode(),
				Lines:      unsubscribeTenantUpdatesReply.GetTenant().GetLocation().GetLines(),
			},
			LandingPageId: req.GetLandingPageId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReplayLandingPageEvents replays a landing page.
func (s *LandingPageIntegrationServer) ReplayLandingPageEvents(ctx context.Context, req *storefrontv1.ReplayLandingPageEventsRequest) (*storefrontv1.ReplayLandingPageEventsResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "replay_landing_page_events"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "replay landing page events", "tenant", req.GetContext().GetTenantId())
	countFeatureFlagsRequest := &featureflagsv1.CountFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter:  &featureflagsv1.CountFeatureFlagsRequest_Filter{CreatedAfter: req.GetDetails().GetReceivedAt()},
	}
	countFeatureFlagsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*featureflagsv1.CountFeatureFlagsResponse, error) {
		return s.featureFlagQueryClient.CountFeatureFlags(ctx, countFeatureFlagsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	auditEvent, err := registry.Lookup[auditv1.AuditEventServiceClient](s.clients, "acme.audit.v1.AuditEventService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	searchAuditEventsRequest := &auditv1.SearchAuditEventsRequest{
		Context: req.GetContext(),
		Filter:  &auditv1.SearchAuditEventsRequest_Filter{CreatedAfter: req.GetDetails().GetReceivedAt()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	searchAuditEventsReply, err := auditEvent.SearchAuditEvents(ctx, searchAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/SearchAuditEvents")
	}
	tenantNotification, err := registry.Lookup[identityv1.TenantNotificationServiceClient](s.clients, "acme.identity.v1.TenantNotificationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "tenant_id", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeTenantUpdatesRequest := &identityv1.SubscribeTenantUpdatesRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		TenantId:    tenancy.Value(ctx, "tenant_id"),
		Details:     &identityv1.SubscribeTenantUpdatesRequest_Details{Payload: packed},
	}
	subscribeTenantUpdatesReply, err := tenantNotification.SubscribeTenantUpdates(ctx, subscribeTenantUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/SubscribeTenantUpdates")
	}
	return &storefrontv1.ReplayLandingPageEventsResponse{
		Result: &commonv1.OperationResult{
			Message:       countFeatureFlagsReply.GetResult().GetMessage(),
			Details:       countFeatureFlagsReply.GetResult().GetDetails(),
			CorrelationId: searchAuditEventsReply.GetResult().GetCorrelationId(),
			Status:        searchAuditEventsReply.GetResult().GetStatus(),
		},
		Outcome: &storefrontv1.ReplayLandingPageEventsResponse_Outcome{
			ReferenceId: subscribeTenantUpdatesReply.GetOutcome().GetReferenceId(),
		},
		LandingPage: &storefrontv1.LandingPage{LandingPageId: req.GetLandingPageId()},
	}, nil
}

// AcknowledgeLandingPageEvent acknowledges a landing page.
func (s *LandingPageIntegrationServer) AcknowledgeLandingPageEvent(ctx context.Context, req *storefrontv1.AcknowledgeLandingPageEventRequest) (*storefrontv1.AcknowledgeLandingPageEventResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "acknowledge_landing_page_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "acknowledge landing page event", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*storefrontv1.AcknowledgeLandingPageEventRequest_Details)
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scoreSearchQueryRequest := &searchv1.ScoreSearchQueryRequest{}
	scoreSearchQueryRequest.Context = req.GetContext()
	scoreSearchQueryRequest.Reason = req.GetReason()
	scoreSearchQueryRequest.Annotations = req.GetAnnotations()
	scoreSearchQueryRequest.SearchQuery = &searchv1.SearchQuery{
		OccurredAt: cloned.GetReceivedAt(),
		Detail:     packed,
	}
	scoreSearchQueryRequest.SearchQueryId = envelope.GetDisplayName()
	scoreSearchQueryReply, err := s.searchQueryValidationClient.ScoreSearchQuery(ctx, scoreSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ScoreSearchQuery")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := landingPageIntegrationServerFindUsersSteps[mode]
	findUsersRequest := &identityv1.FindUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.FindUsersRequest_Filter{
			CreatedAfter: req.GetDetails().GetReceivedAt(),
			OwnerId:      scoreSearchQueryReply.GetSearchQuery().GetSubject().GetSearchIndexId(),
		},
	}
	findUsersReply, err := run(s, ctx, findUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/FindUsers")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	retryShopperViewWorkflowRequest := &storefrontv1.RetryShopperViewWorkflowRequest{
		Context: req.GetContext(),
		ShopperView: &storefrontv1.ShopperView{
			Code: scoreSearchQueryReply.GetSearchQuery().GetSubject().GetCode(),
			Owner: &identityv1.UserRef{
				DisplayName: scoreSearchQueryReply.GetSearchQuery().GetSubject().GetDisplayName(),
				UserId:      tenancy.Value(ctx, "user_id"),
			},
		},
		Reason: req.GetReason(),
	}
	retryShopperViewWorkflowReply, err := grpcx.NewClient(s.conn, storefrontv1.NewShopperViewWorkflowServiceClient).RetryShopperViewWorkflow(ctx, retryShopperViewWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/RetryShopperViewWorkflow")
	}
	packed2, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	acknowledgeStorefrontSessionEventRequest := &storefrontv1.AcknowledgeStorefrontSessionEventRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.AcknowledgeStorefrontSessionEventRequest_Details{
			Endpoint:     req.GetDetails().GetEndpoint(),
			MessageId:    req.GetDetails().GetMessageId(),
			SourceSystem: req.GetDetails().GetSourceSystem(),
			Envelope:     packed2,
		},
		Annotations:         req.GetAnnotations(),
		StorefrontSessionId: envelope2.GetDisplayName(),
	}
	acknowledgeStorefrontSessionEventReply, err := grpcx.Call(ctx, s.storefrontSessionIntegrationClient.AcknowledgeStorefrontSessionEvent, acknowledgeStorefrontSessionEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/AcknowledgeStorefrontSessionEvent")
	}
	return &storefrontv1.AcknowledgeLandingPageEventResponse{
		Result: &commonv1.OperationResult{
			Message:      scoreSearchQueryReply.GetResult().GetMessage(),
			ReferenceIds: retryShopperViewWorkflowReply.GetResult().GetReferenceIds(),
			Status:       acknowledgeStorefrontSessionEventReply.GetOutcome().GetStatus(),
		},
		LandingPage: &storefrontv1.LandingPage{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: scoreSearchQueryReply.GetSearchQuery().GetSubject().GetDisplayName(),
			},
			LandingPageId: req.GetLandingPageId(),
		},
		Outcome: &storefrontv1.AcknowledgeLandingPageEventResponse_Outcome{
			Message: findUsersReply.GetResult().GetMessage(),
			Status:  acknowledgeStorefrontSessionEventReply.GetResult().GetStatus(),
		},
	}, nil
}

// RelayLandingPageMessages exchanges changes of landing pages with a peer.
func (s *LandingPageIntegrationServer) RelayLandingPageMessages(stream grpc.BidiStreamingServer[storefrontv1.RelayLandingPageMessagesRequest, storefrontv1.RelayLandingPageMessagesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *storefrontv1.RelayLandingPageMessagesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "relay landing page messages", "tenant", msg.GetContext().GetTenantId())
		findDriverHomesRequest := &driverappv1.FindDriverHomesRequest{
			Context: msg.GetContext(),
			Filter:  &driverappv1.FindDriverHomesRequest_Filter{Region: msg.GetChange().GetBillingAddress().GetRegion()},
		}
		if _, err := s.meteredDriverHomeQuery.FindDriverHomes(ctx, findDriverHomesRequest); err != nil {
			return grpcx.Wrap(err, "acme.driverapp.v1.DriverHomeQueryService/FindDriverHomes")
		}
		jobs := make(chan *storefrontv1.LandingPageLine)
		failures := make(chan error, 4)
		workCtx, stopWork := context.WithCancel(ctx)
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for line := range jobs {
					if _, err := s.storefrontSessionNotificationClient.NotifyStorefrontSessionWatchers(workCtx, &storefrontv1.NotifyStorefrontSessionWatchersRequest{
						Context: msg.GetContext(),
						Details: &storefrontv1.NotifyStorefrontSessionWatchersRequest_Details{
							Recipient: &commonv1.ContactInfo{Email: msg.GetChange().GetCounterparty().GetEmail()},
						},
						Annotations: line.GetAttributes(),
					}); err != nil {
						failures <- err
						stopWork()
						return
					}
				}
			}()
		}
		for _, queued := range msg.GetChange().GetLines() {
			if workCtx.Err() != nil {
				break
			}
			select {
			case jobs <- queued:
			case <-workCtx.Done():
			}
		}
		close(jobs)
		wg.Wait()
		stopWork()
		close(failures)
		if err := <-failures; err != nil {
			return grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionNotificationService/NotifyStorefrontSessionWatchers")
		}
		createPatientHomeRequest := &patientappv1.CreatePatientHomeRequest{Context: msg.GetContext()}
		if _, err := s.meteredPatientHome.CreatePatientHome(ctx, createPatientHomeRequest); err != nil {
			return grpcx.Wrap(err, "acme.patientapp.v1.PatientHomeService/CreatePatientHome")
		}
		if _, err := s.customerWorkflowClient.AdvanceCustomerWorkflow(ctx, &customersv1.AdvanceCustomerWorkflowRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.customers.v1.CustomerWorkflowService/AdvanceCustomerWorkflow")
		}
		if err := stream.Send(&storefrontv1.RelayLandingPageMessagesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ListLandingPageSources lists landing pages a page at a time.
func (s *LandingPageIntegrationServer) ListLandingPageSources(ctx context.Context, req *storefrontv1.ListLandingPageSourcesRequest) (*storefrontv1.ListLandingPageSourcesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list landing page sources", "tenant", req.GetContext().GetTenantId())
	return &storefrontv1.ListLandingPageSourcesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// TestLandingPageConnection tests a landing page.
func (s *LandingPageIntegrationServer) TestLandingPageConnection(ctx context.Context, req *storefrontv1.TestLandingPageConnectionRequest) (*storefrontv1.TestLandingPageConnectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected TestLandingPageConnection", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "landing_page", req.GetLandingPageId(), "test_landing_page_connection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "test landing page connection", "tenant", req.GetContext().GetTenantId())
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	acknowledgePartnerOrderEventRequest := &partnerapiv1.AcknowledgePartnerOrderEventRequest{
		Context:        req.GetContext(),
		Details:        &partnerapiv1.AcknowledgePartnerOrderEventRequest_Details{SourceSystem: req.GetDetails().GetSourceSystem()},
		PartnerOrderId: envelope.GetDisplayName(),
	}
	type acknowledgePartnerOrderEventResult struct {
		reply *partnerapiv1.AcknowledgePartnerOrderEventResponse
		err   error
	}
	acknowledgePartnerOrderEventResults := make(chan acknowledgePartnerOrderEventResult, 1)
	go func() {
		reply, err := s.partnerOrderIntegrationClient.AcknowledgePartnerOrderEvent(ctx, acknowledgePartnerOrderEventRequest)
		acknowledgePartnerOrderEventResults <- acknowledgePartnerOrderEventResult{reply, err}
	}()
	var acknowledgePartnerOrderEventReply *partnerapiv1.AcknowledgePartnerOrderEventResponse
	select {
	case result := <-acknowledgePartnerOrderEventResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.partnerapi.v1.PartnerOrderIntegrationService/AcknowledgePartnerOrderEvent")
		}
		acknowledgePartnerOrderEventReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	deleteLandingPageReportRequest := &storefrontv1.DeleteLandingPageReportRequest{
		Context:       req.GetContext(),
		LandingPageId: req.GetLandingPageId(),
		Etag:          acknowledgePartnerOrderEventReply.GetPartnerOrder().GetAudit().GetEtag(),
		Reason:        req.GetReason(),
	}
	deleteLandingPageReportReply := new(storefrontv1.DeleteLandingPageReportResponse)
	if err := grpcx.Invoke(ctx, s.conn, storefrontv1.LandingPageReportingService_DeleteLandingPageReport_FullMethodName, deleteLandingPageReportRequest, deleteLandingPageReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/DeleteLandingPageReport")
	}
	lookupUserRequest := &identityv1.LookupUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	}
	lookupUserReply, err := grpcx.NewClient(s.conn, identityv1.NewUserQueryServiceClient).LookupUser(ctx, lookupUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/LookupUser")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	exportSessionsStream, err := s.sessionQueryClient.ExportSessions(ctx, &identityv1.ExportSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ExportSessionsRequest_Filter{
			Region: lookupUserReply.GetUser().GetPrimaryAddress().GetRegion(),
			Query:  envelope2.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
	}
	for {
		received, err := exportSessionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
		}
		_ = received
	}
	return &storefrontv1.TestLandingPageConnectionResponse{
		LandingPage: &storefrontv1.LandingPage{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: acknowledgePartnerOrderEventReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			LandingPageId: req.GetLandingPageId(),
		},
		Result: &commonv1.OperationResult{
			Message:       deleteLandingPageReportReply.GetResult().GetMessage(),
			Status:        deleteLandingPageReportReply.GetResult().GetStatus(),
			CorrelationId: lookupUserReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// landingPageIntegrationServerDisconnectLandingPageSourceStartCustomerWorkflowStep carries the request of one step of
// DisconnectLandingPageSource and, once it ran, its reply.
type landingPageIntegrationServerDisconnectLandingPageSourceStartCustomerWorkflowStep struct {
	request *customersv1.StartCustomerWorkflowRequest
	reply   *customersv1.StartCustomerWorkflowResponse
}

func (s *LandingPageIntegrationServer) runDisconnectLandingPageSourceStartCustomerWorkflow(ctx context.Context, step *landingPageIntegrationServerDisconnectLandingPageSourceStartCustomerWorkflowStep) error {
	reply, err := s.customerWorkflowClient.StartCustomerWorkflow(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// tenantNotificationLazy dials acme.identity.v1.TenantNotificationService the first time a handler needs it.
func (s *LandingPageIntegrationServer) tenantNotificationLazy() identityv1.TenantNotificationServiceClient {
	s.tenantNotificationOnce.Do(func() { s.tenantNotificationCached = identityv1.NewTenantNotificationServiceClient(s.conn) })
	return s.tenantNotificationCached
}

// landingPageIntegrationServerFindUsersSteps are the ways to run acme.identity.v1.UserQueryService/FindUsers, by name.
var landingPageIntegrationServerFindUsersSteps = map[string]func(*LandingPageIntegrationServer, context.Context, *identityv1.FindUsersRequest) (*identityv1.FindUsersResponse, error){
	"default": (*LandingPageIntegrationServer).findUsersStep,
	"audited": (*LandingPageIntegrationServer).findUsersAuditedStep,
}

func (s *LandingPageIntegrationServer) findUsersStep(ctx context.Context, request *identityv1.FindUsersRequest) (*identityv1.FindUsersResponse, error) {
	return s.userQueryClient.FindUsers(ctx, request)
}

func (s *LandingPageIntegrationServer) findUsersAuditedStep(ctx context.Context, request *identityv1.FindUsersRequest) (*identityv1.FindUsersResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.UserQueryService/FindUsers")
	return s.userQueryClient.FindUsers(ctx, request)
}
