package storefront

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	edgegatewayv1 "github.com/hounddogai/protomap-test-golang/gen/acme/edgegateway/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// StorefrontSessionWorkflowServer implements `acme.storefront.v1.StorefrontSessionWorkflowService`.
// Runs the long-lived workflow that takes a storefront session from start to finish.
type StorefrontSessionWorkflowServer struct {
	storefrontv1.UnimplementedStorefrontSessionWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	preferenceReportingClient       customersv1.PreferenceReportingServiceClient
	customerSegmentSyncClient       customersv1.CustomerSegmentSyncServiceClient
	sessionNotificationClient       identityv1.SessionNotificationServiceClient
	householdLifecycleClient        customersv1.HouseholdLifecycleServiceClient
	conn                            *grpc.ClientConn
	giftCardWorkflowClient          giftcardsv1.GiftCardWorkflowServiceClient
	auditEventQueryClient           auditv1.AuditEventQueryServiceClient
	routeBindingPort                routeBindingPort
	partnerOrderWorkflowClient      partnerapiv1.PartnerOrderWorkflowServiceClient
	partnerWebhookIntegrationClient partnerapiv1.PartnerWebhookIntegrationServiceClient
	partnerOrderIntegrationClient   partnerapiv1.PartnerOrderIntegrationServiceClient
	venueClient                     venuesv1.VenueServiceClient
	clients                         *registry.Registry
	accessPolicyClient              authorizationv1.AccessPolicyServiceClient
	partnerIntegrationGatewayPort   partnerIntegrationGatewayPort
	partnerWebhookApprovalClient    partnerapiv1.PartnerWebhookApprovalServiceClient
	bus                             *eventbus.Bus
	searchQueryValidationClient     searchv1.SearchQueryValidationServiceClient
}

// NewStorefrontSessionWorkflowServer wires a StorefrontSessionWorkflowServer.
func NewStorefrontSessionWorkflowServer(deps platform.Deps) StorefrontSessionWorkflowServer {
	return StorefrontSessionWorkflowServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.storefront.v1.StorefrontSessionWorkflowService"),
		preferenceReportingClient:       customersv1.NewPreferenceReportingServiceClient(deps.Conn),
		customerSegmentSyncClient:       customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
		sessionNotificationClient:       identityv1.NewSessionNotificationServiceClient(deps.Conn),
		householdLifecycleClient:        customersv1.NewHouseholdLifecycleServiceClient(deps.Conn),
		conn:                            deps.Conn,
		giftCardWorkflowClient:          giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		auditEventQueryClient:           auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		routeBindingPort:                newRouteBindingPort(deps),
		partnerOrderWorkflowClient:      partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn),
		partnerWebhookIntegrationClient: partnerapiv1.NewPartnerWebhookIntegrationServiceClient(deps.Conn),
		partnerOrderIntegrationClient:   partnerapiv1.NewPartnerOrderIntegrationServiceClient(deps.Conn),
		venueClient:                     venuesv1.NewVenueServiceClient(deps.Conn),
		clients:                         deps.Clients,
		accessPolicyClient:              authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		partnerIntegrationGatewayPort:   newPartnerIntegrationGatewayPort(deps),
		partnerWebhookApprovalClient:    partnerapiv1.NewPartnerWebhookApprovalServiceClient(deps.Conn),
		bus:                             deps.Bus,
		searchQueryValidationClient:     searchv1.NewSearchQueryValidationServiceClient(deps.Conn),
	}
}

// StartStorefrontSessionWorkflow starts a storefront session.
func (s StorefrontSessionWorkflowServer) StartStorefrontSessionWorkflow(ctx context.Context, req *storefrontv1.StartStorefrontSessionWorkflowRequest) (*storefrontv1.StartStorefrontSessionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "start storefront session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	schedulePreferenceReportRequest := &customersv1.SchedulePreferenceReportRequest{}
	schedulePreferenceReportRequest.Context = req.GetContext()
	schedulePreferenceReportRequest.Annotations = annotations
	schedulePreferenceReportReply, err := s.preferenceReportingClient.SchedulePreferenceReport(ctx, schedulePreferenceReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceReportingService/SchedulePreferenceReport")
	}
	var displayName string
	switch choice := req.GetStorefrontSession().GetOrigin().(type) {
	case *storefrontv1.StorefrontSession_Customer:
		displayName = choice.Customer.GetDisplayName()
	case *storefrontv1.StorefrontSession_ExternalReference:
		displayName = choice.ExternalReference
	case *storefrontv1.StorefrontSession_Channel:
		displayName = choice.Channel.GetChannel()
	}
	snapshot, err := protojson.Marshal(req.GetStorefrontSession())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncCustomerSegmentsReply, err := s.customerSegmentSyncClient.SyncCustomerSegments(ctx, &customersv1.SyncCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: displayName,
				Email:       req.GetStorefrontSession().GetRequester().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetStorefrontSession().GetAudit().GetCreatedBy(),
				Etag:      req.GetStorefrontSession().GetAudit().GetEtag(),
			},
		},
		Details: &customersv1.SyncCustomerSegmentsRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/SyncCustomerSegments")
	}
	packed, err := anypb.New(req.GetStorefrontSession())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	step := &storefrontSessionWorkflowServerStartStorefrontSessionWorkflowNotifySessionWatchersStep{request: &identityv1.NotifySessionWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifySessionWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNotify().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Name:             req.GetDetails().GetNotify().GetName(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
			},
			Payload: packed,
		},
	}}
	if err := s.runStartStorefrontSessionWorkflowNotifySessionWatchers(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/NotifySessionWatchers")
	}
	notifySessionWatchersReply := step.reply
	return &storefrontv1.StartStorefrontSessionWorkflowResponse{
		Outcome: &storefrontv1.StartStorefrontSessionWorkflowResponse_Outcome{
			Message: schedulePreferenceReportReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Details: syncCustomerSegmentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				CreatedBy: syncCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
			},
			Assignee:            notifySessionWatchersReply.GetSession().GetAssignee(),
			Tags:                notifySessionWatchersReply.GetSession().GetTags(),
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Subject:             req.GetStorefrontSession().GetSubject(),
		},
	}, nil
}

// AdvanceStorefrontSessionWorkflow advances a storefront session.
func (s StorefrontSessionWorkflowServer) AdvanceStorefrontSessionWorkflow(ctx context.Context, req *storefrontv1.AdvanceStorefrontSessionWorkflowRequest) (*storefrontv1.AdvanceStorefrontSessionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceStorefrontSessionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "advance storefront session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetStorefrontSession())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded storefrontv1.StorefrontSession
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	scheduleHouseholdExpiryRequest := &customersv1.ScheduleHouseholdExpiryRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						Etag:      tenancy.Value(ctx, "etag"),
						UpdatedAt: (&decoded).GetAudit().GetUpdatedAt(),
					},
				}},
				Manager: &identityv1.UserRef{Email: req.GetStorefrontSession().GetAssignee().GetEmail()},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetStorefrontSession().GetAudit().GetCreatedBy(),
			},
		},
	}
	scheduleHouseholdExpiryReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.ScheduleHouseholdExpiryResponse, error) {
		return s.householdLifecycleClient.ScheduleHouseholdExpiry(ctx, scheduleHouseholdExpiryRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdLifecycleService/ScheduleHouseholdExpiry")
	}
	resolveRouteBindingsRequest := &edgegatewayv1.ResolveRouteBindingsRequest{Context: req.GetContext()}
	resolveRouteBindingsReply := new(edgegatewayv1.ResolveRouteBindingsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", edgegatewayRouteBindingQueryServiceName, "ResolveRouteBindings"), resolveRouteBindingsRequest, resolveRouteBindingsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.edgegateway.v1.RouteBindingQueryService/ResolveRouteBindings")
	}
	pauseGiftCardWorkflowRequest := &giftcardsv1.PauseGiftCardWorkflowRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.PauseGiftCardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Name: req.GetStorefrontSession().GetRequester().GetName()},
		},
	}
	pauseGiftCardWorkflowReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*giftcardsv1.PauseGiftCardWorkflowResponse, error) {
		return s.giftCardWorkflowClient.PauseGiftCardWorkflow(ctx, pauseGiftCardWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/PauseGiftCardWorkflow")
	}
	return &storefrontv1.AdvanceStorefrontSessionWorkflowResponse{
		Outcome: &storefrontv1.AdvanceStorefrontSessionWorkflowResponse_Outcome{
			ReferenceId: scheduleHouseholdExpiryReply.GetOutcome().GetReferenceId(),
			Status:      scheduleHouseholdExpiryReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: resolveRouteBindingsReply.GetResult().GetCorrelationId(),
			Status:        resolveRouteBindingsReply.GetResult().GetStatus(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Assignee: &identityv1.UserRef{
				Email: pauseGiftCardWorkflowReply.GetGiftCard().GetContact().GetEmail(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Requester:           req.GetStorefrontSession().GetRequester(),
		},
	}, nil
}

// CancelStorefrontSessionWorkflow cancels a storefront session.
func (s StorefrontSessionWorkflowServer) CancelStorefrontSessionWorkflow(ctx context.Context, req *storefrontv1.CancelStorefrontSessionWorkflowRequest) (*storefrontv1.CancelStorefrontSessionWorkflowResponse, error) {
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "cancel storefront session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetStorefrontSession()).(*storefrontv1.StorefrontSession)
	countAuditEventsRequest := &auditv1.CountAuditEventsRequest{}
	countAuditEventsRequest.Context = req.GetContext()
	countAuditEventsRequest.Filter = &auditv1.CountAuditEventsRequest_Filter{
		CreatedAfter: req.GetStorefrontSession().GetSla().GetDueAt(),
		OwnerId:      cloned.GetAssignee().GetUserId(),
		Labels:       req.GetStorefrontSession().GetCustomFields(),
	}
	countAuditEventsReply, err := s.auditEventQueryClient.CountAuditEvents(ctx, countAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/CountAuditEvents")
	}
	createRouteBindingRequest := &edgegatewayv1.CreateRouteBindingRequest{
		Context: req.GetContext(),
		RouteBinding: &edgegatewayv1.RouteBinding{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetStorefrontSession().GetCustomer().GetDisplayName(),
				Email:       req.GetDetails().GetNotify().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetStorefrontSession().GetAudit().GetCreatedBy(),
			},
		},
	}
	createRouteBindingReply, err := s.routeBindingPort.CreateRouteBinding(ctx, createRouteBindingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.edgegateway.v1.RouteBindingService/CreateRouteBinding")
	}
	for comment := range iterx.Each(req.GetStorefrontSession().GetComments(), func(candidate *storefrontv1.StorefrontSessionComment) bool { return candidate != nil }) {
		if _, err := s.partnerOrderWorkflowClient.ResumePartnerOrderWorkflow(ctx, &partnerapiv1.ResumePartnerOrderWorkflowRequest{
			Context: req.GetContext(),
			PartnerOrder: &partnerapiv1.PartnerOrder{
				Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/ResumePartnerOrderWorkflow")
		}
	}
	return &storefrontv1.CancelStorefrontSessionWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:       countAuditEventsReply.GetResult().GetMessage(),
			CorrelationId: countAuditEventsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Assignee: &identityv1.UserRef{
				Email: createRouteBindingReply.GetRouteBinding().GetOwner().GetEmail(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Requester:           req.GetStorefrontSession().GetRequester(),
		},
		Outcome: &storefrontv1.CancelStorefrontSessionWorkflowResponse_Outcome{Message: createRouteBindingReply.GetResult().GetMessage()},
	}, nil
}

// RetryStorefrontSessionWorkflow retrys a storefront session.
func (s StorefrontSessionWorkflowServer) RetryStorefrontSessionWorkflow(ctx context.Context, req *storefrontv1.RetryStorefrontSessionWorkflowRequest) (*storefrontv1.RetryStorefrontSessionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "retry storefront session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetStorefrontSession())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	testPartnerWebhookConnectionReply, err := s.partnerWebhookIntegrationClient.TestPartnerWebhookConnection(ctx, &partnerapiv1.TestPartnerWebhookConnectionRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
		Details: &partnerapiv1.TestPartnerWebhookConnectionRequest_Details{
			ReceivedAt: req.GetStorefrontSession().GetSla().GetDueAt(),
			MessageId:  req.GetStorefrontSession().GetAssignee().GetUserId(),
			Envelope:   packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookIntegrationService/TestPartnerWebhookConnection")
	}
	resumePartnerOrderWorkflowRequest := buildResumePartnerOrderWorkflowRequestForRetryStorefrontSessionWorkflow(req)
	resumePartnerOrderWorkflowReply, err := s.partnerOrderWorkflowClient.ResumePartnerOrderWorkflow(ctx, resumePartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/ResumePartnerOrderWorkflow")
	}
	for _, item := range req.GetStorefrontSession().GetHistory() {
		if _, err := s.auditEventQueryClient.CountAuditEvents(ctx, &auditv1.CountAuditEventsRequest{
			Context: req.GetContext(),
			Filter: &auditv1.CountAuditEventsRequest_Filter{
				Region:       resumePartnerOrderWorkflowReply.GetPartnerOrder().GetBillingAddress().GetRegion(),
				CreatedAfter: item.GetChangedAt(),
				OwnerId:      req.GetStorefrontSession().GetCustomer().GetShopperViewId(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/CountAuditEvents")
		}
	}
	return &storefrontv1.RetryStorefrontSessionWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: testPartnerWebhookConnectionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				CreatedBy: resumePartnerOrderWorkflowReply.GetPartnerOrder().GetAudit().GetCreatedBy(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Priority:            req.GetStorefrontSession().GetPriority(),
		},
	}, nil
}

// PauseStorefrontSessionWorkflow pauses a storefront session.
func (s StorefrontSessionWorkflowServer) PauseStorefrontSessionWorkflow(ctx context.Context, req *storefrontv1.PauseStorefrontSessionWorkflowRequest) (*storefrontv1.PauseStorefrontSessionWorkflowResponse, error) {
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "pause storefront session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	relayPartnerOrderMessagesStream, err := s.partnerOrderIntegrationClient.RelayPartnerOrderMessages(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/RelayPartnerOrderMessages")
	}
	if err := relayPartnerOrderMessagesStream.Send(&partnerapiv1.RelayPartnerOrderMessagesRequest{
		Context: req.GetContext(),
		Frame: &partnerapiv1.RelayPartnerOrderMessagesRequest_Change{Change: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{Email: req.GetStorefrontSession().GetRequester().GetEmail()},
		}},
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/RelayPartnerOrderMessages")
	}
	if err := relayPartnerOrderMessagesStream.CloseSend(); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/RelayPartnerOrderMessages")
	}
	acknowledged, err := relayPartnerOrderMessagesStream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/RelayPartnerOrderMessages")
	}
	_ = acknowledged
	updateVenueReply, err := s.venueClient.UpdateVenue(ctx, &venuesv1.UpdateVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueService/UpdateVenue")
	}
	return &storefrontv1.PauseStorefrontSessionWorkflowResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				CreatedBy: updateVenueReply.GetVenue().GetAudit().GetCreatedBy(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Status:              req.GetStorefrontSession().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: updateVenueReply.GetResult().GetStatus()},
	}, nil
}

// ResumeStorefrontSessionWorkflow resumes a storefront session.
func (s StorefrontSessionWorkflowServer) ResumeStorefrontSessionWorkflow(ctx context.Context, req *storefrontv1.ResumeStorefrontSessionWorkflowRequest) (*storefrontv1.ResumeStorefrontSessionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "resume storefront session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	customerSegmentSync, ok := s.clients.Get("acme.customers.v1.CustomerSegmentSyncService").(customersv1.CustomerSegmentSyncServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerSegmentSyncService")
	}
	resolveCustomerSegmentConflictRequest := &customersv1.ResolveCustomerSegmentConflictRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetStorefrontSession().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNotify().GetEmail(),
				UserId: req.GetStorefrontSession().GetAssignee().GetUserId(),
			},
		},
	}
	resolveCustomerSegmentConflictReply, err := customerSegmentSync.ResolveCustomerSegmentConflict(ctx, resolveCustomerSegmentConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/ResolveCustomerSegmentConflict")
	}
	jobs := make(chan *storefrontv1.StorefrontSessionComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.preferenceReportingClient.CountPreferenceTotals(workCtx, &customersv1.CountPreferenceTotalsRequest{
					Context: req.GetContext(),
					Filter:  &customersv1.CountPreferenceTotalsRequest_Filter{CreatedAfter: comment.GetCreatedAt()},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetStorefrontSession().GetComments() {
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceReportingService/CountPreferenceTotals")
	}
	step := &storefrontSessionWorkflowServerResumeStorefrontSessionWorkflowBatchGetAccessPoliciesStep{request: &authorizationv1.BatchGetAccessPoliciesRequest{Context: req.GetContext()}}
	if err := s.runResumeStorefrontSessionWorkflowBatchGetAccessPolicies(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/BatchGetAccessPolicies")
	}
	batchGetAccessPoliciesReply := step.reply
	composePartnerIntegrationPageRequest := &partnerapiv1.ComposePartnerIntegrationPageRequest{
		Context: req.GetContext(),
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Contact: &commonv1.ContactInfo{Name: req.GetStorefrontSession().GetRequester().GetName()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetStorefrontSession().GetAudit().GetUpdatedAt(),
			},
		},
	}
	composePartnerIntegrationPageReply, err := s.partnerIntegrationGatewayPort.ComposePartnerIntegrationPage(ctx, composePartnerIntegrationPageRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationGatewayService/ComposePartnerIntegrationPage")
	}
	return &storefrontv1.ResumeStorefrontSessionWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details:       resolveCustomerSegmentConflictReply.GetResult().GetDetails(),
			Status:        batchGetAccessPoliciesReply.GetResult().GetStatus(),
			CorrelationId: batchGetAccessPoliciesReply.GetResult().GetCorrelationId(),
			Message:       composePartnerIntegrationPageReply.GetResult().GetMessage(),
		},
		Outcome: &storefrontv1.ResumeStorefrontSessionWorkflowResponse_Outcome{
			Message: composePartnerIntegrationPageReply.GetResult().GetMessage(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			StorefrontSessionId: req.GetStorefrontSessionId(),
			CustomFields:        req.GetStorefrontSession().GetCustomFields(),
		},
	}, nil
}

// ListStorefrontSessionWorkflows lists storefront sessions a page at a time.
func (s StorefrontSessionWorkflowServer) ListStorefrontSessionWorkflows(ctx context.Context, req *storefrontv1.ListStorefrontSessionWorkflowsRequest) (*storefrontv1.ListStorefrontSessionWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list storefront session workflows", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	step := &storefrontSessionWorkflowServerListStorefrontSessionWorkflowsRejectPartnerWebhookStep{request: &partnerapiv1.RejectPartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.RejectPartnerWebhookRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: cachedContext.GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
	}}
	if err := s.runListStorefrontSessionWorkflowsRejectPartnerWebhook(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/RejectPartnerWebhook")
	}
	rejectPartnerWebhookReply := step.reply
	return &storefrontv1.ListStorefrontSessionWorkflowsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: rejectPartnerWebhookReply.GetPartnerWebhook().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SignalStorefrontSessionWorkflow signals a storefront session.
func (s StorefrontSessionWorkflowServer) SignalStorefrontSessionWorkflow(ctx context.Context, req *storefrontv1.SignalStorefrontSessionWorkflowRequest) (*storefrontv1.SignalStorefrontSessionWorkflowResponse, error) {
	if err := s.store.SaveStorefrontSession(ctx, req.GetContext().GetTenantId(), req.GetStorefrontSession()); err != nil {
		return nil, grpcx.Wrap(err, "save storefront session")
	}
	s.log.InfoContext(ctx, "signal storefront session workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStorefrontSession().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "storefront.signal_storefront_session_workflow.update_access_policy",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"access_policy.owner.email": req.GetStorefrontSession().GetRequester().GetEmail(),
			"context.tenant_id":         req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	cloned := proto.Clone(req.GetStorefrontSession()).(*storefrontv1.StorefrontSession)
	var code string
	switch choice := req.GetStorefrontSession().GetOrigin().(type) {
	case *storefrontv1.StorefrontSession_Customer:
		code = choice.Customer.GetCode()
	case *storefrontv1.StorefrontSession_ExternalReference:
		code = choice.ExternalReference
	case *storefrontv1.StorefrontSession_Channel:
		code = choice.Channel.GetChannel()
	}
	packed, err := anypb.New(req.GetStorefrontSession())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	lintSearchQueriesRequest := newLintSearchQueriesRequest(withLintSearchQueriesContext(req.GetContext()), withLintSearchQueriesDetails(&searchv1.LintSearchQueriesRequest_Details{
		Contact: &commonv1.ContactInfo{Email: cloned.GetRequester().GetEmail()},
	}), withLintSearchQueriesSearchQuery(&searchv1.SearchQuery{
		Subject: &searchv1.SearchIndexRef{
			Code:        code,
			DisplayName: req.GetStorefrontSession().GetAssignee().GetDisplayName(),
		},
		Detail: packed,
	}))
	lintSearchQueriesReply, err := s.searchQueryValidationClient.LintSearchQueries(ctx, lintSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/LintSearchQueries")
	}
	return &storefrontv1.SignalStorefrontSessionWorkflowResponse{
		Outcome: &storefrontv1.SignalStorefrontSessionWorkflowResponse_Outcome{
			Message: lintSearchQueriesReply.GetOutcome().GetMessage(),
			Status:  lintSearchQueriesReply.GetResult().GetStatus(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			StorefrontSessionId: req.GetStorefrontSessionId(),
			Subject:             req.GetStorefrontSession().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// storefrontSessionWorkflowServerStartStorefrontSessionWorkflowNotifySessionWatchersStep carries the request of one
// step of StartStorefrontSessionWorkflow and, once it ran, its reply.
type storefrontSessionWorkflowServerStartStorefrontSessionWorkflowNotifySessionWatchersStep struct {
	request *identityv1.NotifySessionWatchersRequest
	reply   *identityv1.NotifySessionWatchersResponse
}

func (s StorefrontSessionWorkflowServer) runStartStorefrontSessionWorkflowNotifySessionWatchers(ctx context.Context, step *storefrontSessionWorkflowServerStartStorefrontSessionWorkflowNotifySessionWatchersStep) error {
	reply, err := s.sessionNotificationClient.NotifySessionWatchers(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// storefrontSessionWorkflowServerResumeStorefrontSessionWorkflowBatchGetAccessPoliciesStep carries the request of one
// step of ResumeStorefrontSessionWorkflow and, once it ran, its reply.
type storefrontSessionWorkflowServerResumeStorefrontSessionWorkflowBatchGetAccessPoliciesStep struct {
	request *authorizationv1.BatchGetAccessPoliciesRequest
	reply   *authorizationv1.BatchGetAccessPoliciesResponse
}

func (s StorefrontSessionWorkflowServer) runResumeStorefrontSessionWorkflowBatchGetAccessPolicies(ctx context.Context, step *storefrontSessionWorkflowServerResumeStorefrontSessionWorkflowBatchGetAccessPoliciesStep) error {
	reply, err := s.accessPolicyClient.BatchGetAccessPolicies(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// storefrontSessionWorkflowServerListStorefrontSessionWorkflowsRejectPartnerWebhookStep carries the request of one step
// of ListStorefrontSessionWorkflows and, once it ran, its reply.
type storefrontSessionWorkflowServerListStorefrontSessionWorkflowsRejectPartnerWebhookStep struct {
	request *partnerapiv1.RejectPartnerWebhookRequest
	reply   *partnerapiv1.RejectPartnerWebhookResponse
}

func (s StorefrontSessionWorkflowServer) runListStorefrontSessionWorkflowsRejectPartnerWebhook(ctx context.Context, step *storefrontSessionWorkflowServerListStorefrontSessionWorkflowsRejectPartnerWebhookStep) error {
	reply, err := s.partnerWebhookApprovalClient.RejectPartnerWebhook(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
