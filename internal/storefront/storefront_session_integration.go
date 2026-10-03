package storefront

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	patientappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patientapp/v1"
	patientsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patients/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// StorefrontSessionIntegrationServer implements `acme.storefront.v1.StorefrontSessionIntegrationService`.
// Connects external systems that send storefront session events.
type StorefrontSessionIntegrationServer struct {
	storefrontv1.UnimplementedStorefrontSessionIntegrationServiceServer
	store                             *Store
	log                               *slog.Logger
	partnerWebhookNotificationClient  partnerapiv1.PartnerWebhookNotificationServiceClient
	accessPolicyValidationClient      authorizationv1.AccessPolicyValidationServiceClient
	storefrontSessionWorkflowClient   storefrontv1.StorefrontSessionWorkflowServiceClient
	customerSegmentReportingClient    customersv1.CustomerSegmentReportingServiceClient
	shopperViewWorkflowClient         storefrontv1.ShopperViewWorkflowServiceClient
	shopperViewWorkflowClientFallback storefrontv1.ShopperViewWorkflowServiceClient
	patientHomeClient                 patientappv1.PatientHomeServiceClient
	patientHomeQueryClient            patientappv1.PatientHomeQueryServiceClient
	householdQueryClient              customersv1.HouseholdQueryServiceClient
	tenantNotificationClient          identityv1.TenantNotificationServiceClient
	customerApprovalClient            customersv1.CustomerApprovalServiceClient
	clients                           *registry.Registry
	landingPageReportingClient        storefrontv1.LandingPageReportingServiceClient
	meteredStorefrontSessionWorkflow  meteredStorefrontSessionWorkflow
}

// NewStorefrontSessionIntegrationServer wires a StorefrontSessionIntegrationServer.
func NewStorefrontSessionIntegrationServer(deps platform.Deps) *StorefrontSessionIntegrationServer {
	return &StorefrontSessionIntegrationServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.storefront.v1.StorefrontSessionIntegrationService"),
		partnerWebhookNotificationClient:  partnerapiv1.NewPartnerWebhookNotificationServiceClient(deps.Conn),
		accessPolicyValidationClient:      authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		storefrontSessionWorkflowClient:   storefrontv1.NewStorefrontSessionWorkflowServiceClient(deps.Conn),
		customerSegmentReportingClient:    customersv1.NewCustomerSegmentReportingServiceClient(deps.Conn),
		shopperViewWorkflowClient:         storefrontv1.NewShopperViewWorkflowServiceClient(deps.Conn),
		shopperViewWorkflowClientFallback: storefrontv1.NewShopperViewWorkflowServiceClient(deps.Conn),
		patientHomeClient:                 patientappv1.NewPatientHomeServiceClient(deps.Conn),
		patientHomeQueryClient:            patientappv1.NewPatientHomeQueryServiceClient(deps.Conn),
		householdQueryClient:              customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		tenantNotificationClient:          identityv1.NewTenantNotificationServiceClient(deps.Conn),
		customerApprovalClient:            customersv1.NewCustomerApprovalServiceClient(deps.Conn),
		clients:                           deps.Clients,
		landingPageReportingClient:        storefrontv1.NewLandingPageReportingServiceClient(deps.Conn),
		meteredStorefrontSessionWorkflow:  meteredStorefrontSessionWorkflow{StorefrontSessionWorkflowServiceClient: storefrontv1.NewStorefrontSessionWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// ConnectStorefrontSessionSource connects a storefront session.
func (s *StorefrontSessionIntegrationServer) ConnectStorefrontSessionSource(ctx context.Context, req *storefrontv1.ConnectStorefrontSessionSourceRequest) (*storefrontv1.ConnectStorefrontSessionSourceResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ConnectStorefrontSessionSource", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "connect_storefront_session_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "connect storefront session source", "tenant", req.GetContext().GetTenantId())
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	streamPartnerWebhookNoticesStream, err := s.partnerWebhookNotificationClient.StreamPartnerWebhookNotices(ctx, &partnerapiv1.StreamPartnerWebhookNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Since:      req.GetDetails().GetReceivedAt(),
		Filter:     &partnerapiv1.StreamPartnerWebhookNoticesRequest_Filter{Query: envelope.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookNotificationService/StreamPartnerWebhookNotices")
	}
	for {
		received, err := streamPartnerWebhookNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookNotificationService/StreamPartnerWebhookNotices")
		}
		_ = received
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintAccessPoliciesRequest := &authorizationv1.LintAccessPoliciesRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Action: &authorizationv1.AccessPolicy_Block{Block: &authorizationv1.AccessPolicyBlockAction{Reason: req.GetReason()}},
		},
		Details: &authorizationv1.LintAccessPoliciesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		AccessPolicyId: envelope2.GetDisplayName(),
	}
	lintAccessPoliciesReply, err := grpcx.Call(ctx, s.accessPolicyValidationClient.LintAccessPolicies, lintAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
	}
	startStorefrontSessionWorkflowRequest := buildStartStorefrontSessionWorkflowRequestForConnectStorefrontSessionSource(req, lintAccessPoliciesReply)
	startStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowClient.StartStorefrontSessionWorkflow(ctx, startStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/StartStorefrontSessionWorkflow")
	}
	return &storefrontv1.ConnectStorefrontSessionSourceResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lintAccessPoliciesReply.GetAccessPolicy().GetAudit().GetUpdatedAt(),
			},
			Sla: &storefrontv1.StorefrontSessionSla{
				Breached: startStorefrontSessionWorkflowReply.GetStorefrontSession().GetSla().GetBreached(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
		},
		Outcome: &storefrontv1.ConnectStorefrontSessionSourceResponse_Outcome{
			Status: startStorefrontSessionWorkflowReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DisconnectStorefrontSessionSource disconnects a storefront session.
func (s *StorefrontSessionIntegrationServer) DisconnectStorefrontSessionSource(ctx context.Context, req *storefrontv1.DisconnectStorefrontSessionSourceRequest) (*storefrontv1.DisconnectStorefrontSessionSourceResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "disconnect_storefront_session_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "disconnect storefront session source", "tenant", req.GetContext().GetTenantId())
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	generateCustomerSegmentReportReply, err := s.customerSegmentReportingClient.GenerateCustomerSegmentReport(ctx, &customersv1.GenerateCustomerSegmentReportRequest{
		Context:           req.GetContext(),
		Annotations:       req.GetAnnotations(),
		Reason:            req.GetReason(),
		Details:           &customersv1.GenerateCustomerSegmentReportRequest_Details{Filters: req.GetContext().GetBaggage()},
		CustomerSegmentId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentReportingService/GenerateCustomerSegmentReport")
	}
	routes := map[string]func(context.Context, *storefrontv1.ListShopperViewWorkflowsRequest, ...grpc.CallOption) (*storefrontv1.ListShopperViewWorkflowsResponse, error){
		"primary":  s.shopperViewWorkflowClient.ListShopperViewWorkflows,
		"fallback": s.shopperViewWorkflowClientFallback.ListShopperViewWorkflows,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listShopperViewWorkflowsRequest := &storefrontv1.ListShopperViewWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.ListShopperViewWorkflowsRequest_Filter{
			CreatedAfter: req.GetDetails().GetReceivedAt(),
			OwnerId:      generateCustomerSegmentReportReply.GetResult().GetCorrelationId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	listShopperViewWorkflowsReply, err := route(ctx, listShopperViewWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/ListShopperViewWorkflows")
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	updatePatientHomeRequest := &patientappv1.UpdatePatientHomeRequest{}
	updatePatientHomeRequest.Context = req.GetContext()
	updatePatientHomeRequest.PatientHome = &patientappv1.PatientHome{
		Origin:   &patientappv1.PatientHome_Customer{Customer: &patientsv1.PatientRef{Email: req.GetContext().GetActor().GetEmail()}},
		Assignee: &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
	}
	updatePatientHomeReply, err := s.patientHomeClient.UpdatePatientHome(ctx, updatePatientHomeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.patientapp.v1.PatientHomeService/UpdatePatientHome")
	}
	return &storefrontv1.DisconnectStorefrontSessionSourceResponse{
		Result: &commonv1.OperationResult{
			Message: generateCustomerSegmentReportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &storefrontv1.DisconnectStorefrontSessionSourceResponse_Outcome{
			Message: listShopperViewWorkflowsReply.GetResult().GetMessage(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Sla: &storefrontv1.StorefrontSessionSla{
				PausedFor: updatePatientHomeReply.GetPatientHome().GetSla().GetPausedFor(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
		},
	}, nil
}

// ReplayStorefrontSessionEvents replays a storefront session.
func (s *StorefrontSessionIntegrationServer) ReplayStorefrontSessionEvents(ctx context.Context, req *storefrontv1.ReplayStorefrontSessionEventsRequest) (*storefrontv1.ReplayStorefrontSessionEventsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReplayStorefrontSessionEvents", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "replay_storefront_session_events"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "replay storefront session events", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	exportPatientHomesStream, err := s.patientHomeQueryClient.ExportPatientHomes(ctx, &patientappv1.ExportPatientHomesRequest{
		Context: req.GetContext(),
		Filter: &patientappv1.ExportPatientHomesRequest_Filter{
			CreatedAfter: req.GetDetails().GetReceivedAt(),
			OwnerId:      req.GetDetails().GetMessageId(),
			Labels:       annotations,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.patientapp.v1.PatientHomeQueryService/ExportPatientHomes")
	}
	for {
		received, err := exportPatientHomesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.patientapp.v1.PatientHomeQueryService/ExportPatientHomes")
		}
		_ = received
	}
	annotations2 := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations2[key2] = value2
	}
	signalStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowClient.SignalStorefrontSessionWorkflow(ctx, &storefrontv1.SignalStorefrontSessionWorkflowRequest{
		Context:           req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{StorefrontSessionId: req.GetStorefrontSessionId()},
		Details: &storefrontv1.SignalStorefrontSessionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations: annotations2,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/SignalStorefrontSessionWorkflow")
	}
	resolveHouseholdsReply, err := s.householdQueryClient.ResolveHouseholds(ctx, &customersv1.ResolveHouseholdsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ResolveHouseholds")
	}
	return &storefrontv1.ReplayStorefrontSessionEventsResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: signalStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetPreferredChannel(),
			},
			StorefrontSessionId: signalStorefrontSessionWorkflowReply.GetStorefrontSession().GetStorefrontSessionId(),
		},
		Result: &commonv1.OperationResult{Status: resolveHouseholdsReply.GetResult().GetStatus()},
	}, nil
}

// AcknowledgeStorefrontSessionEvent acknowledges a storefront session.
func (s *StorefrontSessionIntegrationServer) AcknowledgeStorefrontSessionEvent(ctx context.Context, req *storefrontv1.AcknowledgeStorefrontSessionEventRequest) (*storefrontv1.AcknowledgeStorefrontSessionEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "acknowledge_storefront_session_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "acknowledge storefront session event", "tenant", req.GetContext().GetTenantId())
	subscribeTenantUpdatesRequest := &identityv1.SubscribeTenantUpdatesRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	var subscribeTenantUpdatesReply *identityv1.SubscribeTenantUpdatesResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.TenantNotificationService/SubscribeTenantUpdates", func(ctx context.Context) error {
		answer, err := s.tenantNotificationClient.SubscribeTenantUpdates(ctx, subscribeTenantUpdatesRequest)
		subscribeTenantUpdatesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/SubscribeTenantUpdates")
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	approveCustomerReply, err := s.customerApprovalClient.ApproveCustomer(ctx, &customersv1.ApproveCustomerRequest{
		Context: req.GetContext(),
		Details: &customersv1.ApproveCustomerRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: subscribeTenantUpdatesReply.GetTenant().GetManager().GetDisplayName(),
			},
		},
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: subscribeTenantUpdatesReply.GetTenant().GetLocation().GetPostalCode(),
			},
		},
		CustomerId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerApprovalService/ApproveCustomer")
	}
	storefrontSessionWorkflow, ok := s.clients.Get("acme.storefront.v1.StorefrontSessionWorkflowService").(storefrontv1.StorefrontSessionWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.storefront.v1.StorefrontSessionWorkflowService")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pauseStorefrontSessionWorkflowRequest := &storefrontv1.PauseStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: subscribeTenantUpdatesReply.GetTenant().GetAudit().GetUpdatedBy(),
			},
		},
		StorefrontSessionId: envelope2.GetDisplayName(),
	}
	pauseStorefrontSessionWorkflowReply, err := storefrontSessionWorkflow.PauseStorefrontSessionWorkflow(ctx, pauseStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/PauseStorefrontSessionWorkflow")
	}
	resumeStorefrontSessionWorkflowRequest := buildResumeStorefrontSessionWorkflowRequestForAcknowledgeStorefrontSessionEvent(req, approveCustomerReply, pauseStorefrontSessionWorkflowReply)
	resumeStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowClient.ResumeStorefrontSessionWorkflow(ctx, resumeStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/ResumeStorefrontSessionWorkflow")
	}
	return &storefrontv1.AcknowledgeStorefrontSessionEventResponse{
		Outcome: &storefrontv1.AcknowledgeStorefrontSessionEventResponse_Outcome{
			Message:     subscribeTenantUpdatesReply.GetResult().GetMessage(),
			ReferenceId: approveCustomerReply.GetOutcome().GetReferenceId(),
			EffectiveAt: resumeStorefrontSessionWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				CreatedBy: subscribeTenantUpdatesReply.GetTenant().GetAudit().GetCreatedBy(),
			},
			Requester: &commonv1.ContactInfo{
				Phone: pauseStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetPhone(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RelayStorefrontSessionMessages exchanges changes of storefront sessions with a peer.
func (s *StorefrontSessionIntegrationServer) RelayStorefrontSessionMessages(stream grpc.BidiStreamingServer[storefrontv1.RelayStorefrontSessionMessagesRequest, storefrontv1.RelayStorefrontSessionMessagesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *storefrontv1.RelayStorefrontSessionMessagesRequest)
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
		s.log.InfoContext(ctx, "relay storefront session messages", "tenant", msg.GetContext().GetTenantId())
		group, groupCtx := errgroup.WithContext(ctx)
		group.SetLimit(8)
		replies := make([]*storefrontv1.RetryStorefrontSessionWorkflowResponse, len(msg.GetChange().GetHistory()))
		for i, item := range msg.GetChange().GetHistory() {
			group.Go(func() error {
				reply, err := s.storefrontSessionWorkflowClient.RetryStorefrontSessionWorkflow(groupCtx, &storefrontv1.RetryStorefrontSessionWorkflowRequest{
					Context: msg.GetContext(),
					StorefrontSession: &storefrontv1.StorefrontSession{
						History: []*storefrontv1.StorefrontSessionStatusChange{&storefrontv1.StorefrontSessionStatusChange{
							ChangedAt:  item.GetChangedAt(),
							FromStatus: item.GetFromStatus(),
							ToStatus:   item.GetToStatus(),
						}},
					},
				})
				if err != nil {
					return err
				}
				replies[i] = reply
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/RetryStorefrontSessionWorkflow")
		}
		s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
		for _, comment := range msg.GetChange().GetComments() {
			if _, err := s.storefrontSessionWorkflowClient.SignalStorefrontSessionWorkflow(ctx, &storefrontv1.SignalStorefrontSessionWorkflowRequest{
				Context: msg.GetContext(),
				StorefrontSession: &storefrontv1.StorefrontSession{
					Comments: []*storefrontv1.StorefrontSessionComment{&storefrontv1.StorefrontSessionComment{
						AuthorEmail: comment.GetAuthorEmail(),
						CommentId:   comment.GetCommentId(),
					}},
					Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
				},
			}); err != nil {
				return grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/SignalStorefrontSessionWorkflow")
			}
		}
		pauseStorefrontSessionWorkflowRequest := &storefrontv1.PauseStorefrontSessionWorkflowRequest{}
		pauseStorefrontSessionWorkflowRequest.Context = msg.GetContext()
		if _, err := s.storefrontSessionWorkflowClient.PauseStorefrontSessionWorkflow(ctx, pauseStorefrontSessionWorkflowRequest); err != nil {
			return grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/PauseStorefrontSessionWorkflow")
		}
		for comment2 := range iterx.Each(msg.GetChange().GetComments(), func(candidate *storefrontv1.StorefrontSessionComment) bool { return candidate != nil }) {
			if _, err := s.landingPageReportingClient.ListLandingPageReports(ctx, &storefrontv1.ListLandingPageReportsRequest{
				Context: msg.GetContext(),
				Filter: &storefrontv1.ListLandingPageReportsRequest_Filter{
					CreatedAfter: comment2.GetCreatedAt(),
					OwnerId:      comment2.GetCommentId(),
				},
			}); err != nil {
				return grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/ListLandingPageReports")
			}
		}
		if err := stream.Send(&storefrontv1.RelayStorefrontSessionMessagesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
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

// ListStorefrontSessionSources lists storefront sessions a page at a time.
func (s *StorefrontSessionIntegrationServer) ListStorefrontSessionSources(ctx context.Context, req *storefrontv1.ListStorefrontSessionSourcesRequest) (*storefrontv1.ListStorefrontSessionSourcesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list storefront session sources", "tenant", req.GetContext().GetTenantId())
	advanceStorefrontSessionWorkflowRequest := buildAdvanceStorefrontSessionWorkflowRequestForListStorefrontSessionSources(req)
	advanceStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowClient.AdvanceStorefrontSessionWorkflow(ctx, advanceStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/AdvanceStorefrontSessionWorkflow")
	}
	return &storefrontv1.ListStorefrontSessionSourcesResponse{
		Result: &commonv1.OperationResult{
			Message: advanceStorefrontSessionWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// TestStorefrontSessionConnection tests a storefront session.
func (s *StorefrontSessionIntegrationServer) TestStorefrontSessionConnection(ctx context.Context, req *storefrontv1.TestStorefrontSessionConnectionRequest) (*storefrontv1.TestStorefrontSessionConnectionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "storefront_session", req.GetStorefrontSessionId(), "test_storefront_session_connection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "test storefront session connection", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	escalateCustomerSegmentRequest := &customersv1.EscalateCustomerSegmentRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetReason()}},
		},
		Details: &customersv1.EscalateCustomerSegmentRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		CustomerSegmentId: envelope.GetDisplayName(),
	}
	escalateCustomerSegmentReply, err := escalateCustomerSegmentCall(ctx, escalateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentApprovalService/EscalateCustomerSegment")
	}
	pauseStorefrontSessionWorkflowRequest := &storefrontv1.PauseStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: escalateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
				CreatedBy: escalateCustomerSegmentReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
			},
			Priority: escalateCustomerSegmentReply.GetCustomerSegment().GetPriority(),
		},
		Details: &storefrontv1.PauseStorefrontSessionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: escalateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
		},
	}
	pauseStorefrontSessionWorkflowReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*storefrontv1.PauseStorefrontSessionWorkflowResponse, error) {
		return s.storefrontSessionWorkflowClient.PauseStorefrontSessionWorkflow(ctx, pauseStorefrontSessionWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/PauseStorefrontSessionWorkflow")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryStorefrontSessionWorkflowRequest := &storefrontv1.RetryStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				Name: pauseStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetName(),
			},
		},
		StorefrontSessionId: envelope2.GetDisplayName(),
	}
	retryStorefrontSessionWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*storefrontv1.RetryStorefrontSessionWorkflowResponse, error) {
		return s.storefrontSessionWorkflowClient.RetryStorefrontSessionWorkflow(ctx, retryStorefrontSessionWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/RetryStorefrontSessionWorkflow")
	}
	cancelStorefrontSessionWorkflowRequest := &storefrontv1.CancelStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: pauseStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetUpdatedAt(),
			},
			Origin: &storefrontv1.StorefrontSession_Customer{Customer: &storefrontv1.ShopperViewRef{
				ShopperViewId: retryStorefrontSessionWorkflowReply.GetStorefrontSession().GetCustomer().GetShopperViewId(),
			}},
		},
	}
	cancelStorefrontSessionWorkflowReply, err := s.meteredStorefrontSessionWorkflow.CancelStorefrontSessionWorkflow(ctx, cancelStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/CancelStorefrontSessionWorkflow")
	}
	return &storefrontv1.TestStorefrontSessionConnectionResponse{
		StorefrontSession: &storefrontv1.StorefrontSession{
			Priority: escalateCustomerSegmentReply.GetCustomerSegment().GetPriority(),
			Sla: &storefrontv1.StorefrontSessionSla{
				PausedFor: pauseStorefrontSessionWorkflowReply.GetStorefrontSession().GetSla().GetPausedFor(),
			},
			Assignee: retryStorefrontSessionWorkflowReply.GetStorefrontSession().GetAssignee(),
			Requester: &commonv1.ContactInfo{
				Email: cancelStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetEmail(),
			},
			StorefrontSessionId: req.GetStorefrontSessionId(),
		},
		Outcome: &storefrontv1.TestStorefrontSessionConnectionResponse_Outcome{
			Message: escalateCustomerSegmentReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
