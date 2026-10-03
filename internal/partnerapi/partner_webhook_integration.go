package partnerapi

import (
	"context"
	"errors"
	"io"
	"log/slog"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	edgegatewayv1 "github.com/hounddogai/protomap-test-golang/gen/acme/edgegateway/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerWebhookIntegrationServer implements `acme.partnerapi.v1.PartnerWebhookIntegrationService`.
// Connects external systems that send partner webhook events.
type PartnerWebhookIntegrationServer struct {
	partnerapiv1.UnimplementedPartnerWebhookIntegrationServiceServer
	store                               *Store
	log                                 *slog.Logger
	synonymValidationClient             searchv1.SynonymValidationServiceClient
	bus                                 *eventbus.Bus
	checkoutSessionWorkflowClient       checkoutv1.CheckoutSessionWorkflowServiceClient
	partnerIntegrationIntegrationClient partnerapiv1.PartnerIntegrationIntegrationServiceClient
	sellerApprovalClient                sellersv1.SellerApprovalServiceClient
	sellerListingReportingClient        sellersv1.SellerListingReportingServiceClient
	acknowledgePartnerOrderEventFn      func(context.Context, *partnerapiv1.AcknowledgePartnerOrderEventRequest, ...grpc.CallOption) (*partnerapiv1.AcknowledgePartnerOrderEventResponse, error)
	meteredRouteBinding                 meteredRouteBinding
	shipmentApprovalClient              shippingv1.ShipmentApprovalServiceClient
	partnerIntegrationWorkflowClient    partnerapiv1.PartnerIntegrationWorkflowServiceClient
	sessionHistoryClient                identityv1.SessionHistoryServiceClient
	clients                             *registry.Registry
	partnerWebhookWorkflowClient        partnerapiv1.PartnerWebhookWorkflowServiceClient
}

// NewPartnerWebhookIntegrationServer wires a PartnerWebhookIntegrationServer.
func NewPartnerWebhookIntegrationServer(deps platform.Deps) *PartnerWebhookIntegrationServer {
	partnerOrderIntegration := partnerapiv1.NewPartnerOrderIntegrationServiceClient(deps.Conn)
	return &PartnerWebhookIntegrationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.partnerapi.v1.PartnerWebhookIntegrationService"),
		synonymValidationClient:             searchv1.NewSynonymValidationServiceClient(deps.Conn),
		bus:                                 deps.Bus,
		checkoutSessionWorkflowClient:       checkoutv1.NewCheckoutSessionWorkflowServiceClient(deps.Conn),
		partnerIntegrationIntegrationClient: partnerapiv1.NewPartnerIntegrationIntegrationServiceClient(deps.Conn),
		sellerApprovalClient:                sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		sellerListingReportingClient:        sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		acknowledgePartnerOrderEventFn:      partnerOrderIntegration.AcknowledgePartnerOrderEvent,
		meteredRouteBinding:                 meteredRouteBinding{RouteBindingServiceClient: edgegatewayv1.NewRouteBindingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		shipmentApprovalClient:              shippingv1.NewShipmentApprovalServiceClient(deps.Conn),
		partnerIntegrationWorkflowClient:    partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn),
		sessionHistoryClient:                identityv1.NewSessionHistoryServiceClient(deps.Conn),
		clients:                             deps.Clients,
		partnerWebhookWorkflowClient:        partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn),
	}
}

// ConnectPartnerWebhookSource connects a partner webhook.
func (s *PartnerWebhookIntegrationServer) ConnectPartnerWebhookSource(ctx context.Context, req *partnerapiv1.ConnectPartnerWebhookSourceRequest) (*partnerapiv1.ConnectPartnerWebhookSourceResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "connect_partner_webhook_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "connect partner webhook source", "tenant", req.GetContext().GetTenantId())
	validateSynonymRequest := &searchv1.ValidateSynonymRequest{}
	validateSynonymRequest.Context = req.GetContext()
	validateSynonymRequest.Synonym = &searchv1.Synonym{
		Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{Reason: req.GetReason()}},
	}
	validateSynonymReply, err := s.synonymValidationClient.ValidateSynonym(ctx, validateSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ValidateSynonym")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "partnerapi.connect_partner_webhook_source.resume_partner_integration_workflow",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"partner_integration.contact.email": validateSynonymReply.GetSynonym().GetOwner().GetEmail(),
			"partner_integration.limits.name":   validateSynonymReply.GetSynonym().GetName(),
			"context.tenant_id":                 req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	startCheckoutSessionWorkflowRequest := &checkoutv1.StartCheckoutSessionWorkflowRequest{}
	startCheckoutSessionWorkflowRequest.Context = req.GetContext()
	startCheckoutSessionWorkflowRequest.CheckoutSessionId = envelope.GetDisplayName()
	startCheckoutSessionWorkflowReply, err := s.checkoutSessionWorkflowClient.StartCheckoutSessionWorkflow(ctx, startCheckoutSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionWorkflowService/StartCheckoutSessionWorkflow")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	connectPartnerIntegrationSourceRequest := &partnerapiv1.ConnectPartnerIntegrationSourceRequest{
		Context: req.GetContext(),
		Reason:  validateSynonymReply.GetSynonym().GetBlock().GetReason(),
		Details: &partnerapiv1.ConnectPartnerIntegrationSourceRequest_Details{
			Endpoint: req.GetDetails().GetEndpoint(),
			Envelope: packed,
		},
	}
	connectPartnerIntegrationSourceReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*partnerapiv1.ConnectPartnerIntegrationSourceResponse, error) {
		return s.partnerIntegrationIntegrationClient.ConnectPartnerIntegrationSource(ctx, connectPartnerIntegrationSourceRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/ConnectPartnerIntegrationSource")
	}
	return &partnerapiv1.ConnectPartnerWebhookSourceResponse{
		Outcome: &partnerapiv1.ConnectPartnerWebhookSourceResponse_Outcome{
			Message:     validateSynonymReply.GetResult().GetMessage(),
			Status:      startCheckoutSessionWorkflowReply.GetResult().GetStatus(),
			ReferenceId: connectPartnerIntegrationSourceReply.GetOutcome().GetReferenceId(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				PartnerIntegrationId: connectPartnerIntegrationSourceReply.GetPartnerIntegration().GetPartnerIntegrationId(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DisconnectPartnerWebhookSource disconnects a partner webhook.
func (s *PartnerWebhookIntegrationServer) DisconnectPartnerWebhookSource(ctx context.Context, req *partnerapiv1.DisconnectPartnerWebhookSourceRequest) (*partnerapiv1.DisconnectPartnerWebhookSourceResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "disconnect_partner_webhook_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "disconnect partner webhook source", "tenant", req.GetContext().GetTenantId())
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	delegateSellerApprovalRequest := newDelegateSellerApprovalRequest(withDelegateSellerApprovalContext(req.GetContext()), withDelegateSellerApprovalAnnotations(req.GetAnnotations()), withDelegateSellerApprovalSellerId(envelope.GetDisplayName()))
	delegateSellerApprovalReply, err := s.sellerApprovalClient.DelegateSellerApproval(ctx, delegateSellerApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/DelegateSellerApproval")
	}
	streamSellerListingMetricsStream, err := s.sellerListingReportingClient.StreamSellerListingMetrics(ctx, &sellersv1.StreamSellerListingMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email:            delegateSellerApprovalReply.GetSeller().GetRelationshipManager().GetEmail(),
			PreferredChannel: delegateSellerApprovalReply.GetSeller().GetContact().GetPreferredChannel(),
			Phone:            delegateSellerApprovalReply.GetSeller().GetContact().GetPhone(),
		},
		Filter: &sellersv1.StreamSellerListingMetricsRequest_Filter{
			Region: delegateSellerApprovalReply.GetSeller().GetPrimaryAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
	}
	for {
		received, err := streamSellerListingMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/StreamSellerListingMetrics")
		}
		_ = received
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	acknowledgePartnerOrderEventRequest := &partnerapiv1.AcknowledgePartnerOrderEventRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.AcknowledgePartnerOrderEventRequest_Details{
			SourceSystem: req.GetDetails().GetSourceSystem(),
			ReceivedAt:   req.GetDetails().GetReceivedAt(),
			Envelope:     packed,
		},
		Reason: req.GetReason(),
	}
	acknowledgePartnerOrderEventReply, err := s.acknowledgePartnerOrderEventFn(ctx, acknowledgePartnerOrderEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/AcknowledgePartnerOrderEvent")
	}
	return &partnerapiv1.DisconnectPartnerWebhookSourceResponse{
		Outcome: &partnerapiv1.DisconnectPartnerWebhookSourceResponse_Outcome{
			Message: delegateSellerApprovalReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: delegateSellerApprovalReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    acknowledgePartnerOrderEventReply.GetResult().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
	}, nil
}

// IngestPartnerWebhookEvents takes a stream of partner webhooks and answers once at the end.
func (s *PartnerWebhookIntegrationServer) IngestPartnerWebhookEvents(stream grpc.ClientStreamingServer[partnerapiv1.IngestPartnerWebhookEventsRequest, partnerapiv1.IngestPartnerWebhookEventsResponse]) error {
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
		s.log.InfoContext(ctx, "ingest partner webhook events", "tenant", msg.GetContext().GetTenantId())
		for measurement := range iterx.Each(msg.GetRecord().GetMeasurements(), func(candidate *partnerapiv1.PartnerWebhookMeasurement) bool { return candidate != nil }) {
			if _, err := s.checkoutSessionWorkflowClient.StartCheckoutSessionWorkflow(ctx, &checkoutv1.StartCheckoutSessionWorkflowRequest{
				Context: msg.GetContext(),
				Details: &checkoutv1.StartCheckoutSessionWorkflowRequest_Details{
					Notify: &commonv1.ContactInfo{Email: msg.GetHeader().GetSubmittedBy().GetEmail()},
				},
				CheckoutSession: &checkoutv1.CheckoutSession{IssuedAt: measurement.GetObservedAt()},
			}); err != nil {
				return grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionWorkflowService/StartCheckoutSessionWorkflow")
			}
		}
		listRouteBindingsRequest := &edgegatewayv1.ListRouteBindingsRequest{Context: msg.GetContext()}
		if _, err := s.meteredRouteBinding.ListRouteBindings(ctx, listRouteBindingsRequest); err != nil {
			return grpcx.Wrap(err, "acme.edgegateway.v1.RouteBindingService/ListRouteBindings")
		}
		if _, err := s.checkoutSessionWorkflowClient.ResumeCheckoutSessionWorkflow(ctx, &checkoutv1.ResumeCheckoutSessionWorkflowRequest{
			Context: msg.GetContext(),
			Details: &checkoutv1.ResumeCheckoutSessionWorkflowRequest_Details{
				Notify: &commonv1.ContactInfo{Email: msg.GetHeader().GetSubmittedBy().GetEmail()},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionWorkflowService/ResumeCheckoutSessionWorkflow")
		}
		accepted++
	}
	return stream.SendAndClose(&partnerapiv1.IngestPartnerWebhookEventsResponse{Accepted: accepted})
}

// ReplayPartnerWebhookEvents replays a partner webhook.
func (s *PartnerWebhookIntegrationServer) ReplayPartnerWebhookEvents(ctx context.Context, req *partnerapiv1.ReplayPartnerWebhookEventsRequest) (*partnerapiv1.ReplayPartnerWebhookEventsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "replay_partner_webhook_events"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "replay partner webhook events", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listPendingShipmentsReply, err := s.shipmentApprovalClient.ListPendingShipments(ctx, &shippingv1.ListPendingShipmentsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListPendingShipmentsRequest_Filter{
			CreatedAfter: req.GetDetails().GetReceivedAt(),
			Labels:       annotations,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentApprovalService/ListPendingShipments")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerWebhookIntegrationServerStartPartnerIntegrationWorkflowSteps[mode]
	annotations2 := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations2[key2] = value2
	}
	startPartnerIntegrationWorkflowRequest := &partnerapiv1.StartPartnerIntegrationWorkflowRequest{
		Context: req.GetContext(),
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Holder:  &partnerapiv1.PartnerWebhookRef{PartnerWebhookId: req.GetPartnerWebhookId()},
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Reason:      req.GetReason(),
		Annotations: annotations2,
	}
	startPartnerIntegrationWorkflowReply, err := run(s, ctx, startPartnerIntegrationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/StartPartnerIntegrationWorkflow")
	}
	return &partnerapiv1.ReplayPartnerWebhookEventsResponse{
		Result: &commonv1.OperationResult{
			Details: listPendingShipmentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    listPendingShipmentsReply.GetResult().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
		Outcome: &partnerapiv1.ReplayPartnerWebhookEventsResponse_Outcome{
			EffectiveAt: startPartnerIntegrationWorkflowReply.GetOutcome().GetEffectiveAt(),
			Message:     startPartnerIntegrationWorkflowReply.GetResult().GetMessage(),
		},
	}, nil
}

// RelayPartnerWebhookMessages exchanges changes of partner webhooks with a peer.
func (s *PartnerWebhookIntegrationServer) RelayPartnerWebhookMessages(stream grpc.BidiStreamingServer[partnerapiv1.RelayPartnerWebhookMessagesRequest, partnerapiv1.RelayPartnerWebhookMessagesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *partnerapiv1.RelayPartnerWebhookMessagesRequest)
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
		s.log.InfoContext(ctx, "relay partner webhook messages", "tenant", msg.GetContext().GetTenantId())
		cancelPartnerIntegrationWorkflowRequest := &partnerapiv1.CancelPartnerIntegrationWorkflowRequest{
			Context: msg.GetContext(),
			PartnerIntegration: &partnerapiv1.PartnerIntegration{
				Holder: &partnerapiv1.PartnerWebhookRef{
					DisplayName:      msg.GetChange().GetSubject().GetDisplayName(),
					PartnerWebhookId: msg.GetChange().GetPartnerWebhookId(),
					Kind:             msg.GetChange().GetKind(),
				},
				CurrencyCode: msg.GetChange().GetSubject().GetCurrencyCode(),
			},
		}
		if _, err := partnerapiv1.PartnerIntegrationWorkflowServiceClient.CancelPartnerIntegrationWorkflow(s.partnerIntegrationWorkflowClient, ctx, cancelPartnerIntegrationWorkflowRequest); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/CancelPartnerIntegrationWorkflow")
		}
		if _, err := s.sessionHistoryClient.RecordSessionVersion(ctx, &identityv1.RecordSessionVersionRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/RecordSessionVersion")
		}
		if err := stream.Send(&partnerapiv1.RelayPartnerWebhookMessagesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
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

// TestPartnerWebhookConnection tests a partner webhook.
func (s *PartnerWebhookIntegrationServer) TestPartnerWebhookConnection(ctx context.Context, req *partnerapiv1.TestPartnerWebhookConnectionRequest) (*partnerapiv1.TestPartnerWebhookConnectionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected TestPartnerWebhookConnection", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "test_partner_webhook_connection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "test partner webhook connection", "tenant", req.GetContext().GetTenantId())
	partnerOrderIntegration, err := registry.Lookup[partnerapiv1.PartnerOrderIntegrationServiceClient](s.clients, "acme.partnerapi.v1.PartnerOrderIntegrationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	testPartnerOrderConnectionRequest := &partnerapiv1.TestPartnerOrderConnectionRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.TestPartnerOrderConnectionRequest_Details{
			Endpoint:   req.GetDetails().GetEndpoint(),
			ReceivedAt: req.GetDetails().GetReceivedAt(),
			Envelope:   packed,
		},
		Reason: tenancy.Value(ctx, "reason"),
	}
	testPartnerOrderConnectionReply, err := partnerOrderIntegration.TestPartnerOrderConnection(ctx, testPartnerOrderConnectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/TestPartnerOrderConnection")
	}
	packed2, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	resumePartnerWebhookWorkflowReply, err := s.partnerWebhookWorkflowClient.ResumePartnerWebhookWorkflow(ctx, &partnerapiv1.ResumePartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: testPartnerOrderConnectionReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			Detail:           packed2,
			PartnerWebhookId: envelope.GetDisplayName(),
		},
		Details: &partnerapiv1.ResumePartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: testPartnerOrderConnectionReply.GetPartnerOrder().GetCounterparty().GetEmail(),
			},
		},
		PartnerWebhookId: req.GetPartnerWebhookId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/ResumePartnerWebhookWorkflow")
	}
	return &partnerapiv1.TestPartnerWebhookConnectionResponse{
		Result: &commonv1.OperationResult{
			Status: testPartnerOrderConnectionReply.GetOutcome().GetStatus(),
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Subject: &partnerapiv1.PartnerIntegrationRef{
				CurrencyCode: resumePartnerWebhookWorkflowReply.GetPartnerWebhook().GetSubject().GetCurrencyCode(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
	}, nil
}

// partnerWebhookIntegrationServerStartPartnerIntegrationWorkflowSteps are the ways to run
// acme.partnerapi.v1.PartnerIntegrationWorkflowService/StartPartnerIntegrationWorkflow, by name.
var partnerWebhookIntegrationServerStartPartnerIntegrationWorkflowSteps = map[string]func(*PartnerWebhookIntegrationServer, context.Context, *partnerapiv1.StartPartnerIntegrationWorkflowRequest) (*partnerapiv1.StartPartnerIntegrationWorkflowResponse, error){
	"default": (*PartnerWebhookIntegrationServer).startPartnerIntegrationWorkflowStep,
	"audited": (*PartnerWebhookIntegrationServer).startPartnerIntegrationWorkflowAuditedStep,
}

func (s *PartnerWebhookIntegrationServer) startPartnerIntegrationWorkflowStep(ctx context.Context, request *partnerapiv1.StartPartnerIntegrationWorkflowRequest) (*partnerapiv1.StartPartnerIntegrationWorkflowResponse, error) {
	return s.partnerIntegrationWorkflowClient.StartPartnerIntegrationWorkflow(ctx, request)
}

func (s *PartnerWebhookIntegrationServer) startPartnerIntegrationWorkflowAuditedStep(ctx context.Context, request *partnerapiv1.StartPartnerIntegrationWorkflowRequest) (*partnerapiv1.StartPartnerIntegrationWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.partnerapi.v1.PartnerIntegrationWorkflowService/StartPartnerIntegrationWorkflow")
	return s.partnerIntegrationWorkflowClient.StartPartnerIntegrationWorkflow(ctx, request)
}
