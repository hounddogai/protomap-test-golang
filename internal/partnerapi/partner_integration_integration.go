package partnerapi

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	edgegatewayv1 "github.com/hounddogai/protomap-test-golang/gen/acme/edgegateway/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// PartnerIntegrationIntegrationServer implements `acme.partnerapi.v1.PartnerIntegrationIntegrationService`.
// Connects external systems that send partner integration events.
type PartnerIntegrationIntegrationServer struct {
	partnerapiv1.UnimplementedPartnerIntegrationIntegrationServiceServer
	store                              *Store
	log                                *slog.Logger
	partnerIntegrationReportingClient  partnerapiv1.PartnerIntegrationReportingServiceClient
	partnerOrderWorkflowClient         partnerapiv1.PartnerOrderWorkflowServiceClient
	conn                               *grpc.ClientConn
	accessPolicyValidationClient       authorizationv1.AccessPolicyValidationServiceClient
	routeBindingClient                 edgegatewayv1.RouteBindingServiceClient
	partnerWebhookValidationClient     partnerapiv1.PartnerWebhookValidationServiceClient
	auditEventClient                   auditv1.AuditEventServiceClient
	ledgerAccountQueryClient           ledgerv1.LedgerAccountQueryServiceClient
	sellerListingWorkflowClient        sellersv1.SellerListingWorkflowServiceClient
	clients                            *registry.Registry
	partnerOrderReportingClient        partnerapiv1.PartnerOrderReportingServiceClient
	partnerIntegrationWorkflowClient   partnerapiv1.PartnerIntegrationWorkflowServiceClient
	sellerApprovalClient               sellersv1.SellerApprovalServiceClient
	meteredPartnerIntegrationReporting meteredPartnerIntegrationReporting
}

// NewPartnerIntegrationIntegrationServer wires a PartnerIntegrationIntegrationServer.
func NewPartnerIntegrationIntegrationServer(deps platform.Deps) *PartnerIntegrationIntegrationServer {
	return &PartnerIntegrationIntegrationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.partnerapi.v1.PartnerIntegrationIntegrationService"),
		partnerIntegrationReportingClient:  partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn),
		partnerOrderWorkflowClient:         partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn),
		conn:                               deps.Conn,
		accessPolicyValidationClient:       authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		routeBindingClient:                 edgegatewayv1.NewRouteBindingServiceClient(deps.Conn),
		partnerWebhookValidationClient:     partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn),
		auditEventClient:                   auditv1.NewAuditEventServiceClient(deps.Conn),
		ledgerAccountQueryClient:           ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		sellerListingWorkflowClient:        sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		clients:                            deps.Clients,
		partnerOrderReportingClient:        partnerapiv1.NewPartnerOrderReportingServiceClient(deps.Conn),
		partnerIntegrationWorkflowClient:   partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn),
		sellerApprovalClient:               sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		meteredPartnerIntegrationReporting: meteredPartnerIntegrationReporting{PartnerIntegrationReportingServiceClient: partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// ConnectPartnerIntegrationSource connects a partner integration.
func (s *PartnerIntegrationIntegrationServer) ConnectPartnerIntegrationSource(ctx context.Context, req *partnerapiv1.ConnectPartnerIntegrationSourceRequest) (*partnerapiv1.ConnectPartnerIntegrationSourceResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ConnectPartnerIntegrationSource", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "connect_partner_integration_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "connect partner integration source", "tenant", req.GetContext().GetTenantId())
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPartnerIntegrationReportsRequest := &partnerapiv1.ListPartnerIntegrationReportsRequest{
		Context: req.GetContext(),
		Filter:  &partnerapiv1.ListPartnerIntegrationReportsRequest_Filter{CreatedAfter: req.GetDetails().GetReceivedAt()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listPartnerIntegrationReportsReply, err := grpcx.Call(ctx, s.partnerIntegrationReportingClient.ListPartnerIntegrationReports, listPartnerIntegrationReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/ListPartnerIntegrationReports")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	signalPartnerOrderWorkflowRequest := &partnerapiv1.SignalPartnerOrderWorkflowRequest{}
	signalPartnerOrderWorkflowRequest.Context = req.GetContext()
	signalPartnerOrderWorkflowRequest.Reason = req.GetReason()
	signalPartnerOrderWorkflowRequest.PartnerOrderId = envelope2.GetDisplayName()
	signalPartnerOrderWorkflowReply, err := s.partnerOrderWorkflowClient.SignalPartnerOrderWorkflow(ctx, signalPartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/SignalPartnerOrderWorkflow")
	}
	return &partnerapiv1.ConnectPartnerIntegrationSourceResponse{
		Outcome: &partnerapiv1.ConnectPartnerIntegrationSourceResponse_Outcome{
			Status: listPartnerIntegrationReportsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: listPartnerIntegrationReportsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Holder: &partnerapiv1.PartnerWebhookRef{
				DisplayName: signalPartnerOrderWorkflowReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: signalPartnerOrderWorkflowReply.GetPartnerOrder().GetAudit().GetUpdatedBy(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
		},
	}, nil
}

// DisconnectPartnerIntegrationSource disconnects a partner integration.
func (s *PartnerIntegrationIntegrationServer) DisconnectPartnerIntegrationSource(ctx context.Context, req *partnerapiv1.DisconnectPartnerIntegrationSourceRequest) (*partnerapiv1.DisconnectPartnerIntegrationSourceResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "disconnect_partner_integration_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "disconnect partner integration source", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	resumePartnerOrderWorkflowRequest := &partnerapiv1.ResumePartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{Email: tenancy.Value(ctx, "email")},
			Audit:        &commonv1.AuditInfo{CreatedAt: req.GetDetails().GetReceivedAt()},
		},
		Annotations:    annotations,
		PartnerOrderId: envelope.GetDisplayName(),
	}
	resumePartnerOrderWorkflowReply, err := grpcx.NewClient(s.conn, partnerapiv1.NewPartnerOrderWorkflowServiceClient).ResumePartnerOrderWorkflow(ctx, resumePartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/ResumePartnerOrderWorkflow")
	}
	explainAccessPolicyDecisionRequest := &authorizationv1.ExplainAccessPolicyDecisionRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: resumePartnerOrderWorkflowReply.GetPartnerOrder().GetAudit().GetUpdatedAt(),
			},
		},
	}
	type explainAccessPolicyDecisionResult struct {
		reply *authorizationv1.ExplainAccessPolicyDecisionResponse
		err   error
	}
	explainAccessPolicyDecisionResults := make(chan explainAccessPolicyDecisionResult, 1)
	go func() {
		reply, err := s.accessPolicyValidationClient.ExplainAccessPolicyDecision(ctx, explainAccessPolicyDecisionRequest)
		explainAccessPolicyDecisionResults <- explainAccessPolicyDecisionResult{reply, err}
	}()
	var explainAccessPolicyDecisionReply *authorizationv1.ExplainAccessPolicyDecisionResponse
	select {
	case result := <-explainAccessPolicyDecisionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision")
		}
		explainAccessPolicyDecisionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	watchRouteBindingsStream, err := s.routeBindingClient.WatchRouteBindings(ctx, &edgegatewayv1.WatchRouteBindingsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: explainAccessPolicyDecisionReply.GetAccessPolicy().GetBlock().GetNotifyEmail(),
		},
		Filter: &edgegatewayv1.WatchRouteBindingsRequest_Filter{
			Region: resumePartnerOrderWorkflowReply.GetPartnerOrder().GetBillingAddress().GetRegion(),
			Query:  envelope2.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.edgegateway.v1.RouteBindingService/WatchRouteBindings")
	}
	for {
		received, err := watchRouteBindingsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.edgegateway.v1.RouteBindingService/WatchRouteBindings")
		}
		_ = received
	}
	return &partnerapiv1.DisconnectPartnerIntegrationSourceResponse{
		Result: &commonv1.OperationResult{
			Message: resumePartnerOrderWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Outcome: &partnerapiv1.DisconnectPartnerIntegrationSourceResponse_Outcome{
			Message: explainAccessPolicyDecisionReply.GetResult().GetMessage(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainAccessPolicyDecisionReply.GetAccessPolicy().GetAudit().GetCreatedAt(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
		},
	}, nil
}

// IngestPartnerIntegrationEvents takes a stream of partner integrations and answers once at the end.
func (s *PartnerIntegrationIntegrationServer) IngestPartnerIntegrationEvents(stream grpc.ClientStreamingServer[partnerapiv1.IngestPartnerIntegrationEventsRequest, partnerapiv1.IngestPartnerIntegrationEventsResponse]) error {
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
		s.log.InfoContext(ctx, "ingest partner integration events", "tenant", msg.GetContext().GetTenantId())
		labels := make(map[string]string, len(msg.GetRecord().GetLabels()))
		for key, value := range msg.GetRecord().GetLabels() {
			labels[key] = value
		}
		listPartnerIntegrationReportsRequest := &partnerapiv1.ListPartnerIntegrationReportsRequest{
			Context: msg.GetContext(),
			Filter: &partnerapiv1.ListPartnerIntegrationReportsRequest_Filter{
				Labels:       labels,
				CreatedAfter: msg.GetRecord().GetAudit().GetCreatedAt(),
			},
		}
		if _, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*partnerapiv1.ListPartnerIntegrationReportsResponse, error) {
			return s.partnerIntegrationReportingClient.ListPartnerIntegrationReports(ctx, listPartnerIntegrationReportsRequest)
		}); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/ListPartnerIntegrationReports")
		}
		lintPartnerWebhooksRequest := &partnerapiv1.LintPartnerWebhooksRequest{
			Context: msg.GetContext(),
			Details: &partnerapiv1.LintPartnerWebhooksRequest_Details{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: msg.GetHeader().GetSubmittedBy().GetPreferredChannel(),
				},
			},
			PartnerWebhook: &partnerapiv1.PartnerWebhook{
				Subject: &partnerapiv1.PartnerIntegrationRef{
					DisplayName:          msg.GetRecord().GetHolder().GetDisplayName(),
					PartnerIntegrationId: msg.GetRecord().GetPartnerIntegrationId(),
				},
			},
		}
		if _, err := partnerapiv1.PartnerWebhookValidationServiceClient.LintPartnerWebhooks(s.partnerWebhookValidationClient, ctx, lintPartnerWebhooksRequest); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/LintPartnerWebhooks")
		}
		watchAuditEventsStream, err := s.auditEventClient.WatchAuditEvents(ctx, &auditv1.WatchAuditEventsRequest{
			Context: msg.GetContext(),
			Subscriber: &commonv1.ContactInfo{
				Name:  msg.GetRecord().GetContact().GetName(),
				Email: msg.GetRecord().GetContact().GetEmail(),
			},
		})
		if err != nil {
			return grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
		}
		for {
			received, err := watchAuditEventsStream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
			}
			_ = received
		}
		accepted++
	}
	return stream.SendAndClose(&partnerapiv1.IngestPartnerIntegrationEventsResponse{Accepted: accepted})
}

// ReplayPartnerIntegrationEvents replays a partner integration.
func (s *PartnerIntegrationIntegrationServer) ReplayPartnerIntegrationEvents(ctx context.Context, req *partnerapiv1.ReplayPartnerIntegrationEventsRequest) (*partnerapiv1.ReplayPartnerIntegrationEventsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "replay_partner_integration_events"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "replay partner integration events", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pullPayoutsRequest := &paymentsv1.PullPayoutsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetDetails().GetReceivedAt(),
				UpdatedAt: req.GetContext().GetDeadline(),
			},
		},
		Details:  &paymentsv1.PullPayoutsRequest_Details{Payload: snapshot},
		PayoutId: envelope.GetDisplayName(),
	}
	pullPayoutsReply, err := grpcx.NewClient(s.conn, paymentsv1.NewPayoutSyncServiceClient).PullPayouts(ctx, pullPayoutsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/PullPayouts")
	}
	resolveLedgerAccountsRequest := &ledgerv1.ResolveLedgerAccountsRequest{Context: req.GetContext()}
	resolveLedgerAccountsReply, err := ledgerv1.LedgerAccountQueryServiceClient.ResolveLedgerAccounts(s.ledgerAccountQueryClient, ctx, resolveLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ResolveLedgerAccounts")
	}
	return &partnerapiv1.ReplayPartnerIntegrationEventsResponse{
		Outcome: &partnerapiv1.ReplayPartnerIntegrationEventsResponse_Outcome{
			Status:  pullPayoutsReply.GetOutcome().GetStatus(),
			Message: resolveLedgerAccountsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Details: pullPayoutsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{PartnerIntegrationId: req.GetPartnerIntegrationId()},
	}, nil
}

// AcknowledgePartnerIntegrationEvent acknowledges a partner integration.
func (s *PartnerIntegrationIntegrationServer) AcknowledgePartnerIntegrationEvent(ctx context.Context, req *partnerapiv1.AcknowledgePartnerIntegrationEventRequest) (*partnerapiv1.AcknowledgePartnerIntegrationEventResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "acknowledge_partner_integration_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "acknowledge partner integration event", "tenant", req.GetContext().GetTenantId())
	pauseCouponWorkflowRequest := &promotionsv1.PauseCouponWorkflowRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
	}
	pauseCouponWorkflowReply, err := pauseCouponWorkflowCall(ctx, pauseCouponWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponWorkflowService/PauseCouponWorkflow")
	}
	startSellerListingWorkflowRequest := buildStartSellerListingWorkflowRequestForAcknowledgePartnerIntegrationEvent(req, pauseCouponWorkflowReply)
	startSellerListingWorkflowReply, err := s.sellerListingWorkflowClient.StartSellerListingWorkflow(ctx, startSellerListingWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/StartSellerListingWorkflow")
	}
	partnerIntegrationReporting, ok := s.clients.Get("acme.partnerapi.v1.PartnerIntegrationReportingService").(partnerapiv1.PartnerIntegrationReportingServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.partnerapi.v1.PartnerIntegrationReportingService")
	}
	countPartnerIntegrationTotalsRequest := &partnerapiv1.CountPartnerIntegrationTotalsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.CountPartnerIntegrationTotalsRequest_Filter{
			Labels:       pauseCouponWorkflowReply.GetCoupon().GetLabels(),
			Region:       startSellerListingWorkflowReply.GetSellerListing().GetAddress().GetRegion(),
			CreatedAfter: startSellerListingWorkflowReply.GetSellerListing().GetAudit().GetUpdatedAt(),
		},
	}
	countPartnerIntegrationTotalsReply, err := partnerIntegrationReporting.CountPartnerIntegrationTotals(ctx, countPartnerIntegrationTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/CountPartnerIntegrationTotals")
	}
	cancelPlacementWorkflowRequest := &merchandisingv1.CancelPlacementWorkflowRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: pauseCouponWorkflowReply.GetCoupon().GetContact().GetPreferredChannel(),
					Email:            startSellerListingWorkflowReply.GetSellerListing().GetOwner().GetEmail(),
				},
			}},
		},
	}
	cancelPlacementWorkflowReply, err := grpcx.NewClient(s.conn, merchandisingv1.NewPlacementWorkflowServiceClient).CancelPlacementWorkflow(ctx, cancelPlacementWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/CancelPlacementWorkflow")
	}
	return &partnerapiv1.AcknowledgePartnerIntegrationEventResponse{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Available: pauseCouponWorkflowReply.GetCoupon().GetAvailable(),
			OpenedAt:  pauseCouponWorkflowReply.GetCoupon().GetOpenedAt(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cancelPlacementWorkflowReply.GetPlacement().GetAudit().GetUpdatedAt(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
		},
		Outcome: &partnerapiv1.AcknowledgePartnerIntegrationEventResponse_Outcome{
			NextSteps: startSellerListingWorkflowReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: countPartnerIntegrationTotalsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// RelayPartnerIntegrationMessages exchanges changes of partner integrations with a peer.
func (s *PartnerIntegrationIntegrationServer) RelayPartnerIntegrationMessages(stream grpc.BidiStreamingServer[partnerapiv1.RelayPartnerIntegrationMessagesRequest, partnerapiv1.RelayPartnerIntegrationMessagesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *partnerapiv1.RelayPartnerIntegrationMessagesRequest)
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
		s.log.InfoContext(ctx, "relay partner integration messages", "tenant", msg.GetContext().GetTenantId())
		if _, err := s.partnerOrderReportingClient.SharePartnerOrderReport(ctx, &partnerapiv1.SharePartnerOrderReportRequest{
			Context: msg.GetContext(),
			Details: &partnerapiv1.SharePartnerOrderReportRequest_Details{
				Recipient: &commonv1.ContactInfo{
					Email:            msg.GetChange().GetContact().GetEmail(),
					PreferredChannel: msg.GetChange().GetContact().GetPreferredChannel(),
					Name:             msg.GetChange().GetContact().GetName(),
					Phone:            msg.GetChange().GetContact().GetPhone(),
				},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderReportingService/SharePartnerOrderReport")
		}
		if _, err := s.partnerIntegrationWorkflowClient.PausePartnerIntegrationWorkflow(ctx, &partnerapiv1.PausePartnerIntegrationWorkflowRequest{
			Context: msg.GetContext(),
			PartnerIntegration: &partnerapiv1.PartnerIntegration{
				Audit: &commonv1.AuditInfo{
					CreatedBy: msg.GetChange().GetAudit().GetCreatedBy(),
					UpdatedAt: msg.GetChange().GetAudit().GetUpdatedAt(),
				},
				Funding: &partnerapiv1.PartnerIntegration_Card{Card: &partnerapiv1.PartnerIntegrationCardFunding{Token: msg.GetChange().GetCard().GetToken()}},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/PausePartnerIntegrationWorkflow")
		}
		if _, err := s.partnerIntegrationReportingClient.ListPartnerIntegrationReports(ctx, &partnerapiv1.ListPartnerIntegrationReportsRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/ListPartnerIntegrationReports")
		}
		escalateSellerRequest := &sellersv1.EscalateSellerRequest{Context: msg.GetContext()}
		if _, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*sellersv1.EscalateSellerResponse, error) {
			return s.sellerApprovalClient.EscalateSeller(ctx, escalateSellerRequest)
		}); err != nil {
			return grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/EscalateSeller")
		}
		if err := stream.Send(&partnerapiv1.RelayPartnerIntegrationMessagesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
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

// ListPartnerIntegrationSources lists partner integrations a page at a time.
func (s *PartnerIntegrationIntegrationServer) ListPartnerIntegrationSources(ctx context.Context, req *partnerapiv1.ListPartnerIntegrationSourcesRequest) (*partnerapiv1.ListPartnerIntegrationSourcesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list partner integration sources", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.ListPartnerIntegrationSourcesRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	resumePartnerIntegrationWorkflowRequest := &partnerapiv1.ResumePartnerIntegrationWorkflowRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ResumePartnerIntegrationWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Labels:  labels,
			Audit:   &commonv1.AuditInfo{UpdatedAt: (&decoded).GetCreatedAfter()},
			Funding: &partnerapiv1.PartnerIntegration_ExternalAccountId{ExternalAccountId: req.GetContext().GetActor().GetUserId()},
		},
	}
	resumePartnerIntegrationWorkflowReply, err := resumePartnerIntegrationWorkflowCall(ctx, resumePartnerIntegrationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/ResumePartnerIntegrationWorkflow")
	}
	return &partnerapiv1.ListPartnerIntegrationSourcesResponse{
		PartnerIntegrations: []*partnerapiv1.PartnerIntegration{&partnerapiv1.PartnerIntegration{
			Labels: resumePartnerIntegrationWorkflowReply.GetPartnerIntegration().GetLabels(),
			Holder: &partnerapiv1.PartnerWebhookRef{
				Kind: resumePartnerIntegrationWorkflowReply.GetPartnerIntegration().GetHolder().GetKind(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TestPartnerIntegrationConnection tests a partner integration.
func (s *PartnerIntegrationIntegrationServer) TestPartnerIntegrationConnection(ctx context.Context, req *partnerapiv1.TestPartnerIntegrationConnectionRequest) (*partnerapiv1.TestPartnerIntegrationConnectionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_integration", req.GetPartnerIntegrationId(), "test_partner_integration_connection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "test partner integration connection", "tenant", req.GetContext().GetTenantId())
	retryCustomerSegmentImportRequest := &customersv1.RetryCustomerSegmentImportRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetReason()}},
		},
	}
	retryCustomerSegmentImportReply := new(customersv1.RetryCustomerSegmentImportResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.CustomerSegmentImportService_RetryCustomerSegmentImport_FullMethodName, retryCustomerSegmentImportRequest, retryCustomerSegmentImportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/RetryCustomerSegmentImport")
	}
	fetchPartnerIntegrationReportRequest := &partnerapiv1.FetchPartnerIntegrationReportRequest{
		Context:              req.GetContext(),
		PartnerIntegrationId: req.GetPartnerIntegrationId(),
	}
	fetchPartnerIntegrationReportReply, err := s.meteredPartnerIntegrationReporting.FetchPartnerIntegrationReport(ctx, fetchPartnerIntegrationReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/FetchPartnerIntegrationReport")
	}
	resumePlacementWorkflowRequest := &merchandisingv1.ResumePlacementWorkflowRequest{
		Context: req.GetContext(),
		Details: &merchandisingv1.ResumePlacementWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: retryCustomerSegmentImportReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
		},
		Placement: &merchandisingv1.Placement{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: retryCustomerSegmentImportReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			},
		},
	}
	resumePlacementWorkflowReply, err := resumePlacementWorkflowCall(ctx, resumePlacementWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/ResumePlacementWorkflow")
	}
	return &partnerapiv1.TestPartnerIntegrationConnectionResponse{
		Result: &commonv1.OperationResult{
			Status: retryCustomerSegmentImportReply.GetResult().GetStatus(),
		},
		Outcome: &partnerapiv1.TestPartnerIntegrationConnectionResponse_Outcome{
			Message: retryCustomerSegmentImportReply.GetOutcome().GetMessage(),
			Status:  fetchPartnerIntegrationReportReply.GetResult().GetStatus(),
		},
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Holder: &partnerapiv1.PartnerWebhookRef{
				DisplayName: resumePlacementWorkflowReply.GetPlacement().GetOrganizer().GetDisplayName(),
			},
			PartnerIntegrationId: req.GetPartnerIntegrationId(),
		},
	}, nil
}
