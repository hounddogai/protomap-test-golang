package partnerapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	employeeportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employeeportal/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerOrderIntegrationServer implements `acme.partnerapi.v1.PartnerOrderIntegrationService`.
// Connects external systems that send partner order events.
type PartnerOrderIntegrationServer struct {
	partnerapiv1.UnsafePartnerOrderIntegrationServiceServer
	store                              *Store
	log                                *slog.Logger
	partnerWebhookValidationPort       partnerWebhookValidationPort
	clients                            *registry.Registry
	partnerWebhookWorkflowClient       partnerapiv1.PartnerWebhookWorkflowServiceClient
	conn                               *grpc.ClientConn
	searchIndexValidationClient        searchv1.SearchIndexValidationServiceClient
	meteredPartnerOrderReporting       meteredPartnerOrderReporting
	sellerApprovalClient               sellersv1.SellerApprovalServiceClient
	apiCredentialNotificationClient    identityv1.ApiCredentialNotificationServiceClient
	bus                                *eventbus.Bus
	checkoutSessionApprovalClient      checkoutv1.CheckoutSessionApprovalServiceClient
	partnerOrderWorkflowClient         partnerapiv1.PartnerOrderWorkflowServiceClient
	attributeSyncClient                catalogv1.AttributeSyncServiceClient
	sellerPayoutScheduleApprovalClient sellersv1.SellerPayoutScheduleApprovalServiceClient
	portalHomeClient                   employeeportalv1.PortalHomeServiceClient
}

// NewPartnerOrderIntegrationServer wires a PartnerOrderIntegrationServer.
func NewPartnerOrderIntegrationServer(deps platform.Deps) *PartnerOrderIntegrationServer {
	return &PartnerOrderIntegrationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.partnerapi.v1.PartnerOrderIntegrationService"),
		partnerWebhookValidationPort:       newPartnerWebhookValidationPort(deps),
		clients:                            deps.Clients,
		partnerWebhookWorkflowClient:       partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn),
		conn:                               deps.Conn,
		searchIndexValidationClient:        searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		meteredPartnerOrderReporting:       meteredPartnerOrderReporting{PartnerOrderReportingServiceClient: partnerapiv1.NewPartnerOrderReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerApprovalClient:               sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		apiCredentialNotificationClient:    identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
		bus:                                deps.Bus,
		checkoutSessionApprovalClient:      checkoutv1.NewCheckoutSessionApprovalServiceClient(deps.Conn),
		partnerOrderWorkflowClient:         partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn),
		attributeSyncClient:                catalogv1.NewAttributeSyncServiceClient(deps.Conn),
		sellerPayoutScheduleApprovalClient: sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn),
		portalHomeClient:                   employeeportalv1.NewPortalHomeServiceClient(deps.Conn),
	}
}

// ConnectPartnerOrderSource connects a partner order.
func (s *PartnerOrderIntegrationServer) ConnectPartnerOrderSource(ctx context.Context, req *partnerapiv1.ConnectPartnerOrderSourceRequest) (*partnerapiv1.ConnectPartnerOrderSourceResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "connect_partner_order_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "connect partner order source", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewPartnerWebhookRequest := &partnerapiv1.PreviewPartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.PreviewPartnerWebhookRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		Annotations:      annotations,
		Reason:           req.GetReason(),
		PartnerWebhook:   &partnerapiv1.PartnerWebhook{Detail: packed},
		PartnerWebhookId: envelope.GetDisplayName(),
	}
	previewPartnerWebhookReply, err := s.partnerWebhookValidationPort.PreviewPartnerWebhook(ctx, previewPartnerWebhookRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/PreviewPartnerWebhook")
	}
	partnerOrderWorkflow, ok := s.clients.Get("acme.partnerapi.v1.PartnerOrderWorkflowService").(partnerapiv1.PartnerOrderWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.partnerapi.v1.PartnerOrderWorkflowService")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryPartnerOrderWorkflowRequest := &partnerapiv1.RetryPartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{
				Attributes: previewPartnerWebhookReply.GetPartnerWebhook().GetAttributes(),
			}},
			Counterparty: &sellersv1.SellerRef{
				DisplayName: previewPartnerWebhookReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
		},
		Annotations:    req.GetAnnotations(),
		PartnerOrderId: envelope2.GetDisplayName(),
	}
	retryPartnerOrderWorkflowReply, err := partnerOrderWorkflow.RetryPartnerOrderWorkflow(ctx, retryPartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/RetryPartnerOrderWorkflow")
	}
	advancePartnerWebhookWorkflowRequest := buildAdvancePartnerWebhookWorkflowRequestForConnectPartnerOrderSource(req, previewPartnerWebhookReply)
	advancePartnerWebhookWorkflowReply, err := s.partnerWebhookWorkflowClient.AdvancePartnerWebhookWorkflow(ctx, advancePartnerWebhookWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/AdvancePartnerWebhookWorkflow")
	}
	return &partnerapiv1.ConnectPartnerOrderSourceResponse{
		Outcome: &partnerapiv1.ConnectPartnerOrderSourceResponse_Outcome{
			EffectiveAt: previewPartnerWebhookReply.GetOutcome().GetEffectiveAt(),
			Status:      previewPartnerWebhookReply.GetOutcome().GetStatus(),
			Message:     advancePartnerWebhookWorkflowReply.GetResult().GetMessage(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: retryPartnerOrderWorkflowReply.GetPartnerOrder().GetBillingAddress().GetRecipient(),
			},
			PartnerOrderId: req.GetPartnerOrderId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DisconnectPartnerOrderSource disconnects a partner order.
func (s *PartnerOrderIntegrationServer) DisconnectPartnerOrderSource(ctx context.Context, req *partnerapiv1.DisconnectPartnerOrderSourceRequest) (*partnerapiv1.DisconnectPartnerOrderSourceResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "disconnect_partner_order_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "disconnect partner order source", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	submitSellerPayoutScheduleRequest := &sellersv1.SubmitSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			}},
			Organizer: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Annotations: req.GetAnnotations(),
	}
	submitSellerPayoutScheduleReply := new(sellersv1.SubmitSellerPayoutScheduleResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerPayoutScheduleApprovalServiceName, "SubmitSellerPayoutSchedule"), submitSellerPayoutScheduleRequest, submitSellerPayoutScheduleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/SubmitSellerPayoutSchedule")
	}
	normalizeSearchIndexRequest := &searchv1.NormalizeSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{
				Region:      submitSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetLocation().GetRegion(),
				CountryCode: submitSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetLocation().GetCountryCode(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: submitSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedBy(),
			},
		},
	}
	type normalizeSearchIndexResult struct {
		reply *searchv1.NormalizeSearchIndexResponse
		err   error
	}
	normalizeSearchIndexResults := make(chan normalizeSearchIndexResult, 1)
	go func() {
		reply, err := s.searchIndexValidationClient.NormalizeSearchIndex(ctx, normalizeSearchIndexRequest)
		normalizeSearchIndexResults <- normalizeSearchIndexResult{reply, err}
	}()
	var normalizeSearchIndexReply *searchv1.NormalizeSearchIndexResponse
	select {
	case result := <-normalizeSearchIndexResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.search.v1.SearchIndexValidationService/NormalizeSearchIndex")
		}
		normalizeSearchIndexReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &partnerapiv1.DisconnectPartnerOrderSourceResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: submitSellerPayoutScheduleReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &partnerapiv1.DisconnectPartnerOrderSourceResponse_Outcome{
			NextSteps: normalizeSearchIndexReply.GetOutcome().GetNextSteps(),
			Message:   normalizeSearchIndexReply.GetResult().GetMessage(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
	}, nil
}

// IngestPartnerOrderEvents takes a stream of partner orders and answers once at the end.
func (s *PartnerOrderIntegrationServer) IngestPartnerOrderEvents(stream grpc.ClientStreamingServer[partnerapiv1.IngestPartnerOrderEventsRequest, partnerapiv1.IngestPartnerOrderEventsResponse]) error {
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
		s.log.InfoContext(ctx, "ingest partner order events", "tenant", msg.GetContext().GetTenantId())
		schedulePartnerOrderReportRequest := &partnerapiv1.SchedulePartnerOrderReportRequest{
			Context:        msg.GetContext(),
			Details:        &partnerapiv1.SchedulePartnerOrderReportRequest_Details{Format: msg.GetHeader().GetFormat()},
			PartnerOrderId: msg.GetRecord().GetPartnerOrderId(),
		}
		if _, err := s.meteredPartnerOrderReporting.SchedulePartnerOrderReport(ctx, schedulePartnerOrderReportRequest); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderReportingService/SchedulePartnerOrderReport")
		}
		countPartnerOrderTotalsRequest := &partnerapiv1.CountPartnerOrderTotalsRequest{
			Context: msg.GetContext(),
			Filter:  &partnerapiv1.CountPartnerOrderTotalsRequest_Filter{Region: msg.GetRecord().GetBillingAddress().GetRegion()},
		}
		if _, err := s.meteredPartnerOrderReporting.CountPartnerOrderTotals(ctx, countPartnerOrderTotalsRequest); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderReportingService/CountPartnerOrderTotals")
		}
		escalateSellerRequest := &sellersv1.EscalateSellerRequest{
			Context: msg.GetContext(),
			Seller: &sellersv1.Seller{
				RelationshipManager: &identityv1.UserRef{
					Email:       msg.GetHeader().GetSubmittedBy().GetEmail(),
					DisplayName: msg.GetRecord().GetCounterparty().GetDisplayName(),
				},
				PrimaryAddress: &commonv1.PostalAddress{Lines: msg.GetRecord().GetBillingAddress().GetLines()},
			},
		}
		if _, err := sellersv1.SellerApprovalServiceClient.EscalateSeller(s.sellerApprovalClient, ctx, escalateSellerRequest); err != nil {
			return grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/EscalateSeller")
		}
		subscribeApiCredentialUpdatesRequest := &identityv1.SubscribeApiCredentialUpdatesRequest{}
		subscribeApiCredentialUpdatesRequest.Context = msg.GetContext()
		if _, err := s.apiCredentialNotificationClient.SubscribeApiCredentialUpdates(ctx, subscribeApiCredentialUpdatesRequest); err != nil {
			return grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SubscribeApiCredentialUpdates")
		}
		accepted++
	}
	return stream.SendAndClose(&partnerapiv1.IngestPartnerOrderEventsResponse{Accepted: accepted})
}

// ReplayPartnerOrderEvents replays a partner order.
func (s *PartnerOrderIntegrationServer) ReplayPartnerOrderEvents(ctx context.Context, req *partnerapiv1.ReplayPartnerOrderEventsRequest) (*partnerapiv1.ReplayPartnerOrderEventsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReplayPartnerOrderEvents", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "replay_partner_order_events"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "replay partner order events", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "partnerapi.replay_partner_order_events.list_seller_listing_reports",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   req.GetDetails().GetMessageId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	escalateCheckoutSessionRequest := &checkoutv1.EscalateCheckoutSessionRequest{
		Context: req.GetContext(),
		Details: &checkoutv1.EscalateCheckoutSessionRequest_Details{
			Approver:      &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			ApproverEmail: cachedContext.GetActor().GetEmail(),
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	send := s.escalateCheckoutSessionSender(escalateCheckoutSessionRequest)
	escalateCheckoutSessionReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/EscalateCheckoutSession")
	}
	return &partnerapiv1.ReplayPartnerOrderEventsResponse{
		Result: &commonv1.OperationResult{
			Status: escalateCheckoutSessionReply.GetOutcome().GetStatus(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{PartnerOrderId: req.GetPartnerOrderId()},
	}, nil
}

// AcknowledgePartnerOrderEvent acknowledges a partner order.
func (s *PartnerOrderIntegrationServer) AcknowledgePartnerOrderEvent(ctx context.Context, req *partnerapiv1.AcknowledgePartnerOrderEventRequest) (*partnerapiv1.AcknowledgePartnerOrderEventResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "acknowledge_partner_order_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "acknowledge partner order event", "tenant", req.GetContext().GetTenantId())
	partnerOrderWorkflow, ok := s.clients.Get("acme.partnerapi.v1.PartnerOrderWorkflowService").(partnerapiv1.PartnerOrderWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.partnerapi.v1.PartnerOrderWorkflowService")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listPartnerOrderWorkflowsRequest := &partnerapiv1.ListPartnerOrderWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerOrderWorkflowsRequest_Filter{
			CreatedAfter: req.GetDetails().GetReceivedAt(),
			OwnerId:      req.GetPartnerOrderId(),
			Labels:       annotations,
		},
	}
	listPartnerOrderWorkflowsReply, err := partnerOrderWorkflow.ListPartnerOrderWorkflows(ctx, listPartnerOrderWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/ListPartnerOrderWorkflows")
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainPartnerWebhookDecisionRequest := &partnerapiv1.ExplainPartnerWebhookDecisionRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId: listPartnerOrderWorkflowsReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Reason:           tenancy.Value(ctx, "reason"),
		PartnerWebhookId: envelope.GetDisplayName(),
	}
	explainPartnerWebhookDecisionReply, err := explainPartnerWebhookDecisionCall(ctx, explainPartnerWebhookDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookValidationService/ExplainPartnerWebhookDecision")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	resolveSynonymConflictRequest := &searchv1.ResolveSynonymConflictRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: explainPartnerWebhookDecisionReply.GetPartnerWebhook().GetSubject().GetDisplayName(),
			},
		},
		Details:   &searchv1.ResolveSynonymConflictRequest_Details{Payload: snapshot},
		SynonymId: envelope2.GetDisplayName(),
	}
	resolveSynonymConflictReply := new(searchv1.ResolveSynonymConflictResponse)
	if err := s.conn.Invoke(ctx, searchSynonymSyncServiceResolveSynonymConflictMethod, resolveSynonymConflictRequest, resolveSynonymConflictReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ResolveSynonymConflict")
	}
	return &partnerapiv1.AcknowledgePartnerOrderEventResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPartnerOrderWorkflowsReply.GetResult().GetCorrelationId(),
			Status:        explainPartnerWebhookDecisionReply.GetResult().GetStatus(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Lines: []*partnerapiv1.PartnerOrderLine{&partnerapiv1.PartnerOrderLine{
				Attributes:  explainPartnerWebhookDecisionReply.GetPartnerWebhook().GetAttributes(),
				Description: resolveSynonymConflictReply.GetSynonym().GetDescription(),
			}},
			PartnerOrderId: req.GetPartnerOrderId(),
		},
	}, nil
}

// RelayPartnerOrderMessages exchanges changes of partner orders with a peer.
func (s *PartnerOrderIntegrationServer) RelayPartnerOrderMessages(stream grpc.BidiStreamingServer[partnerapiv1.RelayPartnerOrderMessagesRequest, partnerapiv1.RelayPartnerOrderMessagesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *partnerapiv1.RelayPartnerOrderMessagesRequest)
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
		s.log.InfoContext(ctx, "relay partner order messages", "tenant", msg.GetContext().GetTenantId())
		if _, err := s.partnerOrderWorkflowClient.RetryPartnerOrderWorkflow(ctx, &partnerapiv1.RetryPartnerOrderWorkflowRequest{
			Context: msg.GetContext(),
			PartnerOrder: &partnerapiv1.PartnerOrder{
				Settlement: &partnerapiv1.PartnerOrder_Bank{Bank: &partnerapiv1.PartnerOrderBankSettlement{Iban: msg.GetChange().GetBank().GetIban()}},
				BillingAddress: &commonv1.PostalAddress{
					Recipient: msg.GetChange().GetBillingAddress().GetRecipient(),
				},
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/RetryPartnerOrderWorkflow")
		}
		retryPartnerWebhookWorkflowRequest := &partnerapiv1.RetryPartnerWebhookWorkflowRequest{}
		retryPartnerWebhookWorkflowRequest.Context = msg.GetContext()
		retryPartnerWebhookWorkflowRequest.Details = &partnerapiv1.RetryPartnerWebhookWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: msg.GetChange().GetCounterparty().GetEmail()},
		}
		if _, err := s.partnerWebhookWorkflowClient.RetryPartnerWebhookWorkflow(ctx, retryPartnerWebhookWorkflowRequest); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/RetryPartnerWebhookWorkflow")
		}
		syncAttributesRequest := &catalogv1.SyncAttributesRequest{
			Context: msg.GetContext(),
			Attribute: &catalogv1.Attribute{
				Audit: &commonv1.AuditInfo{UpdatedAt: msg.GetChange().GetAudit().GetUpdatedAt()},
				Owner: &identityv1.UserRef{Email: msg.GetChange().GetCounterparty().GetEmail()},
			},
		}
		if _, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*catalogv1.SyncAttributesResponse, error) {
			return s.attributeSyncClient.SyncAttributes(ctx, syncAttributesRequest)
		}); err != nil {
			return grpcx.Wrap(err, "acme.catalog.v1.AttributeSyncService/SyncAttributes")
		}
		if _, err := s.sellerPayoutScheduleApprovalClient.DelegateSellerPayoutScheduleApproval(ctx, &sellersv1.DelegateSellerPayoutScheduleApprovalRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/DelegateSellerPayoutScheduleApproval")
		}
		if err := stream.Send(&partnerapiv1.RelayPartnerOrderMessagesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
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

// ListPartnerOrderSources lists partner orders a page at a time.
func (s *PartnerOrderIntegrationServer) ListPartnerOrderSources(ctx context.Context, req *partnerapiv1.ListPartnerOrderSourcesRequest) (*partnerapiv1.ListPartnerOrderSourcesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list partner order sources", "tenant", req.GetContext().GetTenantId())
	watchPortalHomesStream, err := s.portalHomeClient.WatchPortalHomes(ctx, &employeeportalv1.WatchPortalHomesRequest{
		Context: req.GetContext(),
		Filter: &employeeportalv1.WatchPortalHomesRequest_Filter{
			OwnerId:      req.GetFilter().GetOwnerId(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       req.GetFilter().GetLabels(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.employeeportal.v1.PortalHomeService/WatchPortalHomes")
	}
	for {
		received, err := watchPortalHomesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.employeeportal.v1.PortalHomeService/WatchPortalHomes")
		}
		_ = received
	}
	return &partnerapiv1.ListPartnerOrderSourcesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// TestPartnerOrderConnection tests a partner order.
func (s *PartnerOrderIntegrationServer) TestPartnerOrderConnection(ctx context.Context, req *partnerapiv1.TestPartnerOrderConnectionRequest) (*partnerapiv1.TestPartnerOrderConnectionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_order", req.GetPartnerOrderId(), "test_partner_order_connection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "test partner order connection", "tenant", req.GetContext().GetTenantId())
	signalPartnerOrderWorkflowRequest := buildSignalPartnerOrderWorkflowRequestForTestPartnerOrderConnection(req)
	signalPartnerOrderWorkflowReply, err := s.partnerOrderWorkflowClient.SignalPartnerOrderWorkflow(ctx, signalPartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/SignalPartnerOrderWorkflow")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	cancelPartnerWebhookWorkflowRequest := &partnerapiv1.CancelPartnerWebhookWorkflowRequest{}
	cancelPartnerWebhookWorkflowRequest.Context = req.GetContext()
	cancelPartnerWebhookWorkflowRequest.PartnerWebhook = &partnerapiv1.PartnerWebhook{
		Subject: &partnerapiv1.PartnerIntegrationRef{
			DisplayName: signalPartnerOrderWorkflowReply.GetPartnerOrder().GetCounterparty().GetDisplayName(),
		},
	}
	cancelPartnerWebhookWorkflowRequest.Details = &partnerapiv1.CancelPartnerWebhookWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{
			Email: signalPartnerOrderWorkflowReply.GetPartnerOrder().GetCounterparty().GetEmail(),
		},
	}
	cancelPartnerWebhookWorkflowRequest.Annotations = annotations
	cancelPartnerWebhookWorkflowReply, err := s.partnerWebhookWorkflowClient.CancelPartnerWebhookWorkflow(ctx, cancelPartnerWebhookWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/CancelPartnerWebhookWorkflow")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	resumePartnerOrderWorkflowRequest := &partnerapiv1.ResumePartnerOrderWorkflowRequest{
		Context: req.GetContext(),
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Settlement: &partnerapiv1.PartnerOrder_Card{Card: &partnerapiv1.PartnerOrderCardSettlement{
				Token: signalPartnerOrderWorkflowReply.GetPartnerOrder().GetCard().GetToken(),
				Last4: signalPartnerOrderWorkflowReply.GetPartnerOrder().GetCard().GetLast4(),
			}},
		},
		PartnerOrderId: envelope.GetDisplayName(),
	}
	resumePartnerOrderWorkflowReply, err := grpcx.Call(ctx, s.partnerOrderWorkflowClient.ResumePartnerOrderWorkflow, resumePartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/ResumePartnerOrderWorkflow")
	}
	return &partnerapiv1.TestPartnerOrderConnectionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: signalPartnerOrderWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &partnerapiv1.TestPartnerOrderConnectionResponse_Outcome{
			Status: cancelPartnerWebhookWorkflowReply.GetOutcome().GetStatus(),
		},
		PartnerOrder: &partnerapiv1.PartnerOrder{
			BillingAddress: resumePartnerOrderWorkflowReply.GetPartnerOrder().GetBillingAddress(),
			PartnerOrderId: req.GetPartnerOrderId(),
		},
	}, nil
}

// escalateCheckoutSessionSender returns a function that sends one request to
// acme.checkout.v1.CheckoutSessionApprovalService/EscalateCheckoutSession.
func (s *PartnerOrderIntegrationServer) escalateCheckoutSessionSender(request *checkoutv1.EscalateCheckoutSessionRequest) func(context.Context) (*checkoutv1.EscalateCheckoutSessionResponse, error) {
	return func(ctx context.Context) (*checkoutv1.EscalateCheckoutSessionResponse, error) {
		return s.checkoutSessionApprovalClient.EscalateCheckoutSession(ctx, request)
	}
}
