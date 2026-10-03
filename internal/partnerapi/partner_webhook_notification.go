package partnerapi

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartnerWebhookNotificationServer implements `acme.partnerapi.v1.PartnerWebhookNotificationService`.
// Tells the people who follow a partner webhook what changed.
type PartnerWebhookNotificationServer struct {
	partnerapiv1.UnimplementedPartnerWebhookNotificationServiceServer
	store                             *Store
	log                               *slog.Logger
	userClient                        identityv1.UserServiceClient
	conn                              *grpc.ClientConn
	giftCardBatchApprovalClient       giftcardsv1.GiftCardBatchApprovalServiceClient
	sellerScorecardApprovalClient     sellersv1.SellerScorecardApprovalServiceClient
	giftCardTransactionApprovalClient giftcardsv1.GiftCardTransactionApprovalServiceClient
	sessionNotificationClient         identityv1.SessionNotificationServiceClient
	merchandisingRuleApprovalClient   merchandisingv1.MerchandisingRuleApprovalServiceClient
	partnerWebhookApprovalClient      partnerapiv1.PartnerWebhookApprovalServiceClient
	partnerWebhookIntegrationClient   partnerapiv1.PartnerWebhookIntegrationServiceClient
	sessionHistoryClient              identityv1.SessionHistoryServiceClient
	shipmentWorkflowClient            shippingv1.ShipmentWorkflowServiceClient
}

// NewPartnerWebhookNotificationServer wires a PartnerWebhookNotificationServer.
func NewPartnerWebhookNotificationServer(deps platform.Deps) *PartnerWebhookNotificationServer {
	return &PartnerWebhookNotificationServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.partnerapi.v1.PartnerWebhookNotificationService"),
		userClient:                        identityv1.NewUserServiceClient(deps.Conn),
		conn:                              deps.Conn,
		giftCardBatchApprovalClient:       giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		sellerScorecardApprovalClient:     sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		giftCardTransactionApprovalClient: giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		sessionNotificationClient:         identityv1.NewSessionNotificationServiceClient(deps.Conn),
		merchandisingRuleApprovalClient:   merchandisingv1.NewMerchandisingRuleApprovalServiceClient(deps.Conn),
		partnerWebhookApprovalClient:      partnerapiv1.NewPartnerWebhookApprovalServiceClient(deps.Conn),
		partnerWebhookIntegrationClient:   partnerapiv1.NewPartnerWebhookIntegrationServiceClient(deps.Conn),
		sessionHistoryClient:              identityv1.NewSessionHistoryServiceClient(deps.Conn),
		shipmentWorkflowClient:            shippingv1.NewShipmentWorkflowServiceClient(deps.Conn),
	}
}

// SubscribePartnerWebhookUpdates subscribes a partner webhook.
func (s *PartnerWebhookNotificationServer) SubscribePartnerWebhookUpdates(ctx context.Context, req *partnerapiv1.SubscribePartnerWebhookUpdatesRequest) (*partnerapiv1.SubscribePartnerWebhookUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "subscribe_partner_webhook_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe partner webhook updates", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	getUserRequest := &identityv1.GetUserRequest{
		Context: req.GetContext(),
		UserId:  cloned.GetActor().GetUserId(),
	}
	getUserReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*identityv1.GetUserResponse, error) {
		return s.userClient.GetUser(ctx, getUserRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/GetUser")
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	delegateGiftCardBatchApprovalRequest := &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{CreatedBy: getUserReply.GetUser().GetAudit().GetCreatedBy()},
		},
		GiftCardBatchId: envelope.GetDisplayName(),
	}
	delegateGiftCardBatchApprovalReply := new(giftcardsv1.DelegateGiftCardBatchApprovalResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", giftcardsGiftCardBatchApprovalServiceName, "DelegateGiftCardBatchApproval"), delegateGiftCardBatchApprovalRequest, delegateGiftCardBatchApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	return &partnerapiv1.SubscribePartnerWebhookUpdatesResponse{
		Result: &commonv1.OperationResult{
			Details: getUserReply.GetResult().GetDetails(),
			Message: delegateGiftCardBatchApprovalReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId: getUserReply.GetResult().GetCorrelationId(),
			Subject: &partnerapiv1.PartnerIntegrationRef{
				DisplayName: delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
	}, nil
}

// UnsubscribePartnerWebhookUpdates unsubscribes a partner webhook.
func (s *PartnerWebhookNotificationServer) UnsubscribePartnerWebhookUpdates(ctx context.Context, req *partnerapiv1.UnsubscribePartnerWebhookUpdatesRequest) (*partnerapiv1.UnsubscribePartnerWebhookUpdatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "unsubscribe_partner_webhook_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe partner webhook updates", "tenant", req.GetContext().GetTenantId())
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPendingGiftCardBatchesRequest := newListPendingGiftCardBatchesRequest(withListPendingGiftCardBatchesContext(req.GetContext()), withListPendingGiftCardBatchesFilter(&giftcardsv1.ListPendingGiftCardBatchesRequest_Filter{Labels: req.GetAnnotations()}), withListPendingGiftCardBatchesPage(&commonv1.PageRequest{PageToken: envelope.GetDisplayName()}))
	listPendingGiftCardBatchesReply, err := s.giftCardBatchApprovalClient.ListPendingGiftCardBatches(ctx, listPendingGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/ListPendingGiftCardBatches")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope2 partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	escalateSellerScorecardRequest := &sellersv1.EscalateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Details: &sellersv1.EscalateSellerScorecardRequest_Details{
			Approver: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
		},
		Annotations:       annotations,
		SellerScorecardId: envelope2.GetDisplayName(),
	}
	escalateSellerScorecardReply, err := sellersv1.SellerScorecardApprovalServiceClient.EscalateSellerScorecard(s.sellerScorecardApprovalClient, ctx, escalateSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/EscalateSellerScorecard")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	delegateGiftCardTransactionApprovalReply, err := s.giftCardTransactionApprovalClient.DelegateGiftCardTransactionApproval(ctx, &giftcardsv1.DelegateGiftCardTransactionApprovalRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: escalateSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/DelegateGiftCardTransactionApproval")
	}
	return &partnerapiv1.UnsubscribePartnerWebhookUpdatesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPendingGiftCardBatchesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    escalateSellerScorecardReply.GetResult().GetCorrelationId(),
			Sequence:         delegateGiftCardTransactionApprovalReply.GetGiftCardTransaction().GetSequence(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
	}, nil
}

// NotifyPartnerWebhookWatchers notifys a partner webhook.
func (s *PartnerWebhookNotificationServer) NotifyPartnerWebhookWatchers(ctx context.Context, req *partnerapiv1.NotifyPartnerWebhookWatchersRequest) (*partnerapiv1.NotifyPartnerWebhookWatchersResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "notify_partner_webhook_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify partner webhook watchers", "tenant", req.GetContext().GetTenantId())
	inspectPartnerWebhookWorkflowRequest := &partnerapiv1.InspectPartnerWebhookWorkflowRequest{
		Context:          req.GetContext(),
		PartnerWebhookId: req.GetPartnerWebhookId(),
	}
	inspectPartnerWebhookWorkflowReply := new(partnerapiv1.InspectPartnerWebhookWorkflowResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.partnerapi.v1.PartnerWebhookWorkflowService/InspectPartnerWebhookWorkflow", inspectPartnerWebhookWorkflowRequest, inspectPartnerWebhookWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/InspectPartnerWebhookWorkflow")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	replayPartnerWebhookEventsRequest := &partnerapiv1.ReplayPartnerWebhookEventsRequest{
		Context:          req.GetContext(),
		PartnerWebhookId: inspectPartnerWebhookWorkflowReply.GetPartnerWebhook().GetPartnerWebhookId(),
		Annotations:      annotations,
		Reason:           req.GetReason(),
	}
	replayPartnerWebhookEventsReply, err := grpcx.NewClient(s.conn, partnerapiv1.NewPartnerWebhookIntegrationServiceClient).ReplayPartnerWebhookEvents(ctx, replayPartnerWebhookEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookIntegrationService/ReplayPartnerWebhookEvents")
	}
	ctx = tenancy.With(ctx, "channel", req.GetDetails().GetChannel())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifySessionWatchersRequest := &identityv1.NotifySessionWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifySessionWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetDetails().GetRecipient().GetEmail()},
			Channel:   tenancy.Value(ctx, "channel"),
			Payload:   packed,
		},
	}
	var notifySessionWatchersReply *identityv1.NotifySessionWatchersResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.SessionNotificationService/NotifySessionWatchers", func(ctx context.Context) error {
		answer, err := s.sessionNotificationClient.NotifySessionWatchers(ctx, notifySessionWatchersRequest)
		notifySessionWatchersReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/NotifySessionWatchers")
	}
	return &partnerapiv1.NotifyPartnerWebhookWatchersResponse{
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			Attributes: inspectPartnerWebhookWorkflowReply.GetPartnerWebhook().GetAttributes(),
			Kind:       replayPartnerWebhookEventsReply.GetPartnerWebhook().GetKind(),
			Subject: &partnerapiv1.PartnerIntegrationRef{
				CurrencyCode: notifySessionWatchersReply.GetSession().GetCustomer().GetCurrencyCode(),
			},
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
		Outcome: &partnerapiv1.NotifyPartnerWebhookWatchersResponse_Outcome{
			Status: inspectPartnerWebhookWorkflowReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: notifySessionWatchersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ListPartnerWebhookSubscriptions lists partner webhooks a page at a time.
func (s *PartnerWebhookNotificationServer) ListPartnerWebhookSubscriptions(ctx context.Context, req *partnerapiv1.ListPartnerWebhookSubscriptionsRequest) (*partnerapiv1.ListPartnerWebhookSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPartnerWebhookSubscriptions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list partner webhook subscriptions", "tenant", req.GetContext().GetTenantId())
	return &partnerapiv1.ListPartnerWebhookSubscriptionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SendPartnerWebhookTestNotice sends a partner webhook.
func (s *PartnerWebhookNotificationServer) SendPartnerWebhookTestNotice(ctx context.Context, req *partnerapiv1.SendPartnerWebhookTestNoticeRequest) (*partnerapiv1.SendPartnerWebhookTestNoticeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "send_partner_webhook_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send partner webhook test notice", "tenant", req.GetContext().GetTenantId())
	delegateMerchandisingRuleApprovalRequest := buildDelegateMerchandisingRuleApprovalRequestForSendPartnerWebhookTestNotice(req)
	delegateMerchandisingRuleApprovalReply, err := s.merchandisingRuleApprovalClient.DelegateMerchandisingRuleApproval(ctx, delegateMerchandisingRuleApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleApprovalService/DelegateMerchandisingRuleApproval")
	}
	variables := make(map[string]string, len(req.GetDetails().GetVariables()))
	for key, value := range req.GetDetails().GetVariables() {
		variables[key] = value
	}
	var envelope partnerapiv1.PartnerIntegrationRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPendingPartnerWebhooksRequest := &partnerapiv1.ListPendingPartnerWebhooksRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPendingPartnerWebhooksRequest_Filter{
			Labels:       variables,
			CreatedAfter: delegateMerchandisingRuleApprovalReply.GetMerchandisingRule().GetAudit().GetCreatedAt(),
			OwnerId:      delegateMerchandisingRuleApprovalReply.GetMerchandisingRule().GetMerchandisingRuleId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	var listPendingPartnerWebhooksReply *partnerapiv1.ListPendingPartnerWebhooksResponse
	if err := pipeline.Run(ctx, "acme.partnerapi.v1.PartnerWebhookApprovalService/ListPendingPartnerWebhooks", func(ctx context.Context) error {
		answer, err := s.partnerWebhookApprovalClient.ListPendingPartnerWebhooks(ctx, listPendingPartnerWebhooksRequest)
		listPendingPartnerWebhooksReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/ListPendingPartnerWebhooks")
	}
	return &partnerapiv1.SendPartnerWebhookTestNoticeResponse{
		Outcome: &partnerapiv1.SendPartnerWebhookTestNoticeResponse_Outcome{
			Message: delegateMerchandisingRuleApprovalReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: listPendingPartnerWebhooksReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{PartnerWebhookId: req.GetPartnerWebhookId()},
	}, nil
}

// GetPartnerWebhookPreferences returns a partner webhook.
func (s *PartnerWebhookNotificationServer) GetPartnerWebhookPreferences(ctx context.Context, req *partnerapiv1.GetPartnerWebhookPreferencesRequest) (*partnerapiv1.GetPartnerWebhookPreferencesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "partner_webhook", req.GetPartnerWebhookId(), "get_partner_webhook_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get partner webhook preferences", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	step := &partnerWebhookNotificationServerGetPartnerWebhookPreferencesReplayPartnerWebhookEventsStep{request: &partnerapiv1.ReplayPartnerWebhookEventsRequest{
		Context:          req.GetContext(),
		PartnerWebhookId: req.GetPartnerWebhookId(),
		Details: &partnerapiv1.ReplayPartnerWebhookEventsRequest_Details{
			ReceivedAt: req.GetContext().GetDeadline(),
			MessageId:  req.GetContext().GetTraceId(),
			Envelope:   packed,
		},
		Annotations: req.GetContext().GetBaggage(),
	}}
	if err := s.runGetPartnerWebhookPreferencesReplayPartnerWebhookEvents(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookIntegrationService/ReplayPartnerWebhookEvents")
	}
	replayPartnerWebhookEventsReply := step.reply
	return &partnerapiv1.GetPartnerWebhookPreferencesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: replayPartnerWebhookEventsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId:    replayPartnerWebhookEventsReply.GetPartnerWebhook().GetCorrelationId(),
			PartnerWebhookId: req.GetPartnerWebhookId(),
		},
	}, nil
}

// StreamPartnerWebhookNotices streams the changes of partner webhooks as they happen.
func (s *PartnerWebhookNotificationServer) StreamPartnerWebhookNotices(req *partnerapiv1.StreamPartnerWebhookNoticesRequest, stream grpc.ServerStreamingServer[partnerapiv1.StreamPartnerWebhookNoticesResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamPartnerWebhookNotices", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream partner webhook notices", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode snapshot")
	}
	annotateSessionVersionRequest := &identityv1.AnnotateSessionVersionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Assignee:  &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
			Requester: &commonv1.ContactInfo{Phone: req.GetSubscriber().GetPhone()},
		},
		Details: &identityv1.AnnotateSessionVersionRequest_Details{Snapshot: snapshot},
	}
	send := s.annotateSessionVersionSender(annotateSessionVersionRequest)
	annotateSessionVersionReply, err := send(ctx)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/AnnotateSessionVersion")
	}
	resumeShipmentWorkflowRequest := &shippingv1.ResumeShipmentWorkflowRequest{}
	resumeShipmentWorkflowRequest.Context = req.GetContext()
	resumeShipmentWorkflowRequest.Shipment = &shippingv1.Shipment{
		Audit: &commonv1.AuditInfo{
			CreatedAt: annotateSessionVersionReply.GetSession().GetAudit().GetCreatedAt(),
		},
	}
	resumeShipmentWorkflowReply, err := s.shipmentWorkflowClient.ResumeShipmentWorkflow(ctx, resumeShipmentWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/ResumeShipmentWorkflow")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := partnerWebhookNotificationServerListSessionVersionsSteps[mode]
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded partnerapiv1.StreamPartnerWebhookNoticesRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	listSessionVersionsRequest := &identityv1.ListSessionVersionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListSessionVersionsRequest_Filter{
			Region:       resumeShipmentWorkflowReply.GetShipment().GetBillingAddress().GetRegion(),
			OwnerId:      req.GetFilter().GetOwnerId(),
			Query:        req.GetFilter().GetQuery(),
			CreatedAfter: (&decoded).GetCreatedAfter(),
		},
	}
	listSessionVersionsReply, err := run(s, ctx, listSessionVersionsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/ListSessionVersions")
	}
	if err := stream.Send(&partnerapiv1.StreamPartnerWebhookNoticesResponse{
		Result: &commonv1.OperationResult{
			Status:        annotateSessionVersionReply.GetResult().GetStatus(),
			Message:       resumeShipmentWorkflowReply.GetOutcome().GetMessage(),
			CorrelationId: listSessionVersionsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  listSessionVersionsReply.GetResult().GetReferenceIds(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// partnerWebhookNotificationServerGetPartnerWebhookPreferencesReplayPartnerWebhookEventsStep carries the request of one
// step of GetPartnerWebhookPreferences and, once it ran, its reply.
type partnerWebhookNotificationServerGetPartnerWebhookPreferencesReplayPartnerWebhookEventsStep struct {
	request *partnerapiv1.ReplayPartnerWebhookEventsRequest
	reply   *partnerapiv1.ReplayPartnerWebhookEventsResponse
}

func (s *PartnerWebhookNotificationServer) runGetPartnerWebhookPreferencesReplayPartnerWebhookEvents(ctx context.Context, step *partnerWebhookNotificationServerGetPartnerWebhookPreferencesReplayPartnerWebhookEventsStep) error {
	reply, err := s.partnerWebhookIntegrationClient.ReplayPartnerWebhookEvents(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// annotateSessionVersionSender returns a function that sends one request to
// acme.identity.v1.SessionHistoryService/AnnotateSessionVersion.
func (s *PartnerWebhookNotificationServer) annotateSessionVersionSender(request *identityv1.AnnotateSessionVersionRequest) func(context.Context) (*identityv1.AnnotateSessionVersionResponse, error) {
	return func(ctx context.Context) (*identityv1.AnnotateSessionVersionResponse, error) {
		return s.sessionHistoryClient.AnnotateSessionVersion(ctx, request)
	}
}

// partnerWebhookNotificationServerListSessionVersionsSteps are the ways to run
// acme.identity.v1.SessionHistoryService/ListSessionVersions, by name.
var partnerWebhookNotificationServerListSessionVersionsSteps = map[string]func(*PartnerWebhookNotificationServer, context.Context, *identityv1.ListSessionVersionsRequest) (*identityv1.ListSessionVersionsResponse, error){
	"default": (*PartnerWebhookNotificationServer).listSessionVersionsStep,
	"audited": (*PartnerWebhookNotificationServer).listSessionVersionsAuditedStep,
}

func (s *PartnerWebhookNotificationServer) listSessionVersionsStep(ctx context.Context, request *identityv1.ListSessionVersionsRequest) (*identityv1.ListSessionVersionsResponse, error) {
	return s.sessionHistoryClient.ListSessionVersions(ctx, request)
}

func (s *PartnerWebhookNotificationServer) listSessionVersionsAuditedStep(ctx context.Context, request *identityv1.ListSessionVersionsRequest) (*identityv1.ListSessionVersionsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.SessionHistoryService/ListSessionVersions")
	return s.sessionHistoryClient.ListSessionVersions(ctx, request)
}
