package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// OrderNoteSyncServer implements `acme.orders.v1.OrderNoteSyncService`.
// Keeps order notes in step with the systems of record in other regions.
type OrderNoteSyncServer struct {
	ordersv1.UnimplementedOrderNoteSyncServiceServer
	store                         *Store
	log                           *slog.Logger
	customerValidationClient      customersv1.CustomerValidationServiceClient
	mediaAssetCalculationClient   mediav1.MediaAssetCalculationServiceClient
	clients                       *registry.Registry
	conn                          *grpc.ClientConn
	trackingEventReportingOnce    sync.Once
	trackingEventReportingCached  shippingv1.TrackingEventReportingServiceClient
	orderAmendmentReportingClient ordersv1.OrderAmendmentReportingServiceClient
	notificationClient            notificationsv1.NotificationServiceClient
	orderCalculationClient        ordersv1.OrderCalculationServiceClient
	backorderCalculationClient    ordersv1.BackorderCalculationServiceClient
	backorderSyncClient           ordersv1.BackorderSyncServiceClient
	meteredNotificationQuery      meteredNotificationQuery
	carrierRateWorkflowOnce       sync.Once
	carrierRateWorkflowCached     shippingv1.CarrierRateWorkflowServiceClient
	synonymNotificationClient     searchv1.SynonymNotificationServiceClient
}

// NewOrderNoteSyncServer wires an OrderNoteSyncServer.
func NewOrderNoteSyncServer(deps platform.Deps) *OrderNoteSyncServer {
	return &OrderNoteSyncServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.orders.v1.OrderNoteSyncService"),
		customerValidationClient:      customersv1.NewCustomerValidationServiceClient(deps.Conn),
		mediaAssetCalculationClient:   mediav1.NewMediaAssetCalculationServiceClient(deps.Conn),
		clients:                       deps.Clients,
		conn:                          deps.Conn,
		orderAmendmentReportingClient: ordersv1.NewOrderAmendmentReportingServiceClient(deps.Conn),
		notificationClient:            notificationsv1.NewNotificationServiceClient(deps.Conn),
		orderCalculationClient:        ordersv1.NewOrderCalculationServiceClient(deps.Conn),
		backorderCalculationClient:    ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		backorderSyncClient:           ordersv1.NewBackorderSyncServiceClient(deps.Conn),
		meteredNotificationQuery:      meteredNotificationQuery{NotificationQueryServiceClient: notificationsv1.NewNotificationQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		synonymNotificationClient:     searchv1.NewSynonymNotificationServiceClient(deps.Conn),
	}
}

// SyncOrderNotes syncs an order note.
func (s *OrderNoteSyncServer) SyncOrderNotes(ctx context.Context, req *ordersv1.SyncOrderNotesRequest) (*ordersv1.SyncOrderNotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "sync order notes", "tenant", req.GetContext().GetTenantId())
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scoreCustomerRequest := &customersv1.ScoreCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
			},
			IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{Number: req.GetOrderNote().GetSubject().GetNumber()}},
		},
		CustomerId: envelope.GetDisplayName(),
	}
	scoreCustomerReply, err := customersv1.CustomerValidationServiceClient.ScoreCustomer(s.customerValidationClient, ctx, scoreCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ScoreCustomer")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := orderNoteSyncServerCompareMediaAssetOptionsSteps[mode]
	compareMediaAssetOptionsRequest := &mediav1.CompareMediaAssetOptionsRequest{
		Context: req.GetContext(),
		Details: &mediav1.CompareMediaAssetOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: scoreCustomerReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
			},
		},
		MediaAsset: &mediav1.MediaAsset{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreCustomerReply.GetCustomer().GetAudit().GetUpdatedBy(),
			},
		},
	}
	compareMediaAssetOptionsReply, err := run(s, ctx, compareMediaAssetOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetCalculationService/CompareMediaAssetOptions")
	}
	orderAmendmentReporting, err := registry.Lookup[ordersv1.OrderAmendmentReportingServiceClient](s.clients, "acme.orders.v1.OrderAmendmentReportingService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	shareOrderAmendmentReportRequest := &ordersv1.ShareOrderAmendmentReportRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ShareOrderAmendmentReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            scoreCustomerReply.GetCustomer().GetContact().GetEmail(),
				PreferredChannel: scoreCustomerReply.GetCustomer().GetContact().GetPreferredChannel(),
				Name:             scoreCustomerReply.GetCustomer().GetContact().GetName(),
				Phone:            scoreCustomerReply.GetCustomer().GetContact().GetPhone(),
			},
		},
		OrderAmendmentId: envelope2.GetDisplayName(),
	}
	shareOrderAmendmentReportReply, err := orderAmendmentReporting.ShareOrderAmendmentReport(ctx, shareOrderAmendmentReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentReportingService/ShareOrderAmendmentReport")
	}
	cloned := proto.Clone(req.GetOrderNote()).(*ordersv1.OrderNote)
	countTrackingEventTotalsRequest := &shippingv1.CountTrackingEventTotalsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.CountTrackingEventTotalsRequest_Filter{
			Region:       shareOrderAmendmentReportReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
			CreatedAfter: scoreCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
			Labels:       shareOrderAmendmentReportReply.GetResult().GetDetails(),
			OwnerId:      cloned.GetSubject().GetOrderId(),
		},
	}
	countTrackingEventTotalsReply, err := s.trackingEventReportingLazy().CountTrackingEventTotals(ctx, countTrackingEventTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/CountTrackingEventTotals")
	}
	return &ordersv1.SyncOrderNotesResponse{
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				Number: scoreCustomerReply.GetCustomer().GetPassport().GetNumber(),
			},
			CorrelationId: compareMediaAssetOptionsReply.GetResult().GetCorrelationId(),
			OrderNoteId:   req.GetOrderNoteId(),
			Attributes:    req.GetOrderNote().GetAttributes(),
		},
		Result: &commonv1.OperationResult{
			Message: shareOrderAmendmentReportReply.GetOutcome().GetMessage(),
			Details: countTrackingEventTotalsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// PushOrderNote pushs an order note.
func (s *OrderNoteSyncServer) PushOrderNote(ctx context.Context, req *ordersv1.PushOrderNoteRequest) (*ordersv1.PushOrderNoteResponse, error) {
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "push order note", "tenant", req.GetContext().GetTenantId())
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scheduleOrderAmendmentReportRequest := &ordersv1.ScheduleOrderAmendmentReportRequest{}
	scheduleOrderAmendmentReportRequest.Context = req.GetContext()
	scheduleOrderAmendmentReportRequest.Reason = req.GetReason()
	scheduleOrderAmendmentReportRequest.OrderAmendmentId = envelope.GetDisplayName()
	scheduleOrderAmendmentReportReply, err := s.orderAmendmentReportingClient.ScheduleOrderAmendmentReport(ctx, scheduleOrderAmendmentReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentReportingService/ScheduleOrderAmendmentReport")
	}
	for _, measurement := range req.GetOrderNote().GetMeasurements() {
		if _, err := s.notificationClient.ListNotifications(ctx, &notificationsv1.ListNotificationsRequest{
			Context: req.GetContext(),
			Filter: &notificationsv1.ListNotificationsRequest_Filter{
				CreatedAfter: measurement.GetObservedAt(),
				OwnerId:      req.GetOrderNote().GetOrderNoteId(),
				Labels:       req.GetAnnotations(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/ListNotifications")
		}
	}
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.PushOrderNoteRequest_Details)
	simulateOrderRequest := &ordersv1.SimulateOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			OrderId: req.GetOrderNote().GetSubject().GetOrderId(),
			Number:  req.GetOrderNote().GetSubject().GetNumber(),
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
			},
			Attachments: []*ordersv1.OrderAttachment{&ordersv1.OrderAttachment{Checksum: cloned.GetChecksum()}},
		},
	}
	simulateOrderReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*ordersv1.SimulateOrderResponse, error) {
		return s.orderCalculationClient.SimulateOrder(ctx, simulateOrderRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/SimulateOrder")
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	compareConsentRecordOptionsRequest := &customersv1.CompareConsentRecordOptionsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
			},
		},
		Details: &customersv1.CompareConsentRecordOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: simulateOrderReply.GetOrder().GetBillingAddress().GetCountryCode(),
			},
		},
		ConsentRecordId: envelope2.GetDisplayName(),
	}
	compareConsentRecordOptionsReply, err := grpcx.NewClient(s.conn, customersv1.NewConsentRecordCalculationServiceClient).CompareConsentRecordOptions(ctx, compareConsentRecordOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CompareConsentRecordOptions")
	}
	return &ordersv1.PushOrderNoteResponse{
		Result: &commonv1.OperationResult{
			Message: scheduleOrderAmendmentReportReply.GetResult().GetMessage(),
			Status:  simulateOrderReply.GetResult().GetStatus(),
		},
		Outcome: &ordersv1.PushOrderNoteResponse_Outcome{Message: simulateOrderReply.GetResult().GetMessage()},
		OrderNote: &ordersv1.OrderNote{
			Sequence:      compareConsentRecordOptionsReply.GetConsentRecord().GetSequence(),
			CorrelationId: compareConsentRecordOptionsReply.GetConsentRecord().GetCorrelationId(),
			OrderNoteId:   req.GetOrderNoteId(),
			Attributes:    req.GetOrderNote().GetAttributes(),
		},
	}, nil
}

// ReconcileOrderNotes reconciles an order note.
func (s *OrderNoteSyncServer) ReconcileOrderNotes(ctx context.Context, req *ordersv1.ReconcileOrderNotesRequest) (*ordersv1.ReconcileOrderNotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "reconcile order notes", "tenant", req.GetContext().GetTenantId())
	generateOrderAmendmentReportReply, err := s.orderAmendmentReportingClient.GenerateOrderAmendmentReport(ctx, &ordersv1.GenerateOrderAmendmentReportRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentReportingService/GenerateOrderAmendmentReport")
	}
	generateOrderReportRequest := &ordersv1.GenerateOrderReportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	generateOrderReportReply, err := grpcx.NewClient(s.conn, ordersv1.NewOrderReportingServiceClient).GenerateOrderReport(ctx, generateOrderReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/GenerateOrderReport")
	}
	var holder struct{ Checksum string }
	if record := req.GetDetails(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Checksum"); err != nil {
			s.log.WarnContext(ctx, "copy checksum", "error", err)
		}
	}
	recallGiftCardBatchRequest := &giftcardsv1.RecallGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
			},
			Attachments: []*giftcardsv1.GiftCardBatchAttachment{&giftcardsv1.GiftCardBatchAttachment{Checksum: holder.Checksum}},
		},
	}
	recallGiftCardBatchReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardBatchApprovalServiceClient).RecallGiftCardBatch(ctx, recallGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/RecallGiftCardBatch")
	}
	return &ordersv1.ReconcileOrderNotesResponse{
		Result: &commonv1.OperationResult{
			Status:       generateOrderAmendmentReportReply.GetResult().GetStatus(),
			Message:      generateOrderReportReply.GetResult().GetMessage(),
			ReferenceIds: generateOrderReportReply.GetResult().GetReferenceIds(),
		},
		Outcome: &ordersv1.ReconcileOrderNotesResponse_Outcome{
			Status: generateOrderAmendmentReportReply.GetResult().GetStatus(),
		},
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: recallGiftCardBatchReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
			OrderNoteId: req.GetOrderNoteId(),
			Attributes:  req.GetOrderNote().GetAttributes(),
		},
	}, nil
}

// StreamOrderNoteChanges exchanges changes of order notes with a peer.
func (s *OrderNoteSyncServer) StreamOrderNoteChanges(stream grpc.BidiStreamingServer[ordersv1.StreamOrderNoteChangesRequest, ordersv1.StreamOrderNoteChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *ordersv1.StreamOrderNoteChangesRequest)
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
		s.log.InfoContext(ctx, "stream order note changes", "tenant", msg.GetContext().GetTenantId())
		recalculateBackordersRequest := &ordersv1.RecalculateBackordersRequest{}
		recalculateBackordersRequest.Context = msg.GetContext()
		recalculateBackordersRequest.Backorder = &ordersv1.Backorder{
			Origin: &ordersv1.Backorder_Customer{Customer: &customersv1.CustomerRef{DisplayName: msg.GetChange().GetSubject().GetDisplayName()}},
			Assignee: &identityv1.UserRef{
				UserId: msg.GetContext().GetActor().GetUserId(),
				Email:  msg.GetContext().GetActor().GetEmail(),
			},
		}
		if _, err := s.backorderCalculationClient.RecalculateBackorders(ctx, recalculateBackordersRequest); err != nil {
			return grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/RecalculateBackorders")
		}
		generateOrderAmendmentReportRequest := &ordersv1.GenerateOrderAmendmentReportRequest{Context: msg.GetContext()}
		if _, err := grpcx.Call(ctx, s.orderAmendmentReportingClient.GenerateOrderAmendmentReport, generateOrderAmendmentReportRequest); err != nil {
			return grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentReportingService/GenerateOrderAmendmentReport")
		}
		pushBackorderRequest := &ordersv1.PushBackorderRequest{Context: msg.GetContext()}
		if _, err := grpcx.Call(ctx, s.backorderSyncClient.PushBackorder, pushBackorderRequest); err != nil {
			return grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/PushBackorder")
		}
		resolveNotificationsRequest := &notificationsv1.ResolveNotificationsRequest{Context: msg.GetContext()}
		if _, err := s.meteredNotificationQuery.ResolveNotifications(ctx, resolveNotificationsRequest); err != nil {
			return grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ResolveNotifications")
		}
		if err := stream.Send(&ordersv1.StreamOrderNoteChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
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

// ResolveOrderNoteConflict resolves an order note.
func (s *OrderNoteSyncServer) ResolveOrderNoteConflict(ctx context.Context, req *ordersv1.ResolveOrderNoteConflictRequest) (*ordersv1.ResolveOrderNoteConflictResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "resolve order note conflict", "tenant", req.GetContext().GetTenantId())
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	retryCarrierRateWorkflowRequest := &shippingv1.RetryCarrierRateWorkflowRequest{
		Context: req.GetContext(),
		CarrierRate: &shippingv1.CarrierRate{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
				UserId:      req.GetContext().GetActor().GetUserId(),
			},
			Action: &shippingv1.CarrierRate_Block{Block: &shippingv1.CarrierRateBlockAction{Reason: req.GetReason()}},
		},
		Details: &shippingv1.RetryCarrierRateWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		CarrierRateId: envelope.GetDisplayName(),
	}
	retryCarrierRateWorkflowReply, err := s.carrierRateWorkflowLazy().RetryCarrierRateWorkflow(ctx, retryCarrierRateWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateWorkflowService/RetryCarrierRateWorkflow")
	}
	snapshot, err := protojson.Marshal(req.GetOrderNote())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	syncBackordersReply, err := s.backorderSyncClient.SyncBackorders(ctx, &ordersv1.SyncBackordersRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				Email: retryCarrierRateWorkflowReply.GetCarrierRate().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: retryCarrierRateWorkflowReply.GetCarrierRate().GetAudit().GetCreatedAt(),
			},
			Origin: &ordersv1.Backorder_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
			}},
		},
		Details: &ordersv1.SyncBackordersRequest_Details{
			Checksum: req.GetDetails().GetChecksum(),
			Payload:  snapshot,
		},
		BackorderId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/SyncBackorders")
	}
	return &ordersv1.ResolveOrderNoteConflictResponse{
		Outcome: &ordersv1.ResolveOrderNoteConflictResponse_Outcome{
			ReferenceId: retryCarrierRateWorkflowReply.GetOutcome().GetReferenceId(),
			Status:      syncBackordersReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Status:        retryCarrierRateWorkflowReply.GetResult().GetStatus(),
			CorrelationId: syncBackordersReply.GetResult().GetCorrelationId(),
		},
		OrderNote: &ordersv1.OrderNote{
			OrderNoteId: req.GetOrderNoteId(),
			Source:      req.GetOrderNote().GetSource(),
		},
	}, nil
}

// GetOrderNoteSyncState returns an order note.
func (s *OrderNoteSyncServer) GetOrderNoteSyncState(ctx context.Context, req *ordersv1.GetOrderNoteSyncStateRequest) (*ordersv1.GetOrderNoteSyncStateResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "get_order_note_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order note sync state", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	step := &orderNoteSyncServerGetOrderNoteSyncStateListSynonymSubscriptionsStep{request: &searchv1.ListSynonymSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymSubscriptionsRequest_Filter{
			OwnerId:      req.GetOrderNoteId(),
			Labels:       baggage,
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}}
	if err := s.runGetOrderNoteSyncStateListSynonymSubscriptions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/ListSynonymSubscriptions")
	}
	listSynonymSubscriptionsReply := step.reply
	return &ordersv1.GetOrderNoteSyncStateResponse{
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: listSynonymSubscriptionsReply.GetResult().GetCorrelationId(),
			OrderNoteId:   req.GetOrderNoteId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListOrderNoteConflicts lists order notes a page at a time.
func (s *OrderNoteSyncServer) ListOrderNoteConflicts(ctx context.Context, req *ordersv1.ListOrderNoteConflictsRequest) (*ordersv1.ListOrderNoteConflictsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list order note conflicts", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.ListOrderNoteConflictsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// orderNoteSyncServerCompareMediaAssetOptionsSteps are the ways to run
// acme.media.v1.MediaAssetCalculationService/CompareMediaAssetOptions, by name.
var orderNoteSyncServerCompareMediaAssetOptionsSteps = map[string]func(*OrderNoteSyncServer, context.Context, *mediav1.CompareMediaAssetOptionsRequest) (*mediav1.CompareMediaAssetOptionsResponse, error){
	"default": (*OrderNoteSyncServer).compareMediaAssetOptionsStep,
	"audited": (*OrderNoteSyncServer).compareMediaAssetOptionsAuditedStep,
}

func (s *OrderNoteSyncServer) compareMediaAssetOptionsStep(ctx context.Context, request *mediav1.CompareMediaAssetOptionsRequest) (*mediav1.CompareMediaAssetOptionsResponse, error) {
	return s.mediaAssetCalculationClient.CompareMediaAssetOptions(ctx, request)
}

func (s *OrderNoteSyncServer) compareMediaAssetOptionsAuditedStep(ctx context.Context, request *mediav1.CompareMediaAssetOptionsRequest) (*mediav1.CompareMediaAssetOptionsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.media.v1.MediaAssetCalculationService/CompareMediaAssetOptions")
	return s.mediaAssetCalculationClient.CompareMediaAssetOptions(ctx, request)
}

// trackingEventReportingLazy dials acme.shipping.v1.TrackingEventReportingService the first time a handler needs it.
func (s *OrderNoteSyncServer) trackingEventReportingLazy() shippingv1.TrackingEventReportingServiceClient {
	s.trackingEventReportingOnce.Do(func() { s.trackingEventReportingCached = shippingv1.NewTrackingEventReportingServiceClient(s.conn) })
	return s.trackingEventReportingCached
}

// carrierRateWorkflowLazy dials acme.shipping.v1.CarrierRateWorkflowService the first time a handler needs it.
func (s *OrderNoteSyncServer) carrierRateWorkflowLazy() shippingv1.CarrierRateWorkflowServiceClient {
	s.carrierRateWorkflowOnce.Do(func() { s.carrierRateWorkflowCached = shippingv1.NewCarrierRateWorkflowServiceClient(s.conn) })
	return s.carrierRateWorkflowCached
}

// orderNoteSyncServerGetOrderNoteSyncStateListSynonymSubscriptionsStep carries the request of one step of
// GetOrderNoteSyncState and, once it ran, its reply.
type orderNoteSyncServerGetOrderNoteSyncStateListSynonymSubscriptionsStep struct {
	request *searchv1.ListSynonymSubscriptionsRequest
	reply   *searchv1.ListSynonymSubscriptionsResponse
}

func (s *OrderNoteSyncServer) runGetOrderNoteSyncStateListSynonymSubscriptions(ctx context.Context, step *orderNoteSyncServerGetOrderNoteSyncStateListSynonymSubscriptionsStep) error {
	reply, err := s.synonymNotificationClient.ListSynonymSubscriptions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
