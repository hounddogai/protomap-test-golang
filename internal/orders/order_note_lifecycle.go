package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderNoteLifecycleServer implements `acme.orders.v1.OrderNoteLifecycleService`.
// Moves an order note between the states of its lifecycle.
type OrderNoteLifecycleServer struct {
	*orderNoteLifecycleHandlers
}

// orderNoteLifecycleHandlers holds the handlers of OrderNoteLifecycleServer, with what they use.
type orderNoteLifecycleHandlers struct {
	ordersv1.UnimplementedOrderNoteLifecycleServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	orderNoteImportClient          ordersv1.OrderNoteImportServiceClient
	consentRecordCalculationOnce   sync.Once
	consentRecordCalculationCached customersv1.ConsentRecordCalculationServiceClient
	meteredCustomerValidation      meteredCustomerValidation
	reconcileBackordersFn          func(context.Context, *ordersv1.ReconcileBackordersRequest, ...grpc.CallOption) (*ordersv1.ReconcileBackordersResponse, error)
	orderReportingClient           ordersv1.OrderReportingServiceClient
	consentRecordValidationClient  customersv1.ConsentRecordValidationServiceClient
	pauseFulfillmentWorkflowFn     func(context.Context, *fulfillmentv1.PauseFulfillmentWorkflowRequest, ...grpc.CallOption) (*fulfillmentv1.PauseFulfillmentWorkflowResponse, error)
	ledgerAccountValidationClient  ledgerv1.LedgerAccountValidationServiceClient
	documentClient                 documentsv1.DocumentServiceClient
	bus                            *eventbus.Bus
	clients                        *registry.Registry
}

// NewOrderNoteLifecycleServer wires an OrderNoteLifecycleServer.
func NewOrderNoteLifecycleServer(deps platform.Deps) *OrderNoteLifecycleServer {
	backorderSync := ordersv1.NewBackorderSyncServiceClient(deps.Conn)
	fulfillmentWorkflow := fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn)
	return &OrderNoteLifecycleServer{orderNoteLifecycleHandlers: &orderNoteLifecycleHandlers{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.orders.v1.OrderNoteLifecycleService"),
		conn:                          deps.Conn,
		orderNoteImportClient:         ordersv1.NewOrderNoteImportServiceClient(deps.Conn),
		meteredCustomerValidation:     meteredCustomerValidation{CustomerValidationServiceClient: customersv1.NewCustomerValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		reconcileBackordersFn:         backorderSync.ReconcileBackorders,
		orderReportingClient:          ordersv1.NewOrderReportingServiceClient(deps.Conn),
		consentRecordValidationClient: customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		pauseFulfillmentWorkflowFn:    fulfillmentWorkflow.PauseFulfillmentWorkflow,
		ledgerAccountValidationClient: ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		documentClient:                documentsv1.NewDocumentServiceClient(deps.Conn),
		bus:                           deps.Bus,
		clients:                       deps.Clients,
	}}
}

// ActivateOrderNote activates an order note.
func (s *orderNoteLifecycleHandlers) ActivateOrderNote(ctx context.Context, req *ordersv1.ActivateOrderNoteRequest) (*ordersv1.ActivateOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "activate_order_note"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate order note", "tenant", req.GetContext().GetTenantId())
	normalizeApiCredentialRequest := &identityv1.NormalizeApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNotifyContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetNotifyContact().GetName(),
			},
		},
		Details: &identityv1.NormalizeApiCredentialRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
		},
	}
	normalizeApiCredentialReply := new(identityv1.NormalizeApiCredentialResponse)
	if err := s.conn.Invoke(ctx, identityApiCredentialValidationServiceNormalizeApiCredentialMethod, normalizeApiCredentialRequest, normalizeApiCredentialReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/NormalizeApiCredential")
	}
	return &ordersv1.ActivateOrderNoteResponse{
		Outcome: &ordersv1.ActivateOrderNoteResponse_Outcome{
			Status:      normalizeApiCredentialReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		OrderNote: &ordersv1.OrderNote{OrderNoteId: req.GetOrderNoteId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuspendOrderNote suspends an order note.
func (s *orderNoteLifecycleHandlers) SuspendOrderNote(ctx context.Context, req *ordersv1.SuspendOrderNoteRequest) (*ordersv1.SuspendOrderNoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "suspend_order_note"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend order note", "tenant", req.GetContext().GetTenantId())
	searchMetricSeriesesRequest := &metricsv1.SearchMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter:  &metricsv1.SearchMetricSeriesesRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()},
	}
	searchMetricSeriesesReply, err := searchMetricSeriesesCall(ctx, searchMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/SearchMetricSerieses")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	retryOrderNoteImportRequest := &ordersv1.RetryOrderNoteImportRequest{
		Context: req.GetContext(),
		Details: &ordersv1.RetryOrderNoteImportRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
			},
		},
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			CorrelationId: searchMetricSeriesesReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
	}
	type retryOrderNoteImportResult struct {
		reply *ordersv1.RetryOrderNoteImportResponse
		err   error
	}
	retryOrderNoteImportResults := make(chan retryOrderNoteImportResult, 1)
	go func() {
		reply, err := s.orderNoteImportClient.RetryOrderNoteImport(ctx, retryOrderNoteImportRequest)
		retryOrderNoteImportResults <- retryOrderNoteImportResult{reply, err}
	}()
	var retryOrderNoteImportReply *ordersv1.RetryOrderNoteImportResponse
	select {
	case result := <-retryOrderNoteImportResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderNoteImportService/RetryOrderNoteImport")
		}
		retryOrderNoteImportReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &ordersv1.SuspendOrderNoteResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: searchMetricSeriesesReply.GetResult().GetReferenceIds(),
			Status:       retryOrderNoteImportReply.GetResult().GetStatus(),
		},
		OrderNote: &ordersv1.OrderNote{OrderNoteId: req.GetOrderNoteId()},
		Outcome:   &ordersv1.SuspendOrderNoteResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstateOrderNote reinstates an order note.
func (s *orderNoteLifecycleHandlers) ReinstateOrderNote(ctx context.Context, req *ordersv1.ReinstateOrderNoteRequest) (*ordersv1.ReinstateOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "reinstate_order_note"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate order note", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareConsentRecordOptionsRequest := &customersv1.CompareConsentRecordOptionsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	compareConsentRecordOptionsReply, err := s.consentRecordCalculationLazy().CompareConsentRecordOptions(ctx, compareConsentRecordOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CompareConsentRecordOptions")
	}
	return &ordersv1.ReinstateOrderNoteResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: compareConsentRecordOptionsReply.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
		OrderNote: &ordersv1.OrderNote{OrderNoteId: req.GetOrderNoteId()},
		Outcome:   &ordersv1.ReinstateOrderNoteResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminateOrderNote terminates an order note.
func (s *orderNoteLifecycleHandlers) TerminateOrderNote(ctx context.Context, req *ordersv1.TerminateOrderNoteRequest) (*ordersv1.TerminateOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "terminate_order_note"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate order note", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *ordersv1.TerminateOrderNoteRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	checkCustomerEligibilityRequest := &customersv1.CheckCustomerEligibilityRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       cachedDetails.GetNotifyContact().GetEmail(),
			},
		},
	}
	checkCustomerEligibilityReply, err := s.meteredCustomerValidation.CheckCustomerEligibility(ctx, checkCustomerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/CheckCustomerEligibility")
	}
	listOrderNoteImportsRequest := &ordersv1.ListOrderNoteImportsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.ListOrderNoteImportsRequest_Filter{
			Region:       checkCustomerEligibilityReply.GetCustomer().GetPrimaryAddress().GetRegion(),
			CreatedAfter: checkCustomerEligibilityReply.GetCustomer().GetAudit().GetUpdatedAt(),
			OwnerId:      checkCustomerEligibilityReply.GetCustomer().GetRelationshipManager().GetUserId(),
		},
	}
	listOrderNoteImportsReply, err := grpcx.Call(ctx, s.orderNoteImportClient.ListOrderNoteImports, listOrderNoteImportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteImportService/ListOrderNoteImports")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.TerminateOrderNoteRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileBackordersRequest := &ordersv1.ReconcileBackordersRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{Name: (&decoded).GetNotifyContact().GetName()},
		},
		Details: &ordersv1.ReconcileBackordersRequest_Details{Payload: snapshot},
	}
	reconcileBackordersReply, err := s.reconcileBackordersFn(ctx, reconcileBackordersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/ReconcileBackorders")
	}
	_ = reconcileBackordersReply
	return &ordersv1.TerminateOrderNoteResponse{
		Result: &commonv1.OperationResult{
			Message: checkCustomerEligibilityReply.GetResult().GetMessage(),
			Details: checkCustomerEligibilityReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &ordersv1.TerminateOrderNoteResponse_Outcome{
			Message:     listOrderNoteImportsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		OrderNote: &ordersv1.OrderNote{OrderNoteId: req.GetOrderNoteId()},
	}, nil
}

// RenewOrderNote renews an order note.
func (s *orderNoteLifecycleHandlers) RenewOrderNote(ctx context.Context, req *ordersv1.RenewOrderNoteRequest) (*ordersv1.RenewOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewOrderNote", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "renew_order_note"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew order note", "tenant", req.GetContext().GetTenantId())
	countOrderTotalsRequest := buildCountOrderTotalsRequestForRenewOrderNote(req)
	countOrderTotalsReply, err := s.orderReportingClient.CountOrderTotals(ctx, countOrderTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/CountOrderTotals")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainConsentRecordDecisionRequest := &customersv1.ExplainConsentRecordDecisionRequest{}
	explainConsentRecordDecisionRequest.Context = req.GetContext()
	explainConsentRecordDecisionRequest.Details = &customersv1.ExplainConsentRecordDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: req.GetDetails().GetNotifyContact().GetEmail(),
			Name:  req.GetDetails().GetNotifyContact().GetName(),
			Phone: req.GetDetails().GetNotifyContact().GetPhone(),
		},
	}
	explainConsentRecordDecisionRequest.ConsentRecord = &customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{
			DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
		},
		Detail: packed,
	}
	explainConsentRecordDecisionReply, err := s.consentRecordValidationClient.ExplainConsentRecordDecision(ctx, explainConsentRecordDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	return &ordersv1.RenewOrderNoteResponse{
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: countOrderTotalsReply.GetResult().GetCorrelationId(),
			Attributes:    explainConsentRecordDecisionReply.GetConsentRecord().GetAttributes(),
			OrderNoteId:   req.GetOrderNoteId(),
		},
		Outcome: &ordersv1.RenewOrderNoteResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireOrderNote expires an order note.
func (s *orderNoteLifecycleHandlers) ExpireOrderNote(ctx context.Context, req *ordersv1.ExpireOrderNoteRequest) (*ordersv1.ExpireOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "expire_order_note"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire order note", "tenant", req.GetContext().GetTenantId())
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	pauseFulfillmentWorkflowRequest := &fulfillmentv1.PauseFulfillmentWorkflowRequest{
		Context: req.GetContext(),
		Fulfillment: &fulfillmentv1.Fulfillment{
			Counterparty: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				DisplayName: displayName,
			},
		},
	}
	pauseFulfillmentWorkflowReply, err := s.pauseFulfillmentWorkflowFn(ctx, pauseFulfillmentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/PauseFulfillmentWorkflow")
	}
	validateLedgerAccountReply, err := s.ledgerAccountValidationClient.ValidateLedgerAccount(ctx, &ledgerv1.ValidateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
			},
			Funding: &ledgerv1.LedgerAccount_Card{Card: &ledgerv1.LedgerAccountCardFunding{
				Token: pauseFulfillmentWorkflowReply.GetFulfillment().GetCard().GetToken(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ValidateLedgerAccount")
	}
	explainBackorderCalculationRequest := &ordersv1.ExplainBackorderCalculationRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedBy(),
			},
			Requester: &commonv1.ContactInfo{
				Name: validateLedgerAccountReply.GetLedgerAccount().GetContact().GetName(),
			},
		},
	}
	explainBackorderCalculationReply, err := explainBackorderCalculationCall(ctx, explainBackorderCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/ExplainBackorderCalculation")
	}
	watchDocumentsStream, err := s.documentClient.WatchDocuments(ctx, &documentsv1.WatchDocumentsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone: validateLedgerAccountReply.GetLedgerAccount().GetContact().GetPhone(),
			Name:  validateLedgerAccountReply.GetLedgerAccount().GetContact().GetName(),
			Email: req.GetDetails().GetNotifyContact().GetEmail(),
		},
		Filter: &documentsv1.WatchDocumentsRequest_Filter{
			Region: pauseFulfillmentWorkflowReply.GetFulfillment().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
	}
	for {
		received, err := watchDocumentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/WatchDocuments")
		}
		_ = received
	}
	_ = explainBackorderCalculationReply
	return &ordersv1.ExpireOrderNoteResponse{
		Result: &commonv1.OperationResult{
			Message: pauseFulfillmentWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: validateLedgerAccountReply.GetResult().GetCorrelationId(),
			OrderNoteId:   req.GetOrderNoteId(),
		},
		Outcome: &ordersv1.ExpireOrderNoteResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetOrderNoteLifecycle returns an order note.
func (s *orderNoteLifecycleHandlers) GetOrderNoteLifecycle(ctx context.Context, req *ordersv1.GetOrderNoteLifecycleRequest) (*ordersv1.GetOrderNoteLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetOrderNoteLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_note", req.GetOrderNoteId(), "get_order_note_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order note lifecycle", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "orders.get_order_note_lifecycle.quote_form",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"form.counterparty.email": req.GetContext().GetActor().GetEmail(),
			"context.tenant_id":       req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &ordersv1.GetOrderNoteLifecycleResponse{
		OrderNote: &ordersv1.OrderNote{OrderNoteId: req.GetOrderNoteId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleOrderNoteExpiry schedules an order note.
func (s *orderNoteLifecycleHandlers) ScheduleOrderNoteExpiry(ctx context.Context, req *ordersv1.ScheduleOrderNoteExpiryRequest) (*ordersv1.ScheduleOrderNoteExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleOrderNoteExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "schedule order note expiry", "tenant", req.GetContext().GetTenantId())
	orderReporting, err := registry.Lookup[ordersv1.OrderReportingServiceClient](s.clients, "acme.orders.v1.OrderReportingService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	orderNoteCache := cache.New[string, *ordersv1.OrderNote](1)
	orderNoteCache.Put(req.GetContext().GetTraceId(), req.GetOrderNote())
	cachedOrderNote, _ := orderNoteCache.Get(req.GetContext().GetTraceId())
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	generateOrderReportRequest := &ordersv1.GenerateOrderReportRequest{
		Context: req.GetContext(),
		OrderId: cachedOrderNote.GetSubject().GetOrderId(),
		Details: &ordersv1.GenerateOrderReportRequest_Details{Format: envelope.GetDisplayName()},
	}
	generateOrderReportReply, err := orderReporting.GenerateOrderReport(ctx, generateOrderReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/GenerateOrderReport")
	}
	userQuery, ok := s.clients.Get("acme.identity.v1.UserQueryService").(identityv1.UserQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.UserQueryService")
	}
	peekUserRequest := &identityv1.PeekUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	}
	peekUserReply, err := userQuery.PeekUser(ctx, peekUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/PeekUser")
	}
	return &ordersv1.ScheduleOrderNoteExpiryResponse{
		Outcome: &ordersv1.ScheduleOrderNoteExpiryResponse_Outcome{Status: generateOrderReportReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: generateOrderReportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: peekUserReply.GetResult().GetCorrelationId(),
			OrderNoteId:   req.GetOrderNoteId(),
			OccurredAt:    req.GetOrderNote().GetOccurredAt(),
		},
	}, nil
}

// consentRecordCalculationLazy dials acme.customers.v1.ConsentRecordCalculationService the first time a handler needs
// it.
func (s *orderNoteLifecycleHandlers) consentRecordCalculationLazy() customersv1.ConsentRecordCalculationServiceClient {
	s.consentRecordCalculationOnce.Do(func() {
		s.consentRecordCalculationCached = customersv1.NewConsentRecordCalculationServiceClient(s.conn)
	})
	return s.consentRecordCalculationCached
}
