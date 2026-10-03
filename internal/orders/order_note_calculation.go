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
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// OrderNoteCalculationServer implements `acme.orders.v1.OrderNoteCalculationService`.
// Computes the figures of an order note, such as totals, taxes, and estimates.
type OrderNoteCalculationServer struct {
	ordersv1.UnsafeOrderNoteCalculationServiceServer
	store                            *Store
	log                              *slog.Logger
	clients                          *registry.Registry
	conn                             *grpc.ClientConn
	orderValidationOnce              sync.Once
	orderValidationCached            ordersv1.OrderValidationServiceClient
	orderValidationClient            ordersv1.OrderValidationServiceClient
	bus                              *eventbus.Bus
	notificationQueryClient          notificationsv1.NotificationQueryServiceClient
	userHistoryClient                identityv1.UserHistoryServiceClient
	lintOrdersFn                     func(context.Context, *ordersv1.LintOrdersRequest, ...grpc.CallOption) (*ordersv1.LintOrdersResponse, error)
	backorderCalculationClient       ordersv1.BackorderCalculationServiceClient
	orderCalculationClient           ordersv1.OrderCalculationServiceClient
	orderValidationClientFallback    ordersv1.OrderValidationServiceClient
	documentValidationClient         documentsv1.DocumentValidationServiceClient
	documentValidationClientFallback documentsv1.DocumentValidationServiceClient
}

// NewOrderNoteCalculationServer wires an OrderNoteCalculationServer.
func NewOrderNoteCalculationServer(deps platform.Deps) *OrderNoteCalculationServer {
	orderValidation := ordersv1.NewOrderValidationServiceClient(deps.Conn)
	return &OrderNoteCalculationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.orders.v1.OrderNoteCalculationService"),
		clients:                          deps.Clients,
		conn:                             deps.Conn,
		orderValidationClient:            ordersv1.NewOrderValidationServiceClient(deps.Conn),
		bus:                              deps.Bus,
		notificationQueryClient:          notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		userHistoryClient:                identityv1.NewUserHistoryServiceClient(deps.Conn),
		lintOrdersFn:                     orderValidation.LintOrders,
		backorderCalculationClient:       ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		orderCalculationClient:           ordersv1.NewOrderCalculationServiceClient(deps.Conn),
		orderValidationClientFallback:    ordersv1.NewOrderValidationServiceClient(deps.Conn),
		documentValidationClient:         documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		documentValidationClientFallback: documentsv1.NewDocumentValidationServiceClient(deps.Conn),
	}
}

// CalculateOrderNote calculates an order note.
func (s *OrderNoteCalculationServer) CalculateOrderNote(ctx context.Context, req *ordersv1.CalculateOrderNoteRequest) (*ordersv1.CalculateOrderNoteResponse, error) {
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "calculate order note", "tenant", req.GetContext().GetTenantId())
	orderAmendmentValidation, err := registry.Lookup[ordersv1.OrderAmendmentValidationServiceClient](s.clients, "acme.orders.v1.OrderAmendmentValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.CalculateOrderNoteRequest_Details)
	var holder struct{ Recipient string }
	if record := req.GetDetails().GetDestination(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Recipient"); err != nil {
			s.log.WarnContext(ctx, "copy recipient", "error", err)
		}
	}
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintOrderAmendmentsRequest := &ordersv1.LintOrderAmendmentsRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			BillingAddress: &commonv1.PostalAddress{
				Region:     cloned.GetDestination().GetRegion(),
				Recipient:  holder.Recipient,
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
		OrderAmendmentId: envelope.GetDisplayName(),
	}
	lintOrderAmendmentsReply, err := orderAmendmentValidation.LintOrderAmendments(ctx, lintOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/LintOrderAmendments")
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintOrdersRequest := &ordersv1.LintOrdersRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Region:     lintOrderAmendmentsReply.GetOrderAmendment().GetBillingAddress().GetRegion(),
				Locality:   req.GetDetails().GetDestination().GetLocality(),
				PostalCode: lintOrderAmendmentsReply.GetOrderAmendment().GetBillingAddress().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: lintOrderAmendmentsReply.GetOrderAmendment().GetAudit().GetEtag(),
			},
		},
		OrderId: envelope2.GetDisplayName(),
	}
	lintOrdersReply, err := s.orderValidationLazy().LintOrders(ctx, lintOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/LintOrders")
	}
	return &ordersv1.CalculateOrderNoteResponse{
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: lintOrderAmendmentsReply.GetOrderAmendment().GetCounterparty().GetDisplayName(),
				OrderId:     lintOrdersReply.GetOrder().GetOrderId(),
			},
			OrderNoteId: req.GetOrderNoteId(),
		},
		Result: &commonv1.OperationResult{
			Status:        lintOrderAmendmentsReply.GetOutcome().GetStatus(),
			CorrelationId: req.GetOrderNote().GetCorrelationId(),
		},
	}, nil
}

// EstimateOrderNote estimates an order note.
func (s *OrderNoteCalculationServer) EstimateOrderNote(ctx context.Context, req *ordersv1.EstimateOrderNoteRequest) (*ordersv1.EstimateOrderNoteResponse, error) {
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "estimate order note", "tenant", req.GetContext().GetTenantId())
	lintOrdersRequest := buildLintOrdersRequestForEstimateOrderNote(req)
	lintOrdersReply, err := s.orderValidationClient.LintOrders(ctx, lintOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/LintOrders")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "orders.estimate_order_note.estimate_order_amendment",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"order_amendment.card.last4":       lintOrdersReply.GetOrder().GetCard().GetLast4(),
			"details.destination.recipient":    req.GetDetails().GetDestination().GetRecipient(),
			"order_amendment.audit.created_by": lintOrdersReply.GetOrder().GetAudit().GetCreatedBy(),
			"context.tenant_id":                req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	exportNotificationsStream, err := s.notificationQueryClient.ExportNotifications(ctx, &notificationsv1.ExportNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.ExportNotificationsRequest_Filter{
			CreatedAfter: lintOrdersReply.GetOrder().GetAudit().GetCreatedAt(),
			OwnerId:      lintOrdersReply.GetOrder().GetCounterparty().GetCustomerId(),
			Query:        envelope.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ExportNotifications")
	}
	for {
		received, err := exportNotificationsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ExportNotifications")
		}
		_ = received
	}
	storageBinQuery, ok := s.clients.Get("acme.warehousing.v1.StorageBinQueryService").(warehousingv1.StorageBinQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.warehousing.v1.StorageBinQueryService")
	}
	resolveStorageBinsRequest := &warehousingv1.ResolveStorageBinsRequest{Context: req.GetContext()}
	resolveStorageBinsReply, err := storageBinQuery.ResolveStorageBins(ctx, resolveStorageBinsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/ResolveStorageBins")
	}
	return &ordersv1.EstimateOrderNoteResponse{
		OrderNote: &ordersv1.OrderNote{
			Subject:       &ordersv1.OrderRef{Number: lintOrdersReply.GetOrder().GetNumber()},
			OrderNoteId:   req.GetOrderNoteId(),
			CorrelationId: req.GetOrderNote().GetCorrelationId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: resolveStorageBinsReply.GetResult().GetCorrelationId(),
			Details:       resolveStorageBinsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
	}, nil
}

// CompareOrderNoteOptions compares an order note.
func (s *OrderNoteCalculationServer) CompareOrderNoteOptions(ctx context.Context, req *ordersv1.CompareOrderNoteOptionsRequest) (*ordersv1.CompareOrderNoteOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "compare order note options", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listSearchQueryVersionsRequest := &searchv1.ListSearchQueryVersionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQueryVersionsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetOrderNote().GetOccurredAt(),
			OwnerId:      req.GetOrderNote().GetSubject().GetOrderId(),
			Labels:       annotations,
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listSearchQueryVersionsReply, err := listSearchQueryVersionsCall(ctx, listSearchQueryVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/ListSearchQueryVersions")
	}
	var envelope2 ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	revertUserRequest := &identityv1.RevertUserRequest{}
	revertUserRequest.Context = req.GetContext()
	revertUserRequest.User = &identityv1.User{
		RelationshipManager: &identityv1.UserRef{
			DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
		},
		PrimaryAddress: &commonv1.PostalAddress{
			CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
		},
	}
	revertUserRequest.UserId = envelope2.GetDisplayName()
	revertUserReply, err := s.userHistoryClient.RevertUser(ctx, revertUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RevertUser")
	}
	return &ordersv1.CompareOrderNoteOptionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listSearchQueryVersionsReply.GetResult().GetReferenceIds(),
			Message:      revertUserReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		OrderNote: &ordersv1.OrderNote{
			CorrelationId: revertUserReply.GetResult().GetCorrelationId(),
			OrderNoteId:   req.GetOrderNoteId(),
			Attributes:    req.GetOrderNote().GetAttributes(),
		},
	}, nil
}

// RecalculateOrderNotes recalculates an order note.
func (s *OrderNoteCalculationServer) RecalculateOrderNotes(ctx context.Context, req *ordersv1.RecalculateOrderNotesRequest) (*ordersv1.RecalculateOrderNotesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "recalculate order notes", "tenant", req.GetContext().GetTenantId())
	lintOrdersRequest := &ordersv1.LintOrdersRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
	}
	lintOrdersReply, err := s.lintOrdersFn(ctx, lintOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/LintOrders")
	}
	compareBackorderOptionsRequest := buildCompareBackorderOptionsRequestForRecalculateOrderNotes(req, lintOrdersReply)
	compareBackorderOptionsReply, err := s.backorderCalculationClient.CompareBackorderOptions(ctx, compareBackorderOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/CompareBackorderOptions")
	}
	return &ordersv1.RecalculateOrderNotesResponse{
		OrderNote: &ordersv1.OrderNote{
			Subject:     &ordersv1.OrderRef{OrderId: lintOrdersReply.GetOrder().GetOrderId()},
			OrderNoteId: req.GetOrderNoteId(),
		},
		Result: &commonv1.OperationResult{
			Details:       compareBackorderOptionsReply.GetResult().GetDetails(),
			CorrelationId: req.GetOrderNote().GetCorrelationId(),
			Status:        "ok",
		},
		Breakdown: &ordersv1.RecalculateOrderNotesResponse_Breakdown{
			Total: compareBackorderOptionsReply.GetBreakdown().GetTotal(),
		},
	}, nil
}

// SimulateOrderNote simulates an order note.
func (s *OrderNoteCalculationServer) SimulateOrderNote(ctx context.Context, req *ordersv1.SimulateOrderNoteRequest) (*ordersv1.SimulateOrderNoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "simulate order note", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.SimulateOrderNoteRequest_Details)
	previewOrderRequest := &ordersv1.PreviewOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Lines:    cloned.GetDestination().GetLines(),
				Locality: req.GetDetails().GetDestination().GetLocality(),
			},
		},
	}
	send := s.previewOrderSender(previewOrderRequest)
	previewOrderReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/PreviewOrder")
	}
	cloned2 := proto.Clone(req.GetDetails()).(*ordersv1.SimulateOrderNoteRequest_Details)
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	calculateOrderRequest := &ordersv1.CalculateOrderRequest{}
	calculateOrderRequest.Context = req.GetContext()
	calculateOrderRequest.Details = &ordersv1.CalculateOrderRequest_Details{
		Destination: &commonv1.PostalAddress{Recipient: cloned2.GetDestination().GetRecipient()},
	}
	calculateOrderRequest.Order = &ordersv1.Order{
		Counterparty: &customersv1.CustomerRef{
			Email:       previewOrderReply.GetOrder().GetCounterparty().GetEmail(),
			DisplayName: previewOrderReply.GetOrder().GetCounterparty().GetDisplayName(),
		},
		BillingAddress: &commonv1.PostalAddress{
			Region: previewOrderReply.GetOrder().GetBillingAddress().GetRegion(),
		},
	}
	calculateOrderRequest.OrderId = envelope.GetDisplayName()
	calculateOrderReply, err := s.orderCalculationClient.CalculateOrder(ctx, calculateOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/CalculateOrder")
	}
	return &ordersv1.SimulateOrderNoteResponse{
		Result: &commonv1.OperationResult{
			Message:      previewOrderReply.GetOutcome().GetMessage(),
			ReferenceIds: previewOrderReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Breakdown: &ordersv1.SimulateOrderNoteResponse_Breakdown{
			CurrencyCode: calculateOrderReply.GetBreakdown().GetCurrencyCode(),
		},
		OrderNote: &ordersv1.OrderNote{
			OrderNoteId: req.GetOrderNoteId(),
			OccurredAt:  req.GetOrderNote().GetOccurredAt(),
		},
	}, nil
}

// ExplainOrderNoteCalculation explains an order note.
func (s *OrderNoteCalculationServer) ExplainOrderNoteCalculation(ctx context.Context, req *ordersv1.ExplainOrderNoteCalculationRequest) (*ordersv1.ExplainOrderNoteCalculationResponse, error) {
	if err := s.store.SaveOrderNote(ctx, req.GetContext().GetTenantId(), req.GetOrderNote()); err != nil {
		return nil, grpcx.Wrap(err, "save order note")
	}
	s.log.InfoContext(ctx, "explain order note calculation", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *ordersv1.ScoreOrderRequest, ...grpc.CallOption) (*ordersv1.ScoreOrderResponse, error){
		"primary":  s.orderValidationClient.ScoreOrder,
		"fallback": s.orderValidationClientFallback.ScoreOrder,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	scoreOrderRequest := &ordersv1.ScoreOrderRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetOrderNote().GetSubject().GetDisplayName(),
			},
		},
	}
	scoreOrderReply, err := route(ctx, scoreOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/ScoreOrder")
	}
	routes2 := map[string]func(context.Context, *documentsv1.CheckDocumentEligibilityRequest, ...grpc.CallOption) (*documentsv1.CheckDocumentEligibilityResponse, error){
		"primary":  s.documentValidationClient.CheckDocumentEligibility,
		"fallback": s.documentValidationClientFallback.CheckDocumentEligibility,
	}
	route2, ok := routes2[req.GetContext().GetLocale()]
	if !ok {
		route2 = routes2["primary"]
	}
	var envelope ordersv1.OrderRef
	if err := req.GetOrderNote().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkDocumentEligibilityRequest := &documentsv1.CheckDocumentEligibilityRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			Settlement: &documentsv1.Document_Bank{Bank: &documentsv1.DocumentBankSettlement{
				AccountHolder: scoreOrderReply.GetOrder().GetBank().GetAccountHolder(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Region: scoreOrderReply.GetOrder().GetBillingAddress().GetRegion(),
			},
		},
		Details: &documentsv1.CheckDocumentEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
		DocumentId: envelope.GetDisplayName(),
	}
	checkDocumentEligibilityReply, err := route2(ctx, checkDocumentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/CheckDocumentEligibility")
	}
	return &ordersv1.ExplainOrderNoteCalculationResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreOrderReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &ordersv1.ExplainOrderNoteCalculationResponse_Outcome{
			Message: checkDocumentEligibilityReply.GetResult().GetMessage(),
		},
		OrderNote: &ordersv1.OrderNote{
			OrderNoteId: req.GetOrderNoteId(),
			Source:      req.GetOrderNote().GetSource(),
		},
	}, nil
}

// orderValidationLazy dials acme.orders.v1.OrderValidationService the first time a handler needs it.
func (s *OrderNoteCalculationServer) orderValidationLazy() ordersv1.OrderValidationServiceClient {
	s.orderValidationOnce.Do(func() { s.orderValidationCached = ordersv1.NewOrderValidationServiceClient(s.conn) })
	return s.orderValidationCached
}

// previewOrderSender returns a function that sends one request to acme.orders.v1.OrderValidationService/PreviewOrder.
func (s *OrderNoteCalculationServer) previewOrderSender(request *ordersv1.PreviewOrderRequest) func(context.Context) (*ordersv1.PreviewOrderResponse, error) {
	return func(ctx context.Context) (*ordersv1.PreviewOrderResponse, error) {
		return s.orderValidationClient.PreviewOrder(ctx, request)
	}
}
