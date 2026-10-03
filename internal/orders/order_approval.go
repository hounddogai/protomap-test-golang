package orders

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// OrderApprovalServer implements `acme.orders.v1.OrderApprovalService`.
// Routes orders through approval, with escalation and delegation.
type OrderApprovalServer struct {
	ordersv1.UnimplementedOrderApprovalServiceServer
	store                         *Store
	log                           *slog.Logger
	orderNoteCalculationClient    ordersv1.OrderNoteCalculationServiceClient
	customerQueryClient           customersv1.CustomerQueryServiceClient
	roleAdminClient               identityv1.RoleAdminServiceClient
	conn                          *grpc.ClientConn
	customerCalculationPort       customerCalculationPort
	venueValidationClient         venuesv1.VenueValidationServiceClient
	inspectionApprovalClient      returnsv1.InspectionApprovalServiceClient
	trackingEventApprovalClient   shippingv1.TrackingEventApprovalServiceClient
	subscriptionApprovalClient    subscriptionsv1.SubscriptionApprovalServiceClient
	productReviewWorkflowClient   reviewsv1.ProductReviewWorkflowServiceClient
	pointsLedgerWorkflowClient    loyaltyv1.PointsLedgerWorkflowServiceClient
	orderValidationClient         ordersv1.OrderValidationServiceClient
	bus                           *eventbus.Bus
	stockItemWorkflowClient       inventoryv1.StockItemWorkflowServiceClient
	orderNoteSyncClient           ordersv1.OrderNoteSyncServiceClient
	customerSegmentValidationPort customerSegmentValidationPort
	orderReportingClient          ordersv1.OrderReportingServiceClient
}

// NewOrderApprovalServer wires an OrderApprovalServer.
func NewOrderApprovalServer(deps platform.Deps) *OrderApprovalServer {
	return &OrderApprovalServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.orders.v1.OrderApprovalService"),
		orderNoteCalculationClient:    ordersv1.NewOrderNoteCalculationServiceClient(deps.Conn),
		customerQueryClient:           customersv1.NewCustomerQueryServiceClient(deps.Conn),
		roleAdminClient:               identityv1.NewRoleAdminServiceClient(deps.Conn),
		conn:                          deps.Conn,
		customerCalculationPort:       newCustomerCalculationPort(deps),
		venueValidationClient:         venuesv1.NewVenueValidationServiceClient(deps.Conn),
		inspectionApprovalClient:      returnsv1.NewInspectionApprovalServiceClient(deps.Conn),
		trackingEventApprovalClient:   shippingv1.NewTrackingEventApprovalServiceClient(deps.Conn),
		subscriptionApprovalClient:    subscriptionsv1.NewSubscriptionApprovalServiceClient(deps.Conn),
		productReviewWorkflowClient:   reviewsv1.NewProductReviewWorkflowServiceClient(deps.Conn),
		pointsLedgerWorkflowClient:    loyaltyv1.NewPointsLedgerWorkflowServiceClient(deps.Conn),
		orderValidationClient:         ordersv1.NewOrderValidationServiceClient(deps.Conn),
		bus:                           deps.Bus,
		stockItemWorkflowClient:       inventoryv1.NewStockItemWorkflowServiceClient(deps.Conn),
		orderNoteSyncClient:           ordersv1.NewOrderNoteSyncServiceClient(deps.Conn),
		customerSegmentValidationPort: newCustomerSegmentValidationPort(deps),
		orderReportingClient:          ordersv1.NewOrderReportingServiceClient(deps.Conn),
	}
}

// SubmitOrder submits an order.
func (s *OrderApprovalServer) SubmitOrder(ctx context.Context, req *ordersv1.SubmitOrderRequest) (*ordersv1.SubmitOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "submit order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetOrder()).(*ordersv1.Order)
	ctx = tenancy.With(ctx, "number", req.GetOrder().GetNumber())
	simulateOrderNoteRequest := newSimulateOrderNoteRequest(withSimulateOrderNoteContext(req.GetContext()), withSimulateOrderNoteDetails(&ordersv1.SimulateOrderNoteRequest_Details{
		Destination: &commonv1.PostalAddress{
			Lines:     req.GetOrder().GetBillingAddress().GetLines(),
			Recipient: cloned.GetBillingAddress().GetRecipient(),
			Region:    req.GetOrder().GetBillingAddress().GetRegion(),
		},
	}), withSimulateOrderNoteOrderNote(&ordersv1.OrderNote{
		Subject: &ordersv1.OrderRef{Number: tenancy.Value(ctx, "number")},
	}))
	simulateOrderNoteReply, err := s.orderNoteCalculationClient.SimulateOrderNote(ctx, simulateOrderNoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteCalculationService/SimulateOrderNote")
	}
	resolveCustomersRequest := &customersv1.ResolveCustomersRequest{Context: req.GetContext()}
	resolveCustomersReply, err := grpcx.Call(ctx, s.customerQueryClient.ResolveCustomers, resolveCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ResolveCustomers")
	}
	return &ordersv1.SubmitOrderResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: simulateOrderNoteReply.GetResult().GetCorrelationId(),
			ReferenceIds:  resolveCustomersReply.GetResult().GetReferenceIds(),
			Status:        resolveCustomersReply.GetResult().GetStatus(),
		},
		Order: &ordersv1.Order{
			Lines: []*ordersv1.OrderLine{&ordersv1.OrderLine{
				Attributes: simulateOrderNoteReply.GetOrderNote().GetAttributes(),
			}},
			OrderId: req.GetOrderId(),
			Total:   req.GetOrder().GetTotal(),
		},
	}, nil
}

// ApproveOrder approves an order.
func (s *OrderApprovalServer) ApproveOrder(ctx context.Context, req *ordersv1.ApproveOrderRequest) (*ordersv1.ApproveOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "approve order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	repairRoleReply, err := s.roleAdminClient.RepairRole(ctx, &identityv1.RepairRoleRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				Email:       req.GetOrder().GetCounterparty().GetEmail(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleAdminService/RepairRole")
	}
	calculateCustomerRequest := &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetOrder().GetAudit().GetUpdatedBy()},
		},
	}
	calculateCustomerReply := new(customersv1.CalculateCustomerResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.CustomerCalculationService_CalculateCustomer_FullMethodName, calculateCustomerRequest, calculateCustomerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	return &ordersv1.ApproveOrderResponse{
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: repairRoleReply.GetRole().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: calculateCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
			OrderId: req.GetOrderId(),
			Number:  req.GetOrder().GetNumber(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectOrder rejects an order.
func (s *OrderApprovalServer) RejectOrder(ctx context.Context, req *ordersv1.RejectOrderRequest) (*ordersv1.RejectOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "reject order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	getCustomerRatesRequest := &customersv1.GetCustomerRatesRequest{
		Context:    req.GetContext(),
		CustomerId: req.GetOrder().GetCounterparty().GetCustomerId(),
	}
	getCustomerRatesReply, err := s.customerCalculationPort.GetCustomerRates(ctx, getCustomerRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/GetCustomerRates")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.RejectOrderRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	previewVenueRequest := &venuesv1.PreviewVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetOrder().GetCounterparty().GetDisplayName(),
				Email:       (&decoded).GetApprover().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: getCustomerRatesReply.GetCustomer().GetAudit().GetUpdatedBy(),
			},
		},
	}
	previewVenueReply, err := grpcx.Call(ctx, s.venueValidationClient.PreviewVenue, previewVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/PreviewVenue")
	}
	return &ordersv1.RejectOrderResponse{
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Locality: getCustomerRatesReply.GetCustomer().GetPrimaryAddress().GetLocality(),
			},
			OrderId:  req.GetOrderId(),
			IssuedAt: req.GetOrder().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			Message: previewVenueReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// EscalateOrder escalates an order.
func (s *OrderApprovalServer) EscalateOrder(ctx context.Context, req *ordersv1.EscalateOrderRequest) (*ordersv1.EscalateOrderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "escalate order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.EscalateOrderRequest_Details)
	delegateInspectionApprovalRequest := newDelegateInspectionApprovalRequest(withDelegateInspectionApprovalContext(req.GetContext()), withDelegateInspectionApprovalInspection(&returnsv1.Inspection{
		Assignee: &identityv1.UserRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			Email:       cloned.GetApprover().GetEmail(),
		},
		Origin: &returnsv1.Inspection_Customer{Customer: &customersv1.CustomerRef{
			CustomerId: req.GetOrder().GetCounterparty().GetCustomerId(),
		}},
	}))
	delegateInspectionApprovalReply, err := s.inspectionApprovalClient.DelegateInspectionApproval(ctx, delegateInspectionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionApprovalService/DelegateInspectionApproval")
	}
	commentTrackingEventApprovalRequest := &shippingv1.CommentTrackingEventApprovalRequest{
		Context: req.GetContext(),
		TrackingEvent: &shippingv1.TrackingEvent{
			Subject: &shippingv1.ShipmentRef{
				DisplayName: delegateInspectionApprovalReply.GetInspection().GetCustomer().GetDisplayName(),
			},
		},
		Details: &shippingv1.CommentTrackingEventApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       delegateInspectionApprovalReply.GetInspection().GetAssignee().GetEmail(),
				DisplayName: delegateInspectionApprovalReply.GetInspection().GetAssignee().GetDisplayName(),
			},
			ApproverEmail: req.GetDetails().GetApproverEmail(),
		},
	}
	commentTrackingEventApprovalReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*shippingv1.CommentTrackingEventApprovalResponse, error) {
		return s.trackingEventApprovalClient.CommentTrackingEventApproval(ctx, commentTrackingEventApprovalRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventApprovalService/CommentTrackingEventApproval")
	}
	delegateSubscriptionApprovalReply, err := s.subscriptionApprovalClient.DelegateSubscriptionApproval(ctx, &subscriptionsv1.DelegateSubscriptionApprovalRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Contact: &commonv1.ContactInfo{
				Phone: delegateInspectionApprovalReply.GetInspection().GetRequester().GetPhone(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetOrder().GetAudit().GetUpdatedAt()},
		},
		Details: &subscriptionsv1.DelegateSubscriptionApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/DelegateSubscriptionApproval")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*reviewsv1.StartProductReviewWorkflowResponse, len(req.GetOrder().GetAttachments()))
	for i, attachment := range req.GetOrder().GetAttachments() {
		group.Go(func() error {
			reply, err := s.productReviewWorkflowClient.StartProductReviewWorkflow(groupCtx, &reviewsv1.StartProductReviewWorkflowRequest{
				Context: req.GetContext(),
				ProductReview: &reviewsv1.ProductReview{
					Attachments: []*reviewsv1.ProductReviewAttachment{&reviewsv1.ProductReviewAttachment{
						SizeBytes:   attachment.GetSizeBytes(),
						Checksum:    attachment.GetChecksum(),
						FileName:    attachment.GetFileName(),
						ContentType: attachment.GetContentType(),
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
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewWorkflowService/StartProductReviewWorkflow")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	_ = delegateSubscriptionApprovalReply
	return &ordersv1.EscalateOrderResponse{
		Outcome: &ordersv1.EscalateOrderResponse_Outcome{
			Status:  delegateInspectionApprovalReply.GetOutcome().GetStatus(),
			Message: commentTrackingEventApprovalReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Details: commentTrackingEventApprovalReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Order: &ordersv1.Order{
			OrderId:        req.GetOrderId(),
			BillingAddress: req.GetOrder().GetBillingAddress(),
		},
	}, nil
}

// ListPendingOrders lists orders a page at a time.
func (s *OrderApprovalServer) ListPendingOrders(ctx context.Context, req *ordersv1.ListPendingOrdersRequest) (*ordersv1.ListPendingOrdersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending orders", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	retryPointsLedgerWorkflowRequest := &loyaltyv1.RetryPointsLedgerWorkflowRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Labels:  labels,
			Funding: &loyaltyv1.PointsLedger_ExternalAccountId{ExternalAccountId: req.GetFilter().GetOwnerId()},
			Audit:   &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &loyaltyv1.RetryPointsLedgerWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	send := s.retryPointsLedgerWorkflowSender(retryPointsLedgerWorkflowRequest)
	retryPointsLedgerWorkflowReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerWorkflowService/RetryPointsLedgerWorkflow")
	}
	return &ordersv1.ListPendingOrdersResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: retryPointsLedgerWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// DelegateOrderApproval delegates an order.
func (s *OrderApprovalServer) DelegateOrderApproval(ctx context.Context, req *ordersv1.DelegateOrderApprovalRequest) (*ordersv1.DelegateOrderApprovalResponse, error) {
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "delegate order approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*ordersv1.ScoreOrderResponse, len(req.GetOrder().GetLines()))
	for i, line := range req.GetOrder().GetLines() {
		group.Go(func() error {
			reply, err := s.orderValidationClient.ScoreOrder(groupCtx, &ordersv1.ScoreOrderRequest{
				Context: req.GetContext(),
				Order: &ordersv1.Order{
					Lines: []*ordersv1.OrderLine{&ordersv1.OrderLine{
						Tax: &ordersv1.OrderLineTax{Jurisdiction: line.GetTax().GetJurisdiction()},
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
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/ScoreOrder")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "orders.delegate_order_approval.signal_fulfillment_workflow",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"fulfillment.counterparty.customer_id": req.GetOrder().GetCounterparty().GetCustomerId(),
			"context.tenant_id":                    req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	startStockItemWorkflowRequest := &inventoryv1.StartStockItemWorkflowRequest{Context: req.GetContext()}
	startStockItemWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*inventoryv1.StartStockItemWorkflowResponse, error) {
		return s.stockItemWorkflowClient.StartStockItemWorkflow(ctx, startStockItemWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemWorkflowService/StartStockItemWorkflow")
	}
	ctx = tenancy.With(ctx, "region", req.GetOrder().GetBillingAddress().GetRegion())
	listOrderNoteConflictsRequest := &ordersv1.ListOrderNoteConflictsRequest{
		Context: req.GetContext(),
		Filter:  &ordersv1.ListOrderNoteConflictsRequest_Filter{Region: tenancy.Value(ctx, "region")},
	}
	type listOrderNoteConflictsResult struct {
		reply *ordersv1.ListOrderNoteConflictsResponse
		err   error
	}
	listOrderNoteConflictsResults := make(chan listOrderNoteConflictsResult, 1)
	go func() {
		reply, err := s.orderNoteSyncClient.ListOrderNoteConflicts(ctx, listOrderNoteConflictsRequest)
		listOrderNoteConflictsResults <- listOrderNoteConflictsResult{reply, err}
	}()
	var listOrderNoteConflictsReply *ordersv1.ListOrderNoteConflictsResponse
	select {
	case result := <-listOrderNoteConflictsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderNoteSyncService/ListOrderNoteConflicts")
		}
		listOrderNoteConflictsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &ordersv1.DelegateOrderApprovalResponse{
		Result: &commonv1.OperationResult{
			Message: startStockItemWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &ordersv1.DelegateOrderApprovalResponse_Outcome{Status: listOrderNoteConflictsReply.GetResult().GetStatus()},
		Order: &ordersv1.Order{
			OrderId: req.GetOrderId(),
			Status:  req.GetOrder().GetStatus(),
		},
	}, nil
}

// GetOrderApprovalStatus returns an order.
func (s *OrderApprovalServer) GetOrderApprovalStatus(ctx context.Context, req *ordersv1.GetOrderApprovalStatusRequest) (*ordersv1.GetOrderApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetOrderApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "get_order_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order approval status", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	previewCustomerSegmentRequest := &customersv1.PreviewCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:  cloned.GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
		Details: &customersv1.PreviewCustomerSegmentRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	previewCustomerSegmentReply, err := s.customerSegmentValidationPort.PreviewCustomerSegment(ctx, previewCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment")
	}
	shareOrderReportReply, err := s.orderReportingClient.ShareOrderReport(ctx, &ordersv1.ShareOrderReportRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ShareOrderReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: previewCustomerSegmentReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/ShareOrderReport")
	}
	return &ordersv1.GetOrderApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Details: previewCustomerSegmentReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Order: &ordersv1.Order{
			Metadata: shareOrderReportReply.GetOrder().GetMetadata(),
			OrderId:  req.GetOrderId(),
		},
	}, nil
}

// retryPointsLedgerWorkflowSender returns a function that sends one request to
// acme.loyalty.v1.PointsLedgerWorkflowService/RetryPointsLedgerWorkflow.
func (s *OrderApprovalServer) retryPointsLedgerWorkflowSender(request *loyaltyv1.RetryPointsLedgerWorkflowRequest) func(context.Context) (*loyaltyv1.RetryPointsLedgerWorkflowResponse, error) {
	return func(ctx context.Context) (*loyaltyv1.RetryPointsLedgerWorkflowResponse, error) {
		return s.pointsLedgerWorkflowClient.RetryPointsLedgerWorkflow(ctx, request)
	}
}
