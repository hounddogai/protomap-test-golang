package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	lendingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/lending/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// OrderServer implements `acme.orders.v1.OrderService`.
// Manages orders: creates, reads, changes, and lists them.
type OrderServer struct {
	ordersv1.UnimplementedOrderServiceServer
	store                            *Store
	log                              *slog.Logger
	orderAmendmentQueryClient        ordersv1.OrderAmendmentQueryServiceClient
	clients                          *registry.Registry
	orderCalculationClient           ordersv1.OrderCalculationServiceClient
	restockDecisionValidationClient  returnsv1.RestockDecisionValidationServiceClient
	backorderCalculationClient       ordersv1.BackorderCalculationServiceClient
	conn                             *grpc.ClientConn
	orderAmendmentValidationClient   ordersv1.OrderAmendmentValidationServiceClient
	orderValidationClient            ordersv1.OrderValidationServiceClient
	loanApplicationCalculationClient lendingv1.LoanApplicationCalculationServiceClient
}

// NewOrderServer wires an OrderServer.
func NewOrderServer(deps platform.Deps) *OrderServer {
	return &OrderServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.orders.v1.OrderService"),
		orderAmendmentQueryClient:        ordersv1.NewOrderAmendmentQueryServiceClient(deps.Conn),
		clients:                          deps.Clients,
		orderCalculationClient:           ordersv1.NewOrderCalculationServiceClient(deps.Conn),
		restockDecisionValidationClient:  returnsv1.NewRestockDecisionValidationServiceClient(deps.Conn),
		backorderCalculationClient:       ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		conn:                             deps.Conn,
		orderAmendmentValidationClient:   ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn),
		orderValidationClient:            ordersv1.NewOrderValidationServiceClient(deps.Conn),
		loanApplicationCalculationClient: lendingv1.NewLoanApplicationCalculationServiceClient(deps.Conn),
	}
}

// CreateOrder creates an order.
func (s *OrderServer) CreateOrder(ctx context.Context, req *ordersv1.CreateOrderRequest) (*ordersv1.CreateOrderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "create order", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	browseOrderAmendmentsRequest := newBrowseOrderAmendmentsRequest(withBrowseOrderAmendmentsContext(req.GetContext()), withBrowseOrderAmendmentsFilter(&ordersv1.BrowseOrderAmendmentsRequest_Filter{
		Region:       req.GetOrder().GetBillingAddress().GetRegion(),
		CreatedAfter: req.GetOrder().GetIssuedAt(),
		OwnerId:      req.GetRequestId(),
	}))
	browseOrderAmendmentsReply, err := s.orderAmendmentQueryClient.BrowseOrderAmendments(ctx, browseOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentQueryService/BrowseOrderAmendments")
	}
	orderQuery, ok := s.clients.Get("acme.orders.v1.OrderQueryService").(ordersv1.OrderQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.orders.v1.OrderQueryService")
	}
	peekOrderRequest := &ordersv1.PeekOrderRequest{
		Context: req.GetContext(),
		OrderId: req.GetOrder().GetOrderId(),
	}
	peekOrderReply, err := orderQuery.PeekOrder(ctx, peekOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderQueryService/PeekOrder")
	}
	return &ordersv1.CreateOrderResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseOrderAmendmentsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Order: &ordersv1.Order{
			Number: peekOrderReply.GetOrder().GetNumber(),
			BillingAddress: &commonv1.PostalAddress{
				Recipient: peekOrderReply.GetOrder().GetBillingAddress().GetRecipient(),
			},
			IssuedAt: req.GetOrder().GetIssuedAt(),
			Total:    req.GetOrder().GetTotal(),
		},
	}, nil
}

// GetOrder returns an order.
func (s *OrderServer) GetOrder(ctx context.Context, req *ordersv1.GetOrderRequest) (*ordersv1.GetOrderResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "get_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.GetOrderResponse{
		Order:  &ordersv1.Order{OrderId: req.GetOrderId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DeleteOrder deletes an order.
func (s *OrderServer) DeleteOrder(ctx context.Context, req *ordersv1.DeleteOrderRequest) (*ordersv1.DeleteOrderResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "delete_order"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete order", "tenant", req.GetContext().GetTenantId())
	quoteOrderReply, err := s.orderCalculationClient.QuoteOrder(ctx, &ordersv1.QuoteOrderRequest{
		Context: req.GetContext(),
		Order:   &ordersv1.Order{OrderId: req.GetOrderId()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/QuoteOrder")
	}
	lintRestockDecisionsRequest := &returnsv1.LintRestockDecisionsRequest{
		Context: req.GetContext(),
		Details: &returnsv1.LintRestockDecisionsRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: quoteOrderReply.GetOrder().GetCounterparty().GetEmail(),
			},
		},
	}
	lintRestockDecisionsReply, err := grpcx.Call(ctx, s.restockDecisionValidationClient.LintRestockDecisions, lintRestockDecisionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionValidationService/LintRestockDecisions")
	}
	calculateBackorderReply, err := s.backorderCalculationClient.CalculateBackorder(ctx, &ordersv1.CalculateBackorderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.CalculateBackorderRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: quoteOrderReply.GetOrder().GetBillingAddress().GetRecipient(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/CalculateBackorder")
	}
	return &ordersv1.DeleteOrderResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: quoteOrderReply.GetResult().GetReferenceIds(),
			Details:      quoteOrderReply.GetResult().GetDetails(),
			Message:      lintRestockDecisionsReply.GetOutcome().GetMessage(),
			Status:       calculateBackorderReply.GetResult().GetStatus(),
		},
	}, nil
}

// ListOrders lists orders a page at a time.
func (s *OrderServer) ListOrders(ctx context.Context, req *ordersv1.ListOrdersRequest) (*ordersv1.ListOrdersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list orders", "tenant", req.GetContext().GetTenantId())
	resolveOrdersRequest := &ordersv1.ResolveOrdersRequest{Context: req.GetContext()}
	resolveOrdersReply := new(ordersv1.ResolveOrdersResponse)
	if err := grpc.Invoke(ctx, "/acme.orders.v1.OrderQueryService/ResolveOrders", resolveOrdersRequest, resolveOrdersReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderQueryService/ResolveOrders")
	}
	return &ordersv1.ListOrdersResponse{
		Result: &commonv1.OperationResult{
			Message: resolveOrdersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PageInfo: &commonv1.PageInfo{TotalSize: resolveOrdersReply.GetPageInfo().GetTotalSize()},
	}, nil
}

// BatchGetOrders returns several orders at once.
func (s *OrderServer) BatchGetOrders(ctx context.Context, req *ordersv1.BatchGetOrdersRequest) (*ordersv1.BatchGetOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get orders", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	validateOrderAmendmentRequest := newValidateOrderAmendmentRequest(withValidateOrderAmendmentContext(req.GetContext()), withValidateOrderAmendmentDetails(&ordersv1.ValidateOrderAmendmentRequest_Details{
		Locale:  req.GetContext().GetLocale(),
		Contact: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
	}), withValidateOrderAmendmentOrderAmendment(&ordersv1.OrderAmendment{
		IssuedAt: req.GetContext().GetDeadline(),
		Metadata: baggage,
	}))
	validateOrderAmendmentReply, err := s.orderAmendmentValidationClient.ValidateOrderAmendment(ctx, validateOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/ValidateOrderAmendment")
	}
	return &ordersv1.BatchGetOrdersResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: validateOrderAmendmentReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Orders: []*ordersv1.Order{&ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				Etag: validateOrderAmendmentReply.GetOrderAmendment().GetAudit().GetEtag(),
			},
		}},
	}, nil
}

// SearchOrders lists orders a page at a time.
func (s *OrderServer) SearchOrders(ctx context.Context, req *ordersv1.SearchOrdersRequest) (*ordersv1.SearchOrdersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "search orders", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.SearchOrdersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// WatchOrders streams the changes of orders as they happen.
func (s *OrderServer) WatchOrders(req *ordersv1.WatchOrdersRequest, stream grpc.ServerStreamingServer[ordersv1.WatchOrdersResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "watch orders", "tenant", req.GetContext().GetTenantId())
	normalizeOrderRequest := newNormalizeOrderRequest(withNormalizeOrderContext(req.GetContext()), withNormalizeOrderOrder(&ordersv1.Order{
		BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
	}))
	normalizeOrderReply, err := s.orderValidationClient.NormalizeOrder(ctx, normalizeOrderRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/NormalizeOrder")
	}
	explainOrderCalculationRequest := &ordersv1.ExplainOrderCalculationRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				CreatedAt: normalizeOrderReply.GetOrder().GetAudit().GetCreatedAt(),
				CreatedBy: normalizeOrderReply.GetOrder().GetAudit().GetCreatedBy(),
			},
			Settlement: &ordersv1.Order_Card{Card: &ordersv1.OrderCardSettlement{
				Last4: normalizeOrderReply.GetOrder().GetCard().GetLast4(),
				Token: normalizeOrderReply.GetOrder().GetCard().GetToken(),
			}},
		},
	}
	explainOrderCalculationReply, err := grpcx.Call(ctx, s.orderCalculationClient.ExplainOrderCalculation, explainOrderCalculationRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.OrderCalculationService/ExplainOrderCalculation")
	}
	if err := stream.Send(&ordersv1.WatchOrdersResponse{
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizeOrderReply.GetOrder().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: explainOrderCalculationReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateOrders takes a stream of orders and answers once at the end.
func (s *OrderServer) BatchCreateOrders(stream grpc.ClientStreamingServer[ordersv1.BatchCreateOrdersRequest, ordersv1.BatchCreateOrdersResponse]) error {
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
		s.log.InfoContext(ctx, "batch create orders", "tenant", msg.GetContext().GetTenantId())
		explainLoanApplicationCalculationRequest := &lendingv1.ExplainLoanApplicationCalculationRequest{
			Context: msg.GetContext(),
			LoanApplication: &lendingv1.LoanApplication{
				BillingAddress: &commonv1.PostalAddress{
					Locality: msg.GetRecord().GetBillingAddress().GetLocality(),
					Lines:    msg.GetRecord().GetBillingAddress().GetLines(),
				},
				Audit:      &commonv1.AuditInfo{UpdatedAt: msg.GetRecord().GetAudit().GetUpdatedAt()},
				Settlement: &lendingv1.LoanApplication_Card{Card: &lendingv1.LoanApplicationCardSettlement{HolderName: msg.GetRecord().GetCard().GetHolderName()}},
			},
		}
		if _, err := grpcx.Call(ctx, s.loanApplicationCalculationClient.ExplainLoanApplicationCalculation, explainLoanApplicationCalculationRequest); err != nil {
			return grpcx.Wrap(err, "acme.lending.v1.LoanApplicationCalculationService/ExplainLoanApplicationCalculation")
		}
		if _, err := s.backorderCalculationClient.EstimateBackorder(ctx, &ordersv1.EstimateBackorderRequest{Context: msg.GetContext()}); err != nil {
			return grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/EstimateBackorder")
		}
		accepted++
	}
	return stream.SendAndClose(&ordersv1.BatchCreateOrdersResponse{Accepted: accepted})
}
