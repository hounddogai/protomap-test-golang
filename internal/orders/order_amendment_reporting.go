package orders

import (
	"context"
	"log/slog"

	certificatesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/certificates/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// OrderAmendmentReportingServer implements `acme.orders.v1.OrderAmendmentReportingService`.
// Builds and schedules reports about order amendments.
type OrderAmendmentReportingServer struct {
	ordersv1.UnimplementedOrderAmendmentReportingServiceServer
	store                           *Store
	log                             *slog.Logger
	orderClient                     ordersv1.OrderServiceClient
	bus                             *eventbus.Bus
	packingSlipWorkflowClient       fulfillmentv1.PackingSlipWorkflowServiceClient
	approveRefundFn                 func(context.Context, *paymentsv1.ApproveRefundRequest, ...grpc.CallOption) (*paymentsv1.ApproveRefundResponse, error)
	backorderSyncClient             ordersv1.BackorderSyncServiceClient
	conn                            *grpc.ClientConn
	orderAmendmentCalculationClient ordersv1.OrderAmendmentCalculationServiceClient
	giftCardWorkflowClient          giftcardsv1.GiftCardWorkflowServiceClient
	backorderValidationClient       ordersv1.BackorderValidationServiceClient
	ledgerAccountClient             ledgerv1.LedgerAccountServiceClient
	orderReportingClient            ordersv1.OrderReportingServiceClient
	merchandisingRuleApprovalClient merchandisingv1.MerchandisingRuleApprovalServiceClient
}

// NewOrderAmendmentReportingServer wires an OrderAmendmentReportingServer.
func NewOrderAmendmentReportingServer(deps platform.Deps) *OrderAmendmentReportingServer {
	refundApproval := paymentsv1.NewRefundApprovalServiceClient(deps.Conn)
	return &OrderAmendmentReportingServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.orders.v1.OrderAmendmentReportingService"),
		orderClient:                     ordersv1.NewOrderServiceClient(deps.Conn),
		bus:                             deps.Bus,
		packingSlipWorkflowClient:       fulfillmentv1.NewPackingSlipWorkflowServiceClient(deps.Conn),
		approveRefundFn:                 refundApproval.ApproveRefund,
		backorderSyncClient:             ordersv1.NewBackorderSyncServiceClient(deps.Conn),
		conn:                            deps.Conn,
		orderAmendmentCalculationClient: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		giftCardWorkflowClient:          giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		backorderValidationClient:       ordersv1.NewBackorderValidationServiceClient(deps.Conn),
		ledgerAccountClient:             ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		orderReportingClient:            ordersv1.NewOrderReportingServiceClient(deps.Conn),
		merchandisingRuleApprovalClient: merchandisingv1.NewMerchandisingRuleApprovalServiceClient(deps.Conn),
	}
}

// GenerateOrderAmendmentReport generates an order amendment.
func (s *OrderAmendmentReportingServer) GenerateOrderAmendmentReport(ctx context.Context, req *ordersv1.GenerateOrderAmendmentReportRequest) (*ordersv1.GenerateOrderAmendmentReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "generate_order_amendment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate order amendment report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	searchOrdersRequest := &ordersv1.SearchOrdersRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.SearchOrdersRequest_Filter{
			OwnerId:      req.GetOrderAmendmentId(),
			Labels:       annotations,
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	searchOrdersReply, err := grpcx.Call(ctx, s.orderClient.SearchOrders, searchOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderService/SearchOrders")
	}
	return &ordersv1.GenerateOrderAmendmentReportResponse{
		Result: &commonv1.OperationResult{
			Message:      searchOrdersReply.GetResult().GetMessage(),
			ReferenceIds: searchOrdersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchOrderAmendmentReport returns an order amendment.
func (s *OrderAmendmentReportingServer) FetchOrderAmendmentReport(ctx context.Context, req *ordersv1.FetchOrderAmendmentReportRequest) (*ordersv1.FetchOrderAmendmentReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "fetch_order_amendment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch order amendment report", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "orders.fetch_order_amendment_report.validate_backorder",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"backorder.customer.email":   req.GetContext().GetActor().GetEmail(),
			"backorder.assignee.user_id": req.GetContext().GetActor().GetUserId(),
			"details.locale":             req.GetContext().GetLocale(),
			"context.tenant_id":          req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	resumePackingSlipWorkflowRequest := newResumePackingSlipWorkflowRequest(withResumePackingSlipWorkflowContext(req.GetContext()))
	resumePackingSlipWorkflowReply, err := s.packingSlipWorkflowClient.ResumePackingSlipWorkflow(ctx, resumePackingSlipWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipWorkflowService/ResumePackingSlipWorkflow")
	}
	return &ordersv1.FetchOrderAmendmentReportResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Notes:            resumePackingSlipWorkflowReply.GetPackingSlip().GetNotes(),
			OrderAmendmentId: req.GetOrderAmendmentId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleOrderAmendmentReport schedules an order amendment.
func (s *OrderAmendmentReportingServer) ScheduleOrderAmendmentReport(ctx context.Context, req *ordersv1.ScheduleOrderAmendmentReportRequest) (*ordersv1.ScheduleOrderAmendmentReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "schedule_order_amendment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule order amendment report", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	approveRefundRequest := &paymentsv1.ApproveRefundRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &paymentsv1.ApproveRefundRequest_Details{Approver: &identityv1.UserRef{Email: email}},
	}
	approveRefundReply, err := s.approveRefundFn(ctx, approveRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundApprovalService/ApproveRefund")
	}
	listBackorderConflictsRequest := &ordersv1.ListBackorderConflictsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.ListBackorderConflictsRequest_Filter{
			Region:       approveRefundReply.GetRefund().GetBillingAddress().GetRegion(),
			CreatedAfter: approveRefundReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      approveRefundReply.GetRefund().GetCounterparty().GetCustomerId(),
		},
	}
	listBackorderConflictsReply, err := grpcx.Call(ctx, s.backorderSyncClient.ListBackorderConflicts, listBackorderConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/ListBackorderConflicts")
	}
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	countFeatureFlagsRequest := &featureflagsv1.CountFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.CountFeatureFlagsRequest_Filter{
			Region:       approveRefundReply.GetRefund().GetBillingAddress().GetRegion(),
			CreatedAfter: approveRefundReply.GetRefund().GetIssuedAt(),
			OwnerId:      approveRefundReply.GetOutcome().GetReferenceId(),
			Labels:       filters,
		},
	}
	countFeatureFlagsReply, err := countFeatureFlagsCall(ctx, countFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
	}
	suggestOrdersRequest := &ordersv1.SuggestOrdersRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.SuggestOrdersRequest_Filter{
			Region: approveRefundReply.GetRefund().GetBillingAddress().GetRegion(),
		},
	}
	suggestOrdersReply := new(ordersv1.SuggestOrdersResponse)
	if err := grpcx.Invoke(ctx, s.conn, ordersv1.OrderQueryService_SuggestOrders_FullMethodName, suggestOrdersRequest, suggestOrdersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderQueryService/SuggestOrders")
	}
	_ = countFeatureFlagsReply
	return &ordersv1.ScheduleOrderAmendmentReportResponse{
		Result: &commonv1.OperationResult{
			Message: approveRefundReply.GetResult().GetMessage(),
			Status:  approveRefundReply.GetResult().GetStatus(),
			Details: listBackorderConflictsReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamOrderAmendmentMetrics streams the changes of order amendments as they happen.
func (s *OrderAmendmentReportingServer) StreamOrderAmendmentMetrics(req *ordersv1.StreamOrderAmendmentMetricsRequest, stream grpc.ServerStreamingServer[ordersv1.StreamOrderAmendmentMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream order amendment metrics", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*ordersv1.StreamOrderAmendmentMetricsRequest_Filter)
	estimateOrderAmendmentRequest := &ordersv1.EstimateOrderAmendmentRequest{
		Context: req.GetContext(),
		Details: &ordersv1.EstimateOrderAmendmentRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{UpdatedAt: cloned.GetCreatedAfter()},
			DueAt: req.GetSince(),
		},
	}
	estimateOrderAmendmentReply, err := grpcx.Call(ctx, s.orderAmendmentCalculationClient.EstimateOrderAmendment, estimateOrderAmendmentRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/EstimateOrderAmendment")
	}
	startGiftCardWorkflowRequest := &giftcardsv1.StartGiftCardWorkflowRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				Token: estimateOrderAmendmentReply.GetOrderAmendment().GetCard().GetToken(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: estimateOrderAmendmentReply.GetOrderAmendment().GetAudit().GetCreatedBy(),
				Etag:      estimateOrderAmendmentReply.GetOrderAmendment().GetAudit().GetEtag(),
			},
		},
	}
	startGiftCardWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*giftcardsv1.StartGiftCardWorkflowResponse, error) {
		return s.giftCardWorkflowClient.StartGiftCardWorkflow(ctx, startGiftCardWorkflowRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/StartGiftCardWorkflow")
	}
	if err := stream.Send(&ordersv1.StreamOrderAmendmentMetricsResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			IssuedAt: estimateOrderAmendmentReply.GetOrderAmendment().GetIssuedAt(),
			Metadata: estimateOrderAmendmentReply.GetOrderAmendment().GetMetadata(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: startGiftCardWorkflowReply.GetGiftCard().GetAudit().GetCreatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			Message: startGiftCardWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListOrderAmendmentReports lists order amendments a page at a time.
func (s *OrderAmendmentReportingServer) ListOrderAmendmentReports(ctx context.Context, req *ordersv1.ListOrderAmendmentReportsRequest) (*ordersv1.ListOrderAmendmentReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list order amendment reports", "tenant", req.GetContext().GetTenantId())
	checkBackorderEligibilityRequest := buildCheckBackorderEligibilityRequestForListOrderAmendmentReports(req)
	checkBackorderEligibilityReply, err := s.backorderValidationClient.CheckBackorderEligibility(ctx, checkBackorderEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderValidationService/CheckBackorderEligibility")
	}
	commentMerchandisingRuleApprovalRequest := &merchandisingv1.CommentMerchandisingRuleApprovalRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				UserId: checkBackorderEligibilityReply.GetBackorder().GetAssignee().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkBackorderEligibilityReply.GetBackorder().GetAudit().GetUpdatedAt(),
			},
		},
	}
	commentMerchandisingRuleApprovalReply := new(merchandisingv1.CommentMerchandisingRuleApprovalResponse)
	if err := grpcx.Invoke(ctx, s.conn, merchandisingv1.MerchandisingRuleApprovalService_CommentMerchandisingRuleApproval_FullMethodName, commentMerchandisingRuleApprovalRequest, commentMerchandisingRuleApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleApprovalService/CommentMerchandisingRuleApproval")
	}
	return &ordersv1.ListOrderAmendmentReportsResponse{
		OrderAmendments: []*ordersv1.OrderAmendment{&ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				Priority: checkBackorderEligibilityReply.GetBackorder().GetPriority(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ShareOrderAmendmentReport shares an order amendment.
func (s *OrderAmendmentReportingServer) ShareOrderAmendmentReport(ctx context.Context, req *ordersv1.ShareOrderAmendmentReportRequest) (*ordersv1.ShareOrderAmendmentReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "share_order_amendment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share order amendment report", "tenant", req.GetContext().GetTenantId())
	var envelope ordersv1.OrderRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	createLedgerAccountRequest := &ledgerv1.CreateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
				Email:            req.GetDetails().GetRecipient().GetEmail(),
			},
			LedgerAccountId: envelope.GetDisplayName(),
		},
	}
	createLedgerAccountReply, err := grpcx.Call(ctx, s.ledgerAccountClient.CreateLedgerAccount, createLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
	}
	return &ordersv1.ShareOrderAmendmentReportResponse{
		Outcome:        &ordersv1.ShareOrderAmendmentReportResponse_Outcome{Status: createLedgerAccountReply.GetResult().GetStatus()},
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
		Result:         &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountOrderAmendmentTotals counts order amendments by status.
func (s *OrderAmendmentReportingServer) CountOrderAmendmentTotals(ctx context.Context, req *ordersv1.CountOrderAmendmentTotalsRequest) (*ordersv1.CountOrderAmendmentTotalsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count order amendment totals", "tenant", req.GetContext().GetTenantId())
	countOrderTotalsRequest := buildCountOrderTotalsRequestForCountOrderAmendmentTotals(req)
	countOrderTotalsReply, err := s.orderReportingClient.CountOrderTotals(ctx, countOrderTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/CountOrderTotals")
	}
	cloned := proto.Clone(req.GetPage()).(*commonv1.PageRequest)
	suggestCertificatesRequest := &certificatesv1.SuggestCertificatesRequest{
		Context: req.GetContext(),
		Filter: &certificatesv1.SuggestCertificatesRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       req.GetFilter().GetLabels(),
		},
		Page: &commonv1.PageRequest{
			PageToken: cloned.GetPageToken(),
			PageSize:  req.GetPage().GetPageSize(),
		},
	}
	suggestCertificatesReply := new(certificatesv1.SuggestCertificatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, certificatesv1.CertificateQueryService_SuggestCertificates_FullMethodName, suggestCertificatesRequest, suggestCertificatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.certificates.v1.CertificateQueryService/SuggestCertificates")
	}
	return &ordersv1.CountOrderAmendmentTotalsResponse{
		ByStatus: countOrderTotalsReply.GetByStatus(),
		Result: &commonv1.OperationResult{
			Status:        countOrderTotalsReply.GetResult().GetStatus(),
			CorrelationId: suggestCertificatesReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// DeleteOrderAmendmentReport deletes an order amendment.
func (s *OrderAmendmentReportingServer) DeleteOrderAmendmentReport(ctx context.Context, req *ordersv1.DeleteOrderAmendmentReportRequest) (*ordersv1.DeleteOrderAmendmentReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "delete_order_amendment_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete order amendment report", "tenant", req.GetContext().GetTenantId())
	recalculateOrderAmendmentsRequest := newRecalculateOrderAmendmentsRequest(withRecalculateOrderAmendmentsContext(req.GetContext()), withRecalculateOrderAmendmentsOrderAmendment(&ordersv1.OrderAmendment{
		Audit: &commonv1.AuditInfo{
			Etag:      req.GetEtag(),
			CreatedAt: req.GetContext().GetDeadline(),
		},
		OrderAmendmentId: req.GetOrderAmendmentId(),
	}), withRecalculateOrderAmendmentsReason(req.GetReason()))
	recalculateOrderAmendmentsReply, err := s.orderAmendmentCalculationClient.RecalculateOrderAmendments(ctx, recalculateOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := orderAmendmentReportingServerEscalateMerchandisingRuleSteps[mode]
	escalateMerchandisingRuleRequest := &merchandisingv1.EscalateMerchandisingRuleRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				DisplayName: recalculateOrderAmendmentsReply.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateOrderAmendmentsReply.GetOrderAmendment().GetAudit().GetCreatedBy(),
			},
		},
	}
	escalateMerchandisingRuleReply, err := run(s, ctx, escalateMerchandisingRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleApprovalService/EscalateMerchandisingRule")
	}
	return &ordersv1.DeleteOrderAmendmentReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  recalculateOrderAmendmentsReply.GetResult().GetReferenceIds(),
			Details:       recalculateOrderAmendmentsReply.GetResult().GetDetails(),
			Message:       escalateMerchandisingRuleReply.GetResult().GetMessage(),
			CorrelationId: escalateMerchandisingRuleReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// orderAmendmentReportingServerEscalateMerchandisingRuleSteps are the ways to run
// acme.merchandising.v1.MerchandisingRuleApprovalService/EscalateMerchandisingRule, by name.
var orderAmendmentReportingServerEscalateMerchandisingRuleSteps = map[string]func(*OrderAmendmentReportingServer, context.Context, *merchandisingv1.EscalateMerchandisingRuleRequest) (*merchandisingv1.EscalateMerchandisingRuleResponse, error){
	"default": (*OrderAmendmentReportingServer).escalateMerchandisingRuleStep,
	"audited": (*OrderAmendmentReportingServer).escalateMerchandisingRuleAuditedStep,
}

func (s *OrderAmendmentReportingServer) escalateMerchandisingRuleStep(ctx context.Context, request *merchandisingv1.EscalateMerchandisingRuleRequest) (*merchandisingv1.EscalateMerchandisingRuleResponse, error) {
	return s.merchandisingRuleApprovalClient.EscalateMerchandisingRule(ctx, request)
}

func (s *OrderAmendmentReportingServer) escalateMerchandisingRuleAuditedStep(ctx context.Context, request *merchandisingv1.EscalateMerchandisingRuleRequest) (*merchandisingv1.EscalateMerchandisingRuleResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.merchandising.v1.MerchandisingRuleApprovalService/EscalateMerchandisingRule")
	return s.merchandisingRuleApprovalClient.EscalateMerchandisingRule(ctx, request)
}
