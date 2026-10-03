package orders

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// OrderWorkflowServer implements `acme.orders.v1.OrderWorkflowService`.
// Runs the long-lived workflow that takes an order from start to finish.
type OrderWorkflowServer struct {
	ordersv1.UnimplementedOrderWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	productReviewWorkflowClient     reviewsv1.ProductReviewWorkflowServiceClient
	riskAssessmentWorkflowClient    fraudv1.RiskAssessmentWorkflowServiceClient
	ledgerAccountValidationClient   ledgerv1.LedgerAccountValidationServiceClient
	meteredNotificationValidation   meteredNotificationValidation
	sellerWorkflowClient            sellersv1.SellerWorkflowServiceClient
	orderClient                     ordersv1.OrderServiceClient
	orderAmendmentQueryClient       ordersv1.OrderAmendmentQueryServiceClient
	orderApprovalClient             ordersv1.OrderApprovalServiceClient
	conn                            *grpc.ClientConn
	orderNoteImportClient           ordersv1.OrderNoteImportServiceClient
	clients                         *registry.Registry
	explainPreferenceDecisionFn     func(context.Context, *customersv1.ExplainPreferenceDecisionRequest, ...grpc.CallOption) (*customersv1.ExplainPreferenceDecisionResponse, error)
	orderAmendmentApprovalClient    ordersv1.OrderAmendmentApprovalServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	customerValidationClient        customersv1.CustomerValidationServiceClient
	orderReportingClient            ordersv1.OrderReportingServiceClient
}

// NewOrderWorkflowServer wires an OrderWorkflowServer.
func NewOrderWorkflowServer(deps platform.Deps) *OrderWorkflowServer {
	preferenceValidation := customersv1.NewPreferenceValidationServiceClient(deps.Conn)
	return &OrderWorkflowServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.orders.v1.OrderWorkflowService"),
		productReviewWorkflowClient:     reviewsv1.NewProductReviewWorkflowServiceClient(deps.Conn),
		riskAssessmentWorkflowClient:    fraudv1.NewRiskAssessmentWorkflowServiceClient(deps.Conn),
		ledgerAccountValidationClient:   ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		meteredNotificationValidation:   meteredNotificationValidation{NotificationValidationServiceClient: notificationsv1.NewNotificationValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerWorkflowClient:            sellersv1.NewSellerWorkflowServiceClient(deps.Conn),
		orderClient:                     ordersv1.NewOrderServiceClient(deps.Conn),
		orderAmendmentQueryClient:       ordersv1.NewOrderAmendmentQueryServiceClient(deps.Conn),
		orderApprovalClient:             ordersv1.NewOrderApprovalServiceClient(deps.Conn),
		conn:                            deps.Conn,
		orderNoteImportClient:           ordersv1.NewOrderNoteImportServiceClient(deps.Conn),
		clients:                         deps.Clients,
		explainPreferenceDecisionFn:     preferenceValidation.ExplainPreferenceDecision,
		orderAmendmentApprovalClient:    ordersv1.NewOrderAmendmentApprovalServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		customerValidationClient:        customersv1.NewCustomerValidationServiceClient(deps.Conn),
		orderReportingClient:            ordersv1.NewOrderReportingServiceClient(deps.Conn),
	}
}

// StartOrderWorkflow starts an order.
func (s *OrderWorkflowServer) StartOrderWorkflow(ctx context.Context, req *ordersv1.StartOrderWorkflowRequest) (*ordersv1.StartOrderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "start order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	var network string
	switch choice := req.GetOrder().GetSettlement().(type) {
	case *ordersv1.Order_Card:
		network = choice.Card.GetNetwork()
	case *ordersv1.Order_Bank:
		network = choice.Bank.GetIban()
	case *ordersv1.Order_VoucherCode:
		network = choice.VoucherCode
	}
	advanceProductReviewWorkflowRequest := &reviewsv1.AdvanceProductReviewWorkflowRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			Settlement: &reviewsv1.ProductReview_Card{Card: &reviewsv1.ProductReviewCardSettlement{Network: network}},
		},
		Details: &reviewsv1.AdvanceProductReviewWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	advanceProductReviewWorkflowReply, err := grpcx.Call(ctx, s.productReviewWorkflowClient.AdvanceProductReviewWorkflow, advanceProductReviewWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewWorkflowService/AdvanceProductReviewWorkflow")
	}
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.StartOrderWorkflowRequest_Details)
	step := &orderWorkflowServerStartOrderWorkflowRetryRiskAssessmentWorkflowStep{request: &fraudv1.RetryRiskAssessmentWorkflowRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: advanceProductReviewWorkflowReply.GetProductReview().GetCounterparty().GetDisplayName(),
			},
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{Email: cloned.GetNotify().GetEmail()}},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetOrder().GetAudit().GetEtag(),
				CreatedAt: advanceProductReviewWorkflowReply.GetProductReview().GetAudit().GetCreatedAt(),
			},
		},
	}}
	if err := s.runStartOrderWorkflowRetryRiskAssessmentWorkflow(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentWorkflowService/RetryRiskAssessmentWorkflow")
	}
	retryRiskAssessmentWorkflowReply := step.reply
	for line := range iterx.Each(req.GetOrder().GetLines(), func(candidate *ordersv1.OrderLine) bool { return candidate != nil }) {
		if _, err := s.ledgerAccountValidationClient.ValidateLedgerAccount(ctx, &ledgerv1.ValidateLedgerAccountRequest{
			Context: req.GetContext(),
			LedgerAccount: &ledgerv1.LedgerAccount{
				Limits:  []*ledgerv1.LedgerAccountLimit{&ledgerv1.LedgerAccountLimit{Amount: line.GetTax().GetAmount()}},
				Funding: &ledgerv1.LedgerAccount_Bank{Bank: &ledgerv1.LedgerAccountBankFunding{Bic: req.GetOrder().GetBank().GetBic()}},
				Audit: &commonv1.AuditInfo{
					CreatedAt: advanceProductReviewWorkflowReply.GetProductReview().GetAudit().GetCreatedAt(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ValidateLedgerAccount")
		}
	}
	return &ordersv1.StartOrderWorkflowResponse{
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: advanceProductReviewWorkflowReply.GetProductReview().GetAudit().GetUpdatedAt(),
			},
			OrderId:  req.GetOrderId(),
			IssuedAt: req.GetOrder().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: retryRiskAssessmentWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &ordersv1.StartOrderWorkflowResponse_Outcome{
			Message: retryRiskAssessmentWorkflowReply.GetOutcome().GetMessage(),
		},
	}, nil
}

// AdvanceOrderWorkflow advances an order.
func (s *OrderWorkflowServer) AdvanceOrderWorkflow(ctx context.Context, req *ordersv1.AdvanceOrderWorkflowRequest) (*ordersv1.AdvanceOrderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "advance order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	validateNotificationRequest := &notificationsv1.ValidateNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Assignee:  &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			Requester: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
			Audit:     &commonv1.AuditInfo{CreatedBy: req.GetOrder().GetAudit().GetCreatedBy()},
		},
	}
	validateNotificationReply, err := s.meteredNotificationValidation.ValidateNotification(ctx, validateNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ValidateNotification")
	}
	detailsCache := cache.New[string, *ordersv1.AdvanceOrderWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	cancelSellerWorkflowReply, err := s.sellerWorkflowClient.CancelSellerWorkflow(ctx, &sellersv1.CancelSellerWorkflowRequest{
		Context: req.GetContext(),
		Details: &sellersv1.CancelSellerWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: cachedDetails.GetNotify().GetEmail(),
				Name:  validateNotificationReply.GetNotification().GetRequester().GetName(),
			},
		},
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: validateNotificationReply.GetNotification().GetAssignee().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/CancelSellerWorkflow")
	}
	checkLedgerAccountEligibilityRequest := &ledgerv1.CheckLedgerAccountEligibilityRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetOrder().GetAudit().GetCreatedBy()},
			Contact: &commonv1.ContactInfo{
				Email: cancelSellerWorkflowReply.GetSeller().GetContact().GetEmail(),
			},
			Funding: &ledgerv1.LedgerAccount_Card{Card: &ledgerv1.LedgerAccountCardFunding{Token: req.GetOrder().GetCard().GetToken()}},
		},
		Details: &ledgerv1.CheckLedgerAccountEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: validateNotificationReply.GetNotification().GetAssignee().GetEmail(),
			},
		},
	}
	type checkLedgerAccountEligibilityResult struct {
		reply *ledgerv1.CheckLedgerAccountEligibilityResponse
		err   error
	}
	checkLedgerAccountEligibilityResults := make(chan checkLedgerAccountEligibilityResult, 1)
	go func() {
		reply, err := s.ledgerAccountValidationClient.CheckLedgerAccountEligibility(ctx, checkLedgerAccountEligibilityRequest)
		checkLedgerAccountEligibilityResults <- checkLedgerAccountEligibilityResult{reply, err}
	}()
	var checkLedgerAccountEligibilityReply *ledgerv1.CheckLedgerAccountEligibilityResponse
	select {
	case result := <-checkLedgerAccountEligibilityResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.ledger.v1.LedgerAccountValidationService/CheckLedgerAccountEligibility")
		}
		checkLedgerAccountEligibilityReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	reconcileBackordersRequest := &ordersv1.ReconcileBackordersRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Email:            validateNotificationReply.GetNotification().GetAssignee().GetEmail(),
			},
			Origin: &ordersv1.Backorder_Customer{Customer: &customersv1.CustomerRef{
				Email: cancelSellerWorkflowReply.GetSeller().GetRelationshipManager().GetEmail(),
			}},
		},
	}
	reconcileBackordersReply, err := reconcileBackordersCall(ctx, reconcileBackordersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/ReconcileBackorders")
	}
	return &ordersv1.AdvanceOrderWorkflowResponse{
		Order: &ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validateNotificationReply.GetNotification().GetAudit().GetUpdatedBy(),
				CreatedBy: cancelSellerWorkflowReply.GetSeller().GetAudit().GetCreatedBy(),
				CreatedAt: reconcileBackordersReply.GetBackorder().GetAudit().GetCreatedAt(),
			},
			Number:   checkLedgerAccountEligibilityReply.GetLedgerAccount().GetHolder().GetNumber(),
			OrderId:  req.GetOrderId(),
			Metadata: req.GetOrder().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelOrderWorkflow cancels an order.
func (s *OrderWorkflowServer) CancelOrderWorkflow(ctx context.Context, req *ordersv1.CancelOrderWorkflowRequest) (*ordersv1.CancelOrderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "cancel order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	deleteOrderRequest := &ordersv1.DeleteOrderRequest{
		Context: req.GetContext(),
		OrderId: req.GetOrder().GetOrderId(),
		Reason:  req.GetReason(),
		Etag:    req.GetOrder().GetAudit().GetEtag(),
	}
	send := s.deleteOrderSender(deleteOrderRequest)
	deleteOrderReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderService/DeleteOrder")
	}
	metadata2 := make(map[string]string, len(req.GetOrder().GetMetadata()))
	for key, value := range req.GetOrder().GetMetadata() {
		metadata2[key] = value
	}
	countOrderAmendmentsRequest := newCountOrderAmendmentsRequest(withCountOrderAmendmentsContext(req.GetContext()), withCountOrderAmendmentsFilter(&ordersv1.CountOrderAmendmentsRequest_Filter{
		Region:       req.GetOrder().GetBillingAddress().GetRegion(),
		CreatedAfter: req.GetOrder().GetAudit().GetCreatedAt(),
		OwnerId:      deleteOrderReply.GetResult().GetCorrelationId(),
		Labels:       metadata2,
	}))
	countOrderAmendmentsReply, err := s.orderAmendmentQueryClient.CountOrderAmendments(ctx, countOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentQueryService/CountOrderAmendments")
	}
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.CancelOrderWorkflowRequest_Details)
	rejectOrderReply, err := s.orderApprovalClient.RejectOrder(ctx, &ordersv1.RejectOrderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.RejectOrderRequest_Details{
			Approver: &identityv1.UserRef{Email: cloned.GetNotify().GetEmail()},
		},
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetOrder().GetCounterparty().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderApprovalService/RejectOrder")
	}
	listPendingOrderAmendmentsRequest := &ordersv1.ListPendingOrderAmendmentsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.ListPendingOrderAmendmentsRequest_Filter{
			Region:       req.GetOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetOrder().GetDueAt(),
			OwnerId:      rejectOrderReply.GetOrder().GetCounterparty().GetCustomerId(),
		},
	}
	listPendingOrderAmendmentsReply := new(ordersv1.ListPendingOrderAmendmentsResponse)
	if err := s.conn.Invoke(ctx, ordersOrderAmendmentApprovalServiceListPendingOrderAmendmentsMethod, listPendingOrderAmendmentsRequest, listPendingOrderAmendmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentApprovalService/ListPendingOrderAmendments")
	}
	return &ordersv1.CancelOrderWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: deleteOrderReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &ordersv1.CancelOrderWorkflowResponse_Outcome{
			Message: countOrderAmendmentsReply.GetResult().GetMessage(),
			Status:  countOrderAmendmentsReply.GetResult().GetStatus(),
		},
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: rejectOrderReply.GetOrder().GetBillingAddress().GetPostalCode(),
			},
			OrderId: req.GetOrderId(),
			DueAt:   req.GetOrder().GetDueAt(),
		},
	}, nil
}

// RetryOrderWorkflow retrys an order.
func (s *OrderWorkflowServer) RetryOrderWorkflow(ctx context.Context, req *ordersv1.RetryOrderWorkflowRequest) (*ordersv1.RetryOrderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "retry order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	for _, line := range req.GetOrder().GetLines() {
		if _, err := s.orderNoteImportClient.RetryOrderNoteImport(ctx, &ordersv1.RetryOrderNoteImportRequest{
			Context:   req.GetContext(),
			OrderNote: &ordersv1.OrderNote{Attributes: line.GetAttributes()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteImportService/RetryOrderNoteImport")
		}
	}
	return &ordersv1.RetryOrderWorkflowResponse{
		Order: &ordersv1.Order{
			OrderId:  req.GetOrderId(),
			Metadata: req.GetOrder().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseOrderWorkflow pauses an order.
func (s *OrderWorkflowServer) PauseOrderWorkflow(ctx context.Context, req *ordersv1.PauseOrderWorkflowRequest) (*ordersv1.PauseOrderWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "pause order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	var holderName string
	switch choice := req.GetOrder().GetSettlement().(type) {
	case *ordersv1.Order_Card:
		holderName = choice.Card.GetHolderName()
	case *ordersv1.Order_Bank:
		holderName = choice.Bank.GetIban()
	case *ordersv1.Order_VoucherCode:
		holderName = choice.VoucherCode
	}
	submitOrderRequest := &ordersv1.SubmitOrderRequest{}
	submitOrderRequest.Context = req.GetContext()
	submitOrderRequest.Order = &ordersv1.Order{
		Settlement: &ordersv1.Order_Card{Card: &ordersv1.OrderCardSettlement{HolderName: holderName}},
		Audit:      &commonv1.AuditInfo{Etag: req.GetOrder().GetAudit().GetEtag()},
	}
	submitOrderReply, err := s.orderApprovalClient.SubmitOrder(ctx, submitOrderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderApprovalService/SubmitOrder")
	}
	backorderReporting, ok := s.clients.Get("acme.orders.v1.BackorderReportingService").(ordersv1.BackorderReportingServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.orders.v1.BackorderReportingService")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateBackorderReportRequest := &ordersv1.GenerateBackorderReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &ordersv1.GenerateBackorderReportRequest_Details{Filters: req.GetOrder().GetMetadata()},
	}
	generateBackorderReportReply, err := backorderReporting.GenerateBackorderReport(ctx, generateBackorderReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderReportingService/GenerateBackorderReport")
	}
	return &ordersv1.PauseOrderWorkflowResponse{
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{
				Locality: submitOrderReply.GetOrder().GetBillingAddress().GetLocality(),
			},
			OrderId: req.GetOrderId(),
			Status:  req.GetOrder().GetStatus(),
		},
		Outcome: &ordersv1.PauseOrderWorkflowResponse_Outcome{EffectiveAt: submitOrderReply.GetOutcome().GetEffectiveAt()},
		Result: &commonv1.OperationResult{
			ReferenceIds: generateBackorderReportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ResumeOrderWorkflow resumes an order.
func (s *OrderWorkflowServer) ResumeOrderWorkflow(ctx context.Context, req *ordersv1.ResumeOrderWorkflowRequest) (*ordersv1.ResumeOrderWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "resume order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	detailsCache := cache.New[string, *ordersv1.ResumeOrderWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	explainPreferenceDecisionRequest := &customersv1.ExplainPreferenceDecisionRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetOrder().GetAudit().GetUpdatedBy()},
			Owner: &identityv1.UserRef{Email: cachedDetails.GetNotify().GetEmail()},
		},
		Details: &customersv1.ExplainPreferenceDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	explainPreferenceDecisionReply, err := s.explainPreferenceDecisionFn(ctx, explainPreferenceDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ExplainPreferenceDecision")
	}
	for line := range iterx.Each(req.GetOrder().GetLines(), func(candidate *ordersv1.OrderLine) bool { return candidate != nil }) {
		if _, err := s.orderAmendmentApprovalClient.SubmitOrderAmendment(ctx, &ordersv1.SubmitOrderAmendmentRequest{
			Context: req.GetContext(),
			OrderAmendment: &ordersv1.OrderAmendment{
				Lines: []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{
					Tax: &ordersv1.OrderAmendmentLineTax{Amount: line.GetTax().GetAmount()},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentApprovalService/SubmitOrderAmendment")
		}
	}
	pushSynonymRequest := &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{
				Reason:      explainPreferenceDecisionReply.GetPreference().GetBlock().GetReason(),
				NotifyEmail: explainPreferenceDecisionReply.GetPreference().GetBlock().GetNotifyEmail(),
			}},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetOrder().GetAudit().GetCreatedBy()},
		},
	}
	pushSynonymReply, err := grpcx.NewClient(s.conn, searchv1.NewSynonymSyncServiceClient).PushSynonym(ctx, pushSynonymRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PushSynonym")
	}
	return &ordersv1.ResumeOrderWorkflowResponse{
		Outcome: &ordersv1.ResumeOrderWorkflowResponse_Outcome{
			NextSteps: explainPreferenceDecisionReply.GetOutcome().GetNextSteps(),
		},
		Order: &ordersv1.Order{
			Lines:   []*ordersv1.OrderLine{&ordersv1.OrderLine{Description: pushSynonymReply.GetSynonym().GetDescription()}},
			OrderId: req.GetOrderId(),
			Notes:   req.GetOrder().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectOrderWorkflow returns an order.
func (s *OrderWorkflowServer) InspectOrderWorkflow(ctx context.Context, req *ordersv1.InspectOrderWorkflowRequest) (*ordersv1.InspectOrderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order", req.GetOrderId(), "inspect_order_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect order workflow", "tenant", req.GetContext().GetTenantId())
	previewCustomerSegmentRequest := &customersv1.PreviewCustomerSegmentRequest{}
	previewCustomerSegmentRequest.Context = req.GetContext()
	previewCustomerSegmentRequest.CustomerSegment = &customersv1.CustomerSegment{
		Owner: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
	}
	previewCustomerSegmentReply, err := s.customerSegmentValidationClient.PreviewCustomerSegment(ctx, previewCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/PreviewCustomerSegment")
	}
	return &ordersv1.InspectOrderWorkflowResponse{
		Order: &ordersv1.Order{
			Lines: []*ordersv1.OrderLine{&ordersv1.OrderLine{
				Tax: &ordersv1.OrderLineTax{
					Amount: previewCustomerSegmentReply.GetCustomerSegment().GetDiscount().GetAmount(),
				},
			}},
			OrderId: req.GetOrderId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListOrderWorkflows lists orders a page at a time.
func (s *OrderWorkflowServer) ListOrderWorkflows(ctx context.Context, req *ordersv1.ListOrderWorkflowsRequest) (*ordersv1.ListOrderWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list order workflows", "tenant", req.GetContext().GetTenantId())
	scoreCustomerRequest := &customersv1.ScoreCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	scoreCustomerReply, err := customersv1.CustomerValidationServiceClient.ScoreCustomer(s.customerValidationClient, ctx, scoreCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ScoreCustomer")
	}
	return &ordersv1.ListOrderWorkflowsResponse{
		Orders: []*ordersv1.Order{&ordersv1.Order{
			Audit: &commonv1.AuditInfo{
				CreatedAt: scoreCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalOrderWorkflow signals an order.
func (s *OrderWorkflowServer) SignalOrderWorkflow(ctx context.Context, req *ordersv1.SignalOrderWorkflowRequest) (*ordersv1.SignalOrderWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrder(ctx, req.GetContext().GetTenantId(), req.GetOrder()); err != nil {
		return nil, grpcx.Wrap(err, "save order")
	}
	s.log.InfoContext(ctx, "signal order workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrder().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetOrder()).(*ordersv1.Order)
	deleteOrderReportRequest := newDeleteOrderReportRequest(withDeleteOrderReportContext(req.GetContext()), withDeleteOrderReportOrderId(req.GetOrder().GetOrderId()), withDeleteOrderReportEtag(cloned.GetAudit().GetEtag()), withDeleteOrderReportReason(req.GetReason()))
	deleteOrderReportReply, err := s.orderReportingClient.DeleteOrderReport(ctx, deleteOrderReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/DeleteOrderReport")
	}
	return &ordersv1.SignalOrderWorkflowResponse{
		Outcome: &ordersv1.SignalOrderWorkflowResponse_Outcome{Message: deleteOrderReportReply.GetResult().GetMessage()},
		Order: &ordersv1.Order{
			OrderId: req.GetOrderId(),
			Notes:   req.GetOrder().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// orderWorkflowServerStartOrderWorkflowRetryRiskAssessmentWorkflowStep carries the request of one step of
// StartOrderWorkflow and, once it ran, its reply.
type orderWorkflowServerStartOrderWorkflowRetryRiskAssessmentWorkflowStep struct {
	request *fraudv1.RetryRiskAssessmentWorkflowRequest
	reply   *fraudv1.RetryRiskAssessmentWorkflowResponse
}

func (s *OrderWorkflowServer) runStartOrderWorkflowRetryRiskAssessmentWorkflow(ctx context.Context, step *orderWorkflowServerStartOrderWorkflowRetryRiskAssessmentWorkflowStep) error {
	reply, err := s.riskAssessmentWorkflowClient.RetryRiskAssessmentWorkflow(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// deleteOrderSender returns a function that sends one request to acme.orders.v1.OrderService/DeleteOrder.
func (s *OrderWorkflowServer) deleteOrderSender(request *ordersv1.DeleteOrderRequest) func(context.Context) (*ordersv1.DeleteOrderResponse, error) {
	return func(ctx context.Context) (*ordersv1.DeleteOrderResponse, error) {
		return s.orderClient.DeleteOrder(ctx, request)
	}
}
