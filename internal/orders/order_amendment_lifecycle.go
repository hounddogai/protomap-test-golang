package orders

import (
	"context"
	"fmt"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	keymanagementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/keymanagement/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	performancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderAmendmentLifecycleServer implements `acme.orders.v1.OrderAmendmentLifecycleService`.
// Moves an order amendment between the states of its lifecycle.
type OrderAmendmentLifecycleServer struct {
	ordersv1.UnimplementedOrderAmendmentLifecycleServiceServer
	store                              *Store
	log                                *slog.Logger
	conn                               *grpc.ClientConn
	paymentInstrumentSyncClient        paymentsv1.PaymentInstrumentSyncServiceClient
	cryptoKeyClient                    keymanagementv1.CryptoKeyServiceClient
	sellerPayoutScheduleApprovalClient sellersv1.SellerPayoutScheduleApprovalServiceClient
	orderNoteValidationClient          ordersv1.OrderNoteValidationServiceClient
	clients                            *registry.Registry
	orderReportingClient               ordersv1.OrderReportingServiceClient
	consentRecordCalculationClient     customersv1.ConsentRecordCalculationServiceClient
	orderValidationClient              ordersv1.OrderValidationServiceClient
	backorderSyncClient                ordersv1.BackorderSyncServiceClient
	orderAmendmentValidationClient     ordersv1.OrderAmendmentValidationServiceClient
}

// NewOrderAmendmentLifecycleServer wires an OrderAmendmentLifecycleServer.
func NewOrderAmendmentLifecycleServer(deps platform.Deps) *OrderAmendmentLifecycleServer {
	return &OrderAmendmentLifecycleServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.orders.v1.OrderAmendmentLifecycleService"),
		conn:                               deps.Conn,
		paymentInstrumentSyncClient:        paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn),
		cryptoKeyClient:                    keymanagementv1.NewCryptoKeyServiceClient(deps.Conn),
		sellerPayoutScheduleApprovalClient: sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn),
		orderNoteValidationClient:          ordersv1.NewOrderNoteValidationServiceClient(deps.Conn),
		clients:                            deps.Clients,
		orderReportingClient:               ordersv1.NewOrderReportingServiceClient(deps.Conn),
		consentRecordCalculationClient:     customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		orderValidationClient:              ordersv1.NewOrderValidationServiceClient(deps.Conn),
		backorderSyncClient:                ordersv1.NewBackorderSyncServiceClient(deps.Conn),
		orderAmendmentValidationClient:     ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn),
	}
}

// ActivateOrderAmendment activates an order amendment.
func (s *OrderAmendmentLifecycleServer) ActivateOrderAmendment(ctx context.Context, req *ordersv1.ActivateOrderAmendmentRequest) (*ordersv1.ActivateOrderAmendmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "activate_order_amendment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate order amendment", "tenant", req.GetContext().GetTenantId())
	listPaymentInstrumentConflictsRequest := &paymentsv1.ListPaymentInstrumentConflictsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.ListPaymentInstrumentConflictsRequest_Filter{
			Labels:       req.GetAnnotations(),
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
		},
	}
	listPaymentInstrumentConflictsReply := new(paymentsv1.ListPaymentInstrumentConflictsResponse)
	if err := grpcx.Invoke(ctx, s.conn, paymentsv1.PaymentInstrumentSyncService_ListPaymentInstrumentConflicts_FullMethodName, listPaymentInstrumentConflictsRequest, listPaymentInstrumentConflictsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/ListPaymentInstrumentConflicts")
	}
	return &ordersv1.ActivateOrderAmendmentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listPaymentInstrumentConflictsReply.GetResult().GetReferenceIds(),
			Message:      listPaymentInstrumentConflictsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
		Outcome:        &ordersv1.ActivateOrderAmendmentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendOrderAmendment suspends an order amendment.
func (s *OrderAmendmentLifecycleServer) SuspendOrderAmendment(ctx context.Context, req *ordersv1.SuspendOrderAmendmentRequest) (*ordersv1.SuspendOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendOrderAmendment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "suspend_order_amendment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend order amendment", "tenant", req.GetContext().GetTenantId())
	reconcilePaymentInstrumentsRequest := &paymentsv1.ReconcilePaymentInstrumentsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
				Email: req.GetDetails().GetNewOwner().GetEmail(),
			},
			Holder: &customersv1.CustomerRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}
	reconcilePaymentInstrumentsReply, err := grpcx.Call(ctx, s.paymentInstrumentSyncClient.ReconcilePaymentInstruments, reconcilePaymentInstrumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/ReconcilePaymentInstruments")
	}
	updateCryptoKeyReply, err := s.cryptoKeyClient.UpdateCryptoKey(ctx, &keymanagementv1.UpdateCryptoKeyRequest{
		Context: req.GetContext(),
		CryptoKey: &keymanagementv1.CryptoKey{
			Funding: &keymanagementv1.CryptoKey_Card{Card: &keymanagementv1.CryptoKeyCardFunding{
				Last4: reconcilePaymentInstrumentsReply.GetPaymentInstrument().GetCard().GetLast4(),
			}},
			Contact: &commonv1.ContactInfo{
				Email: reconcilePaymentInstrumentsReply.GetPaymentInstrument().GetHolder().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: reconcilePaymentInstrumentsReply.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.keymanagement.v1.CryptoKeyService/UpdateCryptoKey")
	}
	return &ordersv1.SuspendOrderAmendmentResponse{
		Result: &commonv1.OperationResult{
			Message: reconcilePaymentInstrumentsReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Outcome: &ordersv1.SuspendOrderAmendmentResponse_Outcome{
			Message:     updateCryptoKeyReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
	}, nil
}

// ReinstateOrderAmendment reinstates an order amendment.
func (s *OrderAmendmentLifecycleServer) ReinstateOrderAmendment(ctx context.Context, req *ordersv1.ReinstateOrderAmendmentRequest) (*ordersv1.ReinstateOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReinstateOrderAmendment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "reinstate_order_amendment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate order amendment", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.ReinstateOrderAmendmentRequest_Details)
	commentSellerPayoutScheduleApprovalRequest := &sellersv1.CommentSellerPayoutScheduleApprovalRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Phone: cloned.GetNotifyContact().GetPhone(),
					Name:  req.GetDetails().GetNotifyContact().GetName(),
				},
			}},
		},
	}
	commentSellerPayoutScheduleApprovalReply, err := grpcx.Call(ctx, s.sellerPayoutScheduleApprovalClient.CommentSellerPayoutScheduleApproval, commentSellerPayoutScheduleApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/CommentSellerPayoutScheduleApproval")
	}
	cloned2 := proto.Clone(req.GetDetails()).(*ordersv1.ReinstateOrderAmendmentRequest_Details)
	scoreOrderNoteRequest := &ordersv1.ScoreOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{DisplayName: cloned2.GetNewOwner().GetDisplayName()},
		},
	}
	type scoreOrderNoteResult struct {
		reply *ordersv1.ScoreOrderNoteResponse
		err   error
	}
	scoreOrderNoteResults := make(chan scoreOrderNoteResult, 1)
	go func() {
		reply, err := s.orderNoteValidationClient.ScoreOrderNote(ctx, scoreOrderNoteRequest)
		scoreOrderNoteResults <- scoreOrderNoteResult{reply, err}
	}()
	var scoreOrderNoteReply *ordersv1.ScoreOrderNoteResponse
	select {
	case result := <-scoreOrderNoteResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderNoteValidationService/ScoreOrderNote")
		}
		scoreOrderNoteReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	browseReviewsRequest := &performancev1.BrowseReviewsRequest{
		Context: req.GetContext(),
		Filter: &performancev1.BrowseReviewsRequest_Filter{
			Region:       commentSellerPayoutScheduleApprovalReply.GetSellerPayoutSchedule().GetLocation().GetRegion(),
			CreatedAfter: scoreOrderNoteReply.GetOrderNote().GetOccurredAt(),
		},
	}
	browseReviewsReply, err := browseReviewsCall(ctx, browseReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.performance.v1.ReviewQueryService/BrowseReviews")
	}
	createCustomerRequest := &customersv1.CreateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: commentSellerPayoutScheduleApprovalReply.GetSellerPayoutSchedule().GetLocation().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: commentSellerPayoutScheduleApprovalReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedAt(),
			},
		},
	}
	createCustomerReply, err := createCustomerCall(ctx, createCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CreateCustomer")
	}
	_ = createCustomerReply
	return &ordersv1.ReinstateOrderAmendmentResponse{
		Outcome: &ordersv1.ReinstateOrderAmendmentResponse_Outcome{
			Message:     commentSellerPayoutScheduleApprovalReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Number:           scoreOrderNoteReply.GetOrderNote().GetSubject().GetNumber(),
			Total:            scoreOrderNoteReply.GetBreakdown().GetTotal(),
			OrderAmendmentId: req.GetOrderAmendmentId(),
		},
		Result: &commonv1.OperationResult{
			Message: browseReviewsReply.GetResult().GetMessage(),
			Status:  browseReviewsReply.GetResult().GetStatus(),
		},
	}, nil
}

// TerminateOrderAmendment terminates an order amendment.
func (s *OrderAmendmentLifecycleServer) TerminateOrderAmendment(ctx context.Context, req *ordersv1.TerminateOrderAmendmentRequest) (*ordersv1.TerminateOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "terminate_order_amendment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate order amendment", "tenant", req.GetContext().GetTenantId())
	orderAmendmentQuery, err := registry.Lookup[ordersv1.OrderAmendmentQueryServiceClient](s.clients, "acme.orders.v1.OrderAmendmentQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	findOrderAmendmentsRequest := &ordersv1.FindOrderAmendmentsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.FindOrderAmendmentsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       annotations,
		},
	}
	findOrderAmendmentsReply, err := orderAmendmentQuery.FindOrderAmendments(ctx, findOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentQueryService/FindOrderAmendments")
	}
	annotations2 := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations2[key2] = value2
	}
	generateOrderReportRequest := &ordersv1.GenerateOrderReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations2,
		Reason:      req.GetReason(),
		Details:     &ordersv1.GenerateOrderReportRequest_Details{Filters: findOrderAmendmentsReply.GetResult().GetDetails()},
	}
	generateOrderReportReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*ordersv1.GenerateOrderReportResponse, error) {
		return s.orderReportingClient.GenerateOrderReport(ctx, generateOrderReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/GenerateOrderReport")
	}
	_ = generateOrderReportReply
	return &ordersv1.TerminateOrderAmendmentResponse{
		Result: &commonv1.OperationResult{
			Status:        findOrderAmendmentsReply.GetResult().GetStatus(),
			CorrelationId: findOrderAmendmentsReply.GetResult().GetCorrelationId(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
		Outcome:        &ordersv1.TerminateOrderAmendmentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewOrderAmendment renews an order amendment.
func (s *OrderAmendmentLifecycleServer) RenewOrderAmendment(ctx context.Context, req *ordersv1.RenewOrderAmendmentRequest) (*ordersv1.RenewOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewOrderAmendment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "renew_order_amendment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew order amendment", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileOrderNotesRequest := &ordersv1.ReconcileOrderNotesRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Measurements: []*ordersv1.OrderNoteMeasurement{&ordersv1.OrderNoteMeasurement{ObservedAt: req.GetDetails().GetEffectiveAt()}},
			Detail:       packed,
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &ordersv1.ReconcileOrderNotesRequest_Details{Payload: snapshot},
	}
	reconcileOrderNotesReply := new(ordersv1.ReconcileOrderNotesResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", ordersOrderNoteSyncServiceName, "ReconcileOrderNotes"), reconcileOrderNotesRequest, reconcileOrderNotesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteSyncService/ReconcileOrderNotes")
	}
	recalculateConsentRecordsReply, err := s.consentRecordCalculationClient.RecalculateConsentRecords(ctx, &customersv1.RecalculateConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId: reconcileOrderNotesReply.GetOrderNote().GetCorrelationId(),
			Attributes:    reconcileOrderNotesReply.GetOrderNote().GetAttributes(),
			Source:        reconcileOrderNotesReply.GetOrderNote().GetSource(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	return &ordersv1.RenewOrderAmendmentResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Lines: []*ordersv1.OrderAmendmentLine{&ordersv1.OrderAmendmentLine{
				Attributes: reconcileOrderNotesReply.GetOrderNote().GetAttributes(),
			}},
			OrderAmendmentId: req.GetOrderAmendmentId(),
		},
		Result: &commonv1.OperationResult{
			Status:        reconcileOrderNotesReply.GetResult().GetStatus(),
			CorrelationId: recalculateConsentRecordsReply.GetResult().GetCorrelationId(),
		},
		Outcome: &ordersv1.RenewOrderAmendmentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpireOrderAmendment expires an order amendment.
func (s *OrderAmendmentLifecycleServer) ExpireOrderAmendment(ctx context.Context, req *ordersv1.ExpireOrderAmendmentRequest) (*ordersv1.ExpireOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpireOrderAmendment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "expire_order_amendment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire order amendment", "tenant", req.GetContext().GetTenantId())
	checkOrderEligibilityRequest := &ordersv1.CheckOrderEligibilityRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
		Details: &ordersv1.CheckOrderEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	var checkOrderEligibilityReply *ordersv1.CheckOrderEligibilityResponse
	if err := pipeline.Run(ctx, "acme.orders.v1.OrderValidationService/CheckOrderEligibility", func(ctx context.Context) error {
		answer, err := s.orderValidationClient.CheckOrderEligibility(ctx, checkOrderEligibilityRequest)
		checkOrderEligibilityReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/CheckOrderEligibility")
	}
	pushBackorderRequest := &ordersv1.PushBackorderRequest{}
	pushBackorderRequest.Context = req.GetContext()
	pushBackorderRequest.Backorder = &ordersv1.Backorder{
		Assignee: &identityv1.UserRef{
			DisplayName: checkOrderEligibilityReply.GetOrder().GetCounterparty().GetDisplayName(),
			Email:       req.GetDetails().GetNotifyContact().GetEmail(),
		},
		Origin: &ordersv1.Backorder_Customer{Customer: &customersv1.CustomerRef{
			Email: checkOrderEligibilityReply.GetOrder().GetCounterparty().GetEmail(),
		}},
		Comments: []*ordersv1.BackorderComment{&ordersv1.BackorderComment{
			CreatedAt: checkOrderEligibilityReply.GetOrder().GetAudit().GetCreatedAt(),
		}},
	}
	pushBackorderReply, err := s.backorderSyncClient.PushBackorder(ctx, pushBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/PushBackorder")
	}
	countBackorderTotalsRequest := &ordersv1.CountBackorderTotalsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.CountBackorderTotalsRequest_Filter{
			Region:       checkOrderEligibilityReply.GetOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: pushBackorderReply.GetBackorder().GetSla().GetDueAt(),
			OwnerId:      checkOrderEligibilityReply.GetOrder().GetCounterparty().GetCustomerId(),
			Labels:       pushBackorderReply.GetBackorder().GetCustomFields(),
		},
	}
	countBackorderTotalsReply := new(ordersv1.CountBackorderTotalsResponse)
	if err := grpc.Invoke(ctx, "/acme.orders.v1.BackorderReportingService/CountBackorderTotals", countBackorderTotalsRequest, countBackorderTotalsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderReportingService/CountBackorderTotals")
	}
	return &ordersv1.ExpireOrderAmendmentResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: checkOrderEligibilityReply.GetOrder().GetAudit().GetUpdatedBy(),
				CreatedBy: checkOrderEligibilityReply.GetOrder().GetAudit().GetCreatedBy(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
		},
		Result: &commonv1.OperationResult{
			Details:       pushBackorderReply.GetResult().GetDetails(),
			CorrelationId: countBackorderTotalsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &ordersv1.ExpireOrderAmendmentResponse_Outcome{
			Message:     countBackorderTotalsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// GetOrderAmendmentLifecycle returns an order amendment.
func (s *OrderAmendmentLifecycleServer) GetOrderAmendmentLifecycle(ctx context.Context, req *ordersv1.GetOrderAmendmentLifecycleRequest) (*ordersv1.GetOrderAmendmentLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetOrderAmendmentLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "get_order_amendment_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get order amendment lifecycle", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.GetOrderAmendmentLifecycleResponse{
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
		Result:         &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleOrderAmendmentExpiry schedules an order amendment.
func (s *OrderAmendmentLifecycleServer) ScheduleOrderAmendmentExpiry(ctx context.Context, req *ordersv1.ScheduleOrderAmendmentExpiryRequest) (*ordersv1.ScheduleOrderAmendmentExpiryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "schedule order amendment expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	explainOrderAmendmentDecisionRequest := &ordersv1.ExplainOrderAmendmentDecisionRequest{
		Context: req.GetContext(),
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetOrderAmendment().GetAudit().GetCreatedAt(),
				CreatedBy: req.GetOrderAmendment().GetAudit().GetCreatedBy(),
			},
		},
	}
	explainOrderAmendmentDecisionReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*ordersv1.ExplainOrderAmendmentDecisionResponse, error) {
		return s.orderAmendmentValidationClient.ExplainOrderAmendmentDecision(ctx, explainOrderAmendmentDecisionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentValidationService/ExplainOrderAmendmentDecision")
	}
	customerCalculation, ok := s.clients.Get("acme.customers.v1.CustomerCalculationService").(customersv1.CustomerCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.CustomerCalculationService")
	}
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: req.GetOrderAmendment().GetBillingAddress().GetLocality(),
			},
		},
	}
	explainCustomerCalculationReply, err := customerCalculation.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	return &ordersv1.ScheduleOrderAmendmentExpiryResponse{
		Outcome: &ordersv1.ScheduleOrderAmendmentExpiryResponse_Outcome{
			Message: explainOrderAmendmentDecisionReply.GetResult().GetMessage(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainCustomerCalculationReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Counterparty:     req.GetOrderAmendment().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
