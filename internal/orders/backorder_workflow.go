package orders

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// BackorderWorkflowServer implements `acme.orders.v1.BackorderWorkflowService`.
// Runs the long-lived workflow that takes a backorder from start to finish.
type BackorderWorkflowServer struct {
	*backorderWorkflowHandlers
}

// backorderWorkflowHandlers holds the handlers of BackorderWorkflowServer, with what they use.
type backorderWorkflowHandlers struct {
	ordersv1.UnimplementedBackorderWorkflowServiceServer
	store                         *Store
	log                           *slog.Logger
	consentRecordValidationClient customersv1.ConsentRecordValidationServiceClient
	backorderApprovalClient       ordersv1.BackorderApprovalServiceClient
	conn                          *grpc.ClientConn
	customerClient                customersv1.CustomerServiceClient
	orderNoteSyncClient           ordersv1.OrderNoteSyncServiceClient
	refundWorkflowClient          paymentsv1.RefundWorkflowServiceClient
	sellerScorecardSyncClient     sellersv1.SellerScorecardSyncServiceClient
	searchIndexValidationClient   searchv1.SearchIndexValidationServiceClient
	orderNoteApprovalClient       ordersv1.OrderNoteApprovalServiceClient
	meteredSearchQueryValidation  meteredSearchQueryValidation
	fraudSignalWorkflowClient     fraudv1.FraudSignalWorkflowServiceClient
	backorderLifecycleClient      ordersv1.BackorderLifecycleServiceClient
	searchQueryValidationPort     searchQueryValidationPort
}

// NewBackorderWorkflowServer wires a BackorderWorkflowServer.
func NewBackorderWorkflowServer(deps platform.Deps) *BackorderWorkflowServer {
	return &BackorderWorkflowServer{backorderWorkflowHandlers: &backorderWorkflowHandlers{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.orders.v1.BackorderWorkflowService"),
		consentRecordValidationClient: customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		backorderApprovalClient:       ordersv1.NewBackorderApprovalServiceClient(deps.Conn),
		conn:                          deps.Conn,
		customerClient:                customersv1.NewCustomerServiceClient(deps.Conn),
		orderNoteSyncClient:           ordersv1.NewOrderNoteSyncServiceClient(deps.Conn),
		refundWorkflowClient:          paymentsv1.NewRefundWorkflowServiceClient(deps.Conn),
		sellerScorecardSyncClient:     sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		searchIndexValidationClient:   searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		orderNoteApprovalClient:       ordersv1.NewOrderNoteApprovalServiceClient(deps.Conn),
		meteredSearchQueryValidation:  meteredSearchQueryValidation{SearchQueryValidationServiceClient: searchv1.NewSearchQueryValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		fraudSignalWorkflowClient:     fraudv1.NewFraudSignalWorkflowServiceClient(deps.Conn),
		backorderLifecycleClient:      ordersv1.NewBackorderLifecycleServiceClient(deps.Conn),
		searchQueryValidationPort:     newSearchQueryValidationPort(deps),
	}}
}

// StartBackorderWorkflow starts a backorder.
func (s *backorderWorkflowHandlers) StartBackorderWorkflow(ctx context.Context, req *ordersv1.StartBackorderWorkflowRequest) (*ordersv1.StartBackorderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "start backorder workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	for _, comment := range req.GetBackorder().GetComments() {
		if _, err := s.consentRecordValidationClient.ExplainConsentRecordDecision(ctx, &customersv1.ExplainConsentRecordDecisionRequest{
			Context: req.GetContext(),
			Details: &customersv1.ExplainConsentRecordDecisionRequest_Details{
				Contact: &commonv1.ContactInfo{Email: comment.GetAuthorEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
		}
	}
	rejectBackorderRequest := &ordersv1.RejectBackorderRequest{}
	rejectBackorderRequest.Context = req.GetContext()
	rejectBackorderRequest.Details = &ordersv1.RejectBackorderRequest_Details{
		Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
	}
	rejectBackorderReply, err := s.backorderApprovalClient.RejectBackorder(ctx, rejectBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderApprovalService/RejectBackorder")
	}
	calculatePreferenceRequest := &customersv1.CalculatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				UserId:      req.GetBackorder().GetAssignee().GetUserId(),
				DisplayName: req.GetBackorder().GetCustomer().GetDisplayName(),
			},
		},
	}
	calculatePreferenceReply := new(customersv1.CalculatePreferenceResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersPreferenceCalculationServiceName, "CalculatePreference"), calculatePreferenceRequest, calculatePreferenceReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/CalculatePreference")
	}
	return &ordersv1.StartBackorderWorkflowResponse{
		Backorder: &ordersv1.Backorder{
			Tags: rejectBackorderReply.GetBackorder().GetTags(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculatePreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceBackorderWorkflow advances a backorder.
func (s *backorderWorkflowHandlers) AdvanceBackorderWorkflow(ctx context.Context, req *ordersv1.AdvanceBackorderWorkflowRequest) (*ordersv1.AdvanceBackorderWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "advance backorder workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	var customerId string
	switch choice := req.GetBackorder().GetOrigin().(type) {
	case *ordersv1.Backorder_Customer:
		customerId = choice.Customer.GetCustomerId()
	case *ordersv1.Backorder_ExternalReference:
		customerId = choice.ExternalReference
	case *ordersv1.Backorder_Channel:
		customerId = choice.Channel.GetChannel()
	}
	listPendingBackordersRequest := &ordersv1.ListPendingBackordersRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.ListPendingBackordersRequest_Filter{
			CreatedAfter: req.GetBackorder().GetAudit().GetUpdatedAt(),
			OwnerId:      customerId,
		},
	}
	listPendingBackordersReply := new(ordersv1.ListPendingBackordersResponse)
	if err := s.conn.Invoke(ctx, ordersBackorderApprovalServiceListPendingBackordersMethod, listPendingBackordersRequest, listPendingBackordersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderApprovalService/ListPendingBackorders")
	}
	ctx = tenancy.With(ctx, "phone", req.GetBackorder().GetRequester().GetPhone())
	encoded, err := protojson.Marshal(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded ordersv1.Backorder
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	watchCustomersStream, err := s.customerClient.WatchCustomers(ctx, &customersv1.WatchCustomersRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetBackorder().GetRequester().GetEmail(),
			Phone: tenancy.Value(ctx, "phone"),
			Name:  (&decoded).GetRequester().GetName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/WatchCustomers")
	}
	for {
		received, err := watchCustomersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/WatchCustomers")
		}
		_ = received
	}
	var email string
	switch choice2 := req.GetBackorder().GetOrigin().(type) {
	case *ordersv1.Backorder_Customer:
		email = choice2.Customer.GetEmail()
	case *ordersv1.Backorder_ExternalReference:
		email = choice2.ExternalReference
	case *ordersv1.Backorder_Channel:
		email = choice2.Channel.GetChannel()
	}
	commentBackorderApprovalRequest := &ordersv1.CommentBackorderApprovalRequest{
		Context: req.GetContext(),
		Details: &ordersv1.CommentBackorderApprovalRequest_Details{Approver: &identityv1.UserRef{Email: email}},
	}
	type commentBackorderApprovalResult struct {
		reply *ordersv1.CommentBackorderApprovalResponse
		err   error
	}
	commentBackorderApprovalResults := make(chan commentBackorderApprovalResult, 1)
	go func() {
		reply, err := s.backorderApprovalClient.CommentBackorderApproval(ctx, commentBackorderApprovalRequest)
		commentBackorderApprovalResults <- commentBackorderApprovalResult{reply, err}
	}()
	var commentBackorderApprovalReply *ordersv1.CommentBackorderApprovalResponse
	select {
	case result := <-commentBackorderApprovalResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.BackorderApprovalService/CommentBackorderApproval")
		}
		commentBackorderApprovalReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &ordersv1.AdvanceBackorderWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:       listPendingBackordersReply.GetResult().GetStatus(),
			ReferenceIds: listPendingBackordersReply.GetResult().GetReferenceIds(),
		},
		Backorder: &ordersv1.Backorder{
			Sla: &ordersv1.BackorderSla{
				PausedFor: commentBackorderApprovalReply.GetBackorder().GetSla().GetPausedFor(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
	}, nil
}

// CancelBackorderWorkflow cancels a backorder.
func (s *backorderWorkflowHandlers) CancelBackorderWorkflow(ctx context.Context, req *ordersv1.CancelBackorderWorkflowRequest) (*ordersv1.CancelBackorderWorkflowResponse, error) {
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "cancel backorder workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	source := req.GetBackorder().GetSla().ProtoReflect()
	policyId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("policy_id"))).String()
	listPendingBackordersRequest := &ordersv1.ListPendingBackordersRequest{}
	listPendingBackordersRequest.Context = req.GetContext()
	listPendingBackordersRequest.Filter = &ordersv1.ListPendingBackordersRequest_Filter{
		CreatedAfter: req.GetBackorder().GetAudit().GetUpdatedAt(),
		OwnerId:      policyId,
		Labels:       req.GetBackorder().GetCustomFields(),
	}
	listPendingBackordersReply, err := s.backorderApprovalClient.ListPendingBackorders(ctx, listPendingBackordersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderApprovalService/ListPendingBackorders")
	}
	packed, err := anypb.New(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncOrderNotesReply, err := s.orderNoteSyncClient.SyncOrderNotes(ctx, &ordersv1.SyncOrderNotesRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: req.GetBackorder().GetCustomer().GetDisplayName(),
			},
			CorrelationId: listPendingBackordersReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Reason:  req.GetReason(),
		Details: &ordersv1.SyncOrderNotesRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteSyncService/SyncOrderNotes")
	}
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.CancelBackorderWorkflowRequest_Details)
	cancelRefundWorkflowRequest := newCancelRefundWorkflowRequest(withCancelRefundWorkflowContext(req.GetContext()), withCancelRefundWorkflowRefund(&paymentsv1.Refund{
		Counterparty: &customersv1.CustomerRef{Email: req.GetBackorder().GetCustomer().GetEmail()},
	}), withCancelRefundWorkflowDetails(&paymentsv1.CancelRefundWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{
			Name:             req.GetDetails().GetNotify().GetName(),
			PreferredChannel: cloned.GetNotify().GetPreferredChannel(),
		},
	}))
	cancelRefundWorkflowReply, err := s.refundWorkflowClient.CancelRefundWorkflow(ctx, cancelRefundWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundWorkflowService/CancelRefundWorkflow")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := backorderWorkflowHandlersReconcileSellerScorecardsSteps[mode]
	ctx = tenancy.With(ctx, "created_by", req.GetBackorder().GetAudit().GetCreatedBy())
	reconcileSellerScorecardsRequest := &sellersv1.ReconcileSellerScorecardsRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Settlement: &sellersv1.SellerScorecard_Bank{Bank: &sellersv1.SellerScorecardBankSettlement{
				Iban: cancelRefundWorkflowReply.GetRefund().GetBank().GetIban(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: tenancy.Value(ctx, "created_by"),
				Etag:      req.GetBackorder().GetAudit().GetEtag(),
			},
		},
	}
	reconcileSellerScorecardsReply, err := run(s, ctx, reconcileSellerScorecardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardSyncService/ReconcileSellerScorecards")
	}
	return &ordersv1.CancelBackorderWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:  listPendingBackordersReply.GetResult().GetStatus(),
			Message: cancelRefundWorkflowReply.GetResult().GetMessage(),
		},
		Outcome: &ordersv1.CancelBackorderWorkflowResponse_Outcome{
			ReferenceId: syncOrderNotesReply.GetOutcome().GetReferenceId(),
		},
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cancelRefundWorkflowReply.GetRefund().GetAudit().GetCreatedBy(),
			},
			Requester: &commonv1.ContactInfo{
				Email: reconcileSellerScorecardsReply.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
	}, nil
}

// RetryBackorderWorkflow retrys a backorder.
func (s *backorderWorkflowHandlers) RetryBackorderWorkflow(ctx context.Context, req *ordersv1.RetryBackorderWorkflowRequest) (*ordersv1.RetryBackorderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryBackorderWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "retry backorder workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*ordersv1.ResolveOrderNoteConflictResponse, len(req.GetBackorder().GetHistory()))
	for i, item := range req.GetBackorder().GetHistory() {
		group.Go(func() error {
			reply, err := s.orderNoteSyncClient.ResolveOrderNoteConflict(groupCtx, &ordersv1.ResolveOrderNoteConflictRequest{
				Context: req.GetContext(),
				OrderNote: &ordersv1.OrderNote{
					Measurements: []*ordersv1.OrderNoteMeasurement{&ordersv1.OrderNoteMeasurement{ObservedAt: item.GetChangedAt()}},
					Subject: &ordersv1.OrderRef{
						DisplayName: req.GetBackorder().GetCustomer().GetDisplayName(),
					},
				},
				Annotations: req.GetAnnotations(),
				Reason:      req.GetReason(),
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteSyncService/ResolveOrderNoteConflict")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	pauseLoyaltyMemberWorkflowRequest := &loyaltyv1.PauseLoyaltyMemberWorkflowRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
	}
	pauseLoyaltyMemberWorkflowReply := new(loyaltyv1.PauseLoyaltyMemberWorkflowResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.LoyaltyMemberWorkflowService_PauseLoyaltyMemberWorkflow_FullMethodName, pauseLoyaltyMemberWorkflowRequest, pauseLoyaltyMemberWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberWorkflowService/PauseLoyaltyMemberWorkflow")
	}
	return &ordersv1.RetryBackorderWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: pauseLoyaltyMemberWorkflowReply.GetResult().GetStatus(),
		},
		Backorder: &ordersv1.Backorder{
			BackorderId: req.GetBackorderId(),
			Priority:    req.GetBackorder().GetPriority(),
		},
	}, nil
}

// PauseBackorderWorkflow pauses a backorder.
func (s *backorderWorkflowHandlers) PauseBackorderWorkflow(ctx context.Context, req *ordersv1.PauseBackorderWorkflowRequest) (*ordersv1.PauseBackorderWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "pause backorder workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*searchv1.CheckSearchIndexEligibilityResponse, len(req.GetBackorder().GetHistory()))
	for i, item := range req.GetBackorder().GetHistory() {
		group.Go(func() error {
			reply, err := s.searchIndexValidationClient.CheckSearchIndexEligibility(groupCtx, &searchv1.CheckSearchIndexEligibilityRequest{
				Context: req.GetContext(),
				Details: &searchv1.CheckSearchIndexEligibilityRequest_Details{
					Contact: &commonv1.ContactInfo{
						Email: req.GetBackorder().GetCustomer().GetEmail(),
						Phone: req.GetBackorder().GetRequester().GetPhone(),
					},
				},
				SearchIndex: &searchv1.SearchIndex{
					Owner: &identityv1.UserRef{Email: req.GetBackorder().GetRequester().GetEmail()},
					Audit: &commonv1.AuditInfo{CreatedAt: item.GetChangedAt()},
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
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/CheckSearchIndexEligibility")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	packed, err := anypb.New(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	submitOrderNoteReply, err := s.orderNoteApprovalClient.SubmitOrderNote(ctx, &ordersv1.SubmitOrderNoteRequest{
		Context: req.GetContext(),
		Details: &ordersv1.SubmitOrderNoteRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		OrderNote: &ordersv1.OrderNote{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteApprovalService/SubmitOrderNote")
	}
	listPendingOrderNotesRequest := &ordersv1.ListPendingOrderNotesRequest{
		Context: req.GetContext(),
		Filter:  &ordersv1.ListPendingOrderNotesRequest_Filter{CreatedAfter: req.GetBackorder().GetAudit().GetUpdatedAt()},
	}
	type listPendingOrderNotesResult struct {
		reply *ordersv1.ListPendingOrderNotesResponse
		err   error
	}
	listPendingOrderNotesResults := make(chan listPendingOrderNotesResult, 1)
	go func() {
		reply, err := s.orderNoteApprovalClient.ListPendingOrderNotes(ctx, listPendingOrderNotesRequest)
		listPendingOrderNotesResults <- listPendingOrderNotesResult{reply, err}
	}()
	var listPendingOrderNotesReply *ordersv1.ListPendingOrderNotesResponse
	select {
	case result := <-listPendingOrderNotesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderNoteApprovalService/ListPendingOrderNotes")
		}
		listPendingOrderNotesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	_ = listPendingOrderNotesReply
	return &ordersv1.PauseBackorderWorkflowResponse{
		Outcome: &ordersv1.PauseBackorderWorkflowResponse_Outcome{
			ReferenceId: submitOrderNoteReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: submitOrderNoteReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Backorder: &ordersv1.Backorder{
			BackorderId: req.GetBackorderId(),
			Status:      req.GetBackorder().GetStatus(),
		},
	}, nil
}

// ResumeBackorderWorkflow resumes a backorder.
func (s *backorderWorkflowHandlers) ResumeBackorderWorkflow(ctx context.Context, req *ordersv1.ResumeBackorderWorkflowRequest) (*ordersv1.ResumeBackorderWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "resume backorder workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	listOrderNoteConflictsReply, err := s.orderNoteSyncClient.ListOrderNoteConflicts(ctx, &ordersv1.ListOrderNoteConflictsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.ListOrderNoteConflictsRequest_Filter{
			CreatedAfter: req.GetBackorder().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetBackorder().GetCustomer().GetCustomerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteSyncService/ListOrderNoteConflicts")
	}
	var email string
	switch choice := req.GetBackorder().GetOrigin().(type) {
	case *ordersv1.Backorder_Customer:
		email = choice.Customer.GetEmail()
	case *ordersv1.Backorder_ExternalReference:
		email = choice.ExternalReference
	case *ordersv1.Backorder_Channel:
		email = choice.Channel.GetChannel()
	}
	validateSearchQueryRequest := &searchv1.ValidateSearchQueryRequest{
		Context: req.GetContext(),
		Details: &searchv1.ValidateSearchQueryRequest_Details{Contact: &commonv1.ContactInfo{Email: email}},
	}
	validateSearchQueryReply, err := s.meteredSearchQueryValidation.ValidateSearchQuery(ctx, validateSearchQueryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ValidateSearchQuery")
	}
	explainPreferenceDecisionRequest := &customersv1.ExplainPreferenceDecisionRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetBackorder().GetAssignee().GetDisplayName(),
			},
		},
	}
	explainPreferenceDecisionReply, err := grpcx.NewClient(s.conn, customersv1.NewPreferenceValidationServiceClient).ExplainPreferenceDecision(ctx, explainPreferenceDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ExplainPreferenceDecision")
	}
	browseHouseholdsRequest := &customersv1.BrowseHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.BrowseHouseholdsRequest_Filter{
			CreatedAfter: explainPreferenceDecisionReply.GetPreference().GetAudit().GetCreatedAt(),
			Labels:       req.GetDetails().GetParameters(),
			OwnerId:      req.GetBackorder().GetCustomer().GetCustomerId(),
		},
	}
	browseHouseholdsReply := new(customersv1.BrowseHouseholdsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersHouseholdQueryServiceName, "BrowseHouseholds"), browseHouseholdsRequest, browseHouseholdsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/BrowseHouseholds")
	}
	return &ordersv1.ResumeBackorderWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:       listOrderNoteConflictsReply.GetResult().GetStatus(),
			ReferenceIds: validateSearchQueryReply.GetResult().GetReferenceIds(),
			Details:      browseHouseholdsReply.GetResult().GetDetails(),
		},
		Backorder: &ordersv1.Backorder{
			BackorderId: req.GetBackorderId(),
			Assignee:    req.GetBackorder().GetAssignee(),
		},
	}, nil
}

// InspectBackorderWorkflow returns a backorder.
func (s *backorderWorkflowHandlers) InspectBackorderWorkflow(ctx context.Context, req *ordersv1.InspectBackorderWorkflowRequest) (*ordersv1.InspectBackorderWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "inspect_backorder_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect backorder workflow", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := backorderWorkflowHandlersDelegateOrderNoteApprovalSteps[mode]
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	delegateOrderNoteApprovalRequest := &ordersv1.DelegateOrderNoteApprovalRequest{
		Context: req.GetContext(),
		Details: &ordersv1.DelegateOrderNoteApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: (&decoded).GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
		OrderNote: &ordersv1.OrderNote{
			Measurements: []*ordersv1.OrderNoteMeasurement{&ordersv1.OrderNoteMeasurement{ObservedAt: req.GetContext().GetDeadline()}},
			Detail:       packed,
		},
	}
	delegateOrderNoteApprovalReply, err := run(s, ctx, delegateOrderNoteApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteApprovalService/DelegateOrderNoteApproval")
	}
	listFraudSignalWorkflowsRequest := buildListFraudSignalWorkflowsRequestForInspectBackorderWorkflow(req, delegateOrderNoteApprovalReply)
	listFraudSignalWorkflowsReply, err := s.fraudSignalWorkflowClient.ListFraudSignalWorkflows(ctx, listFraudSignalWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalWorkflowService/ListFraudSignalWorkflows")
	}
	return &ordersv1.InspectBackorderWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details:      delegateOrderNoteApprovalReply.GetResult().GetDetails(),
			Message:      delegateOrderNoteApprovalReply.GetOutcome().GetMessage(),
			ReferenceIds: listFraudSignalWorkflowsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Backorder: &ordersv1.Backorder{BackorderId: req.GetBackorderId()},
	}, nil
}

// SignalBackorderWorkflow signals a backorder.
func (s *backorderWorkflowHandlers) SignalBackorderWorkflow(ctx context.Context, req *ordersv1.SignalBackorderWorkflowRequest) (*ordersv1.SignalBackorderWorkflowResponse, error) {
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "signal backorder workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetBackorder()).(*ordersv1.Backorder)
	activateBackorderReply, err := s.backorderLifecycleClient.ActivateBackorder(ctx, &ordersv1.ActivateBackorderRequest{
		Context:     req.GetContext(),
		BackorderId: req.GetBackorder().GetBackorderId(),
		Details: &ordersv1.ActivateBackorderRequest_Details{
			NewOwner: &identityv1.UserRef{UserId: cloned.GetAssignee().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderLifecycleService/ActivateBackorder")
	}
	packed, err := anypb.New(req.GetBackorder())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainSearchQueryDecisionRequest := &searchv1.ExplainSearchQueryDecisionRequest{
		Context: req.GetContext(),
		Details: &searchv1.ExplainSearchQueryDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             activateBackorderReply.GetBackorder().GetRequester().GetName(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: activateBackorderReply.GetBackorder().GetCustomer().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	explainSearchQueryDecisionReply, err := s.searchQueryValidationPort.ExplainSearchQueryDecision(ctx, explainSearchQueryDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryValidationService/ExplainSearchQueryDecision")
	}
	return &ordersv1.SignalBackorderWorkflowResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: activateBackorderReply.GetBackorder().GetAudit().GetUpdatedBy(),
			},
			Assignee: &identityv1.UserRef{
				Email: activateBackorderReply.GetBackorder().GetAssignee().GetEmail(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{
			Status: explainSearchQueryDecisionReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// backorderWorkflowHandlersReconcileSellerScorecardsSteps are the ways to run
// acme.sellers.v1.SellerScorecardSyncService/ReconcileSellerScorecards, by name.
var backorderWorkflowHandlersReconcileSellerScorecardsSteps = map[string]func(*backorderWorkflowHandlers, context.Context, *sellersv1.ReconcileSellerScorecardsRequest) (*sellersv1.ReconcileSellerScorecardsResponse, error){
	"default": (*backorderWorkflowHandlers).reconcileSellerScorecardsStep,
	"audited": (*backorderWorkflowHandlers).reconcileSellerScorecardsAuditedStep,
}

func (s *backorderWorkflowHandlers) reconcileSellerScorecardsStep(ctx context.Context, request *sellersv1.ReconcileSellerScorecardsRequest) (*sellersv1.ReconcileSellerScorecardsResponse, error) {
	return s.sellerScorecardSyncClient.ReconcileSellerScorecards(ctx, request)
}

func (s *backorderWorkflowHandlers) reconcileSellerScorecardsAuditedStep(ctx context.Context, request *sellersv1.ReconcileSellerScorecardsRequest) (*sellersv1.ReconcileSellerScorecardsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.sellers.v1.SellerScorecardSyncService/ReconcileSellerScorecards")
	return s.sellerScorecardSyncClient.ReconcileSellerScorecards(ctx, request)
}

// backorderWorkflowHandlersDelegateOrderNoteApprovalSteps are the ways to run
// acme.orders.v1.OrderNoteApprovalService/DelegateOrderNoteApproval, by name.
var backorderWorkflowHandlersDelegateOrderNoteApprovalSteps = map[string]func(*backorderWorkflowHandlers, context.Context, *ordersv1.DelegateOrderNoteApprovalRequest) (*ordersv1.DelegateOrderNoteApprovalResponse, error){
	"default": (*backorderWorkflowHandlers).delegateOrderNoteApprovalStep,
	"audited": (*backorderWorkflowHandlers).delegateOrderNoteApprovalAuditedStep,
}

func (s *backorderWorkflowHandlers) delegateOrderNoteApprovalStep(ctx context.Context, request *ordersv1.DelegateOrderNoteApprovalRequest) (*ordersv1.DelegateOrderNoteApprovalResponse, error) {
	return s.orderNoteApprovalClient.DelegateOrderNoteApproval(ctx, request)
}

func (s *backorderWorkflowHandlers) delegateOrderNoteApprovalAuditedStep(ctx context.Context, request *ordersv1.DelegateOrderNoteApprovalRequest) (*ordersv1.DelegateOrderNoteApprovalResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.orders.v1.OrderNoteApprovalService/DelegateOrderNoteApproval")
	return s.orderNoteApprovalClient.DelegateOrderNoteApproval(ctx, request)
}
