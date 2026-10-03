package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	bankingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/banking/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
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

// PaymentWorkflowServer implements `acme.payments.v1.PaymentWorkflowService`.
// Runs the long-lived workflow that takes a payment from start to finish.
type PaymentWorkflowServer struct {
	*paymentWorkflowHandlers
}

// paymentWorkflowHandlers holds the handlers of PaymentWorkflowServer, with what they use.
type paymentWorkflowHandlers struct {
	paymentsv1.UnimplementedPaymentWorkflowServiceServer
	store                         *Store
	log                           *slog.Logger
	payoutValidationClient        paymentsv1.PayoutValidationServiceClient
	refundReportingClient         paymentsv1.RefundReportingServiceClient
	conn                          *grpc.ClientConn
	auditEventQueryOnce           sync.Once
	auditEventQueryCached         auditv1.AuditEventQueryServiceClient
	clients                       *registry.Registry
	payoutReportingClient         paymentsv1.PayoutReportingServiceClient
	paymentApprovalClient         paymentsv1.PaymentApprovalServiceClient
	disputeSyncClient             paymentsv1.DisputeSyncServiceClient
	searchQueryNotificationClient searchv1.SearchQueryNotificationServiceClient
	userQueryClient               identityv1.UserQueryServiceClient
	bankAccountQueryClient        bankingv1.BankAccountQueryServiceClient
	stockMovementApprovalClient   inventoryv1.StockMovementApprovalServiceClient
	pointsLedgerApprovalClient    loyaltyv1.PointsLedgerApprovalServiceClient
	apiCredentialValidationClient identityv1.ApiCredentialValidationServiceClient
	householdValidationClient     customersv1.HouseholdValidationServiceClient
}

// NewPaymentWorkflowServer wires a PaymentWorkflowServer.
func NewPaymentWorkflowServer(deps platform.Deps) *PaymentWorkflowServer {
	return &PaymentWorkflowServer{paymentWorkflowHandlers: &paymentWorkflowHandlers{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.payments.v1.PaymentWorkflowService"),
		payoutValidationClient:        paymentsv1.NewPayoutValidationServiceClient(deps.Conn),
		refundReportingClient:         paymentsv1.NewRefundReportingServiceClient(deps.Conn),
		conn:                          deps.Conn,
		clients:                       deps.Clients,
		payoutReportingClient:         paymentsv1.NewPayoutReportingServiceClient(deps.Conn),
		paymentApprovalClient:         paymentsv1.NewPaymentApprovalServiceClient(deps.Conn),
		disputeSyncClient:             paymentsv1.NewDisputeSyncServiceClient(deps.Conn),
		searchQueryNotificationClient: searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		userQueryClient:               identityv1.NewUserQueryServiceClient(deps.Conn),
		bankAccountQueryClient:        bankingv1.NewBankAccountQueryServiceClient(deps.Conn),
		stockMovementApprovalClient:   inventoryv1.NewStockMovementApprovalServiceClient(deps.Conn),
		pointsLedgerApprovalClient:    loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn),
		apiCredentialValidationClient: identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		householdValidationClient:     customersv1.NewHouseholdValidationServiceClient(deps.Conn),
	}}
}

// StartPaymentWorkflow starts a payment.
func (s *paymentWorkflowHandlers) StartPaymentWorkflow(ctx context.Context, req *paymentsv1.StartPaymentWorkflowRequest) (*paymentsv1.StartPaymentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StartPaymentWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "start payment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	for _, attachment := range req.GetPayment().GetAttachments() {
		if _, err := s.payoutValidationClient.ScorePayout(ctx, &paymentsv1.ScorePayoutRequest{
			Context: req.GetContext(),
			Payout: &paymentsv1.Payout{
				Attachments: []*paymentsv1.PayoutAttachment{&paymentsv1.PayoutAttachment{SizeBytes: attachment.GetSizeBytes()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutValidationService/ScorePayout")
		}
	}
	return &paymentsv1.StartPaymentWorkflowResponse{
		Payment: &paymentsv1.Payment{
			PaymentId:    req.GetPaymentId(),
			Counterparty: req.GetPayment().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvancePaymentWorkflow advances a payment.
func (s *paymentWorkflowHandlers) AdvancePaymentWorkflow(ctx context.Context, req *paymentsv1.AdvancePaymentWorkflowRequest) (*paymentsv1.AdvancePaymentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "advance payment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	streamRefundMetricsStream, err := s.refundReportingClient.StreamRefundMetrics(ctx, &paymentsv1.StreamRefundMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetDetails().GetNotify().GetEmail(),
			Name:  req.GetDetails().GetNotify().GetName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/StreamRefundMetrics")
	}
	for {
		received, err := streamRefundMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/StreamRefundMetrics")
		}
		_ = received
	}
	findAuditEventsRequest := &auditv1.FindAuditEventsRequest{
		Context: req.GetContext(),
		Filter:  &auditv1.FindAuditEventsRequest_Filter{Region: req.GetPayment().GetBillingAddress().GetRegion()},
	}
	findAuditEventsReply, err := s.auditEventQueryLazy().FindAuditEvents(ctx, findAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/FindAuditEvents")
	}
	return &paymentsv1.AdvancePaymentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:      findAuditEventsReply.GetResult().GetMessage(),
			ReferenceIds: findAuditEventsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Payment: &paymentsv1.Payment{
			PaymentId:    req.GetPaymentId(),
			Counterparty: &paymentsv1.RefundRef{Number: req.GetPayment().GetNumber()},
		},
	}, nil
}

// CancelPaymentWorkflow cancels a payment.
func (s *paymentWorkflowHandlers) CancelPaymentWorkflow(ctx context.Context, req *paymentsv1.CancelPaymentWorkflowRequest) (*paymentsv1.CancelPaymentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelPaymentWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "cancel payment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := paymentWorkflowHandlersFetchRefundReportSteps[mode]
	fetchRefundReportRequest := &paymentsv1.FetchRefundReportRequest{
		Context:  req.GetContext(),
		RefundId: req.GetPayment().GetCounterparty().GetRefundId(),
	}
	fetchRefundReportReply, err := run(s, ctx, fetchRefundReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/FetchRefundReport")
	}
	customer, err := registry.Lookup[customersv1.CustomerServiceClient](s.clients, "acme.customers.v1.CustomerService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloned := proto.Clone(req.GetPayment()).(*paymentsv1.Payment)
	listCustomersRequest := &customersv1.ListCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListCustomersRequest_Filter{
			Region:       cloned.GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPayment().GetAudit().GetUpdatedAt(),
			Labels:       req.GetPayment().GetMetadata(),
			OwnerId:      fetchRefundReportReply.GetRefund().GetCounterparty().GetCustomerId(),
		},
	}
	listCustomersReply, err := customer.ListCustomers(ctx, listCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/ListCustomers")
	}
	return &paymentsv1.CancelPaymentWorkflowResponse{
		Payment: &paymentsv1.Payment{
			Notes: fetchRefundReportReply.GetRefund().GetNotes(),
			BillingAddress: &commonv1.PostalAddress{
				Recipient: fetchRefundReportReply.GetRefund().GetBillingAddress().GetRecipient(),
			},
			PaymentId:    req.GetPaymentId(),
			Counterparty: req.GetPayment().GetCounterparty(),
		},
		Outcome: &paymentsv1.CancelPaymentWorkflowResponse_Outcome{Message: listCustomersReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: listCustomersReply.GetResult().GetStatus()},
	}, nil
}

// RetryPaymentWorkflow retrys a payment.
func (s *paymentWorkflowHandlers) RetryPaymentWorkflow(ctx context.Context, req *paymentsv1.RetryPaymentWorkflowRequest) (*paymentsv1.RetryPaymentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "retry payment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	schedulePayoutExpiryRequest := &paymentsv1.SchedulePayoutExpiryRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.SchedulePayoutExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
		Payout: &paymentsv1.Payout{
			Settlement: &paymentsv1.Payout_Card{Card: &paymentsv1.PayoutCardSettlement{Network: req.GetPayment().GetCard().GetNetwork()}},
			Audit:      &commonv1.AuditInfo{CreatedAt: req.GetPayment().GetAudit().GetCreatedAt()},
		},
	}
	schedulePayoutExpiryReply, err := grpcx.NewClient(s.conn, paymentsv1.NewPayoutLifecycleServiceClient).SchedulePayoutExpiry(ctx, schedulePayoutExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutLifecycleService/SchedulePayoutExpiry")
	}
	return &paymentsv1.RetryPaymentWorkflowResponse{
		Payment: &paymentsv1.Payment{
			Counterparty: schedulePayoutExpiryReply.GetPayout().GetCounterparty(),
			PaymentId:    req.GetPaymentId(),
			Notes:        req.GetPayment().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PausePaymentWorkflow pauses a payment.
func (s *paymentWorkflowHandlers) PausePaymentWorkflow(ctx context.Context, req *paymentsv1.PausePaymentWorkflowRequest) (*paymentsv1.PausePaymentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "pause payment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	sharePayoutReportRequest := &paymentsv1.SharePayoutReportRequest{}
	sharePayoutReportRequest.Context = req.GetContext()
	sharePayoutReportRequest.Details = &paymentsv1.SharePayoutReportRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Name:             req.GetDetails().GetNotify().GetName(),
			PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
		},
	}
	sharePayoutReportReply, err := s.payoutReportingClient.SharePayoutReport(ctx, sharePayoutReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutReportingService/SharePayoutReport")
	}
	getPaymentApprovalStatusRequest := &paymentsv1.GetPaymentApprovalStatusRequest{
		Context:   req.GetContext(),
		PaymentId: req.GetPayment().GetPaymentId(),
	}
	getPaymentApprovalStatusReply, err := grpcx.Call(ctx, s.paymentApprovalClient.GetPaymentApprovalStatus, getPaymentApprovalStatusRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentApprovalService/GetPaymentApprovalStatus")
	}
	_ = getPaymentApprovalStatusReply
	return &paymentsv1.PausePaymentWorkflowResponse{
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: sharePayoutReportReply.GetPayout().GetAudit().GetUpdatedAt(),
			},
			PaymentId: req.GetPaymentId(),
			Metadata:  req.GetPayment().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: sharePayoutReportReply.GetOutcome().GetStatus()},
	}, nil
}

// ResumePaymentWorkflow resumes a payment.
func (s *paymentWorkflowHandlers) ResumePaymentWorkflow(ctx context.Context, req *paymentsv1.ResumePaymentWorkflowRequest) (*paymentsv1.ResumePaymentWorkflowResponse, error) {
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "resume payment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetPayment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	step := &paymentWorkflowHandlersResumePaymentWorkflowResolveDisputeConflictStep{request: &paymentsv1.ResolveDisputeConflictRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Requester: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotify().GetName(),
				Email: req.GetDetails().GetNotify().GetEmail(),
			},
			Origin: &paymentsv1.Dispute_Customer{Customer: &paymentsv1.PaymentRef{
				Number:      req.GetPayment().GetCounterparty().GetNumber(),
				DisplayName: req.GetPayment().GetCounterparty().GetDisplayName(),
			}},
		},
		Details: &paymentsv1.ResolveDisputeConflictRequest_Details{Payload: snapshot},
	}}
	if err := s.runResumePaymentWorkflowResolveDisputeConflict(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/ResolveDisputeConflict")
	}
	resolveDisputeConflictReply := step.reply
	subscribeSearchQueryUpdatesRequest := newSubscribeSearchQueryUpdatesRequest(withSubscribeSearchQueryUpdatesContext(req.GetContext()), withSubscribeSearchQueryUpdatesDetails(&searchv1.SubscribeSearchQueryUpdatesRequest_Details{
		Recipient: &commonv1.ContactInfo{
			PreferredChannel: resolveDisputeConflictReply.GetDispute().GetRequester().GetPreferredChannel(),
			Phone:            req.GetDetails().GetNotify().GetPhone(),
			Name:             resolveDisputeConflictReply.GetDispute().GetRequester().GetName(),
			Email:            req.GetDetails().GetNotify().GetEmail(),
		},
	}))
	subscribeSearchQueryUpdatesReply, err := s.searchQueryNotificationClient.SubscribeSearchQueryUpdates(ctx, subscribeSearchQueryUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/SubscribeSearchQueryUpdates")
	}
	snapshot2, err := protojson.Marshal(req.GetPayment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileDisputesRequest := &paymentsv1.ReconcileDisputesRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Sla: &paymentsv1.DisputeSla{
				PausedFor: resolveDisputeConflictReply.GetDispute().GetSla().GetPausedFor(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: resolveDisputeConflictReply.GetDispute().GetAssignee().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetPayment().GetAudit().GetCreatedAt()},
		},
		Details: &paymentsv1.ReconcileDisputesRequest_Details{Payload: snapshot2},
	}
	reconcileDisputesReply := new(paymentsv1.ReconcileDisputesResponse)
	if err := s.conn.Invoke(ctx, paymentsDisputeSyncServiceReconcileDisputesMethod, reconcileDisputesRequest, reconcileDisputesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/ReconcileDisputes")
	}
	source := req.GetDetails().GetNotify().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:  reconcileDisputesReply.GetDispute().GetRequester().GetEmail(),
				UserId: resolveDisputeConflictReply.GetDispute().GetAssignee().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: resolveDisputeConflictReply.GetDispute().GetAudit().GetEtag(),
			},
		},
		Details: &customersv1.LintCustomerSegmentsRequest_Details{Contact: &commonv1.ContactInfo{Email: email}},
	}
	lintCustomerSegmentsReply := new(customersv1.LintCustomerSegmentsResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.CustomerSegmentValidationService_LintCustomerSegments_FullMethodName, lintCustomerSegmentsRequest, lintCustomerSegmentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &paymentsv1.ResumePaymentWorkflowResponse{
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: resolveDisputeConflictReply.GetDispute().GetAudit().GetUpdatedBy(),
			},
			PaymentId: reconcileDisputesReply.GetDispute().GetCustomer().GetPaymentId(),
			Number:    req.GetPayment().GetNumber(),
			DueAt:     req.GetPayment().GetDueAt(),
		},
		Outcome: &paymentsv1.ResumePaymentWorkflowResponse_Outcome{
			Status:    subscribeSearchQueryUpdatesReply.GetResult().GetStatus(),
			NextSteps: reconcileDisputesReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{Status: lintCustomerSegmentsReply.GetResult().GetStatus()},
	}, nil
}

// InspectPaymentWorkflow returns a payment.
func (s *paymentWorkflowHandlers) InspectPaymentWorkflow(ctx context.Context, req *paymentsv1.InspectPaymentWorkflowRequest) (*paymentsv1.InspectPaymentWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "inspect_payment_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect payment workflow", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	resumeReviewVoteWorkflowRequest := &reviewsv1.ResumeReviewVoteWorkflowRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject:       &customersv1.CustomerRef{Email: tenancy.Value(ctx, "email")},
			CorrelationId: req.GetPaymentId(),
		},
	}
	resumeReviewVoteWorkflowReply := new(reviewsv1.ResumeReviewVoteWorkflowResponse)
	if err := s.conn.Invoke(ctx, reviewsReviewVoteWorkflowServiceResumeReviewVoteWorkflowMethod, resumeReviewVoteWorkflowRequest, resumeReviewVoteWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteWorkflowService/ResumeReviewVoteWorkflow")
	}
	return &paymentsv1.InspectPaymentWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: resumeReviewVoteWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Payment: &paymentsv1.Payment{PaymentId: req.GetPaymentId()},
	}, nil
}

// ListPaymentWorkflows lists payments a page at a time.
func (s *paymentWorkflowHandlers) ListPaymentWorkflows(ctx context.Context, req *paymentsv1.ListPaymentWorkflowsRequest) (*paymentsv1.ListPaymentWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list payment workflows", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	browseUsersRequest := &identityv1.BrowseUsersRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageSize: req.GetPage().GetPageSize()},
		Filter: &identityv1.BrowseUsersRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Query:        req.GetFilter().GetQuery(),
			Labels:       labels,
		},
	}
	browseUsersReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*identityv1.BrowseUsersResponse, error) {
		return s.userQueryClient.BrowseUsers(ctx, browseUsersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/BrowseUsers")
	}
	encoded, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.ListPaymentWorkflowsRequest_Filter
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	ctx = tenancy.With(ctx, "owner_id", req.GetFilter().GetOwnerId())
	suggestBankAccountsRequest := newSuggestBankAccountsRequest(withSuggestBankAccountsContext(req.GetContext()), withSuggestBankAccountsFilter(&bankingv1.SuggestBankAccountsRequest_Filter{
		Query:   (&decoded).GetQuery(),
		Region:  req.GetFilter().GetRegion(),
		OwnerId: tenancy.Value(ctx, "owner_id"),
	}), withSuggestBankAccountsPage(&commonv1.PageRequest{PageSize: req.GetPage().GetPageSize()}))
	suggestBankAccountsReply, err := s.bankAccountQueryClient.SuggestBankAccounts(ctx, suggestBankAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.banking.v1.BankAccountQueryService/SuggestBankAccounts")
	}
	return &paymentsv1.ListPaymentWorkflowsResponse{
		PageInfo: &commonv1.PageInfo{TotalSize: browseUsersReply.GetPageInfo().GetTotalSize()},
		Result: &commonv1.OperationResult{
			Details:       suggestBankAccountsReply.GetResult().GetDetails(),
			CorrelationId: suggestBankAccountsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SignalPaymentWorkflow signals a payment.
func (s *paymentWorkflowHandlers) SignalPaymentWorkflow(ctx context.Context, req *paymentsv1.SignalPaymentWorkflowRequest) (*paymentsv1.SignalPaymentWorkflowResponse, error) {
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "signal payment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPayment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	commentStockMovementApprovalReply, err := s.stockMovementApprovalClient.CommentStockMovementApproval(ctx, &inventoryv1.CommentStockMovementApprovalRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.CommentStockMovementApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		StockMovement: &inventoryv1.StockMovement{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementApprovalService/CommentStockMovementApproval")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*loyaltyv1.RejectPointsLedgerResponse, len(req.GetPayment().GetLines()))
	for i, line := range req.GetPayment().GetLines() {
		group.Go(func() error {
			reply, err := s.pointsLedgerApprovalClient.RejectPointsLedger(groupCtx, &loyaltyv1.RejectPointsLedgerRequest{
				Context: req.GetContext(),
				PointsLedger: &loyaltyv1.PointsLedger{
					Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{Amount: line.GetAmount()}},
					Audit: &commonv1.AuditInfo{
						CreatedAt: req.GetPayment().GetAudit().GetCreatedAt(),
						UpdatedBy: req.GetPayment().GetAudit().GetUpdatedBy(),
					},
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
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/RejectPointsLedger")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	for _, line2 := range req.GetPayment().GetLines() {
		if _, err := s.apiCredentialValidationClient.LintApiCredentials(ctx, &identityv1.LintApiCredentialsRequest{
			Context: req.GetContext(),
			ApiCredential: &identityv1.ApiCredential{
				Limits: []*identityv1.ApiCredentialLimit{&identityv1.ApiCredentialLimit{Amount: line2.GetTax().GetAmount()}},
			},
			Details: &identityv1.LintApiCredentialsRequest_Details{
				Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/LintApiCredentials")
		}
	}
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.SignalPaymentWorkflowRequest_Details)
	checkHouseholdEligibilityReply, err := s.householdValidationClient.CheckHouseholdEligibility(ctx, &customersv1.CheckHouseholdEligibilityRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						PostalCode: req.GetPayment().GetBillingAddress().GetPostalCode(),
					},
					Manager: &identityv1.UserRef{Email: cloned.GetNotify().GetEmail()},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility")
	}
	return &paymentsv1.SignalPaymentWorkflowResponse{
		Payment: &paymentsv1.Payment{
			Lines: []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{
				Attributes: commentStockMovementApprovalReply.GetStockMovement().GetAttributes(),
			}},
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: checkHouseholdEligibilityReply.GetHousehold().GetManager().GetDisplayName(),
			},
			PaymentId: req.GetPaymentId(),
			Notes:     req.GetPayment().GetNotes(),
		},
		Result: &commonv1.OperationResult{
			Status: commentStockMovementApprovalReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// auditEventQueryLazy dials acme.audit.v1.AuditEventQueryService the first time a handler needs it.
func (s *paymentWorkflowHandlers) auditEventQueryLazy() auditv1.AuditEventQueryServiceClient {
	s.auditEventQueryOnce.Do(func() { s.auditEventQueryCached = auditv1.NewAuditEventQueryServiceClient(s.conn) })
	return s.auditEventQueryCached
}

// paymentWorkflowHandlersFetchRefundReportSteps are the ways to run
// acme.payments.v1.RefundReportingService/FetchRefundReport, by name.
var paymentWorkflowHandlersFetchRefundReportSteps = map[string]func(*paymentWorkflowHandlers, context.Context, *paymentsv1.FetchRefundReportRequest) (*paymentsv1.FetchRefundReportResponse, error){
	"default": (*paymentWorkflowHandlers).fetchRefundReportStep,
	"audited": (*paymentWorkflowHandlers).fetchRefundReportAuditedStep,
}

func (s *paymentWorkflowHandlers) fetchRefundReportStep(ctx context.Context, request *paymentsv1.FetchRefundReportRequest) (*paymentsv1.FetchRefundReportResponse, error) {
	return s.refundReportingClient.FetchRefundReport(ctx, request)
}

func (s *paymentWorkflowHandlers) fetchRefundReportAuditedStep(ctx context.Context, request *paymentsv1.FetchRefundReportRequest) (*paymentsv1.FetchRefundReportResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.payments.v1.RefundReportingService/FetchRefundReport")
	return s.refundReportingClient.FetchRefundReport(ctx, request)
}

// paymentWorkflowHandlersResumePaymentWorkflowResolveDisputeConflictStep carries the request of one step of
// ResumePaymentWorkflow and, once it ran, its reply.
type paymentWorkflowHandlersResumePaymentWorkflowResolveDisputeConflictStep struct {
	request *paymentsv1.ResolveDisputeConflictRequest
	reply   *paymentsv1.ResolveDisputeConflictResponse
}

func (s *paymentWorkflowHandlers) runResumePaymentWorkflowResolveDisputeConflict(ctx context.Context, step *paymentWorkflowHandlersResumePaymentWorkflowResolveDisputeConflictStep) error {
	reply, err := s.disputeSyncClient.ResolveDisputeConflict(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
