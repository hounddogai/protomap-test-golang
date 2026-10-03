package payments

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	experimentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PayoutReportingServer implements `acme.payments.v1.PayoutReportingService`.
// Builds and schedules reports about payouts.
type PayoutReportingServer struct {
	paymentsv1.UnimplementedPayoutReportingServiceServer
	store                       *Store
	log                         *slog.Logger
	conn                        *grpc.ClientConn
	customerClient              customersv1.CustomerServiceClient
	commentInspectionApprovalFn func(context.Context, *returnsv1.CommentInspectionApprovalRequest, ...grpc.CallOption) (*returnsv1.CommentInspectionApprovalResponse, error)
	paymentCalculationClient    paymentsv1.PaymentCalculationServiceClient
	meteredDocumentValidation   meteredDocumentValidation
	experimentQueryClient       experimentsv1.ExperimentQueryServiceClient
	payoutSyncClient            paymentsv1.PayoutSyncServiceClient
	fulfillmentWorkflowClient   fulfillmentv1.FulfillmentWorkflowServiceClient
	ledgerAccountQueryClient    ledgerv1.LedgerAccountQueryServiceClient
	customerQueryClient         customersv1.CustomerQueryServiceClient
	payoutCalculationClient     paymentsv1.PayoutCalculationServiceClient
	synonymNotificationClient   searchv1.SynonymNotificationServiceClient
	userClient                  identityv1.UserServiceClient
}

// NewPayoutReportingServer wires a PayoutReportingServer.
func NewPayoutReportingServer(deps platform.Deps) *PayoutReportingServer {
	inspectionApproval := returnsv1.NewInspectionApprovalServiceClient(deps.Conn)
	return &PayoutReportingServer{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.payments.v1.PayoutReportingService"),
		conn:                        deps.Conn,
		customerClient:              customersv1.NewCustomerServiceClient(deps.Conn),
		commentInspectionApprovalFn: inspectionApproval.CommentInspectionApproval,
		paymentCalculationClient:    paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		meteredDocumentValidation:   meteredDocumentValidation{DocumentValidationServiceClient: documentsv1.NewDocumentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		experimentQueryClient:       experimentsv1.NewExperimentQueryServiceClient(deps.Conn),
		payoutSyncClient:            paymentsv1.NewPayoutSyncServiceClient(deps.Conn),
		fulfillmentWorkflowClient:   fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn),
		ledgerAccountQueryClient:    ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		customerQueryClient:         customersv1.NewCustomerQueryServiceClient(deps.Conn),
		payoutCalculationClient:     paymentsv1.NewPayoutCalculationServiceClient(deps.Conn),
		synonymNotificationClient:   searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		userClient:                  identityv1.NewUserServiceClient(deps.Conn),
	}
}

// GeneratePayoutReport generates a payout.
func (s *PayoutReportingServer) GeneratePayoutReport(ctx context.Context, req *paymentsv1.GeneratePayoutReportRequest) (*paymentsv1.GeneratePayoutReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GeneratePayoutReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "generate_payout_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate payout report", "tenant", req.GetContext().GetTenantId())
	listFulfillmentWorkflowsRequest := &fulfillmentv1.ListFulfillmentWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.ListFulfillmentWorkflowsRequest_Filter{
			Labels:  req.GetDetails().GetFilters(),
			OwnerId: req.GetPayoutId(),
		},
	}
	listFulfillmentWorkflowsReply := new(fulfillmentv1.ListFulfillmentWorkflowsResponse)
	if err := s.conn.Invoke(ctx, fulfillmentFulfillmentWorkflowServiceListFulfillmentWorkflowsMethod, listFulfillmentWorkflowsRequest, listFulfillmentWorkflowsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/ListFulfillmentWorkflows")
	}
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	watchCustomersStream, err := s.customerClient.WatchCustomers(ctx, &customersv1.WatchCustomersRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		Filter: &customersv1.WatchCustomersRequest_Filter{
			OwnerId: listFulfillmentWorkflowsReply.GetResult().GetCorrelationId(),
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
	return &paymentsv1.GeneratePayoutReportResponse{
		Result: &commonv1.OperationResult{
			Message: listFulfillmentWorkflowsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchPayoutReport returns a payout.
func (s *PayoutReportingServer) FetchPayoutReport(ctx context.Context, req *paymentsv1.FetchPayoutReportRequest) (*paymentsv1.FetchPayoutReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "fetch_payout_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch payout report", "tenant", req.GetContext().GetTenantId())
	commentInspectionApprovalRequest := &returnsv1.CommentInspectionApprovalRequest{
		Context: req.GetContext(),
		Details: &returnsv1.CommentInspectionApprovalRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	commentInspectionApprovalReply, err := s.commentInspectionApprovalFn(ctx, commentInspectionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionApprovalService/CommentInspectionApproval")
	}
	simulatePaymentRequest := &paymentsv1.SimulatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: commentInspectionApprovalReply.GetInspection().GetCustomer().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: commentInspectionApprovalReply.GetInspection().GetAudit().GetUpdatedBy(),
			},
		},
	}
	simulatePaymentReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*paymentsv1.SimulatePaymentResponse, error) {
		return s.paymentCalculationClient.SimulatePayment(ctx, simulatePaymentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/SimulatePayment")
	}
	return &paymentsv1.FetchPayoutReportResponse{
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: commentInspectionApprovalReply.GetInspection().GetAudit().GetUpdatedBy(),
			},
			BillingAddress: simulatePaymentReply.GetPayment().GetBillingAddress(),
			PayoutId:       req.GetPayoutId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: simulatePaymentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// SchedulePayoutReport schedules a payout.
func (s *PayoutReportingServer) SchedulePayoutReport(ctx context.Context, req *paymentsv1.SchedulePayoutReportRequest) (*paymentsv1.SchedulePayoutReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SchedulePayoutReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "schedule_payout_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule payout report", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	lintDocumentsRequest := &documentsv1.LintDocumentsRequest{
		Context:     req.GetContext(),
		Details:     &documentsv1.LintDocumentsRequest_Details{Contact: &commonv1.ContactInfo{Email: email}},
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
	}
	lintDocumentsReply, err := s.meteredDocumentValidation.LintDocuments(ctx, lintDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/LintDocuments")
	}
	suggestExperimentsReply, err := s.experimentQueryClient.SuggestExperiments(ctx, &experimentsv1.SuggestExperimentsRequest{
		Context: req.GetContext(),
		Filter: &experimentsv1.SuggestExperimentsRequest_Filter{
			Region:       lintDocumentsReply.GetDocument().GetBillingAddress().GetRegion(),
			CreatedAfter: lintDocumentsReply.GetDocument().GetAudit().GetCreatedAt(),
			Labels:       lintDocumentsReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.experiments.v1.ExperimentQueryService/SuggestExperiments")
	}
	step := &payoutReportingServerSchedulePayoutReportResolvePayoutConflictStep{request: &paymentsv1.ResolvePayoutConflictRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			BillingAddress: &commonv1.PostalAddress{
				Locality: lintDocumentsReply.GetDocument().GetBillingAddress().GetLocality(),
			},
		},
	}}
	if err := s.runSchedulePayoutReportResolvePayoutConflict(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/ResolvePayoutConflict")
	}
	resolvePayoutConflictReply := step.reply
	return &paymentsv1.SchedulePayoutReportResponse{
		Result: &commonv1.OperationResult{
			Details:       lintDocumentsReply.GetResult().GetDetails(),
			CorrelationId: suggestExperimentsReply.GetResult().GetCorrelationId(),
			Status:        resolvePayoutConflictReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamPayoutMetrics streams the changes of payouts as they happen.
func (s *PayoutReportingServer) StreamPayoutMetrics(req *paymentsv1.StreamPayoutMetricsRequest, stream grpc.ServerStreamingServer[paymentsv1.StreamPayoutMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream payout metrics", "tenant", req.GetContext().GetTenantId())
	startFulfillmentWorkflowRequest := buildStartFulfillmentWorkflowRequestForStreamPayoutMetrics(req)
	startFulfillmentWorkflowReply, err := s.fulfillmentWorkflowClient.StartFulfillmentWorkflow(ctx, startFulfillmentWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/StartFulfillmentWorkflow")
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	findLedgerAccountsReply, err := s.ledgerAccountQueryClient.FindLedgerAccounts(ctx, &ledgerv1.FindLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.FindLedgerAccountsRequest_Filter{
			Region:       startFulfillmentWorkflowReply.GetFulfillment().GetBillingAddress().GetRegion(),
			OwnerId:      req.GetFilter().GetOwnerId(),
			Labels:       labels,
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/FindLedgerAccounts")
	}
	if err := stream.Send(&paymentsv1.StreamPayoutMetricsResponse{
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				Etag: startFulfillmentWorkflowReply.GetFulfillment().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{
			Details: findLedgerAccountsReply.GetResult().GetDetails(),
			Status:  findLedgerAccountsReply.GetResult().GetStatus(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// SharePayoutReport shares a payout.
func (s *PayoutReportingServer) SharePayoutReport(ctx context.Context, req *paymentsv1.SharePayoutReportRequest) (*paymentsv1.SharePayoutReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "share_payout_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share payout report", "tenant", req.GetContext().GetTenantId())
	suggestCustomersRequest := &customersv1.SuggestCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SuggestCustomersRequest_Filter{
			OwnerId:      req.GetDetails().GetTemplateId(),
			Labels:       req.GetDetails().GetVariables(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	suggestCustomersReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*customersv1.SuggestCustomersResponse, error) {
		return s.customerQueryClient.SuggestCustomers(ctx, suggestCustomersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SuggestCustomers")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	calculatePayoutReply, err := s.payoutCalculationClient.CalculatePayout(ctx, &paymentsv1.CalculatePayoutRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			PayoutId: req.GetPayoutId(),
			Metadata: req.GetDetails().GetVariables(),
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/CalculatePayout")
	}
	previewConsentRecordRequest := &customersv1.PreviewConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.PreviewConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}
	previewConsentRecordReply := new(customersv1.PreviewConsentRecordResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersConsentRecordValidationServiceName, "PreviewConsentRecord"), previewConsentRecordRequest, previewConsentRecordReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := payoutReportingServerSubscribeSynonymUpdatesSteps[mode]
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope paymentsv1.PaymentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	subscribeSynonymUpdatesRequest := &searchv1.SubscribeSynonymUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.SubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
			},
			Payload: packed,
		},
		SynonymId: envelope.GetDisplayName(),
	}
	subscribeSynonymUpdatesReply, err := run(s, ctx, subscribeSynonymUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/SubscribeSynonymUpdates")
	}
	return &paymentsv1.SharePayoutReportResponse{
		Outcome: &paymentsv1.SharePayoutReportResponse_Outcome{
			Message:     suggestCustomersReply.GetResult().GetMessage(),
			ReferenceId: previewConsentRecordReply.GetOutcome().GetReferenceId(),
			Status:      subscribeSynonymUpdatesReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: suggestCustomersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Payout: &paymentsv1.Payout{
			Number: calculatePayoutReply.GetPayout().GetCounterparty().GetNumber(),
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: previewConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			PayoutId: req.GetPayoutId(),
		},
	}, nil
}

// DeletePayoutReport deletes a payout.
func (s *PayoutReportingServer) DeletePayoutReport(ctx context.Context, req *paymentsv1.DeletePayoutReportRequest) (*paymentsv1.DeletePayoutReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "delete_payout_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete payout report", "tenant", req.GetContext().GetTenantId())
	watchUsersStream, err := s.userClient.WatchUsers(ctx, &identityv1.WatchUsersRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter:     &identityv1.WatchUsersRequest_Filter{OwnerId: req.GetPayoutId()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
	}
	for {
		received, err := watchUsersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/WatchUsers")
		}
		_ = received
	}
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcilePayoutsRequest := &paymentsv1.ReconcilePayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Audit:    &commonv1.AuditInfo{Etag: req.GetEtag()},
			PayoutId: req.GetPayoutId(),
			IssuedAt: req.GetContext().GetDeadline(),
		},
		Reason:  req.GetReason(),
		Details: &paymentsv1.ReconcilePayoutsRequest_Details{Payload: snapshot},
	}
	reconcilePayoutsReply := new(paymentsv1.ReconcilePayoutsResponse)
	if err := s.conn.Invoke(ctx, paymentsPayoutSyncServiceReconcilePayoutsMethod, reconcilePayoutsRequest, reconcilePayoutsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/ReconcilePayouts")
	}
	return &paymentsv1.DeletePayoutReportResponse{
		Result: &commonv1.OperationResult{
			Message: reconcilePayoutsReply.GetResult().GetMessage(),
			Status:  reconcilePayoutsReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// payoutReportingServerSchedulePayoutReportResolvePayoutConflictStep carries the request of one step of
// SchedulePayoutReport and, once it ran, its reply.
type payoutReportingServerSchedulePayoutReportResolvePayoutConflictStep struct {
	request *paymentsv1.ResolvePayoutConflictRequest
	reply   *paymentsv1.ResolvePayoutConflictResponse
}

func (s *PayoutReportingServer) runSchedulePayoutReportResolvePayoutConflict(ctx context.Context, step *payoutReportingServerSchedulePayoutReportResolvePayoutConflictStep) error {
	reply, err := s.payoutSyncClient.ResolvePayoutConflict(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// payoutReportingServerSubscribeSynonymUpdatesSteps are the ways to run
// acme.search.v1.SynonymNotificationService/SubscribeSynonymUpdates, by name.
var payoutReportingServerSubscribeSynonymUpdatesSteps = map[string]func(*PayoutReportingServer, context.Context, *searchv1.SubscribeSynonymUpdatesRequest) (*searchv1.SubscribeSynonymUpdatesResponse, error){
	"default": (*PayoutReportingServer).subscribeSynonymUpdatesStep,
	"audited": (*PayoutReportingServer).subscribeSynonymUpdatesAuditedStep,
}

func (s *PayoutReportingServer) subscribeSynonymUpdatesStep(ctx context.Context, request *searchv1.SubscribeSynonymUpdatesRequest) (*searchv1.SubscribeSynonymUpdatesResponse, error) {
	return s.synonymNotificationClient.SubscribeSynonymUpdates(ctx, request)
}

func (s *PayoutReportingServer) subscribeSynonymUpdatesAuditedStep(ctx context.Context, request *searchv1.SubscribeSynonymUpdatesRequest) (*searchv1.SubscribeSynonymUpdatesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SynonymNotificationService/SubscribeSynonymUpdates")
	return s.synonymNotificationClient.SubscribeSynonymUpdates(ctx, request)
}
