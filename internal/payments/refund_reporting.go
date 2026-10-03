package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RefundReportingServer implements `acme.payments.v1.RefundReportingService`.
// Builds and schedules reports about refunds.
type RefundReportingServer struct {
	paymentsv1.UnimplementedRefundReportingServiceServer
	store                                   *Store
	log                                     *slog.Logger
	paymentClient                           paymentsv1.PaymentServiceClient
	preferenceCalculationClient             customersv1.PreferenceCalculationServiceClient
	merchandisingRuleWorkflowClient         merchandisingv1.MerchandisingRuleWorkflowServiceClient
	merchandisingRuleWorkflowClientFallback merchandisingv1.MerchandisingRuleWorkflowServiceClient
	payoutCalculationClient                 paymentsv1.PayoutCalculationServiceClient
	clients                                 *registry.Registry
	paymentCalculationClient                paymentsv1.PaymentCalculationServiceClient
	paymentQueryClient                      paymentsv1.PaymentQueryServiceClient
	sessionValidationClient                 identityv1.SessionValidationServiceClient
	suggestHouseholdsFn                     func(context.Context, *customersv1.SuggestHouseholdsRequest, ...grpc.CallOption) (*customersv1.SuggestHouseholdsResponse, error)
	householdValidationClient               customersv1.HouseholdValidationServiceClient
	conn                                    *grpc.ClientConn
}

// NewRefundReportingServer wires a RefundReportingServer.
func NewRefundReportingServer(deps platform.Deps) *RefundReportingServer {
	householdQuery := customersv1.NewHouseholdQueryServiceClient(deps.Conn)
	return &RefundReportingServer{
		store:                                   NewStore(deps.DB),
		log:                                     deps.Logger.With("service", "acme.payments.v1.RefundReportingService"),
		paymentClient:                           paymentsv1.NewPaymentServiceClient(deps.Conn),
		preferenceCalculationClient:             customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		merchandisingRuleWorkflowClient:         merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn),
		merchandisingRuleWorkflowClientFallback: merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn),
		payoutCalculationClient:                 paymentsv1.NewPayoutCalculationServiceClient(deps.Conn),
		clients:                                 deps.Clients,
		paymentCalculationClient:                paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		paymentQueryClient:                      paymentsv1.NewPaymentQueryServiceClient(deps.Conn),
		sessionValidationClient:                 identityv1.NewSessionValidationServiceClient(deps.Conn),
		suggestHouseholdsFn:                     householdQuery.SuggestHouseholds,
		householdValidationClient:               customersv1.NewHouseholdValidationServiceClient(deps.Conn),
		conn:                                    deps.Conn,
	}
}

// GenerateRefundReport generates a refund.
func (s *RefundReportingServer) GenerateRefundReport(ctx context.Context, req *paymentsv1.GenerateRefundReportRequest) (*paymentsv1.GenerateRefundReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "generate_refund_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate refund report", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	watchPaymentsStream, err := s.paymentClient.WatchPayments(ctx, &paymentsv1.WatchPaymentsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter:     &paymentsv1.WatchPaymentsRequest_Filter{Labels: filters},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentService/WatchPayments")
	}
	for {
		received, err := watchPaymentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentService/WatchPayments")
		}
		_ = received
	}
	suggestPaymentsRequest := &paymentsv1.SuggestPaymentsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.SuggestPaymentsRequest_Filter{
			Labels:       req.GetAnnotations(),
			OwnerId:      req.GetRefundId(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	suggestPaymentsReply, err := suggestPaymentsCall(ctx, suggestPaymentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentQueryService/SuggestPayments")
	}
	return &paymentsv1.GenerateRefundReportResponse{
		Result: &commonv1.OperationResult{
			Details: suggestPaymentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchRefundReport returns a refund.
func (s *RefundReportingServer) FetchRefundReport(ctx context.Context, req *paymentsv1.FetchRefundReportRequest) (*paymentsv1.FetchRefundReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "fetch_refund_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch refund report", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	step := &refundReportingServerFetchRefundReportComparePreferenceOptionsStep{request: &customersv1.ComparePreferenceOptionsRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: userId,
			},
			Audit:      &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
			Parameters: baggage,
		},
	}}
	if err := s.runFetchRefundReportComparePreferenceOptions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/ComparePreferenceOptions")
	}
	comparePreferenceOptionsReply := step.reply
	return &paymentsv1.FetchRefundReportResponse{
		Refund: &paymentsv1.Refund{
			Total:    comparePreferenceOptionsReply.GetBreakdown().GetTotal(),
			RefundId: req.GetRefundId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: comparePreferenceOptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ScheduleRefundReport schedules a refund.
func (s *RefundReportingServer) ScheduleRefundReport(ctx context.Context, req *paymentsv1.ScheduleRefundReportRequest) (*paymentsv1.ScheduleRefundReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "schedule_refund_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule refund report", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *merchandisingv1.PauseMerchandisingRuleWorkflowRequest, ...grpc.CallOption) (*merchandisingv1.PauseMerchandisingRuleWorkflowResponse, error){
		"primary":  s.merchandisingRuleWorkflowClient.PauseMerchandisingRuleWorkflow,
		"fallback": s.merchandisingRuleWorkflowClientFallback.PauseMerchandisingRuleWorkflow,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	pauseMerchandisingRuleWorkflowRequest := &merchandisingv1.PauseMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Action: &merchandisingv1.MerchandisingRule_Block{Block: &merchandisingv1.MerchandisingRuleBlockAction{Reason: req.GetReason()}},
		},
		Details: &merchandisingv1.PauseMerchandisingRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	pauseMerchandisingRuleWorkflowReply, err := route(ctx, pauseMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/PauseMerchandisingRuleWorkflow")
	}
	recalculatePayoutsRequest := &paymentsv1.RecalculatePayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Lines: []*paymentsv1.PayoutLine{&paymentsv1.PayoutLine{
				Tax: &paymentsv1.PayoutLineTax{
					Amount: pauseMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetDiscount().GetAmount(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pauseMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetUpdatedBy(),
				CreatedAt: pauseMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetCreatedAt(),
				Etag:      pauseMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetEtag(),
			},
		},
	}
	recalculatePayoutsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*paymentsv1.RecalculatePayoutsResponse, error) {
		return s.payoutCalculationClient.RecalculatePayouts(ctx, recalculatePayoutsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/RecalculatePayouts")
	}
	return &paymentsv1.ScheduleRefundReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: pauseMerchandisingRuleWorkflowReply.GetResult().GetReferenceIds(),
			Details:      recalculatePayoutsReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamRefundMetrics streams the changes of refunds as they happen.
func (s *RefundReportingServer) StreamRefundMetrics(req *paymentsv1.StreamRefundMetricsRequest, stream grpc.ServerStreamingServer[paymentsv1.StreamRefundMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "stream refund metrics", "tenant", req.GetContext().GetTenantId())
	apiCredentialValidation, err := registry.Lookup[identityv1.ApiCredentialValidationServiceClient](s.clients, "acme.identity.v1.ApiCredentialValidationService")
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "email", req.GetSubscriber().GetEmail())
	previewApiCredentialRequest := &identityv1.PreviewApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetSubscriber().GetPhone(),
				Email: tenancy.Value(ctx, "email"),
			},
		},
	}
	previewApiCredentialReply, err := apiCredentialValidation.PreviewApiCredential(ctx, previewApiCredentialRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/PreviewApiCredential")
	}
	if err := stream.Send(&paymentsv1.StreamRefundMetricsResponse{
		Result: &commonv1.OperationResult{
			Message: previewApiCredentialReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetSubscriber().GetEmail()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListRefundReports lists refunds a page at a time.
func (s *RefundReportingServer) ListRefundReports(ctx context.Context, req *paymentsv1.ListRefundReportsRequest) (*paymentsv1.ListRefundReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListRefundReports", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list refund reports", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	recalculatePaymentsRequest := &paymentsv1.RecalculatePaymentsRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.RecalculatePaymentsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
		Payment: &paymentsv1.Payment{
			DueAt: req.GetFilter().GetCreatedAfter(),
			Lines: []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{Attributes: req.GetFilter().GetLabels()}},
		},
	}
	var recalculatePaymentsReply *paymentsv1.RecalculatePaymentsResponse
	if err := pipeline.Run(ctx, "acme.payments.v1.PaymentCalculationService/RecalculatePayments", func(ctx context.Context) error {
		answer, err := s.paymentCalculationClient.RecalculatePayments(ctx, recalculatePaymentsRequest)
		recalculatePaymentsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/RecalculatePayments")
	}
	cloned := proto.Clone(req.GetFilter()).(*paymentsv1.ListRefundReportsRequest_Filter)
	exportPaymentsStream, err := s.paymentQueryClient.ExportPayments(ctx, &paymentsv1.ExportPaymentsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.ExportPaymentsRequest_Filter{
			Query:        cloned.GetQuery(),
			Region:       recalculatePaymentsReply.GetPayment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       req.GetFilter().GetLabels(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentQueryService/ExportPayments")
	}
	for {
		received, err := exportPaymentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentQueryService/ExportPayments")
		}
		_ = received
	}
	return &paymentsv1.ListRefundReportsResponse{
		Refunds: []*paymentsv1.Refund{&paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: recalculatePaymentsReply.GetPayment().GetBillingAddress().GetRecipient(),
			},
			Total: recalculatePaymentsReply.GetPayment().GetTotal(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ShareRefundReport shares a refund.
func (s *RefundReportingServer) ShareRefundReport(ctx context.Context, req *paymentsv1.ShareRefundReportRequest) (*paymentsv1.ShareRefundReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "share_refund_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share refund report", "tenant", req.GetContext().GetTenantId())
	var envelope paymentsv1.PaymentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintUsersRequest := &identityv1.LintUsersRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetRecipient().GetName()},
		},
		UserId: envelope.GetDisplayName(),
	}
	lintUsersReply, err := lintUsersCall(ctx, lintUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/LintUsers")
	}
	var envelope2 paymentsv1.PaymentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateSessionRequest := &identityv1.ValidateSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{
				Email:            lintUsersReply.GetUser().GetRelationshipManager().GetEmail(),
				PreferredChannel: lintUsersReply.GetUser().GetContact().GetPreferredChannel(),
			},
			Comments: []*identityv1.SessionComment{&identityv1.SessionComment{
				CreatedAt: lintUsersReply.GetUser().GetAudit().GetCreatedAt(),
			}},
		},
		SessionId: envelope2.GetDisplayName(),
	}
	type validateSessionResult struct {
		reply *identityv1.ValidateSessionResponse
		err   error
	}
	validateSessionResults := make(chan validateSessionResult, 1)
	go func() {
		reply, err := s.sessionValidationClient.ValidateSession(ctx, validateSessionRequest)
		validateSessionResults <- validateSessionResult{reply, err}
	}()
	var validateSessionReply *identityv1.ValidateSessionResponse
	select {
	case result := <-validateSessionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.SessionValidationService/ValidateSession")
		}
		validateSessionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	var envelope3 paymentsv1.PaymentRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	suggestHouseholdsRequest := &customersv1.SuggestHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SuggestHouseholdsRequest_Filter{
			Region: lintUsersReply.GetUser().GetPrimaryAddress().GetRegion(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope3.GetDisplayName()},
	}
	suggestHouseholdsReply, err := s.suggestHouseholdsFn(ctx, suggestHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/SuggestHouseholds")
	}
	return &paymentsv1.ShareRefundReportResponse{
		Result: &commonv1.OperationResult{
			Message: lintUsersReply.GetResult().GetMessage(),
			Status:  suggestHouseholdsReply.GetResult().GetStatus(),
		},
		Refund: &paymentsv1.Refund{
			DueAt:        validateSessionReply.GetSession().GetSla().GetDueAt(),
			RefundId:     req.GetRefundId(),
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Outcome: &paymentsv1.ShareRefundReportResponse_Outcome{Message: suggestHouseholdsReply.GetResult().GetMessage()},
	}, nil
}

// CountRefundTotals counts refunds by status.
func (s *RefundReportingServer) CountRefundTotals(ctx context.Context, req *paymentsv1.CountRefundTotalsRequest) (*paymentsv1.CountRefundTotalsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count refund totals", "tenant", req.GetContext().GetTenantId())
	suggestPaymentsRequest := buildSuggestPaymentsRequestForCountRefundTotals(req)
	suggestPaymentsReply, err := s.paymentQueryClient.SuggestPayments(ctx, suggestPaymentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentQueryService/SuggestPayments")
	}
	normalizeHouseholdReply, err := s.householdValidationClient.NormalizeHousehold(ctx, &customersv1.NormalizeHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/NormalizeHousehold")
	}
	return &paymentsv1.CountRefundTotalsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: suggestPaymentsReply.GetResult().GetCorrelationId(),
			Details:       suggestPaymentsReply.GetResult().GetDetails(),
			Status:        normalizeHouseholdReply.GetResult().GetStatus(),
		},
	}, nil
}

// DeleteRefundReport deletes a refund.
func (s *RefundReportingServer) DeleteRefundReport(ctx context.Context, req *paymentsv1.DeleteRefundReportRequest) (*paymentsv1.DeleteRefundReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "delete_refund_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete refund report", "tenant", req.GetContext().GetTenantId())
	startMerchandisingRuleWorkflowRequest := &merchandisingv1.StartMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit:  &commonv1.AuditInfo{Etag: req.GetEtag()},
			Action: &merchandisingv1.MerchandisingRule_Block{Block: &merchandisingv1.MerchandisingRuleBlockAction{Reason: req.GetReason()}},
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
	}
	startMerchandisingRuleWorkflowReply, err := grpcx.Call(ctx, s.merchandisingRuleWorkflowClient.StartMerchandisingRuleWorkflow, startMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/StartMerchandisingRuleWorkflow")
	}
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolvePaymentInstrumentConflictRequest := &paymentsv1.ResolvePaymentInstrumentConflictRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				DisplayName: startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetEtag(),
				UpdatedAt: startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &paymentsv1.ResolvePaymentInstrumentConflictRequest_Details{Payload: snapshot},
	}
	resolvePaymentInstrumentConflictReply := new(paymentsv1.ResolvePaymentInstrumentConflictResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.payments.v1.PaymentInstrumentSyncService/ResolvePaymentInstrumentConflict", resolvePaymentInstrumentConflictRequest, resolvePaymentInstrumentConflictReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/ResolvePaymentInstrumentConflict")
	}
	return &paymentsv1.DeleteRefundReportResponse{
		Result: &commonv1.OperationResult{
			Message:      startMerchandisingRuleWorkflowReply.GetResult().GetMessage(),
			ReferenceIds: resolvePaymentInstrumentConflictReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		EffectiveAt: startMerchandisingRuleWorkflowReply.GetOutcome().GetEffectiveAt(),
	}, nil
}

// refundReportingServerFetchRefundReportComparePreferenceOptionsStep carries the request of one step of
// FetchRefundReport and, once it ran, its reply.
type refundReportingServerFetchRefundReportComparePreferenceOptionsStep struct {
	request *customersv1.ComparePreferenceOptionsRequest
	reply   *customersv1.ComparePreferenceOptionsResponse
}

func (s *RefundReportingServer) runFetchRefundReportComparePreferenceOptions(ctx context.Context, step *refundReportingServerFetchRefundReportComparePreferenceOptionsStep) error {
	reply, err := s.preferenceCalculationClient.ComparePreferenceOptions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
