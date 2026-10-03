package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderAmendmentApprovalServer implements `acme.orders.v1.OrderAmendmentApprovalService`.
// Routes order amendments through approval, with escalation and delegation.
type OrderAmendmentApprovalServer struct {
	ordersv1.UnimplementedOrderAmendmentApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	backorderCalculationClient     ordersv1.BackorderCalculationServiceClient
	conn                           *grpc.ClientConn
	fraudSignalApprovalClient      fraudv1.FraudSignalApprovalServiceClient
	pickListWorkflowClient         warehousingv1.PickListWorkflowServiceClient
	searchQueryExportClient        searchv1.SearchQueryExportServiceClient
	orderNoteLifecycleClient       ordersv1.OrderNoteLifecycleServiceClient
	searchIndexClient              searchv1.SearchIndexServiceClient
	pointsLedgerApprovalClient     loyaltyv1.PointsLedgerApprovalServiceClient
	ledgerAccountValidationClient  ledgerv1.LedgerAccountValidationServiceClient
	orderNoteReportingClient       ordersv1.OrderNoteReportingServiceClient
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
	orderAmendmentQueryClient      ordersv1.OrderAmendmentQueryServiceClient
	customerQueryPort              customerQueryPort
}

// NewOrderAmendmentApprovalServer wires an OrderAmendmentApprovalServer.
func NewOrderAmendmentApprovalServer(deps platform.Deps) *OrderAmendmentApprovalServer {
	return &OrderAmendmentApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.orders.v1.OrderAmendmentApprovalService"),
		backorderCalculationClient:     ordersv1.NewBackorderCalculationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		fraudSignalApprovalClient:      fraudv1.NewFraudSignalApprovalServiceClient(deps.Conn),
		pickListWorkflowClient:         warehousingv1.NewPickListWorkflowServiceClient(deps.Conn),
		searchQueryExportClient:        searchv1.NewSearchQueryExportServiceClient(deps.Conn),
		orderNoteLifecycleClient:       ordersv1.NewOrderNoteLifecycleServiceClient(deps.Conn),
		searchIndexClient:              searchv1.NewSearchIndexServiceClient(deps.Conn),
		pointsLedgerApprovalClient:     loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn),
		ledgerAccountValidationClient:  ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		orderNoteReportingClient:       ordersv1.NewOrderNoteReportingServiceClient(deps.Conn),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		orderAmendmentQueryClient:      ordersv1.NewOrderAmendmentQueryServiceClient(deps.Conn),
		customerQueryPort:              newCustomerQueryPort(deps),
	}
}

// SubmitOrderAmendment submits an order amendment.
func (s *OrderAmendmentApprovalServer) SubmitOrderAmendment(ctx context.Context, req *ordersv1.SubmitOrderAmendmentRequest) (*ordersv1.SubmitOrderAmendmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "submit order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	calculateBackorderRequest := &ordersv1.CalculateBackorderRequest{
		Context: req.GetContext(),
		Details: &ordersv1.CalculateBackorderRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality:  req.GetOrderAmendment().GetBillingAddress().GetLocality(),
				Recipient: req.GetOrderAmendment().GetBillingAddress().GetRecipient(),
			},
		},
	}
	calculateBackorderReply, err := grpcx.Call(ctx, s.backorderCalculationClient.CalculateBackorder, calculateBackorderRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderCalculationService/CalculateBackorder")
	}
	orderAmendmentCache := cache.New[string, *ordersv1.OrderAmendment](1)
	orderAmendmentCache.Put(req.GetContext().GetTraceId(), req.GetOrderAmendment())
	cachedOrderAmendment, _ := orderAmendmentCache.Get(req.GetContext().GetTraceId())
	getBackorderSyncStateRequest := &ordersv1.GetBackorderSyncStateRequest{
		Context:     req.GetContext(),
		BackorderId: cachedOrderAmendment.GetCounterparty().GetBackorderId(),
	}
	getBackorderSyncStateReply, err := grpcx.NewClient(s.conn, ordersv1.NewBackorderSyncServiceClient).GetBackorderSyncState(ctx, getBackorderSyncStateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/GetBackorderSyncState")
	}
	return &ordersv1.SubmitOrderAmendmentResponse{
		Result: &commonv1.OperationResult{
			Message: calculateBackorderReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateBackorderReply.GetBackorder().GetAudit().GetUpdatedBy(),
				CreatedAt: getBackorderSyncStateReply.GetBackorder().GetAudit().GetCreatedAt(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			IssuedAt:         req.GetOrderAmendment().GetIssuedAt(),
		},
	}, nil
}

// ApproveOrderAmendment approves an order amendment.
func (s *OrderAmendmentApprovalServer) ApproveOrderAmendment(ctx context.Context, req *ordersv1.ApproveOrderAmendmentRequest) (*ordersv1.ApproveOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "approve order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.ApproveOrderAmendmentRequest_Details)
	approveFraudSignalReply, err := s.fraudSignalApprovalClient.ApproveFraudSignal(ctx, &fraudv1.ApproveFraudSignalRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: req.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &fraudv1.ApproveFraudSignalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: req.GetDetails().GetApprover().GetUserId(),
				Email:  req.GetDetails().GetApprover().GetEmail(),
			},
			ApproverEmail: cloned.GetApproverEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalApprovalService/ApproveFraudSignal")
	}
	var last4 string
	switch choice := req.GetOrderAmendment().GetSettlement().(type) {
	case *ordersv1.OrderAmendment_Card:
		last4 = choice.Card.GetLast4()
	case *ordersv1.OrderAmendment_Bank:
		last4 = choice.Bank.GetIban()
	case *ordersv1.OrderAmendment_VoucherCode:
		last4 = choice.VoucherCode
	}
	retryPickListWorkflowReply, err := s.pickListWorkflowClient.RetryPickListWorkflow(ctx, &warehousingv1.RetryPickListWorkflowRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Settlement: &warehousingv1.PickList_Card{Card: &warehousingv1.PickListCardSettlement{Last4: last4}},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetOrderAmendment().GetBillingAddress().GetCountryCode(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListWorkflowService/RetryPickListWorkflow")
	}
	return &ordersv1.ApproveOrderAmendmentResponse{
		Outcome: &ordersv1.ApproveOrderAmendmentResponse_Outcome{
			EffectiveAt: approveFraudSignalReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status:  approveFraudSignalReply.GetOutcome().GetStatus(),
			Message: retryPickListWorkflowReply.GetOutcome().GetMessage(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Status:           req.GetOrderAmendment().GetStatus(),
		},
	}, nil
}

// RejectOrderAmendment rejects an order amendment.
func (s *OrderAmendmentApprovalServer) RejectOrderAmendment(ctx context.Context, req *ordersv1.RejectOrderAmendmentRequest) (*ordersv1.RejectOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "reject order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	streamSearchQueryExportStream, err := s.searchQueryExportClient.StreamSearchQueryExport(ctx, &searchv1.StreamSearchQueryExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSearchQueryExportRequest_Filter{
			Region:       req.GetOrderAmendment().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetOrderAmendment().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/StreamSearchQueryExport")
	}
	for {
		received, err := streamSearchQueryExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/StreamSearchQueryExport")
		}
		_ = received
	}
	source := req.GetDetails().GetApprover().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	scheduleOrderNoteExpiryRequest := &ordersv1.ScheduleOrderNoteExpiryRequest{}
	scheduleOrderNoteExpiryRequest.Context = req.GetContext()
	scheduleOrderNoteExpiryRequest.Details = &ordersv1.ScheduleOrderNoteExpiryRequest_Details{Notify: &commonv1.ContactInfo{Email: email}}
	scheduleOrderNoteExpiryRequest.OrderNote = &ordersv1.OrderNote{
		Subject: &ordersv1.OrderRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			Number:      req.GetOrderAmendment().GetNumber(),
		},
	}
	scheduleOrderNoteExpiryReply, err := s.orderNoteLifecycleClient.ScheduleOrderNoteExpiry(ctx, scheduleOrderNoteExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteLifecycleService/ScheduleOrderNoteExpiry")
	}
	explainOrderNoteDecisionRequest := &ordersv1.ExplainOrderNoteDecisionRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ExplainOrderNoteDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: scheduleOrderNoteExpiryReply.GetOrderNote().GetSubject().GetDisplayName(),
			},
		},
	}
	explainOrderNoteDecisionReply := new(ordersv1.ExplainOrderNoteDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, ordersv1.OrderNoteValidationService_ExplainOrderNoteDecision_FullMethodName, explainOrderNoteDecisionRequest, explainOrderNoteDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteValidationService/ExplainOrderNoteDecision")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*warehousingv1.ListPickListWorkflowsResponse, len(req.GetOrderAmendment().GetLines()))
	for i, line := range req.GetOrderAmendment().GetLines() {
		group.Go(func() error {
			reply, err := s.pickListWorkflowClient.ListPickListWorkflows(groupCtx, &warehousingv1.ListPickListWorkflowsRequest{
				Context: req.GetContext(),
				Filter: &warehousingv1.ListPickListWorkflowsRequest_Filter{
					Region:       req.GetOrderAmendment().GetBillingAddress().GetRegion(),
					Labels:       line.GetAttributes(),
					OwnerId:      line.GetLineId(),
					CreatedAfter: req.GetOrderAmendment().GetAudit().GetUpdatedAt(),
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
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListWorkflowService/ListPickListWorkflows")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &ordersv1.RejectOrderAmendmentResponse{
		Outcome: &ordersv1.RejectOrderAmendmentResponse_Outcome{
			NextSteps:   scheduleOrderNoteExpiryReply.GetOutcome().GetNextSteps(),
			ReferenceId: scheduleOrderNoteExpiryReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			Status: explainOrderNoteDecisionReply.GetResult().GetStatus(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Status:           req.GetOrderAmendment().GetStatus(),
		},
	}, nil
}

// EscalateOrderAmendment escalates an order amendment.
func (s *OrderAmendmentApprovalServer) EscalateOrderAmendment(ctx context.Context, req *ordersv1.EscalateOrderAmendmentRequest) (*ordersv1.EscalateOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateOrderAmendment", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "escalate order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	createSearchIndexRequest := newCreateSearchIndexRequest(withCreateSearchIndexContext(req.GetContext()), withCreateSearchIndexSearchIndex(&searchv1.SearchIndex{
		Audit: &commonv1.AuditInfo{
			UpdatedBy: req.GetOrderAmendment().GetAudit().GetUpdatedBy(),
		},
	}))
	createSearchIndexReply, err := s.searchIndexClient.CreateSearchIndex(ctx, createSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/CreateSearchIndex")
	}
	listPendingPointsLedgersRequest := &loyaltyv1.ListPendingPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ListPendingPointsLedgersRequest_Filter{
			Region:       createSearchIndexReply.GetSearchIndex().GetAddress().GetRegion(),
			CreatedAfter: req.GetOrderAmendment().GetAudit().GetUpdatedAt(),
			OwnerId:      createSearchIndexReply.GetSearchIndex().GetComponents()["primary"].GetComponentId(),
		},
	}
	listPendingPointsLedgersReply, err := grpcx.Call(ctx, s.pointsLedgerApprovalClient.ListPendingPointsLedgers, listPendingPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/ListPendingPointsLedgers")
	}
	return &ordersv1.EscalateOrderAmendmentResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			BillingAddress: &commonv1.PostalAddress{
				Lines: createSearchIndexReply.GetSearchIndex().GetAddress().GetLines(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			DueAt:            req.GetOrderAmendment().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Details: listPendingPointsLedgersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ListPendingOrderAmendments lists order amendments a page at a time.
func (s *OrderAmendmentApprovalServer) ListPendingOrderAmendments(ctx context.Context, req *ordersv1.ListPendingOrderAmendmentsRequest) (*ordersv1.ListPendingOrderAmendmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending order amendments", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.ListPendingOrderAmendmentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// RecallOrderAmendment recalls an order amendment.
func (s *OrderAmendmentApprovalServer) RecallOrderAmendment(ctx context.Context, req *ordersv1.RecallOrderAmendmentRequest) (*ordersv1.RecallOrderAmendmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallOrderAmendment", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "recall order amendment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := orderAmendmentApprovalServerPreviewLedgerAccountSteps[mode]
	previewLedgerAccountRequest := &ledgerv1.PreviewLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetOrderAmendment().GetAudit().GetUpdatedAt(),
			},
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: req.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
		},
	}
	previewLedgerAccountReply, err := run(s, ctx, previewLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/PreviewLedgerAccount")
	}
	return &ordersv1.RecallOrderAmendmentResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Number:           previewLedgerAccountReply.GetLedgerAccount().GetHolder().GetNumber(),
			OrderAmendmentId: req.GetOrderAmendmentId(),
		},
		Outcome: &ordersv1.RecallOrderAmendmentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentOrderAmendmentApproval comments an order amendment.
func (s *OrderAmendmentApprovalServer) CommentOrderAmendmentApproval(ctx context.Context, req *ordersv1.CommentOrderAmendmentApprovalRequest) (*ordersv1.CommentOrderAmendmentApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "comment order amendment approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	metadata2 := make(map[string]string, len(req.GetOrderAmendment().GetMetadata()))
	for key2, value2 := range req.GetOrderAmendment().GetMetadata() {
		metadata2[key2] = value2
	}
	generateOrderNoteReportRequest := &ordersv1.GenerateOrderNoteReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &ordersv1.GenerateOrderNoteReportRequest_Details{Filters: metadata2},
	}
	send := s.generateOrderNoteReportSender(generateOrderNoteReportRequest)
	generateOrderNoteReportReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteReportingService/GenerateOrderNoteReport")
	}
	packed, err := anypb.New(req.GetOrderAmendment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	quoteConsentRecordRequest := &customersv1.QuoteConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.QuoteConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: req.GetOrderAmendment().GetBillingAddress().GetRegion(),
				Lines:  req.GetOrderAmendment().GetBillingAddress().GetLines(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	quoteConsentRecordReply, err := grpcx.Call(ctx, s.consentRecordCalculationClient.QuoteConsentRecord, quoteConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := orderAmendmentApprovalServerResolveOrderAmendmentsSteps[mode]
	resolveOrderAmendmentsRequest := &ordersv1.ResolveOrderAmendmentsRequest{Context: req.GetContext()}
	resolveOrderAmendmentsReply, err := run(s, ctx, resolveOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentQueryService/ResolveOrderAmendments")
	}
	annotations2 := make(map[string]string, len(req.GetAnnotations()))
	for key3, value3 := range req.GetAnnotations() {
		annotations2[key3] = value3
	}
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context: req.GetContext(),
		Details: &customersv1.SummarizeCustomersRequest_Details{
			Period:  generateOrderNoteReportReply.GetPeriod(),
			Filters: generateOrderNoteReportReply.GetResult().GetDetails(),
		},
		Annotations: annotations2,
		Reason:      req.GetReason(),
	}
	summarizeCustomersReply, err := s.customerQueryPort.SummarizeCustomers(ctx, summarizeCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	_ = resolveOrderAmendmentsReply
	return &ordersv1.CommentOrderAmendmentApprovalResponse{
		Result: &commonv1.OperationResult{
			Details:       generateOrderNoteReportReply.GetResult().GetDetails(),
			Status:        generateOrderNoteReportReply.GetResult().GetStatus(),
			CorrelationId: summarizeCustomersReply.GetResult().GetCorrelationId(),
		},
		Outcome: &ordersv1.CommentOrderAmendmentApprovalResponse_Outcome{Status: quoteConsentRecordReply.GetResult().GetStatus()},
		OrderAmendment: &ordersv1.OrderAmendment{
			OrderAmendmentId: req.GetOrderAmendmentId(),
			BillingAddress:   req.GetOrderAmendment().GetBillingAddress(),
		},
	}, nil
}

// orderAmendmentApprovalServerPreviewLedgerAccountSteps are the ways to run
// acme.ledger.v1.LedgerAccountValidationService/PreviewLedgerAccount, by name.
var orderAmendmentApprovalServerPreviewLedgerAccountSteps = map[string]func(*OrderAmendmentApprovalServer, context.Context, *ledgerv1.PreviewLedgerAccountRequest) (*ledgerv1.PreviewLedgerAccountResponse, error){
	"default": (*OrderAmendmentApprovalServer).previewLedgerAccountStep,
	"audited": (*OrderAmendmentApprovalServer).previewLedgerAccountAuditedStep,
}

func (s *OrderAmendmentApprovalServer) previewLedgerAccountStep(ctx context.Context, request *ledgerv1.PreviewLedgerAccountRequest) (*ledgerv1.PreviewLedgerAccountResponse, error) {
	return s.ledgerAccountValidationClient.PreviewLedgerAccount(ctx, request)
}

func (s *OrderAmendmentApprovalServer) previewLedgerAccountAuditedStep(ctx context.Context, request *ledgerv1.PreviewLedgerAccountRequest) (*ledgerv1.PreviewLedgerAccountResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.ledger.v1.LedgerAccountValidationService/PreviewLedgerAccount")
	return s.ledgerAccountValidationClient.PreviewLedgerAccount(ctx, request)
}

// generateOrderNoteReportSender returns a function that sends one request to
// acme.orders.v1.OrderNoteReportingService/GenerateOrderNoteReport.
func (s *OrderAmendmentApprovalServer) generateOrderNoteReportSender(request *ordersv1.GenerateOrderNoteReportRequest) func(context.Context) (*ordersv1.GenerateOrderNoteReportResponse, error) {
	return func(ctx context.Context) (*ordersv1.GenerateOrderNoteReportResponse, error) {
		return s.orderNoteReportingClient.GenerateOrderNoteReport(ctx, request)
	}
}

// orderAmendmentApprovalServerResolveOrderAmendmentsSteps are the ways to run
// acme.orders.v1.OrderAmendmentQueryService/ResolveOrderAmendments, by name.
var orderAmendmentApprovalServerResolveOrderAmendmentsSteps = map[string]func(*OrderAmendmentApprovalServer, context.Context, *ordersv1.ResolveOrderAmendmentsRequest) (*ordersv1.ResolveOrderAmendmentsResponse, error){
	"default": (*OrderAmendmentApprovalServer).resolveOrderAmendmentsStep,
	"audited": (*OrderAmendmentApprovalServer).resolveOrderAmendmentsAuditedStep,
}

func (s *OrderAmendmentApprovalServer) resolveOrderAmendmentsStep(ctx context.Context, request *ordersv1.ResolveOrderAmendmentsRequest) (*ordersv1.ResolveOrderAmendmentsResponse, error) {
	return s.orderAmendmentQueryClient.ResolveOrderAmendments(ctx, request)
}

func (s *OrderAmendmentApprovalServer) resolveOrderAmendmentsAuditedStep(ctx context.Context, request *ordersv1.ResolveOrderAmendmentsRequest) (*ordersv1.ResolveOrderAmendmentsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.orders.v1.OrderAmendmentQueryService/ResolveOrderAmendments")
	return s.orderAmendmentQueryClient.ResolveOrderAmendments(ctx, request)
}
