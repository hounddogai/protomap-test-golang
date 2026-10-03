package checkout

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// ShippingQuoteApprovalServer implements `acme.checkout.v1.ShippingQuoteApprovalService`.
// Routes shipping quotes through approval, with escalation and delegation.
type ShippingQuoteApprovalServer struct {
	*shippingQuoteApprovalHandlers
}

// shippingQuoteApprovalHandlers holds the handlers of ShippingQuoteApprovalServer, with what they use.
type shippingQuoteApprovalHandlers struct {
	checkoutv1.UnimplementedShippingQuoteApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	conn                               *grpc.ClientConn
	checkoutSessionOnce                sync.Once
	checkoutSessionCached              checkoutv1.CheckoutSessionServiceClient
	tenantAdminClient                  identityv1.TenantAdminServiceClient
	customerClient                     customersv1.CustomerServiceClient
	checkoutSessionApprovalClient      checkoutv1.CheckoutSessionApprovalServiceClient
	checkoutSessionQueryClient         checkoutv1.CheckoutSessionQueryServiceClient
	meteredCheckoutSessionApproval     meteredCheckoutSessionApproval
	shippingQuoteQueryClient           checkoutv1.ShippingQuoteQueryServiceClient
	preferenceCalculationPort          preferenceCalculationPort
	orderWorkflowClient                ordersv1.OrderWorkflowServiceClient
	checkoutSessionQueryClientFallback checkoutv1.CheckoutSessionQueryServiceClient
}

// NewShippingQuoteApprovalServer wires a ShippingQuoteApprovalServer.
func NewShippingQuoteApprovalServer(deps platform.Deps) *ShippingQuoteApprovalServer {
	return &ShippingQuoteApprovalServer{shippingQuoteApprovalHandlers: &shippingQuoteApprovalHandlers{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.checkout.v1.ShippingQuoteApprovalService"),
		conn:                               deps.Conn,
		tenantAdminClient:                  identityv1.NewTenantAdminServiceClient(deps.Conn),
		customerClient:                     customersv1.NewCustomerServiceClient(deps.Conn),
		checkoutSessionApprovalClient:      checkoutv1.NewCheckoutSessionApprovalServiceClient(deps.Conn),
		checkoutSessionQueryClient:         checkoutv1.NewCheckoutSessionQueryServiceClient(deps.Conn),
		meteredCheckoutSessionApproval:     meteredCheckoutSessionApproval{CheckoutSessionApprovalServiceClient: checkoutv1.NewCheckoutSessionApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		shippingQuoteQueryClient:           checkoutv1.NewShippingQuoteQueryServiceClient(deps.Conn),
		preferenceCalculationPort:          newPreferenceCalculationPort(deps),
		orderWorkflowClient:                ordersv1.NewOrderWorkflowServiceClient(deps.Conn),
		checkoutSessionQueryClientFallback: checkoutv1.NewCheckoutSessionQueryServiceClient(deps.Conn),
	}}
}

// SubmitShippingQuote submits a shipping quote.
func (s *shippingQuoteApprovalHandlers) SubmitShippingQuote(ctx context.Context, req *checkoutv1.SubmitShippingQuoteRequest) (*checkoutv1.SubmitShippingQuoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "submit shipping quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetShippingQuote())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded checkoutv1.ShippingQuote
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	searchCheckoutSessionsRequest := &checkoutv1.SearchCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.SearchCheckoutSessionsRequest_Filter{
			Region:       req.GetShippingQuote().GetBillingAddress().GetRegion(),
			CreatedAfter: (&decoded).GetAudit().GetCreatedAt(),
		},
	}
	searchCheckoutSessionsReply, err := s.checkoutSessionLazy().SearchCheckoutSessions(ctx, searchCheckoutSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionService/SearchCheckoutSessions")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := shippingQuoteApprovalHandlersLockTenantSteps[mode]
	lockTenantRequest := &identityv1.LockTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
		Details: &identityv1.LockTenantRequest_Details{
			NewOwner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	lockTenantReply, err := run(s, ctx, lockTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/LockTenant")
	}
	searchCustomersRequest := &customersv1.SearchCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SearchCustomersRequest_Filter{
			Region: req.GetShippingQuote().GetBillingAddress().GetRegion(),
		},
	}
	send := s.searchCustomersSender(searchCustomersRequest)
	searchCustomersReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/SearchCustomers")
	}
	listPendingCheckoutSessionsRequest := buildListPendingCheckoutSessionsRequestForSubmitShippingQuote(req, lockTenantReply)
	listPendingCheckoutSessionsReply, err := s.checkoutSessionApprovalClient.ListPendingCheckoutSessions(ctx, listPendingCheckoutSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/ListPendingCheckoutSessions")
	}
	return &checkoutv1.SubmitShippingQuoteResponse{
		Result: &commonv1.OperationResult{
			Details:      searchCheckoutSessionsReply.GetResult().GetDetails(),
			ReferenceIds: searchCustomersReply.GetResult().GetReferenceIds(),
			Message:      listPendingCheckoutSessionsReply.GetResult().GetMessage(),
			Status:       listPendingCheckoutSessionsReply.GetResult().GetStatus(),
		},
		ShippingQuote: &checkoutv1.ShippingQuote{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: lockTenantReply.GetTenant().GetLocation().GetRecipient(),
			},
			ShippingQuoteId: req.GetShippingQuoteId(),
			IssuedAt:        req.GetShippingQuote().GetIssuedAt(),
		},
		Outcome: &checkoutv1.SubmitShippingQuoteResponse_Outcome{
			Status:  lockTenantReply.GetOutcome().GetStatus(),
			Message: searchCustomersReply.GetResult().GetMessage(),
		},
	}, nil
}

// ApproveShippingQuote approves a shipping quote.
func (s *shippingQuoteApprovalHandlers) ApproveShippingQuote(ctx context.Context, req *checkoutv1.ApproveShippingQuoteRequest) (*checkoutv1.ApproveShippingQuoteResponse, error) {
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "approve shipping quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	metadata2 := make(map[string]string, len(req.GetShippingQuote().GetMetadata()))
	for key, value := range req.GetShippingQuote().GetMetadata() {
		metadata2[key] = value
	}
	step := &shippingQuoteApprovalHandlersApproveShippingQuoteBrowseCheckoutSessionsStep{request: &checkoutv1.BrowseCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.BrowseCheckoutSessionsRequest_Filter{
			Region:       req.GetShippingQuote().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetShippingQuote().GetAudit().GetCreatedAt(),
			Labels:       metadata2,
		},
	}}
	if err := s.runApproveShippingQuoteBrowseCheckoutSessions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/BrowseCheckoutSessions")
	}
	browseCheckoutSessionsReply := step.reply
	return &checkoutv1.ApproveShippingQuoteResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: browseCheckoutSessionsReply.GetResult().GetReferenceIds(),
			Status:       browseCheckoutSessionsReply.GetResult().GetStatus(),
		},
		ShippingQuote: &checkoutv1.ShippingQuote{
			ShippingQuoteId: req.GetShippingQuoteId(),
			Total:           req.GetShippingQuote().GetTotal(),
		},
	}, nil
}

// RejectShippingQuote rejects a shipping quote.
func (s *shippingQuoteApprovalHandlers) RejectShippingQuote(ctx context.Context, req *checkoutv1.RejectShippingQuoteRequest) (*checkoutv1.RejectShippingQuoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "reject shipping quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	var last4 string
	switch choice := req.GetShippingQuote().GetSettlement().(type) {
	case *checkoutv1.ShippingQuote_Card:
		last4 = choice.Card.GetLast4()
	case *checkoutv1.ShippingQuote_Bank:
		last4 = choice.Bank.GetIban()
	case *checkoutv1.ShippingQuote_VoucherCode:
		last4 = choice.VoucherCode
	}
	submitCheckoutSessionRequest := &checkoutv1.SubmitCheckoutSessionRequest{}
	submitCheckoutSessionRequest.Context = req.GetContext()
	submitCheckoutSessionRequest.CheckoutSession = &checkoutv1.CheckoutSession{
		Settlement: &checkoutv1.CheckoutSession_Card{Card: &checkoutv1.CheckoutSessionCardSettlement{Last4: last4}},
	}
	submitCheckoutSessionReply, err := s.checkoutSessionApprovalClient.SubmitCheckoutSession(ctx, submitCheckoutSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/SubmitCheckoutSession")
	}
	recallCheckoutSessionRequest := &checkoutv1.RecallCheckoutSessionRequest{
		Context: req.GetContext(),
		Details: &checkoutv1.RecallCheckoutSessionRequest_Details{
			Approver: &identityv1.UserRef{
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: submitCheckoutSessionReply.GetCheckoutSession().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	recallCheckoutSessionReply, err := s.meteredCheckoutSessionApproval.RecallCheckoutSession(ctx, recallCheckoutSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/RecallCheckoutSession")
	}
	return &checkoutv1.RejectShippingQuoteResponse{
		Outcome: &checkoutv1.RejectShippingQuoteResponse_Outcome{
			ReferenceId: submitCheckoutSessionReply.GetOutcome().GetReferenceId(),
			Message:     recallCheckoutSessionReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: submitCheckoutSessionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ShippingQuote: &checkoutv1.ShippingQuote{
			ShippingQuoteId: req.GetShippingQuoteId(),
			BillingAddress:  req.GetShippingQuote().GetBillingAddress(),
		},
	}, nil
}

// EscalateShippingQuote escalates a shipping quote.
func (s *shippingQuoteApprovalHandlers) EscalateShippingQuote(ctx context.Context, req *checkoutv1.EscalateShippingQuoteRequest) (*checkoutv1.EscalateShippingQuoteResponse, error) {
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "escalate shipping quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	peekShippingQuoteRequest := &checkoutv1.PeekShippingQuoteRequest{
		Context:         req.GetContext(),
		ShippingQuoteId: req.GetShippingQuote().GetShippingQuoteId(),
	}
	type peekShippingQuoteResult struct {
		reply *checkoutv1.PeekShippingQuoteResponse
		err   error
	}
	peekShippingQuoteResults := make(chan peekShippingQuoteResult, 1)
	go func() {
		reply, err := s.shippingQuoteQueryClient.PeekShippingQuote(ctx, peekShippingQuoteRequest)
		peekShippingQuoteResults <- peekShippingQuoteResult{reply, err}
	}()
	var peekShippingQuoteReply *checkoutv1.PeekShippingQuoteResponse
	select {
	case result := <-peekShippingQuoteResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.checkout.v1.ShippingQuoteQueryService/PeekShippingQuote")
		}
		peekShippingQuoteReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	ctx = tenancy.With(ctx, "updated_by", req.GetShippingQuote().GetAudit().GetUpdatedBy())
	estimatePreferenceRequest := &customersv1.EstimatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{UpdatedBy: tenancy.Value(ctx, "updated_by")},
			Owner: &identityv1.UserRef{
				Email:       peekShippingQuoteReply.GetShippingQuote().GetCounterparty().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
	estimatePreferenceReply, err := s.preferenceCalculationPort.EstimatePreference(ctx, estimatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/EstimatePreference")
	}
	return &checkoutv1.EscalateShippingQuoteResponse{
		ShippingQuote: &checkoutv1.ShippingQuote{
			Notes:           peekShippingQuoteReply.GetShippingQuote().GetNotes(),
			ShippingQuoteId: req.GetShippingQuoteId(),
		},
		Outcome: &checkoutv1.EscalateShippingQuoteResponse_Outcome{
			Message:     estimatePreferenceReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingShippingQuotes lists shipping quotes a page at a time.
func (s *shippingQuoteApprovalHandlers) ListPendingShippingQuotes(ctx context.Context, req *checkoutv1.ListPendingShippingQuotesRequest) (*checkoutv1.ListPendingShippingQuotesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending shipping quotes", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.ListPendingShippingQuotesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateShippingQuoteApproval delegates a shipping quote.
func (s *shippingQuoteApprovalHandlers) DelegateShippingQuoteApproval(ctx context.Context, req *checkoutv1.DelegateShippingQuoteApprovalRequest) (*checkoutv1.DelegateShippingQuoteApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "delegate shipping quote approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetShippingQuote().GetLines(), func(candidate *checkoutv1.ShippingQuoteLine) bool { return candidate != nil }) {
		if _, err := s.shippingQuoteQueryClient.SuggestShippingQuotes(ctx, &checkoutv1.SuggestShippingQuotesRequest{
			Context: req.GetContext(),
			Filter: &checkoutv1.SuggestShippingQuotesRequest_Filter{
				Region:  req.GetShippingQuote().GetBillingAddress().GetRegion(),
				OwnerId: line.GetLineId(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/SuggestShippingQuotes")
		}
	}
	return &checkoutv1.DelegateShippingQuoteApprovalResponse{
		ShippingQuote: &checkoutv1.ShippingQuote{
			ShippingQuoteId: req.GetShippingQuoteId(),
			Counterparty:    req.GetShippingQuote().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallShippingQuote recalls a shipping quote.
func (s *shippingQuoteApprovalHandlers) RecallShippingQuote(ctx context.Context, req *checkoutv1.RecallShippingQuoteRequest) (*checkoutv1.RecallShippingQuoteResponse, error) {
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "recall shipping quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	cloneCustomerReply, err := s.customerClient.CloneCustomer(ctx, &customersv1.CloneCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email:       tenancy.Value(ctx, "email"),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetShippingQuote().GetAudit().GetUpdatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CloneCustomer")
	}
	exportShippingQuotesStream, err := s.shippingQuoteQueryClient.ExportShippingQuotes(ctx, &checkoutv1.ExportShippingQuotesRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.ExportShippingQuotesRequest_Filter{
			Region:       req.GetShippingQuote().GetBillingAddress().GetRegion(),
			CreatedAfter: cloneCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetShippingQuote().GetCounterparty().GetCustomerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/ExportShippingQuotes")
	}
	for {
		received, err := exportShippingQuotesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/ExportShippingQuotes")
		}
		_ = received
	}
	jobs := make(chan *checkoutv1.ShippingQuoteAttachment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attachment := range jobs {
				if _, err := s.orderWorkflowClient.SignalOrderWorkflow(workCtx, &ordersv1.SignalOrderWorkflowRequest{
					Context: req.GetContext(),
					Order: &ordersv1.Order{
						Attachments: []*ordersv1.OrderAttachment{&ordersv1.OrderAttachment{
							FileName: attachment.GetFileName(),
							Uri:      attachment.GetUri(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetShippingQuote().GetAttachments() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderWorkflowService/SignalOrderWorkflow")
	}
	return &checkoutv1.RecallShippingQuoteResponse{
		ShippingQuote: &checkoutv1.ShippingQuote{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: cloneCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
			ShippingQuoteId: req.GetShippingQuoteId(),
			IssuedAt:        req.GetShippingQuote().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetShippingQuoteApprovalStatus returns a shipping quote.
func (s *shippingQuoteApprovalHandlers) GetShippingQuoteApprovalStatus(ctx context.Context, req *checkoutv1.GetShippingQuoteApprovalStatusRequest) (*checkoutv1.GetShippingQuoteApprovalStatusResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipping_quote", req.GetShippingQuoteId(), "get_shipping_quote_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get shipping quote approval status", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *checkoutv1.SummarizeCheckoutSessionsRequest, ...grpc.CallOption) (*checkoutv1.SummarizeCheckoutSessionsResponse, error){
		"primary":  s.checkoutSessionQueryClient.SummarizeCheckoutSessions,
		"fallback": s.checkoutSessionQueryClientFallback.SummarizeCheckoutSessions,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	summarizeCheckoutSessionsRequest := &checkoutv1.SummarizeCheckoutSessionsRequest{
		Context:     req.GetContext(),
		Annotations: baggage,
	}
	summarizeCheckoutSessionsReply, err := route(ctx, summarizeCheckoutSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/SummarizeCheckoutSessions")
	}
	resolveCheckoutSessionsReply, err := s.checkoutSessionQueryClient.ResolveCheckoutSessions(ctx, &checkoutv1.ResolveCheckoutSessionsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/ResolveCheckoutSessions")
	}
	return &checkoutv1.GetShippingQuoteApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  summarizeCheckoutSessionsReply.GetResult().GetReferenceIds(),
			CorrelationId: resolveCheckoutSessionsReply.GetResult().GetCorrelationId(),
			Details:       resolveCheckoutSessionsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		ShippingQuote: &checkoutv1.ShippingQuote{ShippingQuoteId: req.GetShippingQuoteId()},
	}, nil
}

// checkoutSessionLazy dials acme.checkout.v1.CheckoutSessionService the first time a handler needs it.
func (s *shippingQuoteApprovalHandlers) checkoutSessionLazy() checkoutv1.CheckoutSessionServiceClient {
	s.checkoutSessionOnce.Do(func() { s.checkoutSessionCached = checkoutv1.NewCheckoutSessionServiceClient(s.conn) })
	return s.checkoutSessionCached
}

// shippingQuoteApprovalHandlersLockTenantSteps are the ways to run acme.identity.v1.TenantAdminService/LockTenant, by
// name.
var shippingQuoteApprovalHandlersLockTenantSteps = map[string]func(*shippingQuoteApprovalHandlers, context.Context, *identityv1.LockTenantRequest) (*identityv1.LockTenantResponse, error){
	"default": (*shippingQuoteApprovalHandlers).lockTenantStep,
	"audited": (*shippingQuoteApprovalHandlers).lockTenantAuditedStep,
}

func (s *shippingQuoteApprovalHandlers) lockTenantStep(ctx context.Context, request *identityv1.LockTenantRequest) (*identityv1.LockTenantResponse, error) {
	return s.tenantAdminClient.LockTenant(ctx, request)
}

func (s *shippingQuoteApprovalHandlers) lockTenantAuditedStep(ctx context.Context, request *identityv1.LockTenantRequest) (*identityv1.LockTenantResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.TenantAdminService/LockTenant")
	return s.tenantAdminClient.LockTenant(ctx, request)
}

// searchCustomersSender returns a function that sends one request to acme.customers.v1.CustomerService/SearchCustomers.
func (s *shippingQuoteApprovalHandlers) searchCustomersSender(request *customersv1.SearchCustomersRequest) func(context.Context) (*customersv1.SearchCustomersResponse, error) {
	return func(ctx context.Context) (*customersv1.SearchCustomersResponse, error) {
		return s.customerClient.SearchCustomers(ctx, request)
	}
}

// shippingQuoteApprovalHandlersApproveShippingQuoteBrowseCheckoutSessionsStep carries the request of one step of
// ApproveShippingQuote and, once it ran, its reply.
type shippingQuoteApprovalHandlersApproveShippingQuoteBrowseCheckoutSessionsStep struct {
	request *checkoutv1.BrowseCheckoutSessionsRequest
	reply   *checkoutv1.BrowseCheckoutSessionsResponse
}

func (s *shippingQuoteApprovalHandlers) runApproveShippingQuoteBrowseCheckoutSessions(ctx context.Context, step *shippingQuoteApprovalHandlersApproveShippingQuoteBrowseCheckoutSessionsStep) error {
	reply, err := s.checkoutSessionQueryClient.BrowseCheckoutSessions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
