package checkout

import (
	"context"
	"errors"
	"io"
	"log/slog"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ShippingQuoteWorkflowServer implements `acme.checkout.v1.ShippingQuoteWorkflowService`.
// Runs the long-lived workflow that takes a shipping quote from start to finish.
type ShippingQuoteWorkflowServer struct {
	checkoutv1.UnimplementedShippingQuoteWorkflowServiceServer
	store                               *Store
	log                                 *slog.Logger
	giftCardTransactionWorkflowClient   giftcardsv1.GiftCardTransactionWorkflowServiceClient
	customerClient                      customersv1.CustomerServiceClient
	conn                                *grpc.ClientConn
	customerCalculationClient           customersv1.CustomerCalculationServiceClient
	sellerPayoutScheduleReportingClient sellersv1.SellerPayoutScheduleReportingServiceClient
	paymentIntentApprovalClient         checkoutv1.PaymentIntentApprovalServiceClient
	shippingQuoteQueryClient            checkoutv1.ShippingQuoteQueryServiceClient
	checkoutSessionWorkflowClient       checkoutv1.CheckoutSessionWorkflowServiceClient
	sellerPayoutScheduleApprovalClient  sellersv1.SellerPayoutScheduleApprovalServiceClient
	checkoutSessionClient               checkoutv1.CheckoutSessionServiceClient
}

// NewShippingQuoteWorkflowServer wires a ShippingQuoteWorkflowServer.
func NewShippingQuoteWorkflowServer(deps platform.Deps) *ShippingQuoteWorkflowServer {
	return &ShippingQuoteWorkflowServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.checkout.v1.ShippingQuoteWorkflowService"),
		giftCardTransactionWorkflowClient:   giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn),
		customerClient:                      customersv1.NewCustomerServiceClient(deps.Conn),
		conn:                                deps.Conn,
		customerCalculationClient:           customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		sellerPayoutScheduleReportingClient: sellersv1.NewSellerPayoutScheduleReportingServiceClient(deps.Conn),
		paymentIntentApprovalClient:         checkoutv1.NewPaymentIntentApprovalServiceClient(deps.Conn),
		shippingQuoteQueryClient:            checkoutv1.NewShippingQuoteQueryServiceClient(deps.Conn),
		checkoutSessionWorkflowClient:       checkoutv1.NewCheckoutSessionWorkflowServiceClient(deps.Conn),
		sellerPayoutScheduleApprovalClient:  sellersv1.NewSellerPayoutScheduleApprovalServiceClient(deps.Conn),
		checkoutSessionClient:               checkoutv1.NewCheckoutSessionServiceClient(deps.Conn),
	}
}

// StartShippingQuoteWorkflow starts a shipping quote.
func (s *ShippingQuoteWorkflowServer) StartShippingQuoteWorkflow(ctx context.Context, req *checkoutv1.StartShippingQuoteWorkflowRequest) (*checkoutv1.StartShippingQuoteWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "start shipping quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	resumeGiftCardTransactionWorkflowRequest := buildResumeGiftCardTransactionWorkflowRequestForStartShippingQuoteWorkflow(req)
	resumeGiftCardTransactionWorkflowReply, err := s.giftCardTransactionWorkflowClient.ResumeGiftCardTransactionWorkflow(ctx, resumeGiftCardTransactionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/ResumeGiftCardTransactionWorkflow")
	}
	cloneCustomerRequest := &customersv1.CloneCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress: &commonv1.PostalAddress{
				Lines: req.GetShippingQuote().GetBillingAddress().GetLines(),
			},
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			Contact:             &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
			IdentityDocument:    &customersv1.Customer_NationalId{NationalId: &customersv1.CustomerNationalId{Number: req.GetShippingQuote().GetNumber()}},
		},
	}
	var cloneCustomerReply *customersv1.CloneCustomerResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.CustomerService/CloneCustomer", func(ctx context.Context) error {
		answer, err := s.customerClient.CloneCustomer(ctx, cloneCustomerRequest)
		cloneCustomerReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CloneCustomer")
	}
	explainJobOpeningCalculationRequest := &recruitingv1.ExplainJobOpeningCalculationRequest{
		Context: req.GetContext(),
		JobOpening: &recruitingv1.JobOpening{
			BillingAddress: &commonv1.PostalAddress{
				Locality: cloneCustomerReply.GetCustomer().GetPrimaryAddress().GetLocality(),
			},
		},
		Details: &recruitingv1.ExplainJobOpeningCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: cloneCustomerReply.GetCustomer().GetContact().GetPreferredChannel(),
			},
		},
	}
	explainJobOpeningCalculationReply := new(recruitingv1.ExplainJobOpeningCalculationResponse)
	if err := grpc.Invoke(ctx, "/acme.recruiting.v1.JobOpeningCalculationService/ExplainJobOpeningCalculation", explainJobOpeningCalculationRequest, explainJobOpeningCalculationReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningCalculationService/ExplainJobOpeningCalculation")
	}
	resolveShippingQuotesRequest := &checkoutv1.ResolveShippingQuotesRequest{Context: req.GetContext()}
	resolveShippingQuotesReply, err := resolveShippingQuotesCall(ctx, resolveShippingQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/ResolveShippingQuotes")
	}
	return &checkoutv1.StartShippingQuoteWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:       resumeGiftCardTransactionWorkflowReply.GetOutcome().GetStatus(),
			ReferenceIds: explainJobOpeningCalculationReply.GetResult().GetReferenceIds(),
		},
		ShippingQuote: &checkoutv1.ShippingQuote{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: cloneCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			},
			ShippingQuoteId: req.GetShippingQuoteId(),
			Notes:           req.GetShippingQuote().GetNotes(),
		},
		Outcome: &checkoutv1.StartShippingQuoteWorkflowResponse_Outcome{
			Message: resolveShippingQuotesReply.GetResult().GetMessage(),
		},
	}, nil
}

// AdvanceShippingQuoteWorkflow advances a shipping quote.
func (s *ShippingQuoteWorkflowServer) AdvanceShippingQuoteWorkflow(ctx context.Context, req *checkoutv1.AdvanceShippingQuoteWorkflowRequest) (*checkoutv1.AdvanceShippingQuoteWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "advance shipping quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetShippingQuote().GetLines(), func(candidate *checkoutv1.ShippingQuoteLine) bool { return candidate != nil }) {
		if _, err := s.customerCalculationClient.RecalculateCustomers(ctx, &customersv1.RecalculateCustomersRequest{
			Context: req.GetContext(),
			Customer: &customersv1.Customer{
				Attributes: line.GetAttributes(),
				Contact: &commonv1.ContactInfo{
					PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				},
			},
			Details: &customersv1.RecalculateCustomersRequest_Details{
				Destination: &commonv1.PostalAddress{
					Recipient: req.GetShippingQuote().GetBillingAddress().GetRecipient(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
		}
	}
	streamSellerPayoutScheduleMetricsStream, err := s.sellerPayoutScheduleReportingClient.StreamSellerPayoutScheduleMetrics(ctx, &sellersv1.StreamSellerPayoutScheduleMetricsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.StreamSellerPayoutScheduleMetricsRequest_Filter{
			Region: req.GetShippingQuote().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/StreamSellerPayoutScheduleMetrics")
	}
	for {
		received, err := streamSellerPayoutScheduleMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/StreamSellerPayoutScheduleMetrics")
		}
		_ = received
	}
	rejectPaymentIntentRequest := &checkoutv1.RejectPaymentIntentRequest{Context: req.GetContext()}
	rejectPaymentIntentReply, err := grpcx.NewClient(s.conn, checkoutv1.NewPaymentIntentApprovalServiceClient).RejectPaymentIntent(ctx, rejectPaymentIntentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentApprovalService/RejectPaymentIntent")
	}
	return &checkoutv1.AdvanceShippingQuoteWorkflowResponse{
		ShippingQuote: &checkoutv1.ShippingQuote{
			DueAt:           rejectPaymentIntentReply.GetPaymentIntent().GetSla().GetDueAt(),
			ShippingQuoteId: req.GetShippingQuoteId(),
			Counterparty:    req.GetShippingQuote().GetCounterparty(),
		},
		Outcome: &checkoutv1.AdvanceShippingQuoteWorkflowResponse_Outcome{Status: rejectPaymentIntentReply.GetOutcome().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelShippingQuoteWorkflow cancels a shipping quote.
func (s *ShippingQuoteWorkflowServer) CancelShippingQuoteWorkflow(ctx context.Context, req *checkoutv1.CancelShippingQuoteWorkflowRequest) (*checkoutv1.CancelShippingQuoteWorkflowResponse, error) {
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "cancel shipping quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	listPendingPaymentIntentsRequest := &checkoutv1.ListPendingPaymentIntentsRequest{}
	listPendingPaymentIntentsRequest.Context = req.GetContext()
	listPendingPaymentIntentsRequest.Filter = &checkoutv1.ListPendingPaymentIntentsRequest_Filter{
		Region:       req.GetShippingQuote().GetBillingAddress().GetRegion(),
		CreatedAfter: req.GetShippingQuote().GetIssuedAt(),
	}
	listPendingPaymentIntentsReply, err := s.paymentIntentApprovalClient.ListPendingPaymentIntents(ctx, listPendingPaymentIntentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentApprovalService/ListPendingPaymentIntents")
	}
	cloned := proto.Clone(req.GetShippingQuote()).(*checkoutv1.ShippingQuote)
	browseShippingQuotesRequest := &checkoutv1.BrowseShippingQuotesRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.BrowseShippingQuotesRequest_Filter{
			Region:       cloned.GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetShippingQuote().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetShippingQuote().GetShippingQuoteId(),
		},
	}
	browseShippingQuotesReply, err := grpcx.Call(ctx, s.shippingQuoteQueryClient.BrowseShippingQuotes, browseShippingQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/BrowseShippingQuotes")
	}
	return &checkoutv1.CancelShippingQuoteWorkflowResponse{
		Outcome: &checkoutv1.CancelShippingQuoteWorkflowResponse_Outcome{
			Message: listPendingPaymentIntentsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: browseShippingQuotesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ShippingQuote: &checkoutv1.ShippingQuote{
			ShippingQuoteId: req.GetShippingQuoteId(),
			IssuedAt:        req.GetShippingQuote().GetIssuedAt(),
		},
	}, nil
}

// PauseShippingQuoteWorkflow pauses a shipping quote.
func (s *ShippingQuoteWorkflowServer) PauseShippingQuoteWorkflow(ctx context.Context, req *checkoutv1.PauseShippingQuoteWorkflowRequest) (*checkoutv1.PauseShippingQuoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseShippingQuoteWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "pause shipping quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetShippingQuote().GetBillingAddress().GetRegion())
	resumeCheckoutSessionWorkflowRequest := &checkoutv1.ResumeCheckoutSessionWorkflowRequest{
		Context: req.GetContext(),
		CheckoutSession: &checkoutv1.CheckoutSession{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetShippingQuote().GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetShippingQuote().GetAudit().GetUpdatedAt(),
			},
			Counterparty: &checkoutv1.ShippingQuoteRef{
				DisplayName: req.GetShippingQuote().GetCounterparty().GetDisplayName(),
			},
			BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
	}
	send := s.resumeCheckoutSessionWorkflowSender(resumeCheckoutSessionWorkflowRequest)
	resumeCheckoutSessionWorkflowReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionWorkflowService/ResumeCheckoutSessionWorkflow")
	}
	shippingQuoteCache := cache.New[string, *checkoutv1.ShippingQuote](1)
	shippingQuoteCache.Put(req.GetContext().GetTraceId(), req.GetShippingQuote())
	cachedShippingQuote, _ := shippingQuoteCache.Get(req.GetContext().GetTraceId())
	approveSellerPayoutScheduleRequest := &sellersv1.ApproveSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ApproveSellerPayoutScheduleRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       cachedShippingQuote.GetCounterparty().GetEmail(),
				DisplayName: req.GetShippingQuote().GetCounterparty().GetDisplayName(),
			},
		},
	}
	approveSellerPayoutScheduleReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.ApproveSellerPayoutScheduleResponse, error) {
		return s.sellerPayoutScheduleApprovalClient.ApproveSellerPayoutSchedule(ctx, approveSellerPayoutScheduleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleApprovalService/ApproveSellerPayoutSchedule")
	}
	return &checkoutv1.PauseShippingQuoteWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resumeCheckoutSessionWorkflowReply.GetResult().GetCorrelationId(),
			Details:       approveSellerPayoutScheduleReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		ShippingQuote: &checkoutv1.ShippingQuote{
			ShippingQuoteId: req.GetShippingQuoteId(),
			Total:           req.GetShippingQuote().GetTotal(),
		},
	}, nil
}

// InspectShippingQuoteWorkflow returns a shipping quote.
func (s *ShippingQuoteWorkflowServer) InspectShippingQuoteWorkflow(ctx context.Context, req *checkoutv1.InspectShippingQuoteWorkflowRequest) (*checkoutv1.InspectShippingQuoteWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shipping_quote", req.GetShippingQuoteId(), "inspect_shipping_quote_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect shipping quote workflow", "tenant", req.GetContext().GetTenantId())
	return &checkoutv1.InspectShippingQuoteWorkflowResponse{
		ShippingQuote: &checkoutv1.ShippingQuote{ShippingQuoteId: req.GetShippingQuoteId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListShippingQuoteWorkflows lists shipping quotes a page at a time.
func (s *ShippingQuoteWorkflowServer) ListShippingQuoteWorkflows(ctx context.Context, req *checkoutv1.ListShippingQuoteWorkflowsRequest) (*checkoutv1.ListShippingQuoteWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list shipping quote workflows", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	listCheckoutSessionsRequest := &checkoutv1.ListCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.ListCheckoutSessionsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       labels,
		},
	}
	listCheckoutSessionsReply, err := grpcx.Call(ctx, s.checkoutSessionClient.ListCheckoutSessions, listCheckoutSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionService/ListCheckoutSessions")
	}
	labels2 := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key2, value2 := range req.GetFilter().GetLabels() {
		labels2[key2] = value2
	}
	step := &shippingQuoteWorkflowServerListShippingQuoteWorkflowsSummarizeShippingQuotesStep{request: &checkoutv1.SummarizeShippingQuotesRequest{
		Context:     req.GetContext(),
		Annotations: listCheckoutSessionsReply.GetResult().GetDetails(),
		Details:     &checkoutv1.SummarizeShippingQuotesRequest_Details{Filters: labels2},
	}}
	if err := s.runListShippingQuoteWorkflowsSummarizeShippingQuotes(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/SummarizeShippingQuotes")
	}
	summarizeShippingQuotesReply := step.reply
	return &checkoutv1.ListShippingQuoteWorkflowsResponse{
		PageInfo: &commonv1.PageInfo{
			NextPageToken: listCheckoutSessionsReply.GetPageInfo().GetNextPageToken(),
		},
		Result: &commonv1.OperationResult{
			Status:       summarizeShippingQuotesReply.GetResult().GetStatus(),
			ReferenceIds: summarizeShippingQuotesReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// SignalShippingQuoteWorkflow signals a shipping quote.
func (s *ShippingQuoteWorkflowServer) SignalShippingQuoteWorkflow(ctx context.Context, req *checkoutv1.SignalShippingQuoteWorkflowRequest) (*checkoutv1.SignalShippingQuoteWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveShippingQuote(ctx, req.GetContext().GetTenantId(), req.GetShippingQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save shipping quote")
	}
	s.log.InfoContext(ctx, "signal shipping quote workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShippingQuote().GetAudit().GetEtag())
	delegatePaymentIntentApprovalReply, err := s.paymentIntentApprovalClient.DelegatePaymentIntentApproval(ctx, &checkoutv1.DelegatePaymentIntentApprovalRequest{
		Context: req.GetContext(),
		Details: &checkoutv1.DelegatePaymentIntentApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetShippingQuote().GetCounterparty().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentApprovalService/DelegatePaymentIntentApproval")
	}
	return &checkoutv1.SignalShippingQuoteWorkflowResponse{
		ShippingQuote: &checkoutv1.ShippingQuote{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: delegatePaymentIntentApprovalReply.GetPaymentIntent().GetAssignee().GetDisplayName(),
			},
			ShippingQuoteId: req.GetShippingQuoteId(),
			BillingAddress:  req.GetShippingQuote().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// resumeCheckoutSessionWorkflowSender returns a function that sends one request to
// acme.checkout.v1.CheckoutSessionWorkflowService/ResumeCheckoutSessionWorkflow.
func (s *ShippingQuoteWorkflowServer) resumeCheckoutSessionWorkflowSender(request *checkoutv1.ResumeCheckoutSessionWorkflowRequest) func(context.Context) (*checkoutv1.ResumeCheckoutSessionWorkflowResponse, error) {
	return func(ctx context.Context) (*checkoutv1.ResumeCheckoutSessionWorkflowResponse, error) {
		return s.checkoutSessionWorkflowClient.ResumeCheckoutSessionWorkflow(ctx, request)
	}
}

// shippingQuoteWorkflowServerListShippingQuoteWorkflowsSummarizeShippingQuotesStep carries the request of one step of
// ListShippingQuoteWorkflows and, once it ran, its reply.
type shippingQuoteWorkflowServerListShippingQuoteWorkflowsSummarizeShippingQuotesStep struct {
	request *checkoutv1.SummarizeShippingQuotesRequest
	reply   *checkoutv1.SummarizeShippingQuotesResponse
}

func (s *ShippingQuoteWorkflowServer) runListShippingQuoteWorkflowsSummarizeShippingQuotes(ctx context.Context, step *shippingQuoteWorkflowServerListShippingQuoteWorkflowsSummarizeShippingQuotesStep) error {
	reply, err := s.shippingQuoteQueryClient.SummarizeShippingQuotes(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
