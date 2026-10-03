package checkout

import (
	"context"
	"errors"
	"io"
	"log/slog"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// CheckoutSessionApprovalServer implements `acme.checkout.v1.CheckoutSessionApprovalService`.
// Routes checkout sessions through approval, with escalation and delegation.
type CheckoutSessionApprovalServer struct {
	checkoutv1.UnimplementedCheckoutSessionApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	meteredCustomerValidation      meteredCustomerValidation
	loyaltyMemberApprovalPort      loyaltyMemberApprovalPort
	meteredCheckoutSessionQuery    meteredCheckoutSessionQuery
	shippingQuoteQueryClient       checkoutv1.ShippingQuoteQueryServiceClient
	loyaltyMemberApprovalClient    loyaltyv1.LoyaltyMemberApprovalServiceClient
	customerQueryClient            customersv1.CustomerQueryServiceClient
	conn                           *grpc.ClientConn
	householdQueryClient           customersv1.HouseholdQueryServiceClient
	bus                            *eventbus.Bus
	customerClient                 customersv1.CustomerServiceClient
	checkoutSessionClient          checkoutv1.CheckoutSessionServiceClient
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
}

// NewCheckoutSessionApprovalServer wires a CheckoutSessionApprovalServer.
func NewCheckoutSessionApprovalServer(deps platform.Deps) CheckoutSessionApprovalServer {
	return CheckoutSessionApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.checkout.v1.CheckoutSessionApprovalService"),
		meteredCustomerValidation:      meteredCustomerValidation{CustomerValidationServiceClient: customersv1.NewCustomerValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		loyaltyMemberApprovalPort:      newLoyaltyMemberApprovalPort(deps),
		meteredCheckoutSessionQuery:    meteredCheckoutSessionQuery{CheckoutSessionQueryServiceClient: checkoutv1.NewCheckoutSessionQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		shippingQuoteQueryClient:       checkoutv1.NewShippingQuoteQueryServiceClient(deps.Conn),
		loyaltyMemberApprovalClient:    loyaltyv1.NewLoyaltyMemberApprovalServiceClient(deps.Conn),
		customerQueryClient:            customersv1.NewCustomerQueryServiceClient(deps.Conn),
		conn:                           deps.Conn,
		householdQueryClient:           customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		bus:                            deps.Bus,
		customerClient:                 customersv1.NewCustomerServiceClient(deps.Conn),
		checkoutSessionClient:          checkoutv1.NewCheckoutSessionServiceClient(deps.Conn),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
	}
}

// SubmitCheckoutSession submits a checkout session.
func (s CheckoutSessionApprovalServer) SubmitCheckoutSession(ctx context.Context, req *checkoutv1.SubmitCheckoutSessionRequest) (*checkoutv1.SubmitCheckoutSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitCheckoutSession", "error", err)
		return nil, err
	}
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "submit checkout session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	checkoutSessionCache := cache.New[string, *checkoutv1.CheckoutSession](1)
	checkoutSessionCache.Put(req.GetContext().GetTraceId(), req.GetCheckoutSession())
	cachedCheckoutSession, _ := checkoutSessionCache.Get(req.GetContext().GetTraceId())
	explainCustomerDecisionRequest := &customersv1.ExplainCustomerDecisionRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			IdentityDocument: &customersv1.Customer_NationalId{NationalId: &customersv1.CustomerNationalId{Number: cachedCheckoutSession.GetCounterparty().GetNumber()}},
		},
	}
	explainCustomerDecisionReply, err := s.meteredCustomerValidation.ExplainCustomerDecision(ctx, explainCustomerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	return &checkoutv1.SubmitCheckoutSessionResponse{
		CheckoutSession: &checkoutv1.CheckoutSession{
			BillingAddress: &commonv1.PostalAddress{
				Lines: explainCustomerDecisionReply.GetCustomer().GetPrimaryAddress().GetLines(),
			},
			CheckoutSessionId: req.GetCheckoutSessionId(),
			Counterparty:      &checkoutv1.ShippingQuoteRef{Number: req.GetCheckoutSession().GetNumber()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveCheckoutSession approves a checkout session.
func (s CheckoutSessionApprovalServer) ApproveCheckoutSession(ctx context.Context, req *checkoutv1.ApproveCheckoutSessionRequest) (*checkoutv1.ApproveCheckoutSessionResponse, error) {
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "approve checkout session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	approveLoyaltyMemberRequest := &loyaltyv1.ApproveLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	approveLoyaltyMemberReply, err := s.loyaltyMemberApprovalPort.ApproveLoyaltyMember(ctx, approveLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberApprovalService/ApproveLoyaltyMember")
	}
	return &checkoutv1.ApproveCheckoutSessionResponse{
		Result: &commonv1.OperationResult{
			Status:       approveLoyaltyMemberReply.GetOutcome().GetStatus(),
			ReferenceIds: approveLoyaltyMemberReply.GetResult().GetReferenceIds(),
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			CheckoutSessionId: req.GetCheckoutSessionId(),
			Status:            req.GetCheckoutSession().GetStatus(),
		},
	}, nil
}

// RejectCheckoutSession rejects a checkout session.
func (s CheckoutSessionApprovalServer) RejectCheckoutSession(ctx context.Context, req *checkoutv1.RejectCheckoutSessionRequest) (*checkoutv1.RejectCheckoutSessionResponse, error) {
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "reject checkout session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	summarizeCheckoutSessionsRequest := &checkoutv1.SummarizeCheckoutSessionsRequest{
		Context:           req.GetContext(),
		CheckoutSessionId: req.GetCheckoutSessionId(),
		Annotations:       req.GetAnnotations(),
		Reason:            req.GetReason(),
	}
	summarizeCheckoutSessionsReply, err := s.meteredCheckoutSessionQuery.SummarizeCheckoutSessions(ctx, summarizeCheckoutSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/SummarizeCheckoutSessions")
	}
	lookupShippingQuoteRequest := newLookupShippingQuoteRequest(withLookupShippingQuoteContext(req.GetContext()), withLookupShippingQuoteShippingQuoteId(req.GetCheckoutSession().GetCounterparty().GetShippingQuoteId()))
	lookupShippingQuoteReply, err := s.shippingQuoteQueryClient.LookupShippingQuote(ctx, lookupShippingQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/LookupShippingQuote")
	}
	commentLoyaltyMemberApprovalRequest := buildCommentLoyaltyMemberApprovalRequestForRejectCheckoutSession(req)
	commentLoyaltyMemberApprovalReply, err := s.loyaltyMemberApprovalClient.CommentLoyaltyMemberApproval(ctx, commentLoyaltyMemberApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberApprovalService/CommentLoyaltyMemberApproval")
	}
	for _, line := range req.GetCheckoutSession().GetLines() {
		if _, err := s.customerQueryClient.FindCustomers(ctx, &customersv1.FindCustomersRequest{
			Context: req.GetContext(),
			Filter:  &customersv1.FindCustomersRequest_Filter{Labels: line.GetAttributes()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/FindCustomers")
		}
	}
	return &checkoutv1.RejectCheckoutSessionResponse{
		Result: &commonv1.OperationResult{
			Details: summarizeCheckoutSessionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &checkoutv1.RejectCheckoutSessionResponse_Outcome{
			Status: summarizeCheckoutSessionsReply.GetResult().GetStatus(),
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			BillingAddress: lookupShippingQuoteReply.GetShippingQuote().GetBillingAddress(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: commentLoyaltyMemberApprovalReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
				UpdatedBy: commentLoyaltyMemberApprovalReply.GetLoyaltyMember().GetAudit().GetUpdatedBy(),
			},
			CheckoutSessionId: req.GetCheckoutSessionId(),
			Counterparty:      &checkoutv1.ShippingQuoteRef{Number: req.GetCheckoutSession().GetNumber()},
		},
	}, nil
}

// EscalateCheckoutSession escalates a checkout session.
func (s CheckoutSessionApprovalServer) EscalateCheckoutSession(ctx context.Context, req *checkoutv1.EscalateCheckoutSessionRequest) (*checkoutv1.EscalateCheckoutSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateCheckoutSession", "error", err)
		return nil, err
	}
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "escalate checkout session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	resolveShippingQuotesRequest := &checkoutv1.ResolveShippingQuotesRequest{Context: req.GetContext()}
	resolveShippingQuotesReply, err := grpcx.Call(ctx, s.shippingQuoteQueryClient.ResolveShippingQuotes, resolveShippingQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/ResolveShippingQuotes")
	}
	summarizeShippingQuotesRequest := &checkoutv1.SummarizeShippingQuotesRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	summarizeShippingQuotesReply, err := grpcx.NewClient(s.conn, checkoutv1.NewShippingQuoteQueryServiceClient).SummarizeShippingQuotes(ctx, summarizeShippingQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/SummarizeShippingQuotes")
	}
	resolveCustomersRequest := &customersv1.ResolveCustomersRequest{Context: req.GetContext()}
	type resolveCustomersResult struct {
		reply *customersv1.ResolveCustomersResponse
		err   error
	}
	resolveCustomersResults := make(chan resolveCustomersResult, 1)
	go func() {
		reply, err := s.customerQueryClient.ResolveCustomers(ctx, resolveCustomersRequest)
		resolveCustomersResults <- resolveCustomersResult{reply, err}
	}()
	var resolveCustomersReply *customersv1.ResolveCustomersResponse
	select {
	case result := <-resolveCustomersResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.CustomerQueryService/ResolveCustomers")
		}
		resolveCustomersReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	ctx = tenancy.With(ctx, "region", req.GetCheckoutSession().GetBillingAddress().GetRegion())
	exportShippingQuotesStream, err := s.shippingQuoteQueryClient.ExportShippingQuotes(ctx, &checkoutv1.ExportShippingQuotesRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.ExportShippingQuotesRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetCheckoutSession().GetDueAt(),
			OwnerId:      resolveCustomersReply.GetResult().GetCorrelationId(),
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
	return &checkoutv1.EscalateCheckoutSessionResponse{
		Outcome: &checkoutv1.EscalateCheckoutSessionResponse_Outcome{
			Message:     resolveShippingQuotesReply.GetResult().GetMessage(),
			Status:      resolveShippingQuotesReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status:        summarizeShippingQuotesReply.GetResult().GetStatus(),
			CorrelationId: resolveCustomersReply.GetResult().GetCorrelationId(),
			Message:       resolveCustomersReply.GetResult().GetMessage(),
		},
		CheckoutSession: &checkoutv1.CheckoutSession{CheckoutSessionId: req.GetCheckoutSessionId()},
	}, nil
}

// ListPendingCheckoutSessions lists checkout sessions a page at a time.
func (s CheckoutSessionApprovalServer) ListPendingCheckoutSessions(ctx context.Context, req *checkoutv1.ListPendingCheckoutSessionsRequest) (*checkoutv1.ListPendingCheckoutSessionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending checkout sessions", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key2, value2 := range req.GetContext().GetBaggage() {
		baggage[key2] = value2
	}
	summarizeShippingQuotesReply, err := s.shippingQuoteQueryClient.SummarizeShippingQuotes(ctx, &checkoutv1.SummarizeShippingQuotesRequest{
		Context:     req.GetContext(),
		Annotations: labels,
		Details:     &checkoutv1.SummarizeShippingQuotesRequest_Details{Filters: baggage},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/SummarizeShippingQuotes")
	}
	step := &checkoutSessionApprovalServerListPendingCheckoutSessionsResolveHouseholdsStep{request: &customersv1.ResolveHouseholdsRequest{Context: req.GetContext()}}
	if err := s.runListPendingCheckoutSessionsResolveHouseholds(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ResolveHouseholds")
	}
	resolveHouseholdsReply := step.reply
	return &checkoutv1.ListPendingCheckoutSessionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeShippingQuotesReply.GetResult().GetCorrelationId(),
			ReferenceIds:  resolveHouseholdsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}, nil
}

// RecallCheckoutSession recalls a checkout session.
func (s CheckoutSessionApprovalServer) RecallCheckoutSession(ctx context.Context, req *checkoutv1.RecallCheckoutSessionRequest) (*checkoutv1.RecallCheckoutSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallCheckoutSession", "error", err)
		return nil, err
	}
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "recall checkout session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "checkout.recall_checkout_session.submit_loyalty_member",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"loyalty_member.card.holder_name": req.GetCheckoutSession().GetCard().GetHolderName(),
			"details.approver.email":          req.GetDetails().GetApprover().GetEmail(),
			"loyalty_member.audit.updated_by": req.GetCheckoutSession().GetAudit().GetUpdatedBy(),
			"context.tenant_id":               req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	cloneCustomerReply, err := s.customerClient.CloneCustomer(ctx, &customersv1.CloneCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{Etag: req.GetCheckoutSession().GetAudit().GetEtag()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CloneCustomer")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded checkoutv1.RecallCheckoutSessionRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	quoteConsentRecordRequest := &customersv1.QuoteConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{DisplayName: (&decoded).GetApprover().GetDisplayName()},
		},
		Details: &customersv1.QuoteConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: req.GetCheckoutSession().GetBillingAddress().GetRegion(),
			},
		},
	}
	quoteConsentRecordReply := new(customersv1.QuoteConsentRecordResponse)
	if err := s.conn.Invoke(ctx, customersConsentRecordCalculationServiceQuoteConsentRecordMethod, quoteConsentRecordRequest, quoteConsentRecordReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord")
	}
	return &checkoutv1.RecallCheckoutSessionResponse{
		CheckoutSession: &checkoutv1.CheckoutSession{
			Counterparty: &checkoutv1.ShippingQuoteRef{
				Number: cloneCustomerReply.GetCustomer().GetNationalId().GetNumber(),
			},
			Lines: []*checkoutv1.CheckoutSessionLine{&checkoutv1.CheckoutSessionLine{
				Attributes: cloneCustomerReply.GetCustomer().GetAttributes(),
			}},
			CheckoutSessionId: req.GetCheckoutSessionId(),
			IssuedAt:          req.GetCheckoutSession().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentCheckoutSessionApproval comments a checkout session.
func (s CheckoutSessionApprovalServer) CommentCheckoutSessionApproval(ctx context.Context, req *checkoutv1.CommentCheckoutSessionApprovalRequest) (*checkoutv1.CommentCheckoutSessionApprovalResponse, error) {
	if err := s.store.SaveCheckoutSession(ctx, req.GetContext().GetTenantId(), req.GetCheckoutSession()); err != nil {
		return nil, grpcx.Wrap(err, "save checkout session")
	}
	s.log.InfoContext(ctx, "comment checkout session approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCheckoutSession().GetAudit().GetEtag())
	exportShippingQuotesStream, err := s.shippingQuoteQueryClient.ExportShippingQuotes(ctx, &checkoutv1.ExportShippingQuotesRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.ExportShippingQuotesRequest_Filter{
			Region: req.GetCheckoutSession().GetBillingAddress().GetRegion(),
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
	previewPreferenceRequest := &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	previewPreferenceReply := new(customersv1.PreviewPreferenceResponse)
	if err := grpc.Invoke(ctx, "/acme.customers.v1.PreferenceValidationService/PreviewPreference", previewPreferenceRequest, previewPreferenceReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	updateCheckoutSessionReply, err := s.checkoutSessionClient.UpdateCheckoutSession(ctx, &checkoutv1.UpdateCheckoutSessionRequest{
		Context: req.GetContext(),
		CheckoutSession: &checkoutv1.CheckoutSession{
			Lines: []*checkoutv1.CheckoutSessionLine{&checkoutv1.CheckoutSessionLine{
				Tax: &checkoutv1.CheckoutSessionLineTax{
					Amount: previewPreferenceReply.GetPreference().GetDiscount().GetAmount(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetCheckoutSession().GetAudit().GetCreatedAt(),
				CreatedBy: req.GetCheckoutSession().GetAudit().GetCreatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionService/UpdateCheckoutSession")
	}
	cloned := proto.Clone(req.GetCheckoutSession()).(*checkoutv1.CheckoutSession)
	checkoutSessionCache := cache.New[string, *checkoutv1.CheckoutSession](1)
	checkoutSessionCache.Put(req.GetContext().GetTraceId(), req.GetCheckoutSession())
	cachedCheckoutSession, _ := checkoutSessionCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetCheckoutSession())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateConsentRecordsRequest := &customersv1.RecalculateConsentRecordsRequest{}
	recalculateConsentRecordsRequest.Context = req.GetContext()
	recalculateConsentRecordsRequest.Details = &customersv1.RecalculateConsentRecordsRequest_Details{
		Destination: &commonv1.PostalAddress{
			Region:      updateCheckoutSessionReply.GetCheckoutSession().GetBillingAddress().GetRegion(),
			CountryCode: cloned.GetBillingAddress().GetCountryCode(),
			Lines:       cachedCheckoutSession.GetBillingAddress().GetLines(),
		},
	}
	recalculateConsentRecordsRequest.ConsentRecord = &customersv1.ConsentRecord{Detail: packed}
	recalculateConsentRecordsReply, err := s.consentRecordCalculationClient.RecalculateConsentRecords(ctx, recalculateConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords")
	}
	return &checkoutv1.CommentCheckoutSessionApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: previewPreferenceReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		CheckoutSession: &checkoutv1.CheckoutSession{
			Counterparty: &checkoutv1.ShippingQuoteRef{
				Number: updateCheckoutSessionReply.GetCheckoutSession().GetNumber(),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: updateCheckoutSessionReply.GetCheckoutSession().GetBillingAddress().GetPostalCode(),
			},
			CheckoutSessionId: req.GetCheckoutSessionId(),
			Number:            req.GetCheckoutSession().GetNumber(),
		},
		Outcome: &checkoutv1.CommentCheckoutSessionApprovalResponse_Outcome{
			Status: recalculateConsentRecordsReply.GetResult().GetStatus(),
		},
	}, nil
}

// checkoutSessionApprovalServerListPendingCheckoutSessionsResolveHouseholdsStep carries the request of one step of
// ListPendingCheckoutSessions and, once it ran, its reply.
type checkoutSessionApprovalServerListPendingCheckoutSessionsResolveHouseholdsStep struct {
	request *customersv1.ResolveHouseholdsRequest
	reply   *customersv1.ResolveHouseholdsResponse
}

func (s CheckoutSessionApprovalServer) runListPendingCheckoutSessionsResolveHouseholds(ctx context.Context, step *checkoutSessionApprovalServerListPendingCheckoutSessionsResolveHouseholdsStep) error {
	reply, err := s.householdQueryClient.ResolveHouseholds(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
