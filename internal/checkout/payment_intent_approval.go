package checkout

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	messagingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/messaging/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentIntentApprovalServer implements `acme.checkout.v1.PaymentIntentApprovalService`.
// Routes payment intents through approval, with escalation and delegation.
type PaymentIntentApprovalServer struct {
	checkoutv1.UnimplementedPaymentIntentApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	auditEventClient                   auditv1.AuditEventServiceClient
	accessPolicyQueryClient            authorizationv1.AccessPolicyQueryServiceClient
	shippingQuoteQueryClient           checkoutv1.ShippingQuoteQueryServiceClient
	searchIndexNotificationClient      searchv1.SearchIndexNotificationServiceClient
	preferenceCalculationClient        customersv1.PreferenceCalculationServiceClient
	conn                               *grpc.ClientConn
	preferenceCalculationOnce          sync.Once
	preferenceCalculationCached        customersv1.PreferenceCalculationServiceClient
	shippingQuoteApprovalClient        checkoutv1.ShippingQuoteApprovalServiceClient
	householdQueryClient               customersv1.HouseholdQueryServiceClient
	sellerListingReportingClient       sellersv1.SellerListingReportingServiceClient
	clients                            *registry.Registry
	fulfillmentExceptionWorkflowClient fulfillmentv1.FulfillmentExceptionWorkflowServiceClient
	checkoutSessionClient              checkoutv1.CheckoutSessionServiceClient
	conversationClient                 messagingv1.ConversationServiceClient
	checkoutSessionQueryClient         checkoutv1.CheckoutSessionQueryServiceClient
	paymentIntentValidationClient      checkoutv1.PaymentIntentValidationServiceClient
}

// NewPaymentIntentApprovalServer wires a PaymentIntentApprovalServer.
func NewPaymentIntentApprovalServer(deps platform.Deps) *PaymentIntentApprovalServer {
	return &PaymentIntentApprovalServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.checkout.v1.PaymentIntentApprovalService"),
		auditEventClient:                   auditv1.NewAuditEventServiceClient(deps.Conn),
		accessPolicyQueryClient:            authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn),
		shippingQuoteQueryClient:           checkoutv1.NewShippingQuoteQueryServiceClient(deps.Conn),
		searchIndexNotificationClient:      searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		preferenceCalculationClient:        customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		shippingQuoteApprovalClient:        checkoutv1.NewShippingQuoteApprovalServiceClient(deps.Conn),
		householdQueryClient:               customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		sellerListingReportingClient:       sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		clients:                            deps.Clients,
		fulfillmentExceptionWorkflowClient: fulfillmentv1.NewFulfillmentExceptionWorkflowServiceClient(deps.Conn),
		checkoutSessionClient:              checkoutv1.NewCheckoutSessionServiceClient(deps.Conn),
		conversationClient:                 messagingv1.NewConversationServiceClient(deps.Conn),
		checkoutSessionQueryClient:         checkoutv1.NewCheckoutSessionQueryServiceClient(deps.Conn),
		paymentIntentValidationClient:      checkoutv1.NewPaymentIntentValidationServiceClient(deps.Conn),
	}
}

// SubmitPaymentIntent submits a payment intent.
func (s *PaymentIntentApprovalServer) SubmitPaymentIntent(ctx context.Context, req *checkoutv1.SubmitPaymentIntentRequest) (*checkoutv1.SubmitPaymentIntentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitPaymentIntent", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "submit payment intent", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	paymentIntentCache := cache.New[string, *checkoutv1.PaymentIntent](1)
	paymentIntentCache.Put(req.GetContext().GetTraceId(), req.GetPaymentIntent())
	cachedPaymentIntent, _ := paymentIntentCache.Get(req.GetContext().GetTraceId())
	watchAuditEventsStream, err := s.auditEventClient.WatchAuditEvents(ctx, &auditv1.WatchAuditEventsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:             req.GetPaymentIntent().GetRequester().GetName(),
			Phone:            cachedPaymentIntent.GetRequester().GetPhone(),
			PreferredChannel: req.GetPaymentIntent().GetRequester().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
	}
	for {
		received, err := watchAuditEventsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
		}
		_ = received
	}
	return &checkoutv1.SubmitPaymentIntentResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Priority:        req.GetPaymentIntent().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApprovePaymentIntent approves a payment intent.
func (s *PaymentIntentApprovalServer) ApprovePaymentIntent(ctx context.Context, req *checkoutv1.ApprovePaymentIntentRequest) (*checkoutv1.ApprovePaymentIntentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "approve payment intent", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	customFields := make(map[string]string, len(req.GetPaymentIntent().GetCustomFields()))
	for key, value := range req.GetPaymentIntent().GetCustomFields() {
		customFields[key] = value
	}
	summarizeAccessPoliciesReply, err := s.accessPolicyQueryClient.SummarizeAccessPolicies(ctx, &authorizationv1.SummarizeAccessPoliciesRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &authorizationv1.SummarizeAccessPoliciesRequest_Details{Filters: customFields},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/SummarizeAccessPolicies")
	}
	jobs := make(chan *checkoutv1.PaymentIntentStatusChange)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if _, err := s.shippingQuoteQueryClient.FindShippingQuotes(workCtx, &checkoutv1.FindShippingQuotesRequest{
					Context: req.GetContext(),
					Filter: &checkoutv1.FindShippingQuotesRequest_Filter{
						CreatedAfter: item.GetChangedAt(),
						OwnerId:      summarizeAccessPoliciesReply.GetResult().GetCorrelationId(),
						Labels:       req.GetPaymentIntent().GetCustomFields(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPaymentIntent().GetHistory() {
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
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/FindShippingQuotes")
	}
	return &checkoutv1.ApprovePaymentIntentResponse{
		Result: &commonv1.OperationResult{
			Details: summarizeAccessPoliciesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &checkoutv1.ApprovePaymentIntentResponse_Outcome{
			Message: summarizeAccessPoliciesReply.GetResult().GetMessage(),
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Requester:       req.GetPaymentIntent().GetRequester(),
		},
	}, nil
}

// RejectPaymentIntent rejects a payment intent.
func (s *PaymentIntentApprovalServer) RejectPaymentIntent(ctx context.Context, req *checkoutv1.RejectPaymentIntentRequest) (*checkoutv1.RejectPaymentIntentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "reject payment intent", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	var holder struct{ Email string }
	if record := req.GetPaymentIntent().GetRequester(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetPaymentIntent().GetRequester().GetPreferredChannel())
	packed, err := anypb.New(req.GetPaymentIntent())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendSearchIndexTestNoticeRequest := &searchv1.SendSearchIndexTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchIndexTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            holder.Email,
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
				Name:             req.GetPaymentIntent().GetRequester().GetName(),
			},
			Payload: packed,
		},
	}
	sendSearchIndexTestNoticeReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*searchv1.SendSearchIndexTestNoticeResponse, error) {
		return s.searchIndexNotificationClient.SendSearchIndexTestNotice(ctx, sendSearchIndexTestNoticeRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SendSearchIndexTestNotice")
	}
	for _, comment := range req.GetPaymentIntent().GetComments() {
		if _, err := s.preferenceCalculationClient.QuotePreference(ctx, &customersv1.QuotePreferenceRequest{
			Context: req.GetContext(),
			Preference: &customersv1.Preference{
				Audit: &commonv1.AuditInfo{CreatedAt: comment.GetCreatedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
		}
	}
	recalculatePreferencesRequest := &customersv1.RecalculatePreferencesRequest{Context: req.GetContext()}
	recalculatePreferencesReply, err := s.preferenceCalculationLazy().RecalculatePreferences(ctx, recalculatePreferencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/RecalculatePreferences")
	}
	explainHouseholdCalculationRequest := &customersv1.ExplainHouseholdCalculationRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{UserId: req.GetPaymentIntent().GetAssignee().GetUserId()},
					Location: &commonv1.PostalAddress{
						PostalCode: sendSearchIndexTestNoticeReply.GetSearchIndex().GetAddress().GetPostalCode(),
						Region:     sendSearchIndexTestNoticeReply.GetSearchIndex().GetAddress().GetRegion(),
					},
				}},
			}},
		},
	}
	explainHouseholdCalculationReply := new(customersv1.ExplainHouseholdCalculationResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersHouseholdCalculationServiceName, "ExplainHouseholdCalculation"), explainHouseholdCalculationRequest, explainHouseholdCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation")
	}
	_ = recalculatePreferencesReply
	return &checkoutv1.RejectPaymentIntentResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: sendSearchIndexTestNoticeReply.GetResult().GetCorrelationId(),
			Message:       explainHouseholdCalculationReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Requester:       req.GetPaymentIntent().GetRequester(),
		},
	}, nil
}

// EscalatePaymentIntent escalates a payment intent.
func (s *PaymentIntentApprovalServer) EscalatePaymentIntent(ctx context.Context, req *checkoutv1.EscalatePaymentIntentRequest) (*checkoutv1.EscalatePaymentIntentResponse, error) {
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "escalate payment intent", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	for _, comment := range req.GetPaymentIntent().GetComments() {
		if _, err := s.shippingQuoteApprovalClient.ApproveShippingQuote(ctx, &checkoutv1.ApproveShippingQuoteRequest{
			Context: req.GetContext(),
			Details: &checkoutv1.ApproveShippingQuoteRequest_Details{
				Approver: &identityv1.UserRef{Email: comment.GetAuthorEmail()},
			},
			ShippingQuote: &checkoutv1.ShippingQuote{
				Audit: &commonv1.AuditInfo{
					CreatedAt: comment.GetCreatedAt(),
					Etag:      req.GetPaymentIntent().GetAudit().GetEtag(),
				},
				Counterparty: &customersv1.CustomerRef{
					DisplayName: req.GetPaymentIntent().GetAssignee().GetDisplayName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteApprovalService/ApproveShippingQuote")
		}
	}
	return &checkoutv1.EscalatePaymentIntentResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Priority:        req.GetPaymentIntent().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingPaymentIntents lists payment intents a page at a time.
func (s *PaymentIntentApprovalServer) ListPendingPaymentIntents(ctx context.Context, req *checkoutv1.ListPendingPaymentIntentsRequest) (*checkoutv1.ListPendingPaymentIntentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingPaymentIntents", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending payment intents", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	filterCache := cache.New[string, *checkoutv1.ListPendingPaymentIntentsRequest_Filter](1)
	filterCache.Put(req.GetContext().GetTraceId(), req.GetFilter())
	cachedFilter, _ := filterCache.Get(req.GetContext().GetTraceId())
	browseHouseholdsRequest := &customersv1.BrowseHouseholdsRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
		Filter: &customersv1.BrowseHouseholdsRequest_Filter{
			Region: req.GetFilter().GetRegion(),
			Labels: labels,
			Query:  cachedFilter.GetQuery(),
		},
	}
	var browseHouseholdsReply *customersv1.BrowseHouseholdsResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.HouseholdQueryService/BrowseHouseholds", func(ctx context.Context) error {
		answer, err := s.householdQueryClient.BrowseHouseholds(ctx, browseHouseholdsRequest)
		browseHouseholdsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/BrowseHouseholds")
	}
	return &checkoutv1.ListPendingPaymentIntentsResponse{
		Result: &commonv1.OperationResult{
			Message: browseHouseholdsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// DelegatePaymentIntentApproval delegates a payment intent.
func (s *PaymentIntentApprovalServer) DelegatePaymentIntentApproval(ctx context.Context, req *checkoutv1.DelegatePaymentIntentApprovalRequest) (*checkoutv1.DelegatePaymentIntentApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DelegatePaymentIntentApproval", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "delegate payment intent approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := paymentIntentApprovalServerResolveShippingQuotesSteps[mode]
	resolveShippingQuotesRequest := &checkoutv1.ResolveShippingQuotesRequest{Context: req.GetContext()}
	resolveShippingQuotesReply, err := run(s, ctx, resolveShippingQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/ResolveShippingQuotes")
	}
	ctx = tenancy.With(ctx, "created_by", req.GetPaymentIntent().GetAudit().GetCreatedBy())
	approveShippingQuoteRequest := &checkoutv1.ApproveShippingQuoteRequest{
		Context: req.GetContext(),
		Details: &checkoutv1.ApproveShippingQuoteRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		ShippingQuote: &checkoutv1.ShippingQuote{
			Audit: &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
		},
	}
	approveShippingQuoteReply, err := grpcx.Call(ctx, s.shippingQuoteApprovalClient.ApproveShippingQuote, approveShippingQuoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteApprovalService/ApproveShippingQuote")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleSellerListingReportRequest := &sellersv1.ScheduleSellerListingReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
	}
	type scheduleSellerListingReportResult struct {
		reply *sellersv1.ScheduleSellerListingReportResponse
		err   error
	}
	scheduleSellerListingReportResults := make(chan scheduleSellerListingReportResult, 1)
	go func() {
		reply, err := s.sellerListingReportingClient.ScheduleSellerListingReport(ctx, scheduleSellerListingReportRequest)
		scheduleSellerListingReportResults <- scheduleSellerListingReportResult{reply, err}
	}()
	var scheduleSellerListingReportReply *sellersv1.ScheduleSellerListingReportResponse
	select {
	case result := <-scheduleSellerListingReportResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerListingReportingService/ScheduleSellerListingReport")
		}
		scheduleSellerListingReportReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &checkoutv1.DelegatePaymentIntentApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: resolveShippingQuotesReply.GetResult().GetDetails(),
			Message: scheduleSellerListingReportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &checkoutv1.DelegatePaymentIntentApprovalResponse_Outcome{
			ReferenceId: approveShippingQuoteReply.GetOutcome().GetReferenceId(),
			EffectiveAt: approveShippingQuoteReply.GetOutcome().GetEffectiveAt(),
		},
		PaymentIntent: &checkoutv1.PaymentIntent{
			PaymentIntentId: req.GetPaymentIntentId(),
			Status:          req.GetPaymentIntent().GetStatus(),
		},
	}, nil
}

// RecallPaymentIntent recalls a payment intent.
func (s *PaymentIntentApprovalServer) RecallPaymentIntent(ctx context.Context, req *checkoutv1.RecallPaymentIntentRequest) (*checkoutv1.RecallPaymentIntentResponse, error) {
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "recall payment intent", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	householdCalculation, ok := s.clients.Get("acme.customers.v1.HouseholdCalculationService").(customersv1.HouseholdCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.customers.v1.HouseholdCalculationService")
	}
	encoded, err := protojson.Marshal(req.GetPaymentIntent())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded checkoutv1.PaymentIntent
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	encoded2, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 checkoutv1.RecallPaymentIntentRequest_Details
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	encoded3, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded3 checkoutv1.RecallPaymentIntentRequest_Details
	if err := protojson.Unmarshal(encoded3, &decoded3); err != nil {
		return nil, err
	}
	estimateHouseholdRequest := &customersv1.EstimateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{Etag: (&decoded).GetAudit().GetEtag()},
					Manager: &identityv1.UserRef{
						Email:       (&decoded2).GetApprover().GetEmail(),
						DisplayName: (&decoded3).GetApprover().GetDisplayName(),
					},
				}},
				Manager: &identityv1.UserRef{Email: req.GetPaymentIntent().GetRequester().GetEmail()},
			}},
		},
	}
	estimateHouseholdReply, err := householdCalculation.EstimateHousehold(ctx, estimateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/EstimateHousehold")
	}
	ctx = tenancy.With(ctx, "address", req.GetPaymentIntent().GetChannel().GetAddress())
	signalFulfillmentExceptionWorkflowReply, err := s.fulfillmentExceptionWorkflowClient.SignalFulfillmentExceptionWorkflow(ctx, &fulfillmentv1.SignalFulfillmentExceptionWorkflowRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Assignee: &identityv1.UserRef{UserId: req.GetPaymentIntent().GetAssignee().GetUserId()},
			Origin:   &fulfillmentv1.FulfillmentException_Channel{Channel: &fulfillmentv1.FulfillmentExceptionChannelOrigin{Address: tenancy.Value(ctx, "address")}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionWorkflowService/SignalFulfillmentExceptionWorkflow")
	}
	resolveShippingQuotesRequest := &checkoutv1.ResolveShippingQuotesRequest{Context: req.GetContext()}
	resolveShippingQuotesReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*checkoutv1.ResolveShippingQuotesResponse, error) {
		return s.shippingQuoteQueryClient.ResolveShippingQuotes(ctx, resolveShippingQuotesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/ResolveShippingQuotes")
	}
	batchGetCheckoutSessionsRequest := &checkoutv1.BatchGetCheckoutSessionsRequest{Context: req.GetContext()}
	var batchGetCheckoutSessionsReply *checkoutv1.BatchGetCheckoutSessionsResponse
	if err := pipeline.Run(ctx, "acme.checkout.v1.CheckoutSessionService/BatchGetCheckoutSessions", func(ctx context.Context) error {
		answer, err := s.checkoutSessionClient.BatchGetCheckoutSessions(ctx, batchGetCheckoutSessionsRequest)
		batchGetCheckoutSessionsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionService/BatchGetCheckoutSessions")
	}
	return &checkoutv1.RecallPaymentIntentResponse{
		PaymentIntent: &checkoutv1.PaymentIntent{
			Audit: &commonv1.AuditInfo{
				CreatedBy: estimateHouseholdReply.GetHousehold().GetAudit().GetCreatedBy(),
			},
			Comments: []*checkoutv1.PaymentIntentComment{&checkoutv1.PaymentIntentComment{
				CreatedAt: estimateHouseholdReply.GetHousehold().GetAudit().GetCreatedAt(),
			}},
			Requester: &commonv1.ContactInfo{
				Name: signalFulfillmentExceptionWorkflowReply.GetFulfillmentException().GetRequester().GetName(),
			},
			Tags:            signalFulfillmentExceptionWorkflowReply.GetFulfillmentException().GetTags(),
			PaymentIntentId: req.GetPaymentIntentId(),
			Priority:        req.GetPaymentIntent().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: resolveShippingQuotesReply.GetResult().GetCorrelationId(),
			Details:       resolveShippingQuotesReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Outcome: &checkoutv1.RecallPaymentIntentResponse_Outcome{
			Message: batchGetCheckoutSessionsReply.GetResult().GetMessage(),
		},
	}, nil
}

// GetPaymentIntentApprovalStatus returns a payment intent.
func (s *PaymentIntentApprovalServer) GetPaymentIntentApprovalStatus(ctx context.Context, req *checkoutv1.GetPaymentIntentApprovalStatusRequest) (*checkoutv1.GetPaymentIntentApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_intent", req.GetPaymentIntentId(), "get_payment_intent_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment intent approval status", "tenant", req.GetContext().GetTenantId())
	listConversationsRequest := &messagingv1.ListConversationsRequest{
		Context: req.GetContext(),
		Filter: &messagingv1.ListConversationsRequest_Filter{
			OwnerId:      req.GetPaymentIntentId(),
			Labels:       req.GetContext().GetBaggage(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	}
	listConversationsReply, err := messagingv1.ConversationServiceClient.ListConversations(s.conversationClient, ctx, listConversationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.messaging.v1.ConversationService/ListConversations")
	}
	return &checkoutv1.GetPaymentIntentApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listConversationsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PaymentIntent: &checkoutv1.PaymentIntent{PaymentIntentId: req.GetPaymentIntentId()},
	}, nil
}

// CommentPaymentIntentApproval comments a payment intent.
func (s *PaymentIntentApprovalServer) CommentPaymentIntentApproval(ctx context.Context, req *checkoutv1.CommentPaymentIntentApprovalRequest) (*checkoutv1.CommentPaymentIntentApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentIntent(ctx, req.GetContext().GetTenantId(), req.GetPaymentIntent()); err != nil {
		return nil, grpcx.Wrap(err, "save payment intent")
	}
	s.log.InfoContext(ctx, "comment payment intent approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentIntent().GetAudit().GetEtag())
	findCheckoutSessionsRequest := &checkoutv1.FindCheckoutSessionsRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.FindCheckoutSessionsRequest_Filter{
			CreatedAfter: req.GetPaymentIntent().GetAudit().GetCreatedAt(),
		},
	}
	var findCheckoutSessionsReply *checkoutv1.FindCheckoutSessionsResponse
	if err := pipeline.Run(ctx, "acme.checkout.v1.CheckoutSessionQueryService/FindCheckoutSessions", func(ctx context.Context) error {
		answer, err := s.checkoutSessionQueryClient.FindCheckoutSessions(ctx, findCheckoutSessionsRequest)
		findCheckoutSessionsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionQueryService/FindCheckoutSessions")
	}
	explainHouseholdCalculationRequest := &customersv1.ExplainHouseholdCalculationRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{CreatedAt: req.GetPaymentIntent().GetAudit().GetCreatedAt()},
				}},
			}},
		},
	}
	explainHouseholdCalculationReply := new(customersv1.ExplainHouseholdCalculationResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.HouseholdCalculationService_ExplainHouseholdCalculation_FullMethodName, explainHouseholdCalculationRequest, explainHouseholdCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation")
	}
	exportShippingQuotesStream, err := s.shippingQuoteQueryClient.ExportShippingQuotes(ctx, &checkoutv1.ExportShippingQuotesRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.ExportShippingQuotesRequest_Filter{
			Region:       explainHouseholdCalculationReply.GetHousehold().GetLocation().GetRegion(),
			CreatedAfter: explainHouseholdCalculationReply.GetHousehold().GetAudit().GetCreatedAt(),
			OwnerId:      explainHouseholdCalculationReply.GetHousehold().GetManager().GetUserId(),
			Labels:       explainHouseholdCalculationReply.GetResult().GetDetails(),
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
	scorePaymentIntentReply, err := s.paymentIntentValidationClient.ScorePaymentIntent(ctx, &checkoutv1.ScorePaymentIntentRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			Comments: []*checkoutv1.PaymentIntentComment{&checkoutv1.PaymentIntentComment{CreatedAt: req.GetPaymentIntent().GetAudit().GetCreatedAt()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/ScorePaymentIntent")
	}
	return &checkoutv1.CommentPaymentIntentApprovalResponse{
		Outcome: &checkoutv1.CommentPaymentIntentApprovalResponse_Outcome{Status: findCheckoutSessionsReply.GetResult().GetStatus()},
		PaymentIntent: &checkoutv1.PaymentIntent{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainHouseholdCalculationReply.GetHousehold().GetAudit().GetCreatedBy(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: scorePaymentIntentReply.GetPaymentIntent().GetCustomer().GetDisplayName(),
			},
			PaymentIntentId: req.GetPaymentIntentId(),
			Priority:        req.GetPaymentIntent().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// preferenceCalculationLazy dials acme.customers.v1.PreferenceCalculationService the first time a handler needs it.
func (s *PaymentIntentApprovalServer) preferenceCalculationLazy() customersv1.PreferenceCalculationServiceClient {
	s.preferenceCalculationOnce.Do(func() { s.preferenceCalculationCached = customersv1.NewPreferenceCalculationServiceClient(s.conn) })
	return s.preferenceCalculationCached
}

// paymentIntentApprovalServerResolveShippingQuotesSteps are the ways to run
// acme.checkout.v1.ShippingQuoteQueryService/ResolveShippingQuotes, by name.
var paymentIntentApprovalServerResolveShippingQuotesSteps = map[string]func(*PaymentIntentApprovalServer, context.Context, *checkoutv1.ResolveShippingQuotesRequest) (*checkoutv1.ResolveShippingQuotesResponse, error){
	"default": (*PaymentIntentApprovalServer).resolveShippingQuotesStep,
	"audited": (*PaymentIntentApprovalServer).resolveShippingQuotesAuditedStep,
}

func (s *PaymentIntentApprovalServer) resolveShippingQuotesStep(ctx context.Context, request *checkoutv1.ResolveShippingQuotesRequest) (*checkoutv1.ResolveShippingQuotesResponse, error) {
	return s.shippingQuoteQueryClient.ResolveShippingQuotes(ctx, request)
}

func (s *PaymentIntentApprovalServer) resolveShippingQuotesAuditedStep(ctx context.Context, request *checkoutv1.ResolveShippingQuotesRequest) (*checkoutv1.ResolveShippingQuotesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.checkout.v1.ShippingQuoteQueryService/ResolveShippingQuotes")
	return s.shippingQuoteQueryClient.ResolveShippingQuotes(ctx, request)
}
