package pricing

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PriceOverrideWorkflowServer implements `acme.pricing.v1.PriceOverrideWorkflowService`.
// Runs the long-lived workflow that takes a price override from start to finish.
type PriceOverrideWorkflowServer struct {
	pricingv1.UnimplementedPriceOverrideWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	restockDecisionReportingClient returnsv1.RestockDecisionReportingServiceClient
	clients                        *registry.Registry
	priceOverrideApprovalClient    pricingv1.PriceOverrideApprovalServiceClient
	meteredSynonymNotification     meteredSynonymNotification
	householdQueryClient           customersv1.HouseholdQueryServiceClient
	currencyRateLifecycleClient    pricingv1.CurrencyRateLifecycleServiceClient
	priceListApprovalClient        pricingv1.PriceListApprovalServiceClient
	accessPolicyClient             authorizationv1.AccessPolicyServiceClient
	customerClient                 customersv1.CustomerServiceClient
	restockDecisionLifecycleClient returnsv1.RestockDecisionLifecycleServiceClient
}

// NewPriceOverrideWorkflowServer wires a PriceOverrideWorkflowServer.
func NewPriceOverrideWorkflowServer(deps platform.Deps) *PriceOverrideWorkflowServer {
	return &PriceOverrideWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.pricing.v1.PriceOverrideWorkflowService"),
		restockDecisionReportingClient: returnsv1.NewRestockDecisionReportingServiceClient(deps.Conn),
		clients:                        deps.Clients,
		priceOverrideApprovalClient:    pricingv1.NewPriceOverrideApprovalServiceClient(deps.Conn),
		meteredSynonymNotification:     meteredSynonymNotification{SynonymNotificationServiceClient: searchv1.NewSynonymNotificationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		householdQueryClient:           customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		currencyRateLifecycleClient:    pricingv1.NewCurrencyRateLifecycleServiceClient(deps.Conn),
		priceListApprovalClient:        pricingv1.NewPriceListApprovalServiceClient(deps.Conn),
		accessPolicyClient:             authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		customerClient:                 customersv1.NewCustomerServiceClient(deps.Conn),
		restockDecisionLifecycleClient: returnsv1.NewRestockDecisionLifecycleServiceClient(deps.Conn),
	}
}

// StartPriceOverrideWorkflow starts a price override.
func (s *PriceOverrideWorkflowServer) StartPriceOverrideWorkflow(ctx context.Context, req *pricingv1.StartPriceOverrideWorkflowRequest) (*pricingv1.StartPriceOverrideWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "start price override workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded pricingv1.StartPriceOverrideWorkflowRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var reason string
	switch choice := req.GetPriceOverride().GetAction().(type) {
	case *pricingv1.PriceOverride_Route:
		reason = choice.Route.GetTarget()
	case *pricingv1.PriceOverride_Block:
		reason = choice.Block.GetReason()
	}
	shareRestockDecisionReportReply, err := s.restockDecisionReportingClient.ShareRestockDecisionReport(ctx, &returnsv1.ShareRestockDecisionReportRequest{
		Context: req.GetContext(),
		Details: &returnsv1.ShareRestockDecisionReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            req.GetPriceOverride().GetOwner().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Phone:            (&decoded).GetNotify().GetPhone(),
			},
		},
		Reason: reason,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/ShareRestockDecisionReport")
	}
	currencyRateApproval, err := registry.Lookup[pricingv1.CurrencyRateApprovalServiceClient](s.clients, "acme.pricing.v1.CurrencyRateApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cloned := proto.Clone(req.GetPriceOverride()).(*pricingv1.PriceOverride)
	escalateCurrencyRateRequest := &pricingv1.EscalateCurrencyRateRequest{
		Context: req.GetContext(),
		Details: &pricingv1.EscalateCurrencyRateRequest_Details{
			Approver: &identityv1.UserRef{Email: cloned.GetOwner().GetEmail()},
		},
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetPriceOverride().GetOwner().GetDisplayName(),
			},
		},
	}
	escalateCurrencyRateReply, err := currencyRateApproval.EscalateCurrencyRate(ctx, escalateCurrencyRateRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateApprovalService/EscalateCurrencyRate")
	}
	return &pricingv1.StartPriceOverrideWorkflowResponse{
		Outcome: &pricingv1.StartPriceOverrideWorkflowResponse_Outcome{
			EffectiveAt: shareRestockDecisionReportReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: escalateCurrencyRateReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Name:            req.GetPriceOverride().GetName(),
		},
	}, nil
}

// AdvancePriceOverrideWorkflow advances a price override.
func (s *PriceOverrideWorkflowServer) AdvancePriceOverrideWorkflow(ctx context.Context, req *pricingv1.AdvancePriceOverrideWorkflowRequest) (*pricingv1.AdvancePriceOverrideWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvancePriceOverrideWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "advance price override workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	for condition := range iterx.Each(req.GetPriceOverride().GetConditions(), func(candidate *pricingv1.PriceOverrideCondition) bool { return candidate != nil }) {
		if _, err := s.priceOverrideApprovalClient.DelegatePriceOverrideApproval(ctx, &pricingv1.DelegatePriceOverrideApprovalRequest{
			Context: req.GetContext(),
			PriceOverride: &pricingv1.PriceOverride{
				Conditions: []*pricingv1.PriceOverrideCondition{&pricingv1.PriceOverrideCondition{
					Negate:    condition.GetNegate(),
					Attribute: condition.GetAttribute(),
					Operator:  condition.GetOperator(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideApprovalService/DelegatePriceOverrideApproval")
		}
	}
	return &pricingv1.AdvancePriceOverrideWorkflowResponse{
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Effective:       req.GetPriceOverride().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelPriceOverrideWorkflow cancels a price override.
func (s *PriceOverrideWorkflowServer) CancelPriceOverrideWorkflow(ctx context.Context, req *pricingv1.CancelPriceOverrideWorkflowRequest) (*pricingv1.CancelPriceOverrideWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelPriceOverrideWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "cancel price override workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	restockDecisionReporting, ok := s.clients.Get("acme.returns.v1.RestockDecisionReportingService").(returnsv1.RestockDecisionReportingServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.returns.v1.RestockDecisionReportingService")
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	generateRestockDecisionReportRequest := &returnsv1.GenerateRestockDecisionReportRequest{
		Context: req.GetContext(),
		Reason:  tenancy.Value(ctx, "reason"),
	}
	generateRestockDecisionReportReply, err := restockDecisionReporting.GenerateRestockDecisionReport(ctx, generateRestockDecisionReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/GenerateRestockDecisionReport")
	}
	listSynonymSubscriptionsRequest := &searchv1.ListSynonymSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymSubscriptionsRequest_Filter{
			CreatedAfter: req.GetPriceOverride().GetAudit().GetUpdatedAt(),
			Labels:       req.GetAnnotations(),
			OwnerId:      req.GetPriceOverride().GetOwner().GetUserId(),
		},
	}
	listSynonymSubscriptionsReply, err := s.meteredSynonymNotification.ListSynonymSubscriptions(ctx, listSynonymSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/ListSynonymSubscriptions")
	}
	return &pricingv1.CancelPriceOverrideWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: generateRestockDecisionReportReply.GetResult().GetDetails(),
			Message: generateRestockDecisionReportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &pricingv1.CancelPriceOverrideWorkflowResponse_Outcome{
			Status: listSynonymSubscriptionsReply.GetResult().GetStatus(),
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Priority:        req.GetPriceOverride().GetPriority(),
		},
	}, nil
}

// RetryPriceOverrideWorkflow retrys a price override.
func (s *PriceOverrideWorkflowServer) RetryPriceOverrideWorkflow(ctx context.Context, req *pricingv1.RetryPriceOverrideWorkflowRequest) (*pricingv1.RetryPriceOverrideWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "retry price override workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	parameters := make(map[string]string, len(req.GetDetails().GetParameters()))
	for key, value := range req.GetDetails().GetParameters() {
		parameters[key] = value
	}
	suggestHouseholdsRequest := &customersv1.SuggestHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SuggestHouseholdsRequest_Filter{
			CreatedAfter: req.GetPriceOverride().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetPriceOverride().GetOwner().GetUserId(),
			Labels:       parameters,
		},
	}
	suggestHouseholdsReply, err := grpcx.Call(ctx, s.householdQueryClient.SuggestHouseholds, suggestHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/SuggestHouseholds")
	}
	return &pricingv1.RetryPriceOverrideWorkflowResponse{
		Outcome: &pricingv1.RetryPriceOverrideWorkflowResponse_Outcome{Status: suggestHouseholdsReply.GetResult().GetStatus()},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Parameters:      req.GetDetails().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PausePriceOverrideWorkflow pauses a price override.
func (s *PriceOverrideWorkflowServer) PausePriceOverrideWorkflow(ctx context.Context, req *pricingv1.PausePriceOverrideWorkflowRequest) (*pricingv1.PausePriceOverrideWorkflowResponse, error) {
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "pause price override workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	scheduleCurrencyRateExpiryRequest := buildScheduleCurrencyRateExpiryRequestForPausePriceOverrideWorkflow(req)
	scheduleCurrencyRateExpiryReply, err := s.currencyRateLifecycleClient.ScheduleCurrencyRateExpiry(ctx, scheduleCurrencyRateExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateLifecycleService/ScheduleCurrencyRateExpiry")
	}
	for condition := range iterx.Each(req.GetPriceOverride().GetConditions(), func(candidate *pricingv1.PriceOverrideCondition) bool { return candidate != nil }) {
		if _, err := s.priceListApprovalClient.SubmitPriceList(ctx, &pricingv1.SubmitPriceListRequest{
			Context: req.GetContext(),
			PriceList: &pricingv1.PriceList{
				Conditions: []*pricingv1.PriceListCondition{&pricingv1.PriceListCondition{Value: condition.GetValue()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceListApprovalService/SubmitPriceList")
		}
	}
	return &pricingv1.PausePriceOverrideWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scheduleCurrencyRateExpiryReply.GetCurrencyRate().GetCorrelationId(),
			Status:        "ok",
		},
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Status:          req.GetPriceOverride().GetStatus(),
		},
	}, nil
}

// ResumePriceOverrideWorkflow resumes a price override.
func (s *PriceOverrideWorkflowServer) ResumePriceOverrideWorkflow(ctx context.Context, req *pricingv1.ResumePriceOverrideWorkflowRequest) (*pricingv1.ResumePriceOverrideWorkflowResponse, error) {
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "resume price override workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	watchAccessPoliciesStream, err := s.accessPolicyClient.WatchAccessPolicies(ctx, &authorizationv1.WatchAccessPoliciesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
	}
	for {
		received, err := watchAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
		}
		_ = received
	}
	recallSubscriptionPlanRequest := &subscriptionsv1.RecallSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner:  &identityv1.UserRef{UserId: req.GetPriceOverride().GetOwner().GetUserId()},
			Action: &subscriptionsv1.SubscriptionPlan_Discount{Discount: &subscriptionsv1.SubscriptionPlanDiscountAction{Amount: req.GetPriceOverride().GetDiscount().GetAmount()}},
		},
	}
	recallSubscriptionPlanReply, err := recallSubscriptionPlanCall(ctx, recallSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/RecallSubscriptionPlan")
	}
	return &pricingv1.ResumePriceOverrideWorkflowResponse{
		PriceOverride: &pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{
				DisplayName: recallSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetDisplayName(),
			},
			PriceOverrideId: req.GetPriceOverrideId(),
			Parameters:      req.GetDetails().GetParameters(),
		},
		Outcome: &pricingv1.ResumePriceOverrideWorkflowResponse_Outcome{
			Message: recallSubscriptionPlanReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectPriceOverrideWorkflow returns a price override.
func (s *PriceOverrideWorkflowServer) InspectPriceOverrideWorkflow(ctx context.Context, req *pricingv1.InspectPriceOverrideWorkflowRequest) (*pricingv1.InspectPriceOverrideWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectPriceOverrideWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_override", req.GetPriceOverrideId(), "inspect_price_override_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect price override workflow", "tenant", req.GetContext().GetTenantId())
	return &pricingv1.InspectPriceOverrideWorkflowResponse{
		PriceOverride: &pricingv1.PriceOverride{PriceOverrideId: req.GetPriceOverrideId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPriceOverrideWorkflows lists price overrides a page at a time.
func (s *PriceOverrideWorkflowServer) ListPriceOverrideWorkflows(ctx context.Context, req *pricingv1.ListPriceOverrideWorkflowsRequest) (*pricingv1.ListPriceOverrideWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list price override workflows", "tenant", req.GetContext().GetTenantId())
	createCustomerRequest := &customersv1.CreateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			PrimaryAddress:      &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	createCustomerReply, err := grpcx.Call(ctx, s.customerClient.CreateCustomer, createCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CreateCustomer")
	}
	scheduleRestockDecisionExpiryRequest := newScheduleRestockDecisionExpiryRequest(withScheduleRestockDecisionExpiryContext(req.GetContext()), withScheduleRestockDecisionExpiryDetails(&returnsv1.ScheduleRestockDecisionExpiryRequest_Details{
		Notify: &commonv1.ContactInfo{
			Phone: createCustomerReply.GetCustomer().GetContact().GetPhone(),
		},
	}), withScheduleRestockDecisionExpiryRestockDecision(&returnsv1.RestockDecision{
		Owner: &identityv1.UserRef{
			DisplayName: createCustomerReply.GetCustomer().GetRelationshipManager().GetDisplayName(),
			UserId:      createCustomerReply.GetCustomer().GetRelationshipManager().GetUserId(),
		},
	}))
	scheduleRestockDecisionExpiryReply, err := s.restockDecisionLifecycleClient.ScheduleRestockDecisionExpiry(ctx, scheduleRestockDecisionExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionLifecycleService/ScheduleRestockDecisionExpiry")
	}
	return &pricingv1.ListPriceOverrideWorkflowsResponse{
		PriceOverrides: []*pricingv1.PriceOverride{&pricingv1.PriceOverride{
			Owner: &identityv1.UserRef{
				Email: createCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
			Name: scheduleRestockDecisionExpiryReply.GetRestockDecision().GetName(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalPriceOverrideWorkflow signals a price override.
func (s *PriceOverrideWorkflowServer) SignalPriceOverrideWorkflow(ctx context.Context, req *pricingv1.SignalPriceOverrideWorkflowRequest) (*pricingv1.SignalPriceOverrideWorkflowResponse, error) {
	if err := s.store.SavePriceOverride(ctx, req.GetContext().GetTenantId(), req.GetPriceOverride()); err != nil {
		return nil, grpcx.Wrap(err, "save price override")
	}
	s.log.InfoContext(ctx, "signal price override workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceOverride().GetAudit().GetEtag())
	streamRestockDecisionMetricsStream, err := s.restockDecisionReportingClient.StreamRestockDecisionMetrics(ctx, &returnsv1.StreamRestockDecisionMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/StreamRestockDecisionMetrics")
	}
	for {
		received, err := streamRestockDecisionMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/StreamRestockDecisionMetrics")
		}
		_ = received
	}
	return &pricingv1.SignalPriceOverrideWorkflowResponse{
		PriceOverride: &pricingv1.PriceOverride{
			PriceOverrideId: req.GetPriceOverrideId(),
			Owner:           req.GetPriceOverride().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
