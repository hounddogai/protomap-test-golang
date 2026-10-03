package subscriptions

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
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

// SubscriptionPlanWorkflowServer implements `acme.subscriptions.v1.SubscriptionPlanWorkflowService`.
// Runs the long-lived workflow that takes a subscription plan from start to finish.
type SubscriptionPlanWorkflowServer struct {
	subscriptionsv1.UnimplementedSubscriptionPlanWorkflowServiceServer
	store                                 *Store
	log                                   *slog.Logger
	conn                                  *grpc.ClientConn
	customerSegmentValidationClient       customersv1.CustomerSegmentValidationServiceClient
	subscriptionClient                    subscriptionsv1.SubscriptionServiceClient
	bus                                   *eventbus.Bus
	consentRecordCalculationClient        customersv1.ConsentRecordCalculationServiceClient
	householdQueryClient                  customersv1.HouseholdQueryServiceClient
	renewalScheduleApprovalClient         subscriptionsv1.RenewalScheduleApprovalServiceClient
	renewalScheduleApprovalClientFallback subscriptionsv1.RenewalScheduleApprovalServiceClient
	meteredSubscriptionPlanApproval       meteredSubscriptionPlanApproval
	customerQueryClient                   customersv1.CustomerQueryServiceClient
}

// NewSubscriptionPlanWorkflowServer wires a SubscriptionPlanWorkflowServer.
func NewSubscriptionPlanWorkflowServer(deps platform.Deps) *SubscriptionPlanWorkflowServer {
	return &SubscriptionPlanWorkflowServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.subscriptions.v1.SubscriptionPlanWorkflowService"),
		conn:                                  deps.Conn,
		customerSegmentValidationClient:       customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		subscriptionClient:                    subscriptionsv1.NewSubscriptionServiceClient(deps.Conn),
		bus:                                   deps.Bus,
		consentRecordCalculationClient:        customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		householdQueryClient:                  customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		renewalScheduleApprovalClient:         subscriptionsv1.NewRenewalScheduleApprovalServiceClient(deps.Conn),
		renewalScheduleApprovalClientFallback: subscriptionsv1.NewRenewalScheduleApprovalServiceClient(deps.Conn),
		meteredSubscriptionPlanApproval:       meteredSubscriptionPlanApproval{SubscriptionPlanApprovalServiceClient: subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		customerQueryClient:                   customersv1.NewCustomerQueryServiceClient(deps.Conn),
	}
}

// StartSubscriptionPlanWorkflow starts a subscription plan.
func (s *SubscriptionPlanWorkflowServer) StartSubscriptionPlanWorkflow(ctx context.Context, req *subscriptionsv1.StartSubscriptionPlanWorkflowRequest) (*subscriptionsv1.StartSubscriptionPlanWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "start subscription plan workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetSubscriptionPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.SubscriptionPlan
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	countTierTotalsRequest := &loyaltyv1.CountTierTotalsRequest{
		Context: req.GetContext(),
		Filter:  &loyaltyv1.CountTierTotalsRequest_Filter{CreatedAfter: (&decoded).GetAudit().GetUpdatedAt()},
	}
	countTierTotalsReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewTierReportingServiceClient).CountTierTotals(ctx, countTierTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/CountTierTotals")
	}
	return &subscriptionsv1.StartSubscriptionPlanWorkflowResponse{
		Outcome: &subscriptionsv1.StartSubscriptionPlanWorkflowResponse_Outcome{Message: countTierTotalsReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: countTierTotalsReply.GetResult().GetStatus()},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Version:            req.GetSubscriptionPlan().GetVersion(),
		},
	}, nil
}

// AdvanceSubscriptionPlanWorkflow advances a subscription plan.
func (s *SubscriptionPlanWorkflowServer) AdvanceSubscriptionPlanWorkflow(ctx context.Context, req *subscriptionsv1.AdvanceSubscriptionPlanWorkflowRequest) (*subscriptionsv1.AdvanceSubscriptionPlanWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "advance subscription plan workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	for _, condition := range req.GetSubscriptionPlan().GetConditions() {
		if _, err := s.customerSegmentValidationClient.CheckCustomerSegmentEligibility(ctx, &customersv1.CheckCustomerSegmentEligibilityRequest{
			Context: req.GetContext(),
			CustomerSegment: &customersv1.CustomerSegment{
				Conditions: []*customersv1.CustomerSegmentCondition{&customersv1.CustomerSegmentCondition{
					Negate:    condition.GetNegate(),
					Value:     condition.GetValue(),
					Operator:  condition.GetOperator(),
					Attribute: condition.GetAttribute(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility")
		}
	}
	listSubscriptionsRequest := buildListSubscriptionsRequestForAdvanceSubscriptionPlanWorkflow(req)
	listSubscriptionsReply, err := s.subscriptionClient.ListSubscriptions(ctx, listSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionService/ListSubscriptions")
	}
	return &subscriptionsv1.AdvanceSubscriptionPlanWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listSubscriptionsReply.GetResult().GetReferenceIds(),
			Status:       listSubscriptionsReply.GetResult().GetStatus(),
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Parameters:         req.GetSubscriptionPlan().GetParameters(),
		},
	}, nil
}

// CancelSubscriptionPlanWorkflow cancels a subscription plan.
func (s *SubscriptionPlanWorkflowServer) CancelSubscriptionPlanWorkflow(ctx context.Context, req *subscriptionsv1.CancelSubscriptionPlanWorkflowRequest) (*subscriptionsv1.CancelSubscriptionPlanWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "cancel subscription plan workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "tenant_id", req.GetContext().GetTenantId())
	getTenantPreferencesRequest := &identityv1.GetTenantPreferencesRequest{
		Context:  req.GetContext(),
		TenantId: tenancy.Value(ctx, "tenant_id"),
	}
	getTenantPreferencesReply, err := grpcx.NewClient(s.conn, identityv1.NewTenantNotificationServiceClient).GetTenantPreferences(ctx, getTenantPreferencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/GetTenantPreferences")
	}
	subscriptionPlanCache := cache.New[string, *subscriptionsv1.SubscriptionPlan](1)
	subscriptionPlanCache.Put(req.GetContext().GetTraceId(), req.GetSubscriptionPlan())
	cachedSubscriptionPlan, _ := subscriptionPlanCache.Get(req.GetContext().GetTraceId())
	findCustomersRequest := &customersv1.FindCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.FindCustomersRequest_Filter{
			Region:       getTenantPreferencesReply.GetTenant().GetLocation().GetRegion(),
			CreatedAfter: cachedSubscriptionPlan.GetAudit().GetUpdatedAt(),
		},
	}
	findCustomersReply := new(customersv1.FindCustomersResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.CustomerQueryService_FindCustomers_FullMethodName, findCustomersRequest, findCustomersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/FindCustomers")
	}
	return &subscriptionsv1.CancelSubscriptionPlanWorkflowResponse{
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				Email: getTenantPreferencesReply.GetTenant().GetManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: getTenantPreferencesReply.GetTenant().GetAudit().GetCreatedBy(),
			},
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Parameters:         req.GetSubscriptionPlan().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: findCustomersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &subscriptionsv1.CancelSubscriptionPlanWorkflowResponse_Outcome{Message: findCustomersReply.GetResult().GetMessage()},
	}, nil
}

// RetrySubscriptionPlanWorkflow retrys a subscription plan.
func (s *SubscriptionPlanWorkflowServer) RetrySubscriptionPlanWorkflow(ctx context.Context, req *subscriptionsv1.RetrySubscriptionPlanWorkflowRequest) (*subscriptionsv1.RetrySubscriptionPlanWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "retry subscription plan workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "subscriptions.retry_subscription_plan_workflow.approve_renewal_schedule",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"renewal_schedule.participants.contact.email":             req.GetSubscriptionPlan().GetOwner().GetEmail(),
			"renewal_schedule.participants.contact.preferred_channel": req.GetDetails().GetNotify().GetPreferredChannel(),
			"renewal_schedule.audit.etag":                             req.GetSubscriptionPlan().GetAudit().GetEtag(),
			"renewal_schedule.audit.created_by":                       req.GetSubscriptionPlan().GetAudit().GetCreatedBy(),
			"context.tenant_id":                                       req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	packed, err := anypb.New(req.GetSubscriptionPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateConsentRecordReply, err := s.consentRecordCalculationClient.CalculateConsentRecord(ctx, &customersv1.CalculateConsentRecordRequest{
		Context:       req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
	}
	return &subscriptionsv1.RetrySubscriptionPlanWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: calculateConsentRecordReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Parameters:         req.GetSubscriptionPlan().GetParameters(),
		},
	}, nil
}

// PauseSubscriptionPlanWorkflow pauses a subscription plan.
func (s *SubscriptionPlanWorkflowServer) PauseSubscriptionPlanWorkflow(ctx context.Context, req *subscriptionsv1.PauseSubscriptionPlanWorkflowRequest) (*subscriptionsv1.PauseSubscriptionPlanWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "pause subscription plan workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	checkRenewalScheduleEligibilityRequest := &subscriptionsv1.CheckRenewalScheduleEligibilityRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
			}},
		},
	}
	checkRenewalScheduleEligibilityReply := new(subscriptionsv1.CheckRenewalScheduleEligibilityResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.subscriptions.v1.RenewalScheduleValidationService/CheckRenewalScheduleEligibility", checkRenewalScheduleEligibilityRequest, checkRenewalScheduleEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleValidationService/CheckRenewalScheduleEligibility")
	}
	suggestSubscriptionsRequest := &subscriptionsv1.SuggestSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.SuggestSubscriptionsRequest_Filter{
			Region:       checkRenewalScheduleEligibilityReply.GetRenewalSchedule().GetLocation().GetRegion(),
			CreatedAfter: req.GetSubscriptionPlan().GetAudit().GetUpdatedAt(),
		},
	}
	suggestSubscriptionsReply, err := suggestSubscriptionsCall(ctx, suggestSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionQueryService/SuggestSubscriptions")
	}
	return &subscriptionsv1.PauseSubscriptionPlanWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: checkRenewalScheduleEligibilityReply.GetResult().GetMessage(),
			Status:  suggestSubscriptionsReply.GetResult().GetStatus(),
		},
		Outcome: &subscriptionsv1.PauseSubscriptionPlanWorkflowResponse_Outcome{Message: suggestSubscriptionsReply.GetResult().GetMessage()},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Description:        req.GetSubscriptionPlan().GetDescription(),
		},
	}, nil
}

// ResumeSubscriptionPlanWorkflow resumes a subscription plan.
func (s *SubscriptionPlanWorkflowServer) ResumeSubscriptionPlanWorkflow(ctx context.Context, req *subscriptionsv1.ResumeSubscriptionPlanWorkflowRequest) (*subscriptionsv1.ResumeSubscriptionPlanWorkflowResponse, error) {
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "resume subscription plan workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	var reason string
	switch choice := req.GetSubscriptionPlan().GetAction().(type) {
	case *subscriptionsv1.SubscriptionPlan_Route:
		reason = choice.Route.GetTarget()
	case *subscriptionsv1.SubscriptionPlan_Block:
		reason = choice.Block.GetReason()
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	parameters := make(map[string]string, len(req.GetDetails().GetParameters()))
	for key2, value2 := range req.GetDetails().GetParameters() {
		parameters[key2] = value2
	}
	summarizeHouseholdsRequest := &customersv1.SummarizeHouseholdsRequest{
		Context:     req.GetContext(),
		Reason:      reason,
		Annotations: annotations,
		Details: &customersv1.SummarizeHouseholdsRequest_Details{
			Period:  req.GetSubscriptionPlan().GetEffective(),
			Filters: parameters,
		},
	}
	summarizeHouseholdsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*customersv1.SummarizeHouseholdsResponse, error) {
		return s.householdQueryClient.SummarizeHouseholds(ctx, summarizeHouseholdsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/SummarizeHouseholds")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := subscriptionPlanWorkflowServerExplainCustomerSegmentDecisionSteps[mode]
	ctx = tenancy.With(ctx, "user_id", req.GetSubscriptionPlan().GetOwner().GetUserId())
	explainCustomerSegmentDecisionRequest := &customersv1.ExplainCustomerSegmentDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerSegmentDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetSubscriptionPlan().GetOwner().GetEmail()},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetSubscriptionPlan().GetAudit().GetCreatedBy(),
			},
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{
				NotifyEmail: req.GetSubscriptionPlan().GetBlock().GetNotifyEmail(),
			}},
		},
	}
	explainCustomerSegmentDecisionReply, err := run(s, ctx, explainCustomerSegmentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	}
	return &subscriptionsv1.ResumeSubscriptionPlanWorkflowResponse{
		Outcome: &subscriptionsv1.ResumeSubscriptionPlanWorkflowResponse_Outcome{Status: summarizeHouseholdsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Details: summarizeHouseholdsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainCustomerSegmentDecisionReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			},
			Scopes:             explainCustomerSegmentDecisionReply.GetCustomerSegment().GetScopes(),
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Priority:           req.GetSubscriptionPlan().GetPriority(),
		},
	}, nil
}

// InspectSubscriptionPlanWorkflow returns a subscription plan.
func (s *SubscriptionPlanWorkflowServer) InspectSubscriptionPlanWorkflow(ctx context.Context, req *subscriptionsv1.InspectSubscriptionPlanWorkflowRequest) (*subscriptionsv1.InspectSubscriptionPlanWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription_plan", req.GetSubscriptionPlanId(), "inspect_subscription_plan_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect subscription plan workflow", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *subscriptionsv1.ListPendingRenewalSchedulesRequest, ...grpc.CallOption) (*subscriptionsv1.ListPendingRenewalSchedulesResponse, error){
		"primary":  s.renewalScheduleApprovalClient.ListPendingRenewalSchedules,
		"fallback": s.renewalScheduleApprovalClientFallback.ListPendingRenewalSchedules,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	listPendingRenewalSchedulesRequest := &subscriptionsv1.ListPendingRenewalSchedulesRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.ListPendingRenewalSchedulesRequest_Filter{
			OwnerId:      req.GetSubscriptionPlanId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
	listPendingRenewalSchedulesReply, err := route(ctx, listPendingRenewalSchedulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleApprovalService/ListPendingRenewalSchedules")
	}
	return &subscriptionsv1.InspectSubscriptionPlanWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: listPendingRenewalSchedulesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{SubscriptionPlanId: req.GetSubscriptionPlanId()},
	}, nil
}

// ListSubscriptionPlanWorkflows lists subscription plans a page at a time.
func (s *SubscriptionPlanWorkflowServer) ListSubscriptionPlanWorkflows(ctx context.Context, req *subscriptionsv1.ListSubscriptionPlanWorkflowsRequest) (*subscriptionsv1.ListSubscriptionPlanWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list subscription plan workflows", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	rejectRenewalScheduleRequest := &subscriptionsv1.RejectRenewalScheduleRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Location:  &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Organizer: &identityv1.UserRef{Email: cachedContext.GetActor().GetEmail()},
		},
		Details: &subscriptionsv1.RejectRenewalScheduleRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}
	type rejectRenewalScheduleResult struct {
		reply *subscriptionsv1.RejectRenewalScheduleResponse
		err   error
	}
	rejectRenewalScheduleResults := make(chan rejectRenewalScheduleResult, 1)
	go func() {
		reply, err := s.renewalScheduleApprovalClient.RejectRenewalSchedule(ctx, rejectRenewalScheduleRequest)
		rejectRenewalScheduleResults <- rejectRenewalScheduleResult{reply, err}
	}()
	var rejectRenewalScheduleReply *subscriptionsv1.RejectRenewalScheduleResponse
	select {
	case result := <-rejectRenewalScheduleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.subscriptions.v1.RenewalScheduleApprovalService/RejectRenewalSchedule")
		}
		rejectRenewalScheduleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &subscriptionsv1.ListSubscriptionPlanWorkflowsResponse{
		SubscriptionPlans: []*subscriptionsv1.SubscriptionPlan{&subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				CreatedBy: rejectRenewalScheduleReply.GetRenewalSchedule().GetAudit().GetCreatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalSubscriptionPlanWorkflow signals a subscription plan.
func (s *SubscriptionPlanWorkflowServer) SignalSubscriptionPlanWorkflow(ctx context.Context, req *subscriptionsv1.SignalSubscriptionPlanWorkflowRequest) (*subscriptionsv1.SignalSubscriptionPlanWorkflowResponse, error) {
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "signal subscription plan workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	listPendingSubscriptionPlansRequest := &subscriptionsv1.ListPendingSubscriptionPlansRequest{
		Context: req.GetContext(),
		Filter: &subscriptionsv1.ListPendingSubscriptionPlansRequest_Filter{
			CreatedAfter: req.GetSubscriptionPlan().GetAudit().GetUpdatedAt(),
		},
	}
	listPendingSubscriptionPlansReply, err := s.meteredSubscriptionPlanApproval.ListPendingSubscriptionPlans(ctx, listPendingSubscriptionPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanApprovalService/ListPendingSubscriptionPlans")
	}
	cloned := proto.Clone(req.GetSubscriptionPlan()).(*subscriptionsv1.SubscriptionPlan)
	exportCustomersStream, err := s.customerQueryClient.ExportCustomers(ctx, &customersv1.ExportCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportCustomersRequest_Filter{
			CreatedAfter: req.GetSubscriptionPlan().GetAudit().GetCreatedAt(),
			OwnerId:      cloned.GetOwner().GetUserId(),
			Labels:       listPendingSubscriptionPlansReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ExportCustomers")
	}
	for {
		received, err := exportCustomersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ExportCustomers")
		}
		_ = received
	}
	return &subscriptionsv1.SignalSubscriptionPlanWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: listPendingSubscriptionPlansReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Priority:           req.GetSubscriptionPlan().GetPriority(),
		},
	}, nil
}

// subscriptionPlanWorkflowServerExplainCustomerSegmentDecisionSteps are the ways to run
// acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision, by name.
var subscriptionPlanWorkflowServerExplainCustomerSegmentDecisionSteps = map[string]func(*SubscriptionPlanWorkflowServer, context.Context, *customersv1.ExplainCustomerSegmentDecisionRequest) (*customersv1.ExplainCustomerSegmentDecisionResponse, error){
	"default": (*SubscriptionPlanWorkflowServer).explainCustomerSegmentDecisionStep,
	"audited": (*SubscriptionPlanWorkflowServer).explainCustomerSegmentDecisionAuditedStep,
}

func (s *SubscriptionPlanWorkflowServer) explainCustomerSegmentDecisionStep(ctx context.Context, request *customersv1.ExplainCustomerSegmentDecisionRequest) (*customersv1.ExplainCustomerSegmentDecisionResponse, error) {
	return s.customerSegmentValidationClient.ExplainCustomerSegmentDecision(ctx, request)
}

func (s *SubscriptionPlanWorkflowServer) explainCustomerSegmentDecisionAuditedStep(ctx context.Context, request *customersv1.ExplainCustomerSegmentDecisionRequest) (*customersv1.ExplainCustomerSegmentDecisionResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.CustomerSegmentValidationService/ExplainCustomerSegmentDecision")
	return s.customerSegmentValidationClient.ExplainCustomerSegmentDecision(ctx, request)
}
