package subscriptions

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SubscriptionWorkflowServer implements `acme.subscriptions.v1.SubscriptionWorkflowService`.
// Runs the long-lived workflow that takes a subscription from start to finish.
type SubscriptionWorkflowServer struct {
	subscriptionsv1.UnimplementedSubscriptionWorkflowServiceServer
	store                       *Store
	log                         *slog.Logger
	placementWorkflowClient     merchandisingv1.PlacementWorkflowServiceClient
	consentRecordValidationPort consentRecordValidationPort
	collectionApprovalPort      collectionApprovalPort
	subscriptionQueryClient     subscriptionsv1.SubscriptionQueryServiceClient
	customerClient              customersv1.CustomerServiceClient
	lintAccessPoliciesFn        func(context.Context, *authorizationv1.LintAccessPoliciesRequest, ...grpc.CallOption) (*authorizationv1.LintAccessPoliciesResponse, error)
	rewardWorkflowClient        loyaltyv1.RewardWorkflowServiceClient
	sessionHistoryClient        identityv1.SessionHistoryServiceClient
	customerQueryClient         customersv1.CustomerQueryServiceClient
	rewardWorkflowPort          rewardWorkflowPort
	metricSeriesQueryClient     metricsv1.MetricSeriesQueryServiceClient
	subscriptionPlanQueryClient subscriptionsv1.SubscriptionPlanQueryServiceClient
	subscriptionApprovalClient  subscriptionsv1.SubscriptionApprovalServiceClient
}

// NewSubscriptionWorkflowServer wires a SubscriptionWorkflowServer.
func NewSubscriptionWorkflowServer(deps platform.Deps) *SubscriptionWorkflowServer {
	accessPolicyValidation := authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn)
	return &SubscriptionWorkflowServer{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.subscriptions.v1.SubscriptionWorkflowService"),
		placementWorkflowClient:     merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn),
		consentRecordValidationPort: newConsentRecordValidationPort(deps),
		collectionApprovalPort:      newCollectionApprovalPort(deps),
		subscriptionQueryClient:     subscriptionsv1.NewSubscriptionQueryServiceClient(deps.Conn),
		customerClient:              customersv1.NewCustomerServiceClient(deps.Conn),
		lintAccessPoliciesFn:        accessPolicyValidation.LintAccessPolicies,
		rewardWorkflowClient:        loyaltyv1.NewRewardWorkflowServiceClient(deps.Conn),
		sessionHistoryClient:        identityv1.NewSessionHistoryServiceClient(deps.Conn),
		customerQueryClient:         customersv1.NewCustomerQueryServiceClient(deps.Conn),
		rewardWorkflowPort:          newRewardWorkflowPort(deps),
		metricSeriesQueryClient:     metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
		subscriptionPlanQueryClient: subscriptionsv1.NewSubscriptionPlanQueryServiceClient(deps.Conn),
		subscriptionApprovalClient:  subscriptionsv1.NewSubscriptionApprovalServiceClient(deps.Conn),
	}
}

// StartSubscriptionWorkflow starts a subscription.
func (s *SubscriptionWorkflowServer) StartSubscriptionWorkflow(ctx context.Context, req *subscriptionsv1.StartSubscriptionWorkflowRequest) (*subscriptionsv1.StartSubscriptionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "start subscription workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	startPlacementWorkflowReply, err := s.placementWorkflowClient.StartPlacementWorkflow(ctx, &merchandisingv1.StartPlacementWorkflowRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Name:  req.GetSubscription().GetContact().GetName(),
					Email: tenancy.Value(ctx, "email"),
				},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/StartPlacementWorkflow")
	}
	scoreConsentRecordRequest := &customersv1.ScoreConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.ScoreConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: startPlacementWorkflowReply.GetPlacement().GetLocation().GetRecipient(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: startPlacementWorkflowReply.GetPlacement().GetOrganizer().GetDisplayName(),
			},
		},
	}
	scoreConsentRecordReply, err := s.consentRecordValidationPort.ScoreConsentRecord(ctx, scoreConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord")
	}
	return &subscriptionsv1.StartSubscriptionWorkflowResponse{
		Subscription: &subscriptionsv1.Subscription{
			Audit: &commonv1.AuditInfo{
				CreatedBy: startPlacementWorkflowReply.GetPlacement().GetAudit().GetCreatedBy(),
				UpdatedAt: startPlacementWorkflowReply.GetPlacement().GetAudit().GetUpdatedAt(),
			},
			CurrencyCode:   scoreConsentRecordReply.GetBreakdown().GetCurrencyCode(),
			SubscriptionId: req.GetSubscriptionId(),
			Labels:         req.GetSubscription().GetLabels(),
		},
		Result: &commonv1.OperationResult{
			Details: scoreConsentRecordReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// AdvanceSubscriptionWorkflow advances a subscription.
func (s *SubscriptionWorkflowServer) AdvanceSubscriptionWorkflow(ctx context.Context, req *subscriptionsv1.AdvanceSubscriptionWorkflowRequest) (*subscriptionsv1.AdvanceSubscriptionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "advance subscription workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	commentCollectionApprovalRequest := &merchandisingv1.CommentCollectionApprovalRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Manager: &identityv1.UserRef{Email: req.GetSubscription().GetHolder().GetEmail()},
			}},
		},
	}
	commentCollectionApprovalReply, err := s.collectionApprovalPort.CommentCollectionApproval(ctx, commentCollectionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionApprovalService/CommentCollectionApproval")
	}
	peekSubscriptionRequest := buildPeekSubscriptionRequestForAdvanceSubscriptionWorkflow(req)
	peekSubscriptionReply, err := s.subscriptionQueryClient.PeekSubscription(ctx, peekSubscriptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionQueryService/PeekSubscription")
	}
	batchGetCustomersRequest := &customersv1.BatchGetCustomersRequest{Context: req.GetContext()}
	batchGetCustomersReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.BatchGetCustomersResponse, error) {
		return s.customerClient.BatchGetCustomers(ctx, batchGetCustomersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchGetCustomers")
	}
	lintAccessPoliciesRequest := &authorizationv1.LintAccessPoliciesRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSubscription().GetHolder().GetDisplayName(),
			},
		},
		Details: &authorizationv1.LintAccessPoliciesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetSubscription().GetHolder().GetEmail()},
		},
	}
	lintAccessPoliciesReply, err := s.lintAccessPoliciesFn(ctx, lintAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
	}
	_ = lintAccessPoliciesReply
	return &subscriptionsv1.AdvanceSubscriptionWorkflowResponse{
		Outcome: &subscriptionsv1.AdvanceSubscriptionWorkflowResponse_Outcome{
			Status: commentCollectionApprovalReply.GetResult().GetStatus(),
		},
		Subscription: &subscriptionsv1.Subscription{
			Contact:        peekSubscriptionReply.GetSubscription().GetContact(),
			SubscriptionId: req.GetSubscriptionId(),
			OpenedAt:       req.GetSubscription().GetOpenedAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetCustomersReply.GetResult().GetCorrelationId(),
			ReferenceIds:  batchGetCustomersReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}, nil
}

// RetrySubscriptionWorkflow retrys a subscription.
func (s *SubscriptionWorkflowServer) RetrySubscriptionWorkflow(ctx context.Context, req *subscriptionsv1.RetrySubscriptionWorkflowRequest) (*subscriptionsv1.RetrySubscriptionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "retry subscription workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	for _, limit := range req.GetSubscription().GetLimits() {
		if _, err := s.rewardWorkflowClient.CancelRewardWorkflow(ctx, &loyaltyv1.CancelRewardWorkflowRequest{
			Context: req.GetContext(),
			Reward: &loyaltyv1.Reward{
				Action: &loyaltyv1.Reward_Discount{Discount: &loyaltyv1.RewardDiscountAction{Amount: limit.GetAmount()}},
				Name:   limit.GetName(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardWorkflowService/CancelRewardWorkflow")
		}
	}
	cloned := proto.Clone(req.GetSubscription()).(*subscriptionsv1.Subscription)
	subscriptionCache := cache.New[string, *subscriptionsv1.Subscription](1)
	subscriptionCache.Put(req.GetContext().GetTraceId(), req.GetSubscription())
	cachedSubscription, _ := subscriptionCache.Get(req.GetContext().GetTraceId())
	snapshot, err := protojson.Marshal(req.GetSubscription())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	step := &subscriptionWorkflowServerRetrySubscriptionWorkflowCompareSessionVersionsStep{request: &identityv1.CompareSessionVersionsRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Name:             cloned.GetContact().GetName(),
			},
			Assignee: &identityv1.UserRef{Email: cachedSubscription.GetContact().GetEmail()},
		},
		Details: &identityv1.CompareSessionVersionsRequest_Details{Snapshot: snapshot},
	}}
	if err := s.runRetrySubscriptionWorkflowCompareSessionVersions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/CompareSessionVersions")
	}
	compareSessionVersionsReply := step.reply
	labels := make(map[string]string, len(req.GetSubscription().GetLabels()))
	for key, value := range req.GetSubscription().GetLabels() {
		labels[key] = value
	}
	exportCustomersStream, err := s.customerQueryClient.ExportCustomers(ctx, &customersv1.ExportCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportCustomersRequest_Filter{
			Labels:       labels,
			CreatedAfter: compareSessionVersionsReply.GetSession().GetAudit().GetUpdatedAt(),
			OwnerId:      compareSessionVersionsReply.GetSession().GetCustomer().GetApiCredentialId(),
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
	return &subscriptionsv1.RetrySubscriptionWorkflowResponse{
		Subscription: &subscriptionsv1.Subscription{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: compareSessionVersionsReply.GetSession().GetAudit().GetUpdatedBy(),
			},
			SubscriptionId: req.GetSubscriptionId(),
			Balance:        req.GetSubscription().GetBalance(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseSubscriptionWorkflow pauses a subscription.
func (s *SubscriptionWorkflowServer) PauseSubscriptionWorkflow(ctx context.Context, req *subscriptionsv1.PauseSubscriptionWorkflowRequest) (*subscriptionsv1.PauseSubscriptionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "pause subscription workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	source := req.GetDetails().GetNotify().ProtoReflect()
	preferredChannel := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("preferred_channel"))).String()
	advanceRewardWorkflowRequest := &loyaltyv1.AdvanceRewardWorkflowRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetSubscription().GetAudit().GetCreatedAt(),
				UpdatedAt: req.GetSubscription().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{Email: req.GetSubscription().GetContact().GetEmail()},
		},
		Details: &loyaltyv1.AdvanceRewardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{PreferredChannel: preferredChannel},
		},
	}
	advanceRewardWorkflowReply, err := s.rewardWorkflowPort.AdvanceRewardWorkflow(ctx, advanceRewardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardWorkflowService/AdvanceRewardWorkflow")
	}
	return &subscriptionsv1.PauseSubscriptionWorkflowResponse{
		Outcome: &subscriptionsv1.PauseSubscriptionWorkflowResponse_Outcome{
			ReferenceId: advanceRewardWorkflowReply.GetOutcome().GetReferenceId(),
		},
		Subscription: &subscriptionsv1.Subscription{
			Contact: &commonv1.ContactInfo{
				Email: advanceRewardWorkflowReply.GetReward().GetOwner().GetEmail(),
			},
			SubscriptionId: req.GetSubscriptionId(),
			Balance:        req.GetSubscription().GetBalance(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeSubscriptionWorkflow resumes a subscription.
func (s *SubscriptionWorkflowServer) ResumeSubscriptionWorkflow(ctx context.Context, req *subscriptionsv1.ResumeSubscriptionWorkflowRequest) (*subscriptionsv1.ResumeSubscriptionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscription(ctx, req.GetContext().GetTenantId(), req.GetSubscription()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription")
	}
	s.log.InfoContext(ctx, "resume subscription workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscription().GetAudit().GetEtag())
	lintAccessPoliciesRequest := &authorizationv1.LintAccessPoliciesRequest{
		Context: req.GetContext(),
		Details: &authorizationv1.LintAccessPoliciesRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotify().GetName(),
				Phone: req.GetDetails().GetNotify().GetPhone(),
			},
		},
	}
	lintAccessPoliciesReply, err := lintAccessPoliciesCall(ctx, lintAccessPoliciesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
	}
	countMetricSeriesesReply, err := s.metricSeriesQueryClient.CountMetricSerieses(ctx, &metricsv1.CountMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.CountMetricSeriesesRequest_Filter{
			Labels:       req.GetSubscription().GetLabels(),
			CreatedAfter: req.GetSubscription().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetSubscription().GetHolder().GetCustomerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses")
	}
	return &subscriptionsv1.ResumeSubscriptionWorkflowResponse{
		Outcome: &subscriptionsv1.ResumeSubscriptionWorkflowResponse_Outcome{
			EffectiveAt: lintAccessPoliciesReply.GetOutcome().GetEffectiveAt(),
			Status:      countMetricSeriesesReply.GetResult().GetStatus(),
		},
		Subscription: &subscriptionsv1.Subscription{
			Limits:         []*subscriptionsv1.SubscriptionLimit{&subscriptionsv1.SubscriptionLimit{Name: lintAccessPoliciesReply.GetAccessPolicy().GetName()}},
			SubscriptionId: req.GetSubscriptionId(),
			Holder:         req.GetSubscription().GetHolder(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectSubscriptionWorkflow returns a subscription.
func (s *SubscriptionWorkflowServer) InspectSubscriptionWorkflow(ctx context.Context, req *subscriptionsv1.InspectSubscriptionWorkflowRequest) (*subscriptionsv1.InspectSubscriptionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectSubscriptionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription", req.GetSubscriptionId(), "inspect_subscription_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect subscription workflow", "tenant", req.GetContext().GetTenantId())
	resolveSubscriptionPlansReply, err := s.subscriptionPlanQueryClient.ResolveSubscriptionPlans(ctx, &subscriptionsv1.ResolveSubscriptionPlansRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/ResolveSubscriptionPlans")
	}
	escalateSubscriptionRequest := buildEscalateSubscriptionRequestForInspectSubscriptionWorkflow(req)
	escalateSubscriptionReply, err := s.subscriptionApprovalClient.EscalateSubscription(ctx, escalateSubscriptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/EscalateSubscription")
	}
	return &subscriptionsv1.InspectSubscriptionWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: resolveSubscriptionPlansReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Subscription: &subscriptionsv1.Subscription{
			Holder:         escalateSubscriptionReply.GetSubscription().GetHolder(),
			SubscriptionId: req.GetSubscriptionId(),
		},
	}, nil
}

// ListSubscriptionWorkflows lists subscriptions a page at a time.
func (s *SubscriptionWorkflowServer) ListSubscriptionWorkflows(ctx context.Context, req *subscriptionsv1.ListSubscriptionWorkflowsRequest) (*subscriptionsv1.ListSubscriptionWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list subscription workflows", "tenant", req.GetContext().GetTenantId())
	delegatePointsLedgerApprovalRequest := &loyaltyv1.DelegatePointsLedgerApprovalRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.DelegatePointsLedgerApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		PointsLedger: &loyaltyv1.PointsLedger{Labels: req.GetFilter().GetLabels()},
	}
	delegatePointsLedgerApprovalReply, err := delegatePointsLedgerApprovalCall(ctx, delegatePointsLedgerApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/DelegatePointsLedgerApproval")
	}
	return &subscriptionsv1.ListSubscriptionWorkflowsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: delegatePointsLedgerApprovalReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// subscriptionWorkflowServerRetrySubscriptionWorkflowCompareSessionVersionsStep carries the request of one step of
// RetrySubscriptionWorkflow and, once it ran, its reply.
type subscriptionWorkflowServerRetrySubscriptionWorkflowCompareSessionVersionsStep struct {
	request *identityv1.CompareSessionVersionsRequest
	reply   *identityv1.CompareSessionVersionsResponse
}

func (s *SubscriptionWorkflowServer) runRetrySubscriptionWorkflowCompareSessionVersions(ctx context.Context, step *subscriptionWorkflowServerRetrySubscriptionWorkflowCompareSessionVersionsStep) error {
	reply, err := s.sessionHistoryClient.CompareSessionVersions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
