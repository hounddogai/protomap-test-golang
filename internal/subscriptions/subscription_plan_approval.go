package subscriptions

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// SubscriptionPlanApprovalServer implements `acme.subscriptions.v1.SubscriptionPlanApprovalService`.
// Routes subscription plans through approval, with escalation and delegation.
type SubscriptionPlanApprovalServer struct {
	subscriptionsv1.UnimplementedSubscriptionPlanApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	subscriptionApprovalOnce       sync.Once
	subscriptionApprovalCached     subscriptionsv1.SubscriptionApprovalServiceClient
	roleNotificationClient         identityv1.RoleNotificationServiceClient
	roleNotificationClientFallback identityv1.RoleNotificationServiceClient
	usageRecordValidationClient    subscriptionsv1.UsageRecordValidationServiceClient
	subscriptionPlanValidationPort subscriptionPlanValidationPort
	reviewVoteApprovalClient       reviewsv1.ReviewVoteApprovalServiceClient
	synonymValidationClient        searchv1.SynonymValidationServiceClient
	meteredPreferenceValidation    meteredPreferenceValidation
	customerQueryClient            customersv1.CustomerQueryServiceClient
	ledgerAccountValidationClient  ledgerv1.LedgerAccountValidationServiceClient
	preferenceCalculationClient    customersv1.PreferenceCalculationServiceClient
	notificationValidationClient   notificationsv1.NotificationValidationServiceClient
	reviewModerationApprovalClient reviewsv1.ReviewModerationApprovalServiceClient
	subscriptionApprovalClient     subscriptionsv1.SubscriptionApprovalServiceClient
	giftCardWorkflowClient         giftcardsv1.GiftCardWorkflowServiceClient
}

// NewSubscriptionPlanApprovalServer wires a SubscriptionPlanApprovalServer.
func NewSubscriptionPlanApprovalServer(deps platform.Deps) *SubscriptionPlanApprovalServer {
	return &SubscriptionPlanApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.subscriptions.v1.SubscriptionPlanApprovalService"),
		conn:                           deps.Conn,
		roleNotificationClient:         identityv1.NewRoleNotificationServiceClient(deps.Conn),
		roleNotificationClientFallback: identityv1.NewRoleNotificationServiceClient(deps.Conn),
		usageRecordValidationClient:    subscriptionsv1.NewUsageRecordValidationServiceClient(deps.Conn),
		subscriptionPlanValidationPort: newSubscriptionPlanValidationPort(deps),
		reviewVoteApprovalClient:       reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn),
		synonymValidationClient:        searchv1.NewSynonymValidationServiceClient(deps.Conn),
		meteredPreferenceValidation:    meteredPreferenceValidation{PreferenceValidationServiceClient: customersv1.NewPreferenceValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		customerQueryClient:            customersv1.NewCustomerQueryServiceClient(deps.Conn),
		ledgerAccountValidationClient:  ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		preferenceCalculationClient:    customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		notificationValidationClient:   notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		reviewModerationApprovalClient: reviewsv1.NewReviewModerationApprovalServiceClient(deps.Conn),
		subscriptionApprovalClient:     subscriptionsv1.NewSubscriptionApprovalServiceClient(deps.Conn),
		giftCardWorkflowClient:         giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
	}
}

// SubmitSubscriptionPlan submits a subscription plan.
func (s *SubscriptionPlanApprovalServer) SubmitSubscriptionPlan(ctx context.Context, req *subscriptionsv1.SubmitSubscriptionPlanRequest) (*subscriptionsv1.SubmitSubscriptionPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitSubscriptionPlan", "error", err)
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "submit subscription plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded subscriptionsv1.SubmitSubscriptionPlanRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	submitSubscriptionRequest := &subscriptionsv1.SubmitSubscriptionRequest{
		Context: req.GetContext(),
		Subscription: &subscriptionsv1.Subscription{
			Holder: &customersv1.CustomerRef{DisplayName: (&decoded).GetApprover().GetDisplayName()},
		},
		Details: &subscriptionsv1.SubmitSubscriptionRequest_Details{
			Approver: &identityv1.UserRef{
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: req.GetSubscriptionPlan().GetOwner().GetDisplayName(),
			},
		},
	}
	submitSubscriptionReply, err := s.subscriptionApprovalLazy().SubmitSubscription(ctx, submitSubscriptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/SubmitSubscription")
	}
	routes := map[string]func(context.Context, *identityv1.NotifyRoleWatchersRequest, ...grpc.CallOption) (*identityv1.NotifyRoleWatchersResponse, error){
		"primary":  s.roleNotificationClient.NotifyRoleWatchers,
		"fallback": s.roleNotificationClientFallback.NotifyRoleWatchers,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	notifyRoleWatchersRequest := &identityv1.NotifyRoleWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyRoleWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: submitSubscriptionReply.GetSubscription().GetHolder().GetEmail(),
			},
		},
	}
	notifyRoleWatchersReply, err := route(ctx, notifyRoleWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/NotifyRoleWatchers")
	}
	return &subscriptionsv1.SubmitSubscriptionPlanResponse{
		Outcome: &subscriptionsv1.SubmitSubscriptionPlanResponse_Outcome{Message: submitSubscriptionReply.GetOutcome().GetMessage()},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: notifyRoleWatchersReply.GetRole().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				UserId: notifyRoleWatchersReply.GetRole().GetOwner().GetUserId(),
			},
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Status:             req.GetSubscriptionPlan().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveSubscriptionPlan approves a subscription plan.
func (s *SubscriptionPlanApprovalServer) ApproveSubscriptionPlan(ctx context.Context, req *subscriptionsv1.ApproveSubscriptionPlanRequest) (*subscriptionsv1.ApproveSubscriptionPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "approve subscription plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetSubscriptionPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreUsageRecordReply, err := s.usageRecordValidationClient.ScoreUsageRecord(ctx, &subscriptionsv1.ScoreUsageRecordRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
		Annotations: annotations,
		Reason:      req.GetSubscriptionPlan().GetBlock().GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/ScoreUsageRecord")
	}
	detailsCache := cache.New[string, *subscriptionsv1.ApproveSubscriptionPlanRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	previewSubscriptionPlanRequest := &subscriptionsv1.PreviewSubscriptionPlanRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{UserId: cachedDetails.GetApprover().GetUserId()},
		},
	}
	previewSubscriptionPlanReply, err := s.subscriptionPlanValidationPort.PreviewSubscriptionPlan(ctx, previewSubscriptionPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanValidationService/PreviewSubscriptionPlan")
	}
	escalateReviewVoteRequest := &reviewsv1.EscalateReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email: scoreUsageRecordReply.GetUsageRecord().GetSubject().GetEmail(),
			},
			Kind: scoreUsageRecordReply.GetUsageRecord().GetKind(),
		},
		Details: &reviewsv1.EscalateReviewVoteRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: scoreUsageRecordReply.GetUsageRecord().GetSubject().GetDisplayName(),
			},
			EffectiveAt: previewSubscriptionPlanReply.GetOutcome().GetEffectiveAt(),
		},
	}
	escalateReviewVoteReply, err := grpcx.Call(ctx, s.reviewVoteApprovalClient.EscalateReviewVote, escalateReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/EscalateReviewVote")
	}
	return &subscriptionsv1.ApproveSubscriptionPlanResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scoreUsageRecordReply.GetUsageRecord().GetCorrelationId(),
			Status:        scoreUsageRecordReply.GetResult().GetStatus(),
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				UserId: previewSubscriptionPlanReply.GetSubscriptionPlan().GetOwner().GetUserId(),
			},
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
		},
		Outcome: &subscriptionsv1.ApproveSubscriptionPlanResponse_Outcome{
			Status:      escalateReviewVoteReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// RejectSubscriptionPlan rejects a subscription plan.
func (s *SubscriptionPlanApprovalServer) RejectSubscriptionPlan(ctx context.Context, req *subscriptionsv1.RejectSubscriptionPlanRequest) (*subscriptionsv1.RejectSubscriptionPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectSubscriptionPlan", "error", err)
		return nil, err
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "reject subscription plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	packed, err := anypb.New(req.GetSubscriptionPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	rejectReviewVoteReply, err := s.reviewVoteApprovalClient.RejectReviewVote(ctx, &reviewsv1.RejectReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				Email:       tenancy.Value(ctx, "email"),
			},
			Detail: packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteApprovalService/RejectReviewVote")
	}
	validateSynonymReply, err := s.synonymValidationClient.ValidateSynonym(ctx, &searchv1.ValidateSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: rejectReviewVoteReply.GetReviewVote().GetSubject().GetDisplayName(),
				UserId:      req.GetSubscriptionPlan().GetOwner().GetUserId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ValidateSynonym")
	}
	return &subscriptionsv1.RejectSubscriptionPlanResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: rejectReviewVoteReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &subscriptionsv1.RejectSubscriptionPlanResponse_Outcome{
			ReferenceId: rejectReviewVoteReply.GetOutcome().GetReferenceId(),
			EffectiveAt: validateSynonymReply.GetOutcome().GetEffectiveAt(),
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Description:        req.GetSubscriptionPlan().GetDescription(),
		},
	}, nil
}

// EscalateSubscriptionPlan escalates a subscription plan.
func (s *SubscriptionPlanApprovalServer) EscalateSubscriptionPlan(ctx context.Context, req *subscriptionsv1.EscalateSubscriptionPlanRequest) (*subscriptionsv1.EscalateSubscriptionPlanResponse, error) {
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "escalate subscription plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	checkPreferenceEligibilityRequest := &customersv1.CheckPreferenceEligibilityRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetSubscriptionPlan().GetOwner().GetDisplayName(),
				UserId:      req.GetSubscriptionPlan().GetOwner().GetUserId(),
			},
			Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{Reason: req.GetSubscriptionPlan().GetBlock().GetReason()}},
		},
		Details: &customersv1.CheckPreferenceEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	checkPreferenceEligibilityReply, err := s.meteredPreferenceValidation.CheckPreferenceEligibility(ctx, checkPreferenceEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/CheckPreferenceEligibility")
	}
	suggestCustomersRequest := &customersv1.SuggestCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SuggestCustomersRequest_Filter{
			CreatedAfter: req.GetSubscriptionPlan().GetAudit().GetUpdatedAt(),
		},
	}
	send := s.suggestCustomersSender(suggestCustomersRequest)
	suggestCustomersReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SuggestCustomers")
	}
	return &subscriptionsv1.EscalateSubscriptionPlanResponse{
		Outcome: &subscriptionsv1.EscalateSubscriptionPlanResponse_Outcome{
			EffectiveAt: checkPreferenceEligibilityReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestCustomersReply.GetResult().GetReferenceIds(),
			Details:      suggestCustomersReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Description:        req.GetSubscriptionPlan().GetDescription(),
		},
	}, nil
}

// ListPendingSubscriptionPlans lists subscription plans a page at a time.
func (s *SubscriptionPlanApprovalServer) ListPendingSubscriptionPlans(ctx context.Context, req *subscriptionsv1.ListPendingSubscriptionPlansRequest) (*subscriptionsv1.ListPendingSubscriptionPlansResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending subscription plans", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*subscriptionsv1.ListPendingSubscriptionPlansRequest_Filter)
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	scoreLedgerAccountRequest := &ledgerv1.ScoreLedgerAccountRequest{
		Context: req.GetContext(),
		Details: &ledgerv1.ScoreLedgerAccountRequest_Details{
			Destination: &commonv1.PostalAddress{Region: cloned.GetRegion()},
		},
		LedgerAccount: &ledgerv1.LedgerAccount{
			Labels:  labels,
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	scoreLedgerAccountReply, err := ledgerv1.LedgerAccountValidationServiceClient.ScoreLedgerAccount(s.ledgerAccountValidationClient, ctx, scoreLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount")
	}
	return &subscriptionsv1.ListPendingSubscriptionPlansResponse{
		SubscriptionPlans: []*subscriptionsv1.SubscriptionPlan{&subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scoreLedgerAccountReply.GetLedgerAccount().GetAudit().GetUpdatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateSubscriptionPlanApproval delegates a subscription plan.
func (s *SubscriptionPlanApprovalServer) DelegateSubscriptionPlanApproval(ctx context.Context, req *subscriptionsv1.DelegateSubscriptionPlanApprovalRequest) (*subscriptionsv1.DelegateSubscriptionPlanApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "delegate subscription plan approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	lintUsageRecordsRequest := buildLintUsageRecordsRequestForDelegateSubscriptionPlanApproval(req)
	lintUsageRecordsReply, err := s.usageRecordValidationClient.LintUsageRecords(ctx, lintUsageRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordValidationService/LintUsageRecords")
	}
	for _, condition := range req.GetSubscriptionPlan().GetConditions() {
		if _, err := s.preferenceCalculationClient.EstimatePreference(ctx, &customersv1.EstimatePreferenceRequest{
			Context: req.GetContext(),
			Preference: &customersv1.Preference{
				Conditions: []*customersv1.PreferenceCondition{&customersv1.PreferenceCondition{
					Attribute: condition.GetAttribute(),
					Negate:    condition.GetNegate(),
					Operator:  condition.GetOperator(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/EstimatePreference")
		}
	}
	cloned := proto.Clone(req.GetDetails()).(*subscriptionsv1.DelegateSubscriptionPlanApprovalRequest_Details)
	scoreNotificationRequest := &notificationsv1.ScoreNotificationRequest{}
	scoreNotificationRequest.Context = req.GetContext()
	scoreNotificationRequest.Notification = &notificationsv1.Notification{
		Requester: &commonv1.ContactInfo{Email: cloned.GetApprover().GetEmail()},
		Assignee:  &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
	}
	scoreNotificationReply, err := s.notificationValidationClient.ScoreNotification(ctx, scoreNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ScoreNotification")
	}
	return &subscriptionsv1.DelegateSubscriptionPlanApprovalResponse{
		Result:  &commonv1.OperationResult{Status: lintUsageRecordsReply.GetResult().GetStatus()},
		Outcome: &subscriptionsv1.DelegateSubscriptionPlanApprovalResponse_Outcome{Status: lintUsageRecordsReply.GetOutcome().GetStatus()},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				Email: scoreNotificationReply.GetNotification().GetAssignee().GetEmail(),
			},
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Description:        req.GetSubscriptionPlan().GetDescription(),
		},
	}, nil
}

// RecallSubscriptionPlan recalls a subscription plan.
func (s *SubscriptionPlanApprovalServer) RecallSubscriptionPlan(ctx context.Context, req *subscriptionsv1.RecallSubscriptionPlanRequest) (*subscriptionsv1.RecallSubscriptionPlanResponse, error) {
	if err := s.store.SaveSubscriptionPlan(ctx, req.GetContext().GetTenantId(), req.GetSubscriptionPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save subscription plan")
	}
	s.log.InfoContext(ctx, "recall subscription plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSubscriptionPlan().GetAudit().GetEtag())
	summarizeSubscriptionPlansRequest := &subscriptionsv1.SummarizeSubscriptionPlansRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	summarizeSubscriptionPlansReply := new(subscriptionsv1.SummarizeSubscriptionPlansResponse)
	if err := s.conn.Invoke(ctx, subscriptionsSubscriptionPlanQueryServiceSummarizeSubscriptionPlansMethod, summarizeSubscriptionPlansRequest, summarizeSubscriptionPlansReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/SummarizeSubscriptionPlans")
	}
	lintLedgerAccountsRequest := &ledgerv1.LintLedgerAccountsRequest{
		Context: req.GetContext(),
		Details: &ledgerv1.LintLedgerAccountsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		LedgerAccount: &ledgerv1.LedgerAccount{
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
	send := s.lintLedgerAccountsSender(lintLedgerAccountsRequest)
	lintLedgerAccountsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts")
	}
	source := req.GetDetails().GetApprover().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	rejectReviewModerationReply, err := s.reviewModerationApprovalClient.RejectReviewModeration(ctx, &reviewsv1.RejectReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
			Origin:   &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{Email: email}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationApprovalService/RejectReviewModeration")
	}
	recallSubscriptionRequest := &subscriptionsv1.RecallSubscriptionRequest{}
	recallSubscriptionRequest.Context = req.GetContext()
	recallSubscriptionRequest.Subscription = &subscriptionsv1.Subscription{
		Holder: &customersv1.CustomerRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
		Audit: &commonv1.AuditInfo{
			UpdatedBy: lintLedgerAccountsReply.GetLedgerAccount().GetAudit().GetUpdatedBy(),
			CreatedBy: rejectReviewModerationReply.GetReviewModeration().GetAudit().GetCreatedBy(),
		},
	}
	recallSubscriptionRequest.Details = &subscriptionsv1.RecallSubscriptionRequest_Details{
		Approver: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
	}
	recallSubscriptionReply, err := s.subscriptionApprovalClient.RecallSubscription(ctx, recallSubscriptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/RecallSubscription")
	}
	return &subscriptionsv1.RecallSubscriptionPlanResponse{
		Outcome: &subscriptionsv1.RecallSubscriptionPlanResponse_Outcome{
			Message: summarizeSubscriptionPlansReply.GetResult().GetMessage(),
			Status:  lintLedgerAccountsReply.GetOutcome().GetStatus(),
		},
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintLedgerAccountsReply.GetLedgerAccount().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				Email: recallSubscriptionReply.GetSubscription().GetContact().GetEmail(),
			},
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
			Description:        req.GetSubscriptionPlan().GetDescription(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: rejectReviewModerationReply.GetResult().GetCorrelationId(),
			Status:        rejectReviewModerationReply.GetResult().GetStatus(),
		},
	}, nil
}

// GetSubscriptionPlanApprovalStatus returns a subscription plan.
func (s *SubscriptionPlanApprovalServer) GetSubscriptionPlanApprovalStatus(ctx context.Context, req *subscriptionsv1.GetSubscriptionPlanApprovalStatusRequest) (*subscriptionsv1.GetSubscriptionPlanApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "subscription_plan", req.GetSubscriptionPlanId(), "get_subscription_plan_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get subscription plan approval status", "tenant", req.GetContext().GetTenantId())
	startGiftCardWorkflowRequest := &giftcardsv1.StartGiftCardWorkflowRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Funding: &giftcardsv1.GiftCard_ExternalAccountId{ExternalAccountId: req.GetSubscriptionPlanId()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Labels:  req.GetContext().GetBaggage(),
		},
	}
	type startGiftCardWorkflowResult struct {
		reply *giftcardsv1.StartGiftCardWorkflowResponse
		err   error
	}
	startGiftCardWorkflowResults := make(chan startGiftCardWorkflowResult, 1)
	go func() {
		reply, err := s.giftCardWorkflowClient.StartGiftCardWorkflow(ctx, startGiftCardWorkflowRequest)
		startGiftCardWorkflowResults <- startGiftCardWorkflowResult{reply, err}
	}()
	var startGiftCardWorkflowReply *giftcardsv1.StartGiftCardWorkflowResponse
	select {
	case result := <-startGiftCardWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardWorkflowService/StartGiftCardWorkflow")
		}
		startGiftCardWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &subscriptionsv1.GetSubscriptionPlanApprovalStatusResponse{
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{
				DisplayName: startGiftCardWorkflowReply.GetGiftCard().GetHolder().GetDisplayName(),
				Email:       startGiftCardWorkflowReply.GetGiftCard().GetContact().GetEmail(),
			},
			SubscriptionPlanId: req.GetSubscriptionPlanId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// subscriptionApprovalLazy dials acme.subscriptions.v1.SubscriptionApprovalService the first time a handler needs it.
func (s *SubscriptionPlanApprovalServer) subscriptionApprovalLazy() subscriptionsv1.SubscriptionApprovalServiceClient {
	s.subscriptionApprovalOnce.Do(func() { s.subscriptionApprovalCached = subscriptionsv1.NewSubscriptionApprovalServiceClient(s.conn) })
	return s.subscriptionApprovalCached
}

// suggestCustomersSender returns a function that sends one request to
// acme.customers.v1.CustomerQueryService/SuggestCustomers.
func (s *SubscriptionPlanApprovalServer) suggestCustomersSender(request *customersv1.SuggestCustomersRequest) func(context.Context) (*customersv1.SuggestCustomersResponse, error) {
	return func(ctx context.Context) (*customersv1.SuggestCustomersResponse, error) {
		return s.customerQueryClient.SuggestCustomers(ctx, request)
	}
}

// lintLedgerAccountsSender returns a function that sends one request to
// acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts.
func (s *SubscriptionPlanApprovalServer) lintLedgerAccountsSender(request *ledgerv1.LintLedgerAccountsRequest) func(context.Context) (*ledgerv1.LintLedgerAccountsResponse, error) {
	return func(ctx context.Context) (*ledgerv1.LintLedgerAccountsResponse, error) {
		return s.ledgerAccountValidationClient.LintLedgerAccounts(ctx, request)
	}
}
