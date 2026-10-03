package subscriptions

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the subscriptions domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.customers.v1.ConsentRecordValidationService", customersv1.NewConsentRecordValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.LoyaltyMemberQueryService", loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.metrics.v1.MetricSeriesService", metricsv1.NewMetricSeriesServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.RenewalScheduleApprovalService", subscriptionsv1.NewRenewalScheduleApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.SubscriptionApprovalService", subscriptionsv1.NewSubscriptionApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.SubscriptionPlanValidationService", subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.SubscriptionQueryService", subscriptionsv1.NewSubscriptionQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.SubscriptionWorkflowService", subscriptionsv1.NewSubscriptionWorkflowServiceClient(deps.Conn))
	subscriptionsv1.RegisterSubscriptionServiceServer(server, NewSubscriptionServer(deps))
	subscriptionPlanQueryServer := NewSubscriptionPlanQueryServer(deps)
	server.RegisterService(&subscriptionsv1.SubscriptionPlanQueryService_ServiceDesc, subscriptionPlanQueryServer)
	subscriptionsv1.RegisterRenewalScheduleWorkflowServiceServer(server, NewRenewalScheduleWorkflowServer(deps))
	subscriptionsv1.RegisterUsageRecordApprovalServiceServer(server, NewUsageRecordApprovalServer(deps))
	subscriptionsv1.RegisterSubscriptionQueryServiceServer(server, NewSubscriptionQueryServer(deps))
	subscriptionsv1.RegisterSubscriptionPlanWorkflowServiceServer(server, NewSubscriptionPlanWorkflowServer(deps))
	subscriptionsv1.RegisterRenewalScheduleApprovalServiceServer(server, NewRenewalScheduleApprovalServer(deps))
	subscriptionsv1.RegisterUsageRecordValidationServiceServer(server, NewUsageRecordValidationServer(deps))
	subscriptionsv1.RegisterSubscriptionWorkflowServiceServer(server, NewSubscriptionWorkflowServer(deps))
	subscriptionPlanApprovalServer := NewSubscriptionPlanApprovalServer(deps)
	server.RegisterService(&subscriptionsv1.SubscriptionPlanApprovalService_ServiceDesc, subscriptionPlanApprovalServer)
	subscriptionsv1.RegisterRenewalScheduleValidationServiceServer(server, NewRenewalScheduleValidationServer(deps))
	subscriptionsv1.RegisterUsageRecordCalculationServiceServer(server, NewUsageRecordCalculationServer(deps))
	subscriptionsv1.RegisterSubscriptionApprovalServiceServer(server, NewSubscriptionApprovalServer(deps))
	subscriptionsv1.RegisterSubscriptionPlanValidationServiceServer(server, NewSubscriptionPlanValidationServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	validateUsageRecordCall              func(context.Context, *subscriptionsv1.ValidateUsageRecordRequest) (*subscriptionsv1.ValidateUsageRecordResponse, error)
	explainSellerDecisionCall            func(context.Context, *sellersv1.ExplainSellerDecisionRequest) (*sellersv1.ExplainSellerDecisionResponse, error)
	checkSubscriptionPlanEligibilityCall func(context.Context, *subscriptionsv1.CheckSubscriptionPlanEligibilityRequest) (*subscriptionsv1.CheckSubscriptionPlanEligibilityResponse, error)
	commentSellerScorecardApprovalCall   func(context.Context, *sellersv1.CommentSellerScorecardApprovalRequest) (*sellersv1.CommentSellerScorecardApprovalResponse, error)
	suggestSubscriptionsCall             func(context.Context, *subscriptionsv1.SuggestSubscriptionsRequest) (*subscriptionsv1.SuggestSubscriptionsResponse, error)
	startMerchandisingRuleWorkflowCall   func(context.Context, *merchandisingv1.StartMerchandisingRuleWorkflowRequest, ...grpc.CallOption) (*merchandisingv1.StartMerchandisingRuleWorkflowResponse, error)
	resolveLoyaltyMembersCall            func(context.Context, *loyaltyv1.ResolveLoyaltyMembersRequest) (*loyaltyv1.ResolveLoyaltyMembersResponse, error)
	summarizeProductReviewsCall          func(context.Context, *reviewsv1.SummarizeProductReviewsRequest, ...grpc.CallOption) (*reviewsv1.SummarizeProductReviewsResponse, error)
	lintAccessPoliciesCall               func(context.Context, *authorizationv1.LintAccessPoliciesRequest, ...grpc.CallOption) (*authorizationv1.LintAccessPoliciesResponse, error)
	delegatePointsLedgerApprovalCall     func(context.Context, *loyaltyv1.DelegatePointsLedgerApprovalRequest, ...grpc.CallOption) (*loyaltyv1.DelegatePointsLedgerApprovalResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	usageRecordValidationForValidateUsageRecord := subscriptionsv1.NewUsageRecordValidationServiceClient(deps.Conn)
	validateUsageRecordCall = func(ctx context.Context, request *subscriptionsv1.ValidateUsageRecordRequest) (*subscriptionsv1.ValidateUsageRecordResponse, error) {
		return usageRecordValidationForValidateUsageRecord.ValidateUsageRecord(ctx, request)
	}
	sellerValidationForExplainSellerDecision := sellersv1.NewSellerValidationServiceClient(deps.Conn)
	explainSellerDecisionCall = func(ctx context.Context, request *sellersv1.ExplainSellerDecisionRequest) (*sellersv1.ExplainSellerDecisionResponse, error) {
		return sellerValidationForExplainSellerDecision.ExplainSellerDecision(ctx, request)
	}
	subscriptionPlanValidationForCheckSubscriptionPlanEligibility := subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn)
	checkSubscriptionPlanEligibilityCall = func(ctx context.Context, request *subscriptionsv1.CheckSubscriptionPlanEligibilityRequest) (*subscriptionsv1.CheckSubscriptionPlanEligibilityResponse, error) {
		return subscriptionPlanValidationForCheckSubscriptionPlanEligibility.CheckSubscriptionPlanEligibility(ctx, request)
	}
	sellerScorecardApprovalForCommentSellerScorecardApproval := sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn)
	commentSellerScorecardApprovalCall = func(ctx context.Context, request *sellersv1.CommentSellerScorecardApprovalRequest) (*sellersv1.CommentSellerScorecardApprovalResponse, error) {
		return sellerScorecardApprovalForCommentSellerScorecardApproval.CommentSellerScorecardApproval(ctx, request)
	}
	subscriptionQueryForSuggestSubscriptions := subscriptionsv1.NewSubscriptionQueryServiceClient(deps.Conn)
	suggestSubscriptionsCall = func(ctx context.Context, request *subscriptionsv1.SuggestSubscriptionsRequest) (*subscriptionsv1.SuggestSubscriptionsResponse, error) {
		return subscriptionQueryForSuggestSubscriptions.SuggestSubscriptions(ctx, request)
	}
	merchandisingRuleWorkflowForStartMerchandisingRuleWorkflow := merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn)
	startMerchandisingRuleWorkflowCall = merchandisingRuleWorkflowForStartMerchandisingRuleWorkflow.StartMerchandisingRuleWorkflow
	loyaltyMemberQueryForResolveLoyaltyMembers := loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn)
	resolveLoyaltyMembersCall = func(ctx context.Context, request *loyaltyv1.ResolveLoyaltyMembersRequest) (*loyaltyv1.ResolveLoyaltyMembersResponse, error) {
		return loyaltyMemberQueryForResolveLoyaltyMembers.ResolveLoyaltyMembers(ctx, request)
	}
	productReviewQueryForSummarizeProductReviews := reviewsv1.NewProductReviewQueryServiceClient(deps.Conn)
	summarizeProductReviewsCall = productReviewQueryForSummarizeProductReviews.SummarizeProductReviews
	accessPolicyValidationForLintAccessPolicies := authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn)
	lintAccessPoliciesCall = accessPolicyValidationForLintAccessPolicies.LintAccessPolicies
	pointsLedgerApprovalForDelegatePointsLedgerApproval := loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn)
	delegatePointsLedgerApprovalCall = pointsLedgerApprovalForDelegatePointsLedgerApproval.DelegatePointsLedgerApproval
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                           *slog.Logger
	renewalScheduleApprovalClient subscriptionsv1.RenewalScheduleApprovalServiceClient
	productReviewClient           reviewsv1.ProductReviewServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                           deps.Logger,
		renewalScheduleApprovalClient: subscriptionsv1.NewRenewalScheduleApprovalServiceClient(deps.Conn),
		productReviewClient:           reviewsv1.NewProductReviewServiceClient(deps.Conn),
	}
	go w.consume(ctx, "subscriptions.retry_subscription_plan_workflow.approve_renewal_schedule", deps.Bus.Subscribe("subscriptions.retry_subscription_plan_workflow.approve_renewal_schedule"), w.handleSubscriptionsRetrySubscriptionPlanWorkflowApproveRenewalSchedule)
	go w.consume(ctx, "subscriptions.normalize_subscription_plan.search_product_reviews", deps.Bus.Subscribe("subscriptions.normalize_subscription_plan.search_product_reviews"), w.handleSubscriptionsNormalizeSubscriptionPlanSearchProductReviews)
}

func (w *workers) consume(ctx context.Context, topic string, events <-chan eventbus.Event, handle func(context.Context, eventbus.Event) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			if err := handle(ctx, event); err != nil {
				w.log.ErrorContext(ctx, "event failed", "topic", topic, "error", err)
			}
		}
	}
}

// handleSubscriptionsRetrySubscriptionPlanWorkflowApproveRenewalSchedule makes the call the
// subscriptions.retry_subscription_plan_workflow.approve_renewal_schedule event asks for.
func (w *workers) handleSubscriptionsRetrySubscriptionPlanWorkflowApproveRenewalSchedule(ctx context.Context, event eventbus.Event) error {
	_, err := w.renewalScheduleApprovalClient.ApproveRenewalSchedule(ctx, &subscriptionsv1.ApproveRenewalScheduleRequest{
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            event.Attributes["renewal_schedule.participants.contact.email"],
					PreferredChannel: event.Attributes["renewal_schedule.participants.contact.preferred_channel"],
				},
			}},
			Audit: &commonv1.AuditInfo{
				Etag:      event.Attributes["renewal_schedule.audit.etag"],
				CreatedBy: event.Attributes["renewal_schedule.audit.created_by"],
			},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}

// handleSubscriptionsNormalizeSubscriptionPlanSearchProductReviews makes the call the
// subscriptions.normalize_subscription_plan.search_product_reviews event asks for.
func (w *workers) handleSubscriptionsNormalizeSubscriptionPlanSearchProductReviews(ctx context.Context, event eventbus.Event) error {
	_, err := w.productReviewClient.SearchProductReviews(ctx, &reviewsv1.SearchProductReviewsRequest{
		Filter: &reviewsv1.SearchProductReviewsRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}
