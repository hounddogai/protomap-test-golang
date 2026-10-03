package loyalty

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	budgetingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	performancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the loyalty domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.audit.v1.AuditEventValidationService", auditv1.NewAuditEventValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.budgeting.v1.BudgetService", budgetingv1.NewBudgetServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardBatchCalculationService", giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardTransactionWorkflowService", giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.SessionQueryService", identityv1.NewSessionQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.LoyaltyMemberValidationService", loyaltyv1.NewLoyaltyMemberValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.RewardLifecycleService", loyaltyv1.NewRewardLifecycleServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.RewardValidationService", loyaltyv1.NewRewardValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.performance.v1.ReviewQueryService", performancev1.NewReviewQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.pipelines.v1.PipelineQueryService", pipelinesv1.NewPipelineQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchIndexExportService", searchv1.NewSearchIndexExportServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchIndexHistoryService", searchv1.NewSearchIndexHistoryServiceClient(deps.Conn))
	deps.Clients.Put("acme.venues.v1.VenueValidationService", venuesv1.NewVenueValidationServiceClient(deps.Conn))
	loyaltyv1.RegisterLoyaltyMemberServiceServer(server, NewLoyaltyMemberServer(deps))
	loyaltyv1.RegisterPointsLedgerQueryServiceServer(server, NewPointsLedgerQueryServer(deps))
	loyaltyv1.RegisterRewardWorkflowServiceServer(server, NewRewardWorkflowServer(deps))
	loyaltyv1.RegisterTierApprovalServiceServer(server, NewTierApprovalServer(deps))
	loyaltyv1.RegisterLoyaltyMemberQueryServiceServer(server, NewLoyaltyMemberQueryServer(deps))
	loyaltyv1.RegisterPointsLedgerWorkflowServiceServer(server, NewPointsLedgerWorkflowServer(deps))
	loyaltyv1.RegisterRewardApprovalServiceServer(server, NewRewardApprovalServer(deps))
	loyaltyv1.RegisterTierValidationServiceServer(server, NewTierValidationServer(deps))
	loyaltyv1.RegisterLoyaltyMemberWorkflowServiceServer(server, NewLoyaltyMemberWorkflowServer(deps))
	loyaltyv1.RegisterPointsLedgerApprovalServiceServer(server, NewPointsLedgerApprovalServer(deps))
	loyaltyv1.RegisterRewardValidationServiceServer(server, NewRewardValidationServer(deps))
	loyaltyv1.RegisterTierCalculationServiceServer(server, NewTierCalculationServer(deps))
	loyaltyMemberApprovalServer := NewLoyaltyMemberApprovalServer(deps)
	server.RegisterService(&loyaltyv1.LoyaltyMemberApprovalService_ServiceDesc, loyaltyMemberApprovalServer)
	loyaltyv1.RegisterPointsLedgerValidationServiceServer(server, NewPointsLedgerValidationServer(deps))
	rewardCalculationServer := NewRewardCalculationServer(deps)
	server.RegisterService(&loyaltyv1.RewardCalculationService_ServiceDesc, rewardCalculationServer)
	loyaltyv1.RegisterTierLifecycleServiceServer(server, NewTierLifecycleServer(deps))
	loyaltyv1.RegisterLoyaltyMemberValidationServiceServer(server, NewLoyaltyMemberValidationServer(deps))
	loyaltyv1.RegisterPointsLedgerCalculationServiceServer(server, NewPointsLedgerCalculationServer(deps))
	loyaltyv1.RegisterRewardLifecycleServiceServer(server, NewRewardLifecycleServer(deps))
	loyaltyv1.RegisterTierReportingServiceServer(server, NewTierReportingServer(deps))
	loyaltyv1.RegisterLoyaltyMemberCalculationServiceServer(server, NewLoyaltyMemberCalculationServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	commentReviewVoteApprovalCall func(context.Context, *reviewsv1.CommentReviewVoteApprovalRequest) (*reviewsv1.CommentReviewVoteApprovalResponse, error)
	reinstateRewardCall           func(context.Context, *loyaltyv1.ReinstateRewardRequest) (*loyaltyv1.ReinstateRewardResponse, error)
	estimateSellerCall            func(context.Context, *sellersv1.EstimateSellerRequest) (*sellersv1.EstimateSellerResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	reviewVoteApprovalForCommentReviewVoteApproval := reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn)
	commentReviewVoteApprovalCall = func(ctx context.Context, request *reviewsv1.CommentReviewVoteApprovalRequest) (*reviewsv1.CommentReviewVoteApprovalResponse, error) {
		return reviewVoteApprovalForCommentReviewVoteApproval.CommentReviewVoteApproval(ctx, request)
	}
	rewardLifecycleForReinstateReward := loyaltyv1.NewRewardLifecycleServiceClient(deps.Conn)
	reinstateRewardCall = func(ctx context.Context, request *loyaltyv1.ReinstateRewardRequest) (*loyaltyv1.ReinstateRewardResponse, error) {
		return rewardLifecycleForReinstateReward.ReinstateReward(ctx, request)
	}
	sellerCalculationForEstimateSeller := sellersv1.NewSellerCalculationServiceClient(deps.Conn)
	estimateSellerCall = func(ctx context.Context, request *sellersv1.EstimateSellerRequest) (*sellersv1.EstimateSellerResponse, error) {
		return sellerCalculationForEstimateSeller.EstimateSeller(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                           *slog.Logger
	loyaltyMemberValidationClient loyaltyv1.LoyaltyMemberValidationServiceClient
	synonymNotificationClient     searchv1.SynonymNotificationServiceClient
	giftCardWorkflowClient        giftcardsv1.GiftCardWorkflowServiceClient
	ledgerAccountValidationClient ledgerv1.LedgerAccountValidationServiceClient
	userValidationClient          identityv1.UserValidationServiceClient
	pointsLedgerQueryClient       loyaltyv1.PointsLedgerQueryServiceClient
	metricSeriesValidationClient  metricsv1.MetricSeriesValidationServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                           deps.Logger,
		loyaltyMemberValidationClient: loyaltyv1.NewLoyaltyMemberValidationServiceClient(deps.Conn),
		synonymNotificationClient:     searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		giftCardWorkflowClient:        giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		ledgerAccountValidationClient: ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		userValidationClient:          identityv1.NewUserValidationServiceClient(deps.Conn),
		pointsLedgerQueryClient:       loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		metricSeriesValidationClient:  metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
	}
	go w.consume(ctx, "loyalty.peek_loyalty_member.score_loyalty_member", deps.Bus.Subscribe("loyalty.peek_loyalty_member.score_loyalty_member"), w.handleLoyaltyPeekLoyaltyMemberScoreLoyaltyMember)
	go w.consume(ctx, "loyalty.approve_reward.unsubscribe_synonym_updates", deps.Bus.Subscribe("loyalty.approve_reward.unsubscribe_synonym_updates"), w.handleLoyaltyApproveRewardUnsubscribeSynonymUpdates)
	go w.consume(ctx, "loyalty.pause_loyalty_member_workflow.retry_gift_card_workflow", deps.Bus.Subscribe("loyalty.pause_loyalty_member_workflow.retry_gift_card_workflow"), w.handleLoyaltyPauseLoyaltyMemberWorkflowRetryGiftCardWorkflow)
	go w.consume(ctx, "loyalty.estimate_tier.check_ledger_account_eligibility", deps.Bus.Subscribe("loyalty.estimate_tier.check_ledger_account_eligibility"), w.handleLoyaltyEstimateTierCheckLedgerAccountEligibility)
	go w.consume(ctx, "loyalty.approve_loyalty_member.check_user_eligibility", deps.Bus.Subscribe("loyalty.approve_loyalty_member.check_user_eligibility"), w.handleLoyaltyApproveLoyaltyMemberCheckUserEligibility)
	go w.consume(ctx, "loyalty.quote_reward.explain_loyalty_member_decision", deps.Bus.Subscribe("loyalty.quote_reward.explain_loyalty_member_decision"), w.handleLoyaltyQuoteRewardExplainLoyaltyMemberDecision)
	go w.consume(ctx, "loyalty.schedule_reward_expiry.suggest_points_ledgers", deps.Bus.Subscribe("loyalty.schedule_reward_expiry.suggest_points_ledgers"), w.handleLoyaltyScheduleRewardExpirySuggestPointsLedgers)
	go w.consume(ctx, "loyalty.estimate_loyalty_member.check_metric_series_eligibility", deps.Bus.Subscribe("loyalty.estimate_loyalty_member.check_metric_series_eligibility"), w.handleLoyaltyEstimateLoyaltyMemberCheckMetricSeriesEligibility)
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

// handleLoyaltyPeekLoyaltyMemberScoreLoyaltyMember makes the call the loyalty.peek_loyalty_member.score_loyalty_member
// event asks for.
func (w *workers) handleLoyaltyPeekLoyaltyMemberScoreLoyaltyMember(ctx context.Context, event eventbus.Event) error {
	_, err := w.loyaltyMemberValidationClient.ScoreLoyaltyMember(ctx, &loyaltyv1.ScoreLoyaltyMemberRequest{
		LoyaltyMemberId: event.Attributes["loyalty_member_id"],
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: event.Attributes["loyalty_member.contact.email"]},
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

// handleLoyaltyApproveRewardUnsubscribeSynonymUpdates makes the call the
// loyalty.approve_reward.unsubscribe_synonym_updates event asks for.
func (w *workers) handleLoyaltyApproveRewardUnsubscribeSynonymUpdates(ctx context.Context, event eventbus.Event) error {
	_, err := w.synonymNotificationClient.UnsubscribeSynonymUpdates(ctx, &searchv1.UnsubscribeSynonymUpdatesRequest{
		Details: &searchv1.UnsubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: event.Attributes["details.recipient.email"]},
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

// handleLoyaltyPauseLoyaltyMemberWorkflowRetryGiftCardWorkflow makes the call the
// loyalty.pause_loyalty_member_workflow.retry_gift_card_workflow event asks for.
func (w *workers) handleLoyaltyPauseLoyaltyMemberWorkflowRetryGiftCardWorkflow(ctx context.Context, event eventbus.Event) error {
	_, err := w.giftCardWorkflowClient.RetryGiftCardWorkflow(ctx, &giftcardsv1.RetryGiftCardWorkflowRequest{
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: event.Attributes["gift_card.audit.created_by"],
				UpdatedBy: event.Attributes["gift_card.audit.updated_by"],
			},
		},
		Details: &giftcardsv1.RetryGiftCardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: event.Attributes["details.notify.phone"]},
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

// handleLoyaltyEstimateTierCheckLedgerAccountEligibility makes the call the
// loyalty.estimate_tier.check_ledger_account_eligibility event asks for.
func (w *workers) handleLoyaltyEstimateTierCheckLedgerAccountEligibility(ctx context.Context, event eventbus.Event) error {
	_, err := w.ledgerAccountValidationClient.CheckLedgerAccountEligibility(ctx, &ledgerv1.CheckLedgerAccountEligibilityRequest{
		LedgerAccount: &ledgerv1.LedgerAccount{
			Funding: &ledgerv1.LedgerAccount_Card{Card: &ledgerv1.LedgerAccountCardFunding{
				HolderName: event.Attributes["ledger_account.card.holder_name"],
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: event.Attributes["ledger_account.audit.updated_by"],
			},
			Contact: &commonv1.ContactInfo{Phone: event.Attributes["ledger_account.contact.phone"]},
		},
		Details: &ledgerv1.CheckLedgerAccountEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: event.Attributes["details.contact.email"]},
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

// handleLoyaltyApproveLoyaltyMemberCheckUserEligibility makes the call the
// loyalty.approve_loyalty_member.check_user_eligibility event asks for.
func (w *workers) handleLoyaltyApproveLoyaltyMemberCheckUserEligibility(ctx context.Context, event eventbus.Event) error {
	_, err := w.userValidationClient.CheckUserEligibility(ctx, &identityv1.CheckUserEligibilityRequest{
		Details: &identityv1.CheckUserEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: event.Attributes["details.contact.email"],
				Phone: event.Attributes["details.contact.phone"],
			},
		},
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{UpdatedBy: event.Attributes["user.audit.updated_by"]},
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

// handleLoyaltyQuoteRewardExplainLoyaltyMemberDecision makes the call the
// loyalty.quote_reward.explain_loyalty_member_decision event asks for.
func (w *workers) handleLoyaltyQuoteRewardExplainLoyaltyMemberDecision(ctx context.Context, event eventbus.Event) error {
	_, err := w.loyaltyMemberValidationClient.ExplainLoyaltyMemberDecision(ctx, &loyaltyv1.ExplainLoyaltyMemberDecisionRequest{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				CreatedBy: event.Attributes["loyalty_member.audit.created_by"],
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

// handleLoyaltyScheduleRewardExpirySuggestPointsLedgers makes the call the
// loyalty.schedule_reward_expiry.suggest_points_ledgers event asks for.
func (w *workers) handleLoyaltyScheduleRewardExpirySuggestPointsLedgers(ctx context.Context, event eventbus.Event) error {
	_, err := w.pointsLedgerQueryClient.SuggestPointsLedgers(ctx, &loyaltyv1.SuggestPointsLedgersRequest{
		Filter: &loyaltyv1.SuggestPointsLedgersRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
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

// handleLoyaltyEstimateLoyaltyMemberCheckMetricSeriesEligibility makes the call the
// loyalty.estimate_loyalty_member.check_metric_series_eligibility event asks for.
func (w *workers) handleLoyaltyEstimateLoyaltyMemberCheckMetricSeriesEligibility(ctx context.Context, event eventbus.Event) error {
	_, err := w.metricSeriesValidationClient.CheckMetricSeriesEligibility(ctx, &metricsv1.CheckMetricSeriesEligibilityRequest{
		MetricSeries: &metricsv1.MetricSeries{
			Owner: &identityv1.UserRef{Email: event.Attributes["metric_series.owner.email"]},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: event.Attributes["metric_series.audit.updated_by"],
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
