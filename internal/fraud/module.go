package fraud

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the fraud domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.customers.v1.CustomerCalculationService", customersv1.NewCustomerCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerQueryService", customersv1.NewCustomerQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerSegmentValidationService", customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.HouseholdQueryService", customersv1.NewHouseholdQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.documents.v1.DocumentService", documentsv1.NewDocumentServiceClient(deps.Conn))
	deps.Clients.Put("acme.fraud.v1.FraudRuleCalculationService", fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.fraud.v1.FraudSignalQueryService", fraudv1.NewFraudSignalQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.fraud.v1.RiskAssessmentCalculationService", fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.fraud.v1.RiskAssessmentValidationService", fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.fraud.v1.WatchlistReportingService", fraudv1.NewWatchlistReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardCalculationService", giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.RoleHistoryService", identityv1.NewRoleHistoryServiceClient(deps.Conn))
	deps.Clients.Put("acme.returns.v1.ReturnRequestValidationService", returnsv1.NewReturnRequestValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.reviews.v1.ReviewModerationQueryService", reviewsv1.NewReviewModerationQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SynonymSyncService", searchv1.NewSynonymSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerListingQueryService", sellersv1.NewSellerListingQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.sponsorships.v1.SponsorService", sponsorshipsv1.NewSponsorServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.SubscriptionPlanQueryService", subscriptionsv1.NewSubscriptionPlanQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.timekeeping.v1.TimesheetQueryService", timekeepingv1.NewTimesheetQueryServiceClient(deps.Conn))
	fraudv1.RegisterRiskAssessmentServiceServer(server, NewRiskAssessmentServer(deps))
	fraudSignalQueryServer := NewFraudSignalQueryServer(deps)
	server.RegisterService(&fraudv1.FraudSignalQueryService_ServiceDesc, fraudSignalQueryServer)
	fraudRuleWorkflowServer := NewFraudRuleWorkflowServer(deps)
	server.RegisterService(&fraudv1.FraudRuleWorkflowService_ServiceDesc, fraudRuleWorkflowServer)
	fraudv1.RegisterWatchlistApprovalServiceServer(server, NewWatchlistApprovalServer(deps))
	fraudv1.RegisterRiskAssessmentQueryServiceServer(server, NewRiskAssessmentQueryServer(deps))
	fraudv1.RegisterFraudSignalWorkflowServiceServer(server, NewFraudSignalWorkflowServer(deps))
	fraudv1.RegisterFraudRuleApprovalServiceServer(server, NewFraudRuleApprovalServer(deps))
	fraudv1.RegisterWatchlistValidationServiceServer(server, NewWatchlistValidationServer(deps))
	fraudv1.RegisterRiskAssessmentWorkflowServiceServer(server, NewRiskAssessmentWorkflowServer(deps))
	fraudv1.RegisterFraudSignalApprovalServiceServer(server, NewFraudSignalApprovalServer(deps))
	fraudv1.RegisterFraudRuleValidationServiceServer(server, NewFraudRuleValidationServer(deps))
	watchlistCalculationServer := NewWatchlistCalculationServer(deps)
	server.RegisterService(&fraudv1.WatchlistCalculationService_ServiceDesc, watchlistCalculationServer)
	fraudv1.RegisterRiskAssessmentApprovalServiceServer(server, NewRiskAssessmentApprovalServer(deps))
	fraudSignalValidationServer := NewFraudSignalValidationServer(deps)
	server.RegisterService(&fraudv1.FraudSignalValidationService_ServiceDesc, fraudSignalValidationServer)
	fraudRuleCalculationServer := NewFraudRuleCalculationServer(deps)
	server.RegisterService(&fraudv1.FraudRuleCalculationService_ServiceDesc, fraudRuleCalculationServer)
	fraudv1.RegisterWatchlistLifecycleServiceServer(server, NewWatchlistLifecycleServer(deps))
	fraudv1.RegisterRiskAssessmentValidationServiceServer(server, NewRiskAssessmentValidationServer(deps))
	fraudv1.RegisterFraudSignalCalculationServiceServer(server, NewFraudSignalCalculationServer(deps))
	fraudRuleLifecycleServer := NewFraudRuleLifecycleServer(deps)
	server.RegisterService(&fraudv1.FraudRuleLifecycleService_ServiceDesc, fraudRuleLifecycleServer)
	fraudv1.RegisterWatchlistReportingServiceServer(server, NewWatchlistReportingServer(deps))
	fraudv1.RegisterRiskAssessmentCalculationServiceServer(server, NewRiskAssessmentCalculationServer(deps))
	fraudv1.RegisterFraudSignalLifecycleServiceServer(server, NewFraudSignalLifecycleServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	commentSellerListingApprovalCall func(context.Context, *sellersv1.CommentSellerListingApprovalRequest) (*sellersv1.CommentSellerListingApprovalResponse, error)
	estimateFraudRuleCall            func(context.Context, *fraudv1.EstimateFraudRuleRequest) (*fraudv1.EstimateFraudRuleResponse, error)
	explainReturnLabelDecisionCall   func(context.Context, *returnsv1.ExplainReturnLabelDecisionRequest, ...grpc.CallOption) (*returnsv1.ExplainReturnLabelDecisionResponse, error)
	normalizeRiskAssessmentCall      func(context.Context, *fraudv1.NormalizeRiskAssessmentRequest) (*fraudv1.NormalizeRiskAssessmentResponse, error)
	scoreRiskAssessmentCall          func(context.Context, *fraudv1.ScoreRiskAssessmentRequest, ...grpc.CallOption) (*fraudv1.ScoreRiskAssessmentResponse, error)
	normalizeFraudSignalCall         func(context.Context, *fraudv1.NormalizeFraudSignalRequest) (*fraudv1.NormalizeFraudSignalResponse, error)
	getTenantPreferencesCall         func(context.Context, *identityv1.GetTenantPreferencesRequest) (*identityv1.GetTenantPreferencesResponse, error)
	lintRiskAssessmentsCall          func(context.Context, *fraudv1.LintRiskAssessmentsRequest, ...grpc.CallOption) (*fraudv1.LintRiskAssessmentsResponse, error)
	estimateFraudSignalCall          func(context.Context, *fraudv1.EstimateFraudSignalRequest, ...grpc.CallOption) (*fraudv1.EstimateFraudSignalResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	sellerListingApprovalForCommentSellerListingApproval := sellersv1.NewSellerListingApprovalServiceClient(deps.Conn)
	commentSellerListingApprovalCall = func(ctx context.Context, request *sellersv1.CommentSellerListingApprovalRequest) (*sellersv1.CommentSellerListingApprovalResponse, error) {
		return sellerListingApprovalForCommentSellerListingApproval.CommentSellerListingApproval(ctx, request)
	}
	fraudRuleCalculationForEstimateFraudRule := fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn)
	estimateFraudRuleCall = func(ctx context.Context, request *fraudv1.EstimateFraudRuleRequest) (*fraudv1.EstimateFraudRuleResponse, error) {
		return fraudRuleCalculationForEstimateFraudRule.EstimateFraudRule(ctx, request)
	}
	returnLabelValidationForExplainReturnLabelDecision := returnsv1.NewReturnLabelValidationServiceClient(deps.Conn)
	explainReturnLabelDecisionCall = returnLabelValidationForExplainReturnLabelDecision.ExplainReturnLabelDecision
	riskAssessmentValidationForNormalizeRiskAssessment := fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn)
	normalizeRiskAssessmentCall = func(ctx context.Context, request *fraudv1.NormalizeRiskAssessmentRequest) (*fraudv1.NormalizeRiskAssessmentResponse, error) {
		return riskAssessmentValidationForNormalizeRiskAssessment.NormalizeRiskAssessment(ctx, request)
	}
	riskAssessmentValidationForScoreRiskAssessment := fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn)
	scoreRiskAssessmentCall = riskAssessmentValidationForScoreRiskAssessment.ScoreRiskAssessment
	fraudSignalValidationForNormalizeFraudSignal := fraudv1.NewFraudSignalValidationServiceClient(deps.Conn)
	normalizeFraudSignalCall = func(ctx context.Context, request *fraudv1.NormalizeFraudSignalRequest) (*fraudv1.NormalizeFraudSignalResponse, error) {
		return fraudSignalValidationForNormalizeFraudSignal.NormalizeFraudSignal(ctx, request)
	}
	tenantNotificationForGetTenantPreferences := identityv1.NewTenantNotificationServiceClient(deps.Conn)
	getTenantPreferencesCall = func(ctx context.Context, request *identityv1.GetTenantPreferencesRequest) (*identityv1.GetTenantPreferencesResponse, error) {
		return tenantNotificationForGetTenantPreferences.GetTenantPreferences(ctx, request)
	}
	riskAssessmentValidationForLintRiskAssessments := fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn)
	lintRiskAssessmentsCall = riskAssessmentValidationForLintRiskAssessments.LintRiskAssessments
	fraudSignalCalculationForEstimateFraudSignal := fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn)
	estimateFraudSignalCall = fraudSignalCalculationForEstimateFraudSignal.EstimateFraudSignal
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                             *slog.Logger
	watchlistValidationClient       fraudv1.WatchlistValidationServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	consentRecordValidationClient   customersv1.ConsentRecordValidationServiceClient
	sellerScorecardApprovalClient   sellersv1.SellerScorecardApprovalServiceClient
	notificationQueryClient         notificationsv1.NotificationQueryServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                             deps.Logger,
		watchlistValidationClient:       fraudv1.NewWatchlistValidationServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		consentRecordValidationClient:   customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		sellerScorecardApprovalClient:   sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		notificationQueryClient:         notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
	}
	go w.consume(ctx, "fraud.create_risk_assessment.score_watchlist", deps.Bus.Subscribe("fraud.create_risk_assessment.score_watchlist"), w.handleFraudCreateRiskAssessmentScoreWatchlist)
	go w.consume(ctx, "fraud.update_risk_assessment.validate_watchlist", deps.Bus.Subscribe("fraud.update_risk_assessment.validate_watchlist"), w.handleFraudUpdateRiskAssessmentValidateWatchlist)
	go w.consume(ctx, "fraud.delegate_fraud_rule_approval.explain_customer_segment_decision", deps.Bus.Subscribe("fraud.delegate_fraud_rule_approval.explain_customer_segment_decision"), w.handleFraudDelegateFraudRuleApprovalExplainCustomerSegmentDecision)
	go w.consume(ctx, "fraud.cancel_risk_assessment_workflow.score_consent_record", deps.Bus.Subscribe("fraud.cancel_risk_assessment_workflow.score_consent_record"), w.handleFraudCancelRiskAssessmentWorkflowScoreConsentRecord)
	go w.consume(ctx, "fraud.comment_risk_assessment_approval.submit_seller_scorecard", deps.Bus.Subscribe("fraud.comment_risk_assessment_approval.submit_seller_scorecard"), w.handleFraudCommentRiskAssessmentApprovalSubmitSellerScorecard)
	go w.consume(ctx, "fraud.expire_fraud_rule.summarize_notifications", deps.Bus.Subscribe("fraud.expire_fraud_rule.summarize_notifications"), w.handleFraudExpireFraudRuleSummarizeNotifications)
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

// handleFraudCreateRiskAssessmentScoreWatchlist makes the call the fraud.create_risk_assessment.score_watchlist event
// asks for.
func (w *workers) handleFraudCreateRiskAssessmentScoreWatchlist(ctx context.Context, event eventbus.Event) error {
	_, err := w.watchlistValidationClient.ScoreWatchlist(ctx, &fraudv1.ScoreWatchlistRequest{
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						Email: event.Attributes["watchlist.children.children.manager.email"],
					},
				}},
				Manager: &identityv1.UserRef{
					DisplayName: event.Attributes["watchlist.children.manager.display_name"],
				},
			}},
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

// handleFraudUpdateRiskAssessmentValidateWatchlist makes the call the fraud.update_risk_assessment.validate_watchlist
// event asks for.
func (w *workers) handleFraudUpdateRiskAssessmentValidateWatchlist(ctx context.Context, event eventbus.Event) error {
	_, err := w.watchlistValidationClient.ValidateWatchlist(ctx, &fraudv1.ValidateWatchlistRequest{
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Manager: &identityv1.UserRef{
						UserId: event.Attributes["watchlist.children.children.manager.user_id"],
					},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: event.Attributes["watchlist.children.children.audit.updated_by"],
					},
				}},
			}},
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

// handleFraudDelegateFraudRuleApprovalExplainCustomerSegmentDecision makes the call the
// fraud.delegate_fraud_rule_approval.explain_customer_segment_decision event asks for.
func (w *workers) handleFraudDelegateFraudRuleApprovalExplainCustomerSegmentDecision(ctx context.Context, event eventbus.Event) error {
	_, err := w.customerSegmentValidationClient.ExplainCustomerSegmentDecision(ctx, &customersv1.ExplainCustomerSegmentDecisionRequest{
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				UserId: event.Attributes["customer_segment.owner.user_id"],
				Email:  event.Attributes["customer_segment.owner.email"],
			},
		},
		Details: &customersv1.ExplainCustomerSegmentDecisionRequest_Details{
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

// handleFraudCancelRiskAssessmentWorkflowScoreConsentRecord makes the call the
// fraud.cancel_risk_assessment_workflow.score_consent_record event asks for.
func (w *workers) handleFraudCancelRiskAssessmentWorkflowScoreConsentRecord(ctx context.Context, event eventbus.Event) error {
	_, err := w.consentRecordValidationClient.ScoreConsentRecord(ctx, &customersv1.ScoreConsentRecordRequest{
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: event.Attributes["consent_record.subject.display_name"],
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

// handleFraudCommentRiskAssessmentApprovalSubmitSellerScorecard makes the call the
// fraud.comment_risk_assessment_approval.submit_seller_scorecard event asks for.
func (w *workers) handleFraudCommentRiskAssessmentApprovalSubmitSellerScorecard(ctx context.Context, event eventbus.Event) error {
	_, err := w.sellerScorecardApprovalClient.SubmitSellerScorecard(ctx, &sellersv1.SubmitSellerScorecardRequest{
		Details: &sellersv1.SubmitSellerScorecardRequest_Details{
			Approver: &identityv1.UserRef{Email: event.Attributes["details.approver.email"]},
		},
		SellerScorecard: &sellersv1.SellerScorecard{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: event.Attributes["seller_scorecard.counterparty.display_name"],
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

// handleFraudExpireFraudRuleSummarizeNotifications makes the call the fraud.expire_fraud_rule.summarize_notifications
// event asks for.
func (w *workers) handleFraudExpireFraudRuleSummarizeNotifications(ctx context.Context, event eventbus.Event) error {
	_, err := w.notificationQueryClient.SummarizeNotifications(ctx, &notificationsv1.SummarizeNotificationsRequest{
		Reason: event.Attributes["reason"],
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
