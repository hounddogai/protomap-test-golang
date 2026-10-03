package returns

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the returns domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.audit.v1.AuditEventQueryService", auditv1.NewAuditEventQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.PreferenceValidationService", customersv1.NewPreferenceValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.LoyaltyMemberService", loyaltyv1.NewLoyaltyMemberServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.PointsLedgerApprovalService", loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.returns.v1.InspectionValidationService", returnsv1.NewInspectionValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchQueryHistoryService", searchv1.NewSearchQueryHistoryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchQueryQueryService", searchv1.NewSearchQueryQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SynonymSyncService", searchv1.NewSynonymSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerListingWorkflowService", sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.RenewalScheduleValidationService", subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn))
	returnsv1.RegisterReturnRequestServiceServer(server, NewReturnRequestServer(deps))
	returnLabelQueryServer := NewReturnLabelQueryServer(deps)
	server.RegisterService(&returnsv1.ReturnLabelQueryService_ServiceDesc, returnLabelQueryServer)
	returnsv1.RegisterInspectionWorkflowServiceServer(server, NewInspectionWorkflowServer(deps))
	returnsv1.RegisterRestockDecisionApprovalServiceServer(server, NewRestockDecisionApprovalServer(deps))
	returnsv1.RegisterReturnRequestQueryServiceServer(server, NewReturnRequestQueryServer(deps))
	returnLabelWorkflowServer := NewReturnLabelWorkflowServer(deps)
	server.RegisterService(&returnsv1.ReturnLabelWorkflowService_ServiceDesc, returnLabelWorkflowServer)
	returnsv1.RegisterInspectionApprovalServiceServer(server, NewInspectionApprovalServer(deps))
	returnsv1.RegisterRestockDecisionValidationServiceServer(server, NewRestockDecisionValidationServer(deps))
	returnRequestWorkflowServer := NewReturnRequestWorkflowServer(deps)
	server.RegisterService(&returnsv1.ReturnRequestWorkflowService_ServiceDesc, returnRequestWorkflowServer)
	returnsv1.RegisterReturnLabelApprovalServiceServer(server, NewReturnLabelApprovalServer(deps))
	returnsv1.RegisterInspectionValidationServiceServer(server, NewInspectionValidationServer(deps))
	returnsv1.RegisterRestockDecisionCalculationServiceServer(server, NewRestockDecisionCalculationServer(deps))
	returnsv1.RegisterReturnRequestApprovalServiceServer(server, NewReturnRequestApprovalServer(deps))
	returnsv1.RegisterReturnLabelValidationServiceServer(server, NewReturnLabelValidationServer(deps))
	inspectionCalculationServer := NewInspectionCalculationServer(deps)
	server.RegisterService(&returnsv1.InspectionCalculationService_ServiceDesc, inspectionCalculationServer)
	returnsv1.RegisterRestockDecisionLifecycleServiceServer(server, NewRestockDecisionLifecycleServer(deps))
	returnsv1.RegisterReturnRequestValidationServiceServer(server, NewReturnRequestValidationServer(deps))
	returnsv1.RegisterReturnLabelCalculationServiceServer(server, NewReturnLabelCalculationServer(deps))
	returnsv1.RegisterInspectionLifecycleServiceServer(server, NewInspectionLifecycleServer(deps))
	returnsv1.RegisterRestockDecisionReportingServiceServer(server, NewRestockDecisionReportingServer(deps))
	returnsv1.RegisterReturnRequestCalculationServiceServer(server, NewReturnRequestCalculationServer(deps))
	returnsv1.RegisterReturnLabelLifecycleServiceServer(server, NewReturnLabelLifecycleServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	estimateReturnRequestCall           func(context.Context, *returnsv1.EstimateReturnRequestRequest) (*returnsv1.EstimateReturnRequestResponse, error)
	countSellerScorecardTotalsCall      func(context.Context, *sellersv1.CountSellerScorecardTotalsRequest) (*sellersv1.CountSellerScorecardTotalsResponse, error)
	calculateReturnLabelCall            func(context.Context, *returnsv1.CalculateReturnLabelRequest, ...grpc.CallOption) (*returnsv1.CalculateReturnLabelResponse, error)
	createCustomerCall                  func(context.Context, *customersv1.CreateCustomerRequest, ...grpc.CallOption) (*customersv1.CreateCustomerResponse, error)
	explainReturnRequestCalculationCall func(context.Context, *returnsv1.ExplainReturnRequestCalculationRequest, ...grpc.CallOption) (*returnsv1.ExplainReturnRequestCalculationResponse, error)
	listPendingTiersCall                func(context.Context, *loyaltyv1.ListPendingTiersRequest) (*loyaltyv1.ListPendingTiersResponse, error)
	simulateCustomerSegmentCall         func(context.Context, *customersv1.SimulateCustomerSegmentRequest, ...grpc.CallOption) (*customersv1.SimulateCustomerSegmentResponse, error)
	batchGetTranslationKeysCall         func(context.Context, *localizationv1.BatchGetTranslationKeysRequest, ...grpc.CallOption) (*localizationv1.BatchGetTranslationKeysResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	returnRequestCalculationForEstimateReturnRequest := returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn)
	estimateReturnRequestCall = func(ctx context.Context, request *returnsv1.EstimateReturnRequestRequest) (*returnsv1.EstimateReturnRequestResponse, error) {
		return returnRequestCalculationForEstimateReturnRequest.EstimateReturnRequest(ctx, request)
	}
	sellerScorecardReportingForCountSellerScorecardTotals := sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn)
	countSellerScorecardTotalsCall = func(ctx context.Context, request *sellersv1.CountSellerScorecardTotalsRequest) (*sellersv1.CountSellerScorecardTotalsResponse, error) {
		return sellerScorecardReportingForCountSellerScorecardTotals.CountSellerScorecardTotals(ctx, request)
	}
	returnLabelCalculationForCalculateReturnLabel := returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn)
	calculateReturnLabelCall = returnLabelCalculationForCalculateReturnLabel.CalculateReturnLabel
	customerForCreateCustomer := customersv1.NewCustomerServiceClient(deps.Conn)
	createCustomerCall = customerForCreateCustomer.CreateCustomer
	returnRequestCalculationForExplainReturnRequestCalculation := returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn)
	explainReturnRequestCalculationCall = returnRequestCalculationForExplainReturnRequestCalculation.ExplainReturnRequestCalculation
	tierApprovalForListPendingTiers := loyaltyv1.NewTierApprovalServiceClient(deps.Conn)
	listPendingTiersCall = func(ctx context.Context, request *loyaltyv1.ListPendingTiersRequest) (*loyaltyv1.ListPendingTiersResponse, error) {
		return tierApprovalForListPendingTiers.ListPendingTiers(ctx, request)
	}
	customerSegmentCalculationForSimulateCustomerSegment := customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn)
	simulateCustomerSegmentCall = customerSegmentCalculationForSimulateCustomerSegment.SimulateCustomerSegment
	translationKeyForBatchGetTranslationKeys := localizationv1.NewTranslationKeyServiceClient(deps.Conn)
	batchGetTranslationKeysCall = translationKeyForBatchGetTranslationKeys.BatchGetTranslationKeys
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                            *slog.Logger
	sellerScorecardSyncClient      sellersv1.SellerScorecardSyncServiceClient
	searchQueryQueryClient         searchv1.SearchQueryQueryServiceClient
	returnLabelValidationClient    returnsv1.ReturnLabelValidationServiceClient
	returnLabelCalculationClient   returnsv1.ReturnLabelCalculationServiceClient
	subscriptionClient             subscriptionsv1.SubscriptionServiceClient
	returnRequestCalculationClient returnsv1.ReturnRequestCalculationServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                            deps.Logger,
		sellerScorecardSyncClient:      sellersv1.NewSellerScorecardSyncServiceClient(deps.Conn),
		searchQueryQueryClient:         searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		returnLabelValidationClient:    returnsv1.NewReturnLabelValidationServiceClient(deps.Conn),
		returnLabelCalculationClient:   returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
		subscriptionClient:             subscriptionsv1.NewSubscriptionServiceClient(deps.Conn),
		returnRequestCalculationClient: returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn),
	}
	go w.consume(ctx, "returns.cancel_return_label_workflow.resolve_seller_scorecard_conflict", deps.Bus.Subscribe("returns.cancel_return_label_workflow.resolve_seller_scorecard_conflict"), w.handleReturnsCancelReturnLabelWorkflowResolveSellerScorecardConflict)
	go w.consume(ctx, "returns.lint_restock_decisions.suggest_search_queries", deps.Bus.Subscribe("returns.lint_restock_decisions.suggest_search_queries"), w.handleReturnsLintRestockDecisionsSuggestSearchQueries)
	go w.consume(ctx, "returns.cancel_return_request_workflow.explain_return_label_decision", deps.Bus.Subscribe("returns.cancel_return_request_workflow.explain_return_label_decision"), w.handleReturnsCancelReturnRequestWorkflowExplainReturnLabelDecision)
	go w.consume(ctx, "returns.normalize_return_label.quote_return_label", deps.Bus.Subscribe("returns.normalize_return_label.quote_return_label"), w.handleReturnsNormalizeReturnLabelQuoteReturnLabel)
	go w.consume(ctx, "returns.normalize_return_request.update_subscription", deps.Bus.Subscribe("returns.normalize_return_request.update_subscription"), w.handleReturnsNormalizeReturnRequestUpdateSubscription)
	go w.consume(ctx, "returns.get_return_label_rates.quote_return_request", deps.Bus.Subscribe("returns.get_return_label_rates.quote_return_request"), w.handleReturnsGetReturnLabelRatesQuoteReturnRequest)
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

// handleReturnsCancelReturnLabelWorkflowResolveSellerScorecardConflict makes the call the
// returns.cancel_return_label_workflow.resolve_seller_scorecard_conflict event asks for.
func (w *workers) handleReturnsCancelReturnLabelWorkflowResolveSellerScorecardConflict(ctx context.Context, event eventbus.Event) error {
	_, err := w.sellerScorecardSyncClient.ResolveSellerScorecardConflict(ctx, &sellersv1.ResolveSellerScorecardConflictRequest{
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				CreatedBy: event.Attributes["seller_scorecard.audit.created_by"],
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

// handleReturnsLintRestockDecisionsSuggestSearchQueries makes the call the
// returns.lint_restock_decisions.suggest_search_queries event asks for.
func (w *workers) handleReturnsLintRestockDecisionsSuggestSearchQueries(ctx context.Context, event eventbus.Event) error {
	_, err := w.searchQueryQueryClient.SuggestSearchQueries(ctx, &searchv1.SuggestSearchQueriesRequest{
		Filter: &searchv1.SuggestSearchQueriesRequest_Filter{Region: event.Attributes["filter.region"]},
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

// handleReturnsCancelReturnRequestWorkflowExplainReturnLabelDecision makes the call the
// returns.cancel_return_request_workflow.explain_return_label_decision event asks for.
func (w *workers) handleReturnsCancelReturnRequestWorkflowExplainReturnLabelDecision(ctx context.Context, event eventbus.Event) error {
	_, err := w.returnLabelValidationClient.ExplainReturnLabelDecision(ctx, &returnsv1.ExplainReturnLabelDecisionRequest{
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: event.Attributes["return_label.billing_address.country_code"],
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

// handleReturnsNormalizeReturnLabelQuoteReturnLabel makes the call the
// returns.normalize_return_label.quote_return_label event asks for.
func (w *workers) handleReturnsNormalizeReturnLabelQuoteReturnLabel(ctx context.Context, event eventbus.Event) error {
	_, err := w.returnLabelCalculationClient.QuoteReturnLabel(ctx, &returnsv1.QuoteReturnLabelRequest{
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: event.Attributes["return_label.audit.updated_by"],
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

// handleReturnsNormalizeReturnRequestUpdateSubscription makes the call the
// returns.normalize_return_request.update_subscription event asks for.
func (w *workers) handleReturnsNormalizeReturnRequestUpdateSubscription(ctx context.Context, event eventbus.Event) error {
	_, err := w.subscriptionClient.UpdateSubscription(ctx, &subscriptionsv1.UpdateSubscriptionRequest{
		Subscription: &subscriptionsv1.Subscription{
			Funding: &subscriptionsv1.Subscription_Card{Card: &subscriptionsv1.SubscriptionCardFunding{Last4: event.Attributes["subscription.card.last4"]}},
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

// handleReturnsGetReturnLabelRatesQuoteReturnRequest makes the call the
// returns.get_return_label_rates.quote_return_request event asks for.
func (w *workers) handleReturnsGetReturnLabelRatesQuoteReturnRequest(ctx context.Context, event eventbus.Event) error {
	_, err := w.returnRequestCalculationClient.QuoteReturnRequest(ctx, &returnsv1.QuoteReturnRequestRequest{
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email: event.Attributes["return_request.counterparty.email"],
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
