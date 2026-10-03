package pricing

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	billingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/billing/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	lineagev1 "github.com/hounddogai/protomap-test-golang/gen/acme/lineage/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	riskv1 "github.com/hounddogai/protomap-test-golang/gen/acme/risk/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the pricing domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.audit.v1.AuditEventValidationService", auditv1.NewAuditEventValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.billing.v1.BillingAccountQueryService", billingv1.NewBillingAccountQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.checkout.v1.CheckoutSessionApprovalService", checkoutv1.NewCheckoutSessionApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerCalculationService", customersv1.NewCustomerCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.HouseholdQueryService", customersv1.NewHouseholdQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.PreferenceValidationService", customersv1.NewPreferenceValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.ApiCredentialValidationService", identityv1.NewApiCredentialValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.SessionQueryService", identityv1.NewSessionQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.pricing.v1.CurrencyRateApprovalService", pricingv1.NewCurrencyRateApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.pricing.v1.CurrencyRateReportingService", pricingv1.NewCurrencyRateReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.pricing.v1.PriceOverrideCalculationService", pricingv1.NewPriceOverrideCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.pricing.v1.PriceQuoteApprovalService", pricingv1.NewPriceQuoteApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.pricing.v1.PriceQuoteCalculationService", pricingv1.NewPriceQuoteCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.pricing.v1.PriceQuoteQueryService", pricingv1.NewPriceQuoteQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.pricing.v1.PriceQuoteValidationService", pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.returns.v1.RestockDecisionReportingService", returnsv1.NewRestockDecisionReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SynonymSyncService", searchv1.NewSynonymSyncServiceClient(deps.Conn))
	pricingv1.RegisterPriceListServiceServer(server, NewPriceListServer(deps))
	pricingv1.RegisterPriceQuoteQueryServiceServer(server, NewPriceQuoteQueryServer(deps))
	pricingv1.RegisterPriceOverrideWorkflowServiceServer(server, NewPriceOverrideWorkflowServer(deps))
	pricingv1.RegisterCurrencyRateApprovalServiceServer(server, NewCurrencyRateApprovalServer(deps))
	pricingv1.RegisterPriceListQueryServiceServer(server, NewPriceListQueryServer(deps))
	pricingv1.RegisterPriceQuoteWorkflowServiceServer(server, NewPriceQuoteWorkflowServer(deps))
	pricingv1.RegisterPriceOverrideApprovalServiceServer(server, NewPriceOverrideApprovalServer(deps))
	currencyRateValidationServer := NewCurrencyRateValidationServer(deps)
	server.RegisterService(&pricingv1.CurrencyRateValidationService_ServiceDesc, currencyRateValidationServer)
	priceListWorkflowServer := NewPriceListWorkflowServer(deps)
	server.RegisterService(&pricingv1.PriceListWorkflowService_ServiceDesc, priceListWorkflowServer)
	priceQuoteApprovalServer := NewPriceQuoteApprovalServer(deps)
	server.RegisterService(&pricingv1.PriceQuoteApprovalService_ServiceDesc, priceQuoteApprovalServer)
	priceOverrideValidationServer := NewPriceOverrideValidationServer(deps)
	server.RegisterService(&pricingv1.PriceOverrideValidationService_ServiceDesc, priceOverrideValidationServer)
	pricingv1.RegisterCurrencyRateCalculationServiceServer(server, NewCurrencyRateCalculationServer(deps))
	pricingv1.RegisterPriceListApprovalServiceServer(server, NewPriceListApprovalServer(deps))
	pricingv1.RegisterPriceQuoteValidationServiceServer(server, NewPriceQuoteValidationServer(deps))
	pricingv1.RegisterPriceOverrideCalculationServiceServer(server, NewPriceOverrideCalculationServer(deps))
	pricingv1.RegisterCurrencyRateLifecycleServiceServer(server, NewCurrencyRateLifecycleServer(deps))
	priceListValidationServer := NewPriceListValidationServer(deps)
	server.RegisterService(&pricingv1.PriceListValidationService_ServiceDesc, priceListValidationServer)
	pricingv1.RegisterPriceQuoteCalculationServiceServer(server, NewPriceQuoteCalculationServer(deps))
	pricingv1.RegisterPriceOverrideLifecycleServiceServer(server, NewPriceOverrideLifecycleServer(deps))
	pricingv1.RegisterCurrencyRateReportingServiceServer(server, NewCurrencyRateReportingServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	scorePriceQuoteCall                   func(context.Context, *pricingv1.ScorePriceQuoteRequest, ...grpc.CallOption) (*pricingv1.ScorePriceQuoteResponse, error)
	recallSubscriptionPlanCall            func(context.Context, *subscriptionsv1.RecallSubscriptionPlanRequest) (*subscriptionsv1.RecallSubscriptionPlanResponse, error)
	escalatePriceListCall                 func(context.Context, *pricingv1.EscalatePriceListRequest, ...grpc.CallOption) (*pricingv1.EscalatePriceListResponse, error)
	startFulfillmentExceptionWorkflowCall func(context.Context, *fulfillmentv1.StartFulfillmentExceptionWorkflowRequest, ...grpc.CallOption) (*fulfillmentv1.StartFulfillmentExceptionWorkflowResponse, error)
	suggestMetricSeriesesCall             func(context.Context, *metricsv1.SuggestMetricSeriesesRequest) (*metricsv1.SuggestMetricSeriesesResponse, error)
	resolveLineageNodesCall               func(context.Context, *lineagev1.ResolveLineageNodesRequest) (*lineagev1.ResolveLineageNodesResponse, error)
	estimatePreferenceCall                func(context.Context, *customersv1.EstimatePreferenceRequest) (*customersv1.EstimatePreferenceResponse, error)
	resolvePriceQuotesCall                func(context.Context, *pricingv1.ResolvePriceQuotesRequest, ...grpc.CallOption) (*pricingv1.ResolvePriceQuotesResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	priceQuoteValidationForScorePriceQuote := pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn)
	scorePriceQuoteCall = priceQuoteValidationForScorePriceQuote.ScorePriceQuote
	subscriptionPlanApprovalForRecallSubscriptionPlan := subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn)
	recallSubscriptionPlanCall = func(ctx context.Context, request *subscriptionsv1.RecallSubscriptionPlanRequest) (*subscriptionsv1.RecallSubscriptionPlanResponse, error) {
		return subscriptionPlanApprovalForRecallSubscriptionPlan.RecallSubscriptionPlan(ctx, request)
	}
	priceListApprovalForEscalatePriceList := pricingv1.NewPriceListApprovalServiceClient(deps.Conn)
	escalatePriceListCall = priceListApprovalForEscalatePriceList.EscalatePriceList
	fulfillmentExceptionWorkflowForStartFulfillmentExceptionWorkflow := fulfillmentv1.NewFulfillmentExceptionWorkflowServiceClient(deps.Conn)
	startFulfillmentExceptionWorkflowCall = fulfillmentExceptionWorkflowForStartFulfillmentExceptionWorkflow.StartFulfillmentExceptionWorkflow
	metricSeriesQueryForSuggestMetricSerieses := metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn)
	suggestMetricSeriesesCall = func(ctx context.Context, request *metricsv1.SuggestMetricSeriesesRequest) (*metricsv1.SuggestMetricSeriesesResponse, error) {
		return metricSeriesQueryForSuggestMetricSerieses.SuggestMetricSerieses(ctx, request)
	}
	lineageNodeQueryForResolveLineageNodes := lineagev1.NewLineageNodeQueryServiceClient(deps.Conn)
	resolveLineageNodesCall = func(ctx context.Context, request *lineagev1.ResolveLineageNodesRequest) (*lineagev1.ResolveLineageNodesResponse, error) {
		return lineageNodeQueryForResolveLineageNodes.ResolveLineageNodes(ctx, request)
	}
	preferenceCalculationForEstimatePreference := customersv1.NewPreferenceCalculationServiceClient(deps.Conn)
	estimatePreferenceCall = func(ctx context.Context, request *customersv1.EstimatePreferenceRequest) (*customersv1.EstimatePreferenceResponse, error) {
		return preferenceCalculationForEstimatePreference.EstimatePreference(ctx, request)
	}
	priceQuoteQueryForResolvePriceQuotes := pricingv1.NewPriceQuoteQueryServiceClient(deps.Conn)
	resolvePriceQuotesCall = priceQuoteQueryForResolvePriceQuotes.ResolvePriceQuotes
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                        *slog.Logger
	priceQuoteValidationClient pricingv1.PriceQuoteValidationServiceClient
	riskRegisterClient         riskv1.RiskRegisterServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                        deps.Logger,
		priceQuoteValidationClient: pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		riskRegisterClient:         riskv1.NewRiskRegisterServiceClient(deps.Conn),
	}
	go w.consume(ctx, "pricing.submit_price_quote.normalize_price_quote", deps.Bus.Subscribe("pricing.submit_price_quote.normalize_price_quote"), w.handlePricingSubmitPriceQuoteNormalizePriceQuote)
	go w.consume(ctx, "pricing.lint_price_lists.update_risk_register", deps.Bus.Subscribe("pricing.lint_price_lists.update_risk_register"), w.handlePricingLintPriceListsUpdateRiskRegister)
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

// handlePricingSubmitPriceQuoteNormalizePriceQuote makes the call the pricing.submit_price_quote.normalize_price_quote
// event asks for.
func (w *workers) handlePricingSubmitPriceQuoteNormalizePriceQuote(ctx context.Context, event eventbus.Event) error {
	_, err := w.priceQuoteValidationClient.NormalizePriceQuote(ctx, &pricingv1.NormalizePriceQuoteRequest{
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{Etag: event.Attributes["price_quote.audit.etag"]},
			Settlement: &pricingv1.PriceQuote_Bank{Bank: &pricingv1.PriceQuoteBankSettlement{
				AccountHolder: event.Attributes["price_quote.bank.account_holder"],
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

// handlePricingLintPriceListsUpdateRiskRegister makes the call the pricing.lint_price_lists.update_risk_register event
// asks for.
func (w *workers) handlePricingLintPriceListsUpdateRiskRegister(ctx context.Context, event eventbus.Event) error {
	_, err := w.riskRegisterClient.UpdateRiskRegister(ctx, &riskv1.UpdateRiskRegisterRequest{
		RiskRegister: &riskv1.RiskRegister{
			Children: []*riskv1.RiskRegister{&riskv1.RiskRegister{
				Children: []*riskv1.RiskRegister{&riskv1.RiskRegister{
					Audit: &commonv1.AuditInfo{
						CreatedBy: event.Attributes["risk_register.children.children.audit.created_by"],
					},
					Manager: &identityv1.UserRef{
						UserId: event.Attributes["risk_register.children.children.manager.user_id"],
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
