package fraud

import (
	"context"
	"time"

	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredCarePlanCalculation records how long each call to acme.careplans.v1.CarePlanCalculationService takes.
type meteredCarePlanCalculation struct {
	careplansv1.CarePlanCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCarePlanCalculation) CompareCarePlanOptions(ctx context.Context, request *careplansv1.CompareCarePlanOptionsRequest, options ...grpc.CallOption) (*careplansv1.CompareCarePlanOptionsResponse, error) {
	defer c.recorder.Observe("acme.careplans.v1.CarePlanCalculationService/CompareCarePlanOptions", time.Now())
	return c.CarePlanCalculationServiceClient.CompareCarePlanOptions(ctx, request, options...)
}

// meteredCustomer records how long each call to acme.customers.v1.CustomerService takes.
type meteredCustomer struct {
	customersv1.CustomerServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomer) ListCustomers(ctx context.Context, request *customersv1.ListCustomersRequest, options ...grpc.CallOption) (*customersv1.ListCustomersResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerService/ListCustomers", time.Now())
	return c.CustomerServiceClient.ListCustomers(ctx, request, options...)
}

// meteredFraudRuleApproval records how long each call to acme.fraud.v1.FraudRuleApprovalService takes.
type meteredFraudRuleApproval struct {
	fraudv1.FraudRuleApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredFraudRuleApproval) ListPendingFraudRules(ctx context.Context, request *fraudv1.ListPendingFraudRulesRequest, options ...grpc.CallOption) (*fraudv1.ListPendingFraudRulesResponse, error) {
	defer c.recorder.Observe("acme.fraud.v1.FraudRuleApprovalService/ListPendingFraudRules", time.Now())
	return c.FraudRuleApprovalServiceClient.ListPendingFraudRules(ctx, request, options...)
}

// meteredLoyaltyMemberQuery records how long each call to acme.loyalty.v1.LoyaltyMemberQueryService takes.
type meteredLoyaltyMemberQuery struct {
	loyaltyv1.LoyaltyMemberQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredLoyaltyMemberQuery) SuggestLoyaltyMembers(ctx context.Context, request *loyaltyv1.SuggestLoyaltyMembersRequest, options ...grpc.CallOption) (*loyaltyv1.SuggestLoyaltyMembersResponse, error) {
	defer c.recorder.Observe("acme.loyalty.v1.LoyaltyMemberQueryService/SuggestLoyaltyMembers", time.Now())
	return c.LoyaltyMemberQueryServiceClient.SuggestLoyaltyMembers(ctx, request, options...)
}

// meteredMetricSeriesQuery records how long each call to acme.metrics.v1.MetricSeriesQueryService takes.
type meteredMetricSeriesQuery struct {
	metricsv1.MetricSeriesQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredMetricSeriesQuery) CountMetricSerieses(ctx context.Context, request *metricsv1.CountMetricSeriesesRequest, options ...grpc.CallOption) (*metricsv1.CountMetricSeriesesResponse, error) {
	defer c.recorder.Observe("acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses", time.Now())
	return c.MetricSeriesQueryServiceClient.CountMetricSerieses(ctx, request, options...)
}

// meteredRiskAssessmentCalculation records how long each call to acme.fraud.v1.RiskAssessmentCalculationService takes.
type meteredRiskAssessmentCalculation struct {
	fraudv1.RiskAssessmentCalculationServiceClient
	recorder *metrics.Recorder
}

// meteredRiskAssessmentWorkflow records how long each call to acme.fraud.v1.RiskAssessmentWorkflowService takes.
type meteredRiskAssessmentWorkflow struct {
	fraudv1.RiskAssessmentWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredRiskAssessmentWorkflow) PauseRiskAssessmentWorkflow(ctx context.Context, request *fraudv1.PauseRiskAssessmentWorkflowRequest, options ...grpc.CallOption) (*fraudv1.PauseRiskAssessmentWorkflowResponse, error) {
	defer c.recorder.Observe("acme.fraud.v1.RiskAssessmentWorkflowService/PauseRiskAssessmentWorkflow", time.Now())
	return c.RiskAssessmentWorkflowServiceClient.PauseRiskAssessmentWorkflow(ctx, request, options...)
}

// meteredRoleNotification records how long each call to acme.identity.v1.RoleNotificationService takes.
type meteredRoleNotification struct {
	identityv1.RoleNotificationServiceClient
	recorder *metrics.Recorder
}

func (c meteredRoleNotification) UnsubscribeRoleUpdates(ctx context.Context, request *identityv1.UnsubscribeRoleUpdatesRequest, options ...grpc.CallOption) (*identityv1.UnsubscribeRoleUpdatesResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.RoleNotificationService/UnsubscribeRoleUpdates", time.Now())
	return c.RoleNotificationServiceClient.UnsubscribeRoleUpdates(ctx, request, options...)
}

// meteredStockMovementValidation records how long each call to acme.inventory.v1.StockMovementValidationService takes.
type meteredStockMovementValidation struct {
	inventoryv1.StockMovementValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredStockMovementValidation) ScoreStockMovement(ctx context.Context, request *inventoryv1.ScoreStockMovementRequest, options ...grpc.CallOption) (*inventoryv1.ScoreStockMovementResponse, error) {
	defer c.recorder.Observe("acme.inventory.v1.StockMovementValidationService/ScoreStockMovement", time.Now())
	return c.StockMovementValidationServiceClient.ScoreStockMovement(ctx, request, options...)
}

// meteredWatchlistCalculation records how long each call to acme.fraud.v1.WatchlistCalculationService takes.
type meteredWatchlistCalculation struct {
	fraudv1.WatchlistCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredWatchlistCalculation) GetWatchlistRates(ctx context.Context, request *fraudv1.GetWatchlistRatesRequest, options ...grpc.CallOption) (*fraudv1.GetWatchlistRatesResponse, error) {
	defer c.recorder.Observe("acme.fraud.v1.WatchlistCalculationService/GetWatchlistRates", time.Now())
	return c.WatchlistCalculationServiceClient.GetWatchlistRates(ctx, request, options...)
}
