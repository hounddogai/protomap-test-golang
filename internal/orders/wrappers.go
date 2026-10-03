package orders

import (
	"context"
	"time"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredCustomerCalculation records how long each call to acme.customers.v1.CustomerCalculationService takes.
type meteredCustomerCalculation struct {
	customersv1.CustomerCalculationServiceClient
	recorder *metrics.Recorder
}

// meteredCustomerValidation records how long each call to acme.customers.v1.CustomerValidationService takes.
type meteredCustomerValidation struct {
	customersv1.CustomerValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomerValidation) CheckCustomerEligibility(ctx context.Context, request *customersv1.CheckCustomerEligibilityRequest, options ...grpc.CallOption) (*customersv1.CheckCustomerEligibilityResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerValidationService/CheckCustomerEligibility", time.Now())
	return c.CustomerValidationServiceClient.CheckCustomerEligibility(ctx, request, options...)
}

func (c meteredCustomerValidation) ExplainCustomerDecision(ctx context.Context, request *customersv1.ExplainCustomerDecisionRequest, options ...grpc.CallOption) (*customersv1.ExplainCustomerDecisionResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerValidationService/ExplainCustomerDecision", time.Now())
	return c.CustomerValidationServiceClient.ExplainCustomerDecision(ctx, request, options...)
}

// meteredGiftCardValidation records how long each call to acme.giftcards.v1.GiftCardValidationService takes.
type meteredGiftCardValidation struct {
	giftcardsv1.GiftCardValidationServiceClient
	recorder *metrics.Recorder
}

// meteredMerchandisingRuleWorkflow records how long each call to acme.merchandising.v1.MerchandisingRuleWorkflowService
// takes.
type meteredMerchandisingRuleWorkflow struct {
	merchandisingv1.MerchandisingRuleWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredMerchandisingRuleWorkflow) StartMerchandisingRuleWorkflow(ctx context.Context, request *merchandisingv1.StartMerchandisingRuleWorkflowRequest, options ...grpc.CallOption) (*merchandisingv1.StartMerchandisingRuleWorkflowResponse, error) {
	defer c.recorder.Observe("acme.merchandising.v1.MerchandisingRuleWorkflowService/StartMerchandisingRuleWorkflow", time.Now())
	return c.MerchandisingRuleWorkflowServiceClient.StartMerchandisingRuleWorkflow(ctx, request, options...)
}

// meteredNotificationQuery records how long each call to acme.notifications.v1.NotificationQueryService takes.
type meteredNotificationQuery struct {
	notificationsv1.NotificationQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredNotificationQuery) ResolveNotifications(ctx context.Context, request *notificationsv1.ResolveNotificationsRequest, options ...grpc.CallOption) (*notificationsv1.ResolveNotificationsResponse, error) {
	defer c.recorder.Observe("acme.notifications.v1.NotificationQueryService/ResolveNotifications", time.Now())
	return c.NotificationQueryServiceClient.ResolveNotifications(ctx, request, options...)
}

// meteredNotificationValidation records how long each call to acme.notifications.v1.NotificationValidationService
// takes.
type meteredNotificationValidation struct {
	notificationsv1.NotificationValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredNotificationValidation) ValidateNotification(ctx context.Context, request *notificationsv1.ValidateNotificationRequest, options ...grpc.CallOption) (*notificationsv1.ValidateNotificationResponse, error) {
	defer c.recorder.Observe("acme.notifications.v1.NotificationValidationService/ValidateNotification", time.Now())
	return c.NotificationValidationServiceClient.ValidateNotification(ctx, request, options...)
}

// meteredOrderAmendmentCalculation records how long each call to acme.orders.v1.OrderAmendmentCalculationService takes.
type meteredOrderAmendmentCalculation struct {
	ordersv1.OrderAmendmentCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredOrderAmendmentCalculation) QuoteOrderAmendment(ctx context.Context, request *ordersv1.QuoteOrderAmendmentRequest, options ...grpc.CallOption) (*ordersv1.QuoteOrderAmendmentResponse, error) {
	defer c.recorder.Observe("acme.orders.v1.OrderAmendmentCalculationService/QuoteOrderAmendment", time.Now())
	return c.OrderAmendmentCalculationServiceClient.QuoteOrderAmendment(ctx, request, options...)
}

func (c meteredOrderAmendmentCalculation) RecalculateOrderAmendments(ctx context.Context, request *ordersv1.RecalculateOrderAmendmentsRequest, options ...grpc.CallOption) (*ordersv1.RecalculateOrderAmendmentsResponse, error) {
	defer c.recorder.Observe("acme.orders.v1.OrderAmendmentCalculationService/RecalculateOrderAmendments", time.Now())
	return c.OrderAmendmentCalculationServiceClient.RecalculateOrderAmendments(ctx, request, options...)
}

// meteredOrderCalculation records how long each call to acme.orders.v1.OrderCalculationService takes.
type meteredOrderCalculation struct {
	ordersv1.OrderCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredOrderCalculation) ExplainOrderCalculation(ctx context.Context, request *ordersv1.ExplainOrderCalculationRequest, options ...grpc.CallOption) (*ordersv1.ExplainOrderCalculationResponse, error) {
	defer c.recorder.Observe("acme.orders.v1.OrderCalculationService/ExplainOrderCalculation", time.Now())
	return c.OrderCalculationServiceClient.ExplainOrderCalculation(ctx, request, options...)
}

func (c meteredOrderCalculation) SimulateOrder(ctx context.Context, request *ordersv1.SimulateOrderRequest, options ...grpc.CallOption) (*ordersv1.SimulateOrderResponse, error) {
	defer c.recorder.Observe("acme.orders.v1.OrderCalculationService/SimulateOrder", time.Now())
	return c.OrderCalculationServiceClient.SimulateOrder(ctx, request, options...)
}

// meteredSearchIndexQuery records how long each call to acme.search.v1.SearchIndexQueryService takes.
type meteredSearchIndexQuery struct {
	searchv1.SearchIndexQueryServiceClient
	recorder *metrics.Recorder
}

// meteredSearchQueryValidation records how long each call to acme.search.v1.SearchQueryValidationService takes.
type meteredSearchQueryValidation struct {
	searchv1.SearchQueryValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredSearchQueryValidation) ValidateSearchQuery(ctx context.Context, request *searchv1.ValidateSearchQueryRequest, options ...grpc.CallOption) (*searchv1.ValidateSearchQueryResponse, error) {
	defer c.recorder.Observe("acme.search.v1.SearchQueryValidationService/ValidateSearchQuery", time.Now())
	return c.SearchQueryValidationServiceClient.ValidateSearchQuery(ctx, request, options...)
}

// meteredWarehouseQuery records how long each call to acme.warehousing.v1.WarehouseQueryService takes.
type meteredWarehouseQuery struct {
	warehousingv1.WarehouseQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredWarehouseQuery) FindWarehouses(ctx context.Context, request *warehousingv1.FindWarehousesRequest, options ...grpc.CallOption) (*warehousingv1.FindWarehousesResponse, error) {
	defer c.recorder.Observe("acme.warehousing.v1.WarehouseQueryService/FindWarehouses", time.Now())
	return c.WarehouseQueryServiceClient.FindWarehouses(ctx, request, options...)
}
