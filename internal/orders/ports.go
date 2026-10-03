package orders

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	toursv1 "github.com/hounddogai/protomap-test-golang/gen/acme/tours/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// accessPolicyQueryPort is what the orders domain needs of acme.authorization.v1.AccessPolicyQueryService.
type accessPolicyQueryPort interface {
	BrowseAccessPolicies(ctx context.Context, request *authorizationv1.BrowseAccessPoliciesRequest) (*authorizationv1.BrowseAccessPoliciesResponse, error)
}

// grpcAccessPolicyQueryPort reaches acme.authorization.v1.AccessPolicyQueryService over gRPC.
type grpcAccessPolicyQueryPort struct {
	client authorizationv1.AccessPolicyQueryServiceClient
}

// sandboxAccessPolicyQueryPort answers in the sandbox environment, where acme.authorization.v1.AccessPolicyQueryService
// is not deployed.
type sandboxAccessPolicyQueryPort struct{ log *slog.Logger }

func (p grpcAccessPolicyQueryPort) BrowseAccessPolicies(ctx context.Context, request *authorizationv1.BrowseAccessPoliciesRequest) (*authorizationv1.BrowseAccessPoliciesResponse, error) {
	return p.client.BrowseAccessPolicies(ctx, request)
}

func (p sandboxAccessPolicyQueryPort) BrowseAccessPolicies(ctx context.Context, request *authorizationv1.BrowseAccessPoliciesRequest) (*authorizationv1.BrowseAccessPoliciesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers BrowseAccessPolicies")
	return &authorizationv1.BrowseAccessPoliciesResponse{}, nil
}

func newAccessPolicyQueryPort(deps platform.Deps) accessPolicyQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxAccessPolicyQueryPort{log: deps.Logger}
	}
	return grpcAccessPolicyQueryPort{client: authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn)}
}

// customerCalculationPort is what the orders domain needs of acme.customers.v1.CustomerCalculationService.
type customerCalculationPort interface {
	ExplainCustomerCalculation(ctx context.Context, request *customersv1.ExplainCustomerCalculationRequest) (*customersv1.ExplainCustomerCalculationResponse, error)
	GetCustomerRates(ctx context.Context, request *customersv1.GetCustomerRatesRequest) (*customersv1.GetCustomerRatesResponse, error)
}

// grpcCustomerCalculationPort reaches acme.customers.v1.CustomerCalculationService over gRPC.
type grpcCustomerCalculationPort struct {
	client customersv1.CustomerCalculationServiceClient
}

// sandboxCustomerCalculationPort answers in the sandbox environment, where acme.customers.v1.CustomerCalculationService
// is not deployed.
type sandboxCustomerCalculationPort struct{ log *slog.Logger }

func (p grpcCustomerCalculationPort) ExplainCustomerCalculation(ctx context.Context, request *customersv1.ExplainCustomerCalculationRequest) (*customersv1.ExplainCustomerCalculationResponse, error) {
	return p.client.ExplainCustomerCalculation(ctx, request)
}

func (p sandboxCustomerCalculationPort) ExplainCustomerCalculation(ctx context.Context, request *customersv1.ExplainCustomerCalculationRequest) (*customersv1.ExplainCustomerCalculationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainCustomerCalculation")
	return &customersv1.ExplainCustomerCalculationResponse{}, nil
}

func (p grpcCustomerCalculationPort) GetCustomerRates(ctx context.Context, request *customersv1.GetCustomerRatesRequest) (*customersv1.GetCustomerRatesResponse, error) {
	return p.client.GetCustomerRates(ctx, request)
}

func (p sandboxCustomerCalculationPort) GetCustomerRates(ctx context.Context, request *customersv1.GetCustomerRatesRequest) (*customersv1.GetCustomerRatesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetCustomerRates")
	return &customersv1.GetCustomerRatesResponse{}, nil
}

func newCustomerCalculationPort(deps platform.Deps) customerCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerCalculationPort{log: deps.Logger}
	}
	return grpcCustomerCalculationPort{client: customersv1.NewCustomerCalculationServiceClient(deps.Conn)}
}

// customerQueryPort is what the orders domain needs of acme.customers.v1.CustomerQueryService.
type customerQueryPort interface {
	BrowseCustomers(ctx context.Context, request *customersv1.BrowseCustomersRequest) (*customersv1.BrowseCustomersResponse, error)
	SummarizeCustomers(ctx context.Context, request *customersv1.SummarizeCustomersRequest) (*customersv1.SummarizeCustomersResponse, error)
}

// grpcCustomerQueryPort reaches acme.customers.v1.CustomerQueryService over gRPC.
type grpcCustomerQueryPort struct {
	client customersv1.CustomerQueryServiceClient
}

// sandboxCustomerQueryPort answers in the sandbox environment, where acme.customers.v1.CustomerQueryService is not
// deployed.
type sandboxCustomerQueryPort struct{ log *slog.Logger }

func (p grpcCustomerQueryPort) BrowseCustomers(ctx context.Context, request *customersv1.BrowseCustomersRequest) (*customersv1.BrowseCustomersResponse, error) {
	return p.client.BrowseCustomers(ctx, request)
}

func (p sandboxCustomerQueryPort) BrowseCustomers(ctx context.Context, request *customersv1.BrowseCustomersRequest) (*customersv1.BrowseCustomersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers BrowseCustomers")
	return &customersv1.BrowseCustomersResponse{}, nil
}

func (p grpcCustomerQueryPort) SummarizeCustomers(ctx context.Context, request *customersv1.SummarizeCustomersRequest) (*customersv1.SummarizeCustomersResponse, error) {
	return p.client.SummarizeCustomers(ctx, request)
}

func (p sandboxCustomerQueryPort) SummarizeCustomers(ctx context.Context, request *customersv1.SummarizeCustomersRequest) (*customersv1.SummarizeCustomersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SummarizeCustomers")
	return &customersv1.SummarizeCustomersResponse{}, nil
}

func newCustomerQueryPort(deps platform.Deps) customerQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerQueryPort{log: deps.Logger}
	}
	return grpcCustomerQueryPort{client: customersv1.NewCustomerQueryServiceClient(deps.Conn)}
}

// customerSegmentValidationPort is what the orders domain needs of acme.customers.v1.CustomerSegmentValidationService.
type customerSegmentValidationPort interface {
	PreviewCustomerSegment(ctx context.Context, request *customersv1.PreviewCustomerSegmentRequest) (*customersv1.PreviewCustomerSegmentResponse, error)
}

// grpcCustomerSegmentValidationPort reaches acme.customers.v1.CustomerSegmentValidationService over gRPC.
type grpcCustomerSegmentValidationPort struct {
	client customersv1.CustomerSegmentValidationServiceClient
}

// sandboxCustomerSegmentValidationPort answers in the sandbox environment, where
// acme.customers.v1.CustomerSegmentValidationService is not deployed.
type sandboxCustomerSegmentValidationPort struct{ log *slog.Logger }

func (p grpcCustomerSegmentValidationPort) PreviewCustomerSegment(ctx context.Context, request *customersv1.PreviewCustomerSegmentRequest) (*customersv1.PreviewCustomerSegmentResponse, error) {
	return p.client.PreviewCustomerSegment(ctx, request)
}

func (p sandboxCustomerSegmentValidationPort) PreviewCustomerSegment(ctx context.Context, request *customersv1.PreviewCustomerSegmentRequest) (*customersv1.PreviewCustomerSegmentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewCustomerSegment")
	return &customersv1.PreviewCustomerSegmentResponse{}, nil
}

func newCustomerSegmentValidationPort(deps platform.Deps) customerSegmentValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerSegmentValidationPort{log: deps.Logger}
	}
	return grpcCustomerSegmentValidationPort{client: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn)}
}

// householdValidationPort is what the orders domain needs of acme.customers.v1.HouseholdValidationService.
type householdValidationPort interface {
	ScoreHousehold(ctx context.Context, request *customersv1.ScoreHouseholdRequest) (*customersv1.ScoreHouseholdResponse, error)
}

// grpcHouseholdValidationPort reaches acme.customers.v1.HouseholdValidationService over gRPC.
type grpcHouseholdValidationPort struct {
	client customersv1.HouseholdValidationServiceClient
}

// sandboxHouseholdValidationPort answers in the sandbox environment, where acme.customers.v1.HouseholdValidationService
// is not deployed.
type sandboxHouseholdValidationPort struct{ log *slog.Logger }

func (p grpcHouseholdValidationPort) ScoreHousehold(ctx context.Context, request *customersv1.ScoreHouseholdRequest) (*customersv1.ScoreHouseholdResponse, error) {
	return p.client.ScoreHousehold(ctx, request)
}

func (p sandboxHouseholdValidationPort) ScoreHousehold(ctx context.Context, request *customersv1.ScoreHouseholdRequest) (*customersv1.ScoreHouseholdResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScoreHousehold")
	return &customersv1.ScoreHouseholdResponse{}, nil
}

func newHouseholdValidationPort(deps platform.Deps) householdValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxHouseholdValidationPort{log: deps.Logger}
	}
	return grpcHouseholdValidationPort{client: customersv1.NewHouseholdValidationServiceClient(deps.Conn)}
}

// orderAmendmentCalculationPort is what the orders domain needs of acme.orders.v1.OrderAmendmentCalculationService.
type orderAmendmentCalculationPort interface {
	CalculateOrderAmendment(ctx context.Context, request *ordersv1.CalculateOrderAmendmentRequest) (*ordersv1.CalculateOrderAmendmentResponse, error)
}

// grpcOrderAmendmentCalculationPort reaches acme.orders.v1.OrderAmendmentCalculationService over gRPC.
type grpcOrderAmendmentCalculationPort struct {
	client ordersv1.OrderAmendmentCalculationServiceClient
}

// sandboxOrderAmendmentCalculationPort answers in the sandbox environment, where
// acme.orders.v1.OrderAmendmentCalculationService is not deployed.
type sandboxOrderAmendmentCalculationPort struct{ log *slog.Logger }

func (p grpcOrderAmendmentCalculationPort) CalculateOrderAmendment(ctx context.Context, request *ordersv1.CalculateOrderAmendmentRequest) (*ordersv1.CalculateOrderAmendmentResponse, error) {
	return p.client.CalculateOrderAmendment(ctx, request)
}

func (p sandboxOrderAmendmentCalculationPort) CalculateOrderAmendment(ctx context.Context, request *ordersv1.CalculateOrderAmendmentRequest) (*ordersv1.CalculateOrderAmendmentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateOrderAmendment")
	return &ordersv1.CalculateOrderAmendmentResponse{}, nil
}

func newOrderAmendmentCalculationPort(deps platform.Deps) orderAmendmentCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxOrderAmendmentCalculationPort{log: deps.Logger}
	}
	return grpcOrderAmendmentCalculationPort{client: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn)}
}

// orderAmendmentReportingPort is what the orders domain needs of acme.orders.v1.OrderAmendmentReportingService.
type orderAmendmentReportingPort interface {
	ScheduleOrderAmendmentReport(ctx context.Context, request *ordersv1.ScheduleOrderAmendmentReportRequest) (*ordersv1.ScheduleOrderAmendmentReportResponse, error)
}

// grpcOrderAmendmentReportingPort reaches acme.orders.v1.OrderAmendmentReportingService over gRPC.
type grpcOrderAmendmentReportingPort struct {
	client ordersv1.OrderAmendmentReportingServiceClient
}

// sandboxOrderAmendmentReportingPort answers in the sandbox environment, where
// acme.orders.v1.OrderAmendmentReportingService is not deployed.
type sandboxOrderAmendmentReportingPort struct{ log *slog.Logger }

func (p grpcOrderAmendmentReportingPort) ScheduleOrderAmendmentReport(ctx context.Context, request *ordersv1.ScheduleOrderAmendmentReportRequest) (*ordersv1.ScheduleOrderAmendmentReportResponse, error) {
	return p.client.ScheduleOrderAmendmentReport(ctx, request)
}

func (p sandboxOrderAmendmentReportingPort) ScheduleOrderAmendmentReport(ctx context.Context, request *ordersv1.ScheduleOrderAmendmentReportRequest) (*ordersv1.ScheduleOrderAmendmentReportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScheduleOrderAmendmentReport")
	return &ordersv1.ScheduleOrderAmendmentReportResponse{}, nil
}

func newOrderAmendmentReportingPort(deps platform.Deps) orderAmendmentReportingPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxOrderAmendmentReportingPort{log: deps.Logger}
	}
	return grpcOrderAmendmentReportingPort{client: ordersv1.NewOrderAmendmentReportingServiceClient(deps.Conn)}
}

// orderAmendmentValidationPort is what the orders domain needs of acme.orders.v1.OrderAmendmentValidationService.
type orderAmendmentValidationPort interface {
	ValidateOrderAmendment(ctx context.Context, request *ordersv1.ValidateOrderAmendmentRequest) (*ordersv1.ValidateOrderAmendmentResponse, error)
}

// grpcOrderAmendmentValidationPort reaches acme.orders.v1.OrderAmendmentValidationService over gRPC.
type grpcOrderAmendmentValidationPort struct {
	client ordersv1.OrderAmendmentValidationServiceClient
}

// sandboxOrderAmendmentValidationPort answers in the sandbox environment, where
// acme.orders.v1.OrderAmendmentValidationService is not deployed.
type sandboxOrderAmendmentValidationPort struct{ log *slog.Logger }

func (p grpcOrderAmendmentValidationPort) ValidateOrderAmendment(ctx context.Context, request *ordersv1.ValidateOrderAmendmentRequest) (*ordersv1.ValidateOrderAmendmentResponse, error) {
	return p.client.ValidateOrderAmendment(ctx, request)
}

func (p sandboxOrderAmendmentValidationPort) ValidateOrderAmendment(ctx context.Context, request *ordersv1.ValidateOrderAmendmentRequest) (*ordersv1.ValidateOrderAmendmentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidateOrderAmendment")
	return &ordersv1.ValidateOrderAmendmentResponse{}, nil
}

func newOrderAmendmentValidationPort(deps platform.Deps) orderAmendmentValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxOrderAmendmentValidationPort{log: deps.Logger}
	}
	return grpcOrderAmendmentValidationPort{client: ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn)}
}

// orderCalculationPort is what the orders domain needs of acme.orders.v1.OrderCalculationService.
type orderCalculationPort interface {
	CalculateOrder(ctx context.Context, request *ordersv1.CalculateOrderRequest) (*ordersv1.CalculateOrderResponse, error)
	EstimateOrder(ctx context.Context, request *ordersv1.EstimateOrderRequest) (*ordersv1.EstimateOrderResponse, error)
}

// grpcOrderCalculationPort reaches acme.orders.v1.OrderCalculationService over gRPC.
type grpcOrderCalculationPort struct {
	client ordersv1.OrderCalculationServiceClient
}

// sandboxOrderCalculationPort answers in the sandbox environment, where acme.orders.v1.OrderCalculationService is not
// deployed.
type sandboxOrderCalculationPort struct{ log *slog.Logger }

func (p grpcOrderCalculationPort) CalculateOrder(ctx context.Context, request *ordersv1.CalculateOrderRequest) (*ordersv1.CalculateOrderResponse, error) {
	return p.client.CalculateOrder(ctx, request)
}

func (p sandboxOrderCalculationPort) CalculateOrder(ctx context.Context, request *ordersv1.CalculateOrderRequest) (*ordersv1.CalculateOrderResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateOrder")
	return &ordersv1.CalculateOrderResponse{}, nil
}

func (p grpcOrderCalculationPort) EstimateOrder(ctx context.Context, request *ordersv1.EstimateOrderRequest) (*ordersv1.EstimateOrderResponse, error) {
	return p.client.EstimateOrder(ctx, request)
}

func (p sandboxOrderCalculationPort) EstimateOrder(ctx context.Context, request *ordersv1.EstimateOrderRequest) (*ordersv1.EstimateOrderResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateOrder")
	return &ordersv1.EstimateOrderResponse{}, nil
}

func newOrderCalculationPort(deps platform.Deps) orderCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxOrderCalculationPort{log: deps.Logger}
	}
	return grpcOrderCalculationPort{client: ordersv1.NewOrderCalculationServiceClient(deps.Conn)}
}

// orderReportingPort is what the orders domain needs of acme.orders.v1.OrderReportingService.
type orderReportingPort interface {
	GenerateOrderReport(ctx context.Context, request *ordersv1.GenerateOrderReportRequest) (*ordersv1.GenerateOrderReportResponse, error)
}

// grpcOrderReportingPort reaches acme.orders.v1.OrderReportingService over gRPC.
type grpcOrderReportingPort struct {
	client ordersv1.OrderReportingServiceClient
}

// sandboxOrderReportingPort answers in the sandbox environment, where acme.orders.v1.OrderReportingService is not
// deployed.
type sandboxOrderReportingPort struct{ log *slog.Logger }

func (p grpcOrderReportingPort) GenerateOrderReport(ctx context.Context, request *ordersv1.GenerateOrderReportRequest) (*ordersv1.GenerateOrderReportResponse, error) {
	return p.client.GenerateOrderReport(ctx, request)
}

func (p sandboxOrderReportingPort) GenerateOrderReport(ctx context.Context, request *ordersv1.GenerateOrderReportRequest) (*ordersv1.GenerateOrderReportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GenerateOrderReport")
	return &ordersv1.GenerateOrderReportResponse{}, nil
}

func newOrderReportingPort(deps platform.Deps) orderReportingPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxOrderReportingPort{log: deps.Logger}
	}
	return grpcOrderReportingPort{client: ordersv1.NewOrderReportingServiceClient(deps.Conn)}
}

// orderValidationPort is what the orders domain needs of acme.orders.v1.OrderValidationService.
type orderValidationPort interface {
	CheckOrderEligibility(ctx context.Context, request *ordersv1.CheckOrderEligibilityRequest) (*ordersv1.CheckOrderEligibilityResponse, error)
}

// grpcOrderValidationPort reaches acme.orders.v1.OrderValidationService over gRPC.
type grpcOrderValidationPort struct {
	client ordersv1.OrderValidationServiceClient
}

// sandboxOrderValidationPort answers in the sandbox environment, where acme.orders.v1.OrderValidationService is not
// deployed.
type sandboxOrderValidationPort struct{ log *slog.Logger }

func (p grpcOrderValidationPort) CheckOrderEligibility(ctx context.Context, request *ordersv1.CheckOrderEligibilityRequest) (*ordersv1.CheckOrderEligibilityResponse, error) {
	return p.client.CheckOrderEligibility(ctx, request)
}

func (p sandboxOrderValidationPort) CheckOrderEligibility(ctx context.Context, request *ordersv1.CheckOrderEligibilityRequest) (*ordersv1.CheckOrderEligibilityResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CheckOrderEligibility")
	return &ordersv1.CheckOrderEligibilityResponse{}, nil
}

func newOrderValidationPort(deps platform.Deps) orderValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxOrderValidationPort{log: deps.Logger}
	}
	return grpcOrderValidationPort{client: ordersv1.NewOrderValidationServiceClient(deps.Conn)}
}

// orderWorkflowPort is what the orders domain needs of acme.orders.v1.OrderWorkflowService.
type orderWorkflowPort interface {
	PauseOrderWorkflow(ctx context.Context, request *ordersv1.PauseOrderWorkflowRequest) (*ordersv1.PauseOrderWorkflowResponse, error)
}

// grpcOrderWorkflowPort reaches acme.orders.v1.OrderWorkflowService over gRPC.
type grpcOrderWorkflowPort struct {
	client ordersv1.OrderWorkflowServiceClient
}

// sandboxOrderWorkflowPort answers in the sandbox environment, where acme.orders.v1.OrderWorkflowService is not
// deployed.
type sandboxOrderWorkflowPort struct{ log *slog.Logger }

func (p grpcOrderWorkflowPort) PauseOrderWorkflow(ctx context.Context, request *ordersv1.PauseOrderWorkflowRequest) (*ordersv1.PauseOrderWorkflowResponse, error) {
	return p.client.PauseOrderWorkflow(ctx, request)
}

func (p sandboxOrderWorkflowPort) PauseOrderWorkflow(ctx context.Context, request *ordersv1.PauseOrderWorkflowRequest) (*ordersv1.PauseOrderWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PauseOrderWorkflow")
	return &ordersv1.PauseOrderWorkflowResponse{}, nil
}

func newOrderWorkflowPort(deps platform.Deps) orderWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxOrderWorkflowPort{log: deps.Logger}
	}
	return grpcOrderWorkflowPort{client: ordersv1.NewOrderWorkflowServiceClient(deps.Conn)}
}

// pointsLedgerValidationPort is what the orders domain needs of acme.loyalty.v1.PointsLedgerValidationService.
type pointsLedgerValidationPort interface {
	ExplainPointsLedgerDecision(ctx context.Context, request *loyaltyv1.ExplainPointsLedgerDecisionRequest) (*loyaltyv1.ExplainPointsLedgerDecisionResponse, error)
}

// grpcPointsLedgerValidationPort reaches acme.loyalty.v1.PointsLedgerValidationService over gRPC.
type grpcPointsLedgerValidationPort struct {
	client loyaltyv1.PointsLedgerValidationServiceClient
}

// sandboxPointsLedgerValidationPort answers in the sandbox environment, where
// acme.loyalty.v1.PointsLedgerValidationService is not deployed.
type sandboxPointsLedgerValidationPort struct{ log *slog.Logger }

func (p grpcPointsLedgerValidationPort) ExplainPointsLedgerDecision(ctx context.Context, request *loyaltyv1.ExplainPointsLedgerDecisionRequest) (*loyaltyv1.ExplainPointsLedgerDecisionResponse, error) {
	return p.client.ExplainPointsLedgerDecision(ctx, request)
}

func (p sandboxPointsLedgerValidationPort) ExplainPointsLedgerDecision(ctx context.Context, request *loyaltyv1.ExplainPointsLedgerDecisionRequest) (*loyaltyv1.ExplainPointsLedgerDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainPointsLedgerDecision")
	return &loyaltyv1.ExplainPointsLedgerDecisionResponse{}, nil
}

func newPointsLedgerValidationPort(deps platform.Deps) pointsLedgerValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPointsLedgerValidationPort{log: deps.Logger}
	}
	return grpcPointsLedgerValidationPort{client: loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn)}
}

// preferenceCalculationPort is what the orders domain needs of acme.customers.v1.PreferenceCalculationService.
type preferenceCalculationPort interface {
	CalculatePreference(ctx context.Context, request *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error)
}

// grpcPreferenceCalculationPort reaches acme.customers.v1.PreferenceCalculationService over gRPC.
type grpcPreferenceCalculationPort struct {
	client customersv1.PreferenceCalculationServiceClient
}

// sandboxPreferenceCalculationPort answers in the sandbox environment, where
// acme.customers.v1.PreferenceCalculationService is not deployed.
type sandboxPreferenceCalculationPort struct{ log *slog.Logger }

func (p grpcPreferenceCalculationPort) CalculatePreference(ctx context.Context, request *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error) {
	return p.client.CalculatePreference(ctx, request)
}

func (p sandboxPreferenceCalculationPort) CalculatePreference(ctx context.Context, request *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculatePreference")
	return &customersv1.CalculatePreferenceResponse{}, nil
}

func newPreferenceCalculationPort(deps platform.Deps) preferenceCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPreferenceCalculationPort{log: deps.Logger}
	}
	return grpcPreferenceCalculationPort{client: customersv1.NewPreferenceCalculationServiceClient(deps.Conn)}
}

// productReviewQueryPort is what the orders domain needs of acme.reviews.v1.ProductReviewQueryService.
type productReviewQueryPort interface {
	CountProductReviews(ctx context.Context, request *reviewsv1.CountProductReviewsRequest) (*reviewsv1.CountProductReviewsResponse, error)
}

// grpcProductReviewQueryPort reaches acme.reviews.v1.ProductReviewQueryService over gRPC.
type grpcProductReviewQueryPort struct {
	client reviewsv1.ProductReviewQueryServiceClient
}

// sandboxProductReviewQueryPort answers in the sandbox environment, where acme.reviews.v1.ProductReviewQueryService is
// not deployed.
type sandboxProductReviewQueryPort struct{ log *slog.Logger }

func (p grpcProductReviewQueryPort) CountProductReviews(ctx context.Context, request *reviewsv1.CountProductReviewsRequest) (*reviewsv1.CountProductReviewsResponse, error) {
	return p.client.CountProductReviews(ctx, request)
}

func (p sandboxProductReviewQueryPort) CountProductReviews(ctx context.Context, request *reviewsv1.CountProductReviewsRequest) (*reviewsv1.CountProductReviewsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CountProductReviews")
	return &reviewsv1.CountProductReviewsResponse{}, nil
}

func newProductReviewQueryPort(deps platform.Deps) productReviewQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxProductReviewQueryPort{log: deps.Logger}
	}
	return grpcProductReviewQueryPort{client: reviewsv1.NewProductReviewQueryServiceClient(deps.Conn)}
}

// searchQueryValidationPort is what the orders domain needs of acme.search.v1.SearchQueryValidationService.
type searchQueryValidationPort interface {
	ExplainSearchQueryDecision(ctx context.Context, request *searchv1.ExplainSearchQueryDecisionRequest) (*searchv1.ExplainSearchQueryDecisionResponse, error)
}

// grpcSearchQueryValidationPort reaches acme.search.v1.SearchQueryValidationService over gRPC.
type grpcSearchQueryValidationPort struct {
	client searchv1.SearchQueryValidationServiceClient
}

// sandboxSearchQueryValidationPort answers in the sandbox environment, where
// acme.search.v1.SearchQueryValidationService is not deployed.
type sandboxSearchQueryValidationPort struct{ log *slog.Logger }

func (p grpcSearchQueryValidationPort) ExplainSearchQueryDecision(ctx context.Context, request *searchv1.ExplainSearchQueryDecisionRequest) (*searchv1.ExplainSearchQueryDecisionResponse, error) {
	return p.client.ExplainSearchQueryDecision(ctx, request)
}

func (p sandboxSearchQueryValidationPort) ExplainSearchQueryDecision(ctx context.Context, request *searchv1.ExplainSearchQueryDecisionRequest) (*searchv1.ExplainSearchQueryDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainSearchQueryDecision")
	return &searchv1.ExplainSearchQueryDecisionResponse{}, nil
}

func newSearchQueryValidationPort(deps platform.Deps) searchQueryValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSearchQueryValidationPort{log: deps.Logger}
	}
	return grpcSearchQueryValidationPort{client: searchv1.NewSearchQueryValidationServiceClient(deps.Conn)}
}

// synonymHistoryPort is what the orders domain needs of acme.search.v1.SynonymHistoryService.
type synonymHistoryPort interface {
	RevertSynonym(ctx context.Context, request *searchv1.RevertSynonymRequest) (*searchv1.RevertSynonymResponse, error)
}

// grpcSynonymHistoryPort reaches acme.search.v1.SynonymHistoryService over gRPC.
type grpcSynonymHistoryPort struct {
	client searchv1.SynonymHistoryServiceClient
}

// sandboxSynonymHistoryPort answers in the sandbox environment, where acme.search.v1.SynonymHistoryService is not
// deployed.
type sandboxSynonymHistoryPort struct{ log *slog.Logger }

func (p grpcSynonymHistoryPort) RevertSynonym(ctx context.Context, request *searchv1.RevertSynonymRequest) (*searchv1.RevertSynonymResponse, error) {
	return p.client.RevertSynonym(ctx, request)
}

func (p sandboxSynonymHistoryPort) RevertSynonym(ctx context.Context, request *searchv1.RevertSynonymRequest) (*searchv1.RevertSynonymResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RevertSynonym")
	return &searchv1.RevertSynonymResponse{}, nil
}

func newSynonymHistoryPort(deps platform.Deps) synonymHistoryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSynonymHistoryPort{log: deps.Logger}
	}
	return grpcSynonymHistoryPort{client: searchv1.NewSynonymHistoryServiceClient(deps.Conn)}
}

// tourQueryPort is what the orders domain needs of acme.tours.v1.TourQueryService.
type tourQueryPort interface {
	ResolveTours(ctx context.Context, request *toursv1.ResolveToursRequest) (*toursv1.ResolveToursResponse, error)
}

// grpcTourQueryPort reaches acme.tours.v1.TourQueryService over gRPC.
type grpcTourQueryPort struct {
	client toursv1.TourQueryServiceClient
}

// sandboxTourQueryPort answers in the sandbox environment, where acme.tours.v1.TourQueryService is not deployed.
type sandboxTourQueryPort struct{ log *slog.Logger }

func (p grpcTourQueryPort) ResolveTours(ctx context.Context, request *toursv1.ResolveToursRequest) (*toursv1.ResolveToursResponse, error) {
	return p.client.ResolveTours(ctx, request)
}

func (p sandboxTourQueryPort) ResolveTours(ctx context.Context, request *toursv1.ResolveToursRequest) (*toursv1.ResolveToursResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ResolveTours")
	return &toursv1.ResolveToursResponse{}, nil
}

func newTourQueryPort(deps platform.Deps) tourQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxTourQueryPort{log: deps.Logger}
	}
	return grpcTourQueryPort{client: toursv1.NewTourQueryServiceClient(deps.Conn)}
}

// trackingEventCalculationPort is what the orders domain needs of acme.shipping.v1.TrackingEventCalculationService.
type trackingEventCalculationPort interface {
	CalculateTrackingEvent(ctx context.Context, request *shippingv1.CalculateTrackingEventRequest) (*shippingv1.CalculateTrackingEventResponse, error)
}

// grpcTrackingEventCalculationPort reaches acme.shipping.v1.TrackingEventCalculationService over gRPC.
type grpcTrackingEventCalculationPort struct {
	client shippingv1.TrackingEventCalculationServiceClient
}

// sandboxTrackingEventCalculationPort answers in the sandbox environment, where
// acme.shipping.v1.TrackingEventCalculationService is not deployed.
type sandboxTrackingEventCalculationPort struct{ log *slog.Logger }

func (p grpcTrackingEventCalculationPort) CalculateTrackingEvent(ctx context.Context, request *shippingv1.CalculateTrackingEventRequest) (*shippingv1.CalculateTrackingEventResponse, error) {
	return p.client.CalculateTrackingEvent(ctx, request)
}

func (p sandboxTrackingEventCalculationPort) CalculateTrackingEvent(ctx context.Context, request *shippingv1.CalculateTrackingEventRequest) (*shippingv1.CalculateTrackingEventResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateTrackingEvent")
	return &shippingv1.CalculateTrackingEventResponse{}, nil
}

func newTrackingEventCalculationPort(deps platform.Deps) trackingEventCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxTrackingEventCalculationPort{log: deps.Logger}
	}
	return grpcTrackingEventCalculationPort{client: shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn)}
}

// userPort is what the orders domain needs of acme.identity.v1.UserService.
type userPort interface {
	GetUser(ctx context.Context, request *identityv1.GetUserRequest) (*identityv1.GetUserResponse, error)
}

// grpcUserPort reaches acme.identity.v1.UserService over gRPC.
type grpcUserPort struct{ client identityv1.UserServiceClient }

// sandboxUserPort answers in the sandbox environment, where acme.identity.v1.UserService is not deployed.
type sandboxUserPort struct{ log *slog.Logger }

func (p grpcUserPort) GetUser(ctx context.Context, request *identityv1.GetUserRequest) (*identityv1.GetUserResponse, error) {
	return p.client.GetUser(ctx, request)
}

func (p sandboxUserPort) GetUser(ctx context.Context, request *identityv1.GetUserRequest) (*identityv1.GetUserResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetUser")
	return &identityv1.GetUserResponse{}, nil
}

func newUserPort(deps platform.Deps) userPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxUserPort{log: deps.Logger}
	}
	return grpcUserPort{client: identityv1.NewUserServiceClient(deps.Conn)}
}
