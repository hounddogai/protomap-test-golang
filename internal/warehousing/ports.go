package warehousing

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	telematicsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telematics/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// customerSegmentValidationPort is what the warehousing domain needs of
// acme.customers.v1.CustomerSegmentValidationService.
type customerSegmentValidationPort interface {
	CheckCustomerSegmentEligibility(ctx context.Context, request *customersv1.CheckCustomerSegmentEligibilityRequest) (*customersv1.CheckCustomerSegmentEligibilityResponse, error)
}

// grpcCustomerSegmentValidationPort reaches acme.customers.v1.CustomerSegmentValidationService over gRPC.
type grpcCustomerSegmentValidationPort struct {
	client customersv1.CustomerSegmentValidationServiceClient
}

// sandboxCustomerSegmentValidationPort answers in the sandbox environment, where
// acme.customers.v1.CustomerSegmentValidationService is not deployed.
type sandboxCustomerSegmentValidationPort struct{ log *slog.Logger }

func (p grpcCustomerSegmentValidationPort) CheckCustomerSegmentEligibility(ctx context.Context, request *customersv1.CheckCustomerSegmentEligibilityRequest) (*customersv1.CheckCustomerSegmentEligibilityResponse, error) {
	return p.client.CheckCustomerSegmentEligibility(ctx, request)
}

func (p sandboxCustomerSegmentValidationPort) CheckCustomerSegmentEligibility(ctx context.Context, request *customersv1.CheckCustomerSegmentEligibilityRequest) (*customersv1.CheckCustomerSegmentEligibilityResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CheckCustomerSegmentEligibility")
	return &customersv1.CheckCustomerSegmentEligibilityResponse{}, nil
}

func newCustomerSegmentValidationPort(deps platform.Deps) customerSegmentValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerSegmentValidationPort{log: deps.Logger}
	}
	return grpcCustomerSegmentValidationPort{client: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn)}
}

// fulfillmentWorkflowPort is what the warehousing domain needs of acme.fulfillment.v1.FulfillmentWorkflowService.
type fulfillmentWorkflowPort interface {
	PauseFulfillmentWorkflow(ctx context.Context, request *fulfillmentv1.PauseFulfillmentWorkflowRequest) (*fulfillmentv1.PauseFulfillmentWorkflowResponse, error)
}

// grpcFulfillmentWorkflowPort reaches acme.fulfillment.v1.FulfillmentWorkflowService over gRPC.
type grpcFulfillmentWorkflowPort struct {
	client fulfillmentv1.FulfillmentWorkflowServiceClient
}

// sandboxFulfillmentWorkflowPort answers in the sandbox environment, where
// acme.fulfillment.v1.FulfillmentWorkflowService is not deployed.
type sandboxFulfillmentWorkflowPort struct{ log *slog.Logger }

func (p grpcFulfillmentWorkflowPort) PauseFulfillmentWorkflow(ctx context.Context, request *fulfillmentv1.PauseFulfillmentWorkflowRequest) (*fulfillmentv1.PauseFulfillmentWorkflowResponse, error) {
	return p.client.PauseFulfillmentWorkflow(ctx, request)
}

func (p sandboxFulfillmentWorkflowPort) PauseFulfillmentWorkflow(ctx context.Context, request *fulfillmentv1.PauseFulfillmentWorkflowRequest) (*fulfillmentv1.PauseFulfillmentWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PauseFulfillmentWorkflow")
	return &fulfillmentv1.PauseFulfillmentWorkflowResponse{}, nil
}

func newFulfillmentWorkflowPort(deps platform.Deps) fulfillmentWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFulfillmentWorkflowPort{log: deps.Logger}
	}
	return grpcFulfillmentWorkflowPort{client: fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn)}
}

// loyaltyMemberWorkflowPort is what the warehousing domain needs of acme.loyalty.v1.LoyaltyMemberWorkflowService.
type loyaltyMemberWorkflowPort interface {
	CancelLoyaltyMemberWorkflow(ctx context.Context, request *loyaltyv1.CancelLoyaltyMemberWorkflowRequest) (*loyaltyv1.CancelLoyaltyMemberWorkflowResponse, error)
}

// grpcLoyaltyMemberWorkflowPort reaches acme.loyalty.v1.LoyaltyMemberWorkflowService over gRPC.
type grpcLoyaltyMemberWorkflowPort struct {
	client loyaltyv1.LoyaltyMemberWorkflowServiceClient
}

// sandboxLoyaltyMemberWorkflowPort answers in the sandbox environment, where
// acme.loyalty.v1.LoyaltyMemberWorkflowService is not deployed.
type sandboxLoyaltyMemberWorkflowPort struct{ log *slog.Logger }

func (p grpcLoyaltyMemberWorkflowPort) CancelLoyaltyMemberWorkflow(ctx context.Context, request *loyaltyv1.CancelLoyaltyMemberWorkflowRequest) (*loyaltyv1.CancelLoyaltyMemberWorkflowResponse, error) {
	return p.client.CancelLoyaltyMemberWorkflow(ctx, request)
}

func (p sandboxLoyaltyMemberWorkflowPort) CancelLoyaltyMemberWorkflow(ctx context.Context, request *loyaltyv1.CancelLoyaltyMemberWorkflowRequest) (*loyaltyv1.CancelLoyaltyMemberWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CancelLoyaltyMemberWorkflow")
	return &loyaltyv1.CancelLoyaltyMemberWorkflowResponse{}, nil
}

func newLoyaltyMemberWorkflowPort(deps platform.Deps) loyaltyMemberWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxLoyaltyMemberWorkflowPort{log: deps.Logger}
	}
	return grpcLoyaltyMemberWorkflowPort{client: loyaltyv1.NewLoyaltyMemberWorkflowServiceClient(deps.Conn)}
}

// pointsLedgerValidationPort is what the warehousing domain needs of acme.loyalty.v1.PointsLedgerValidationService.
type pointsLedgerValidationPort interface {
	ValidatePointsLedger(ctx context.Context, request *loyaltyv1.ValidatePointsLedgerRequest) (*loyaltyv1.ValidatePointsLedgerResponse, error)
}

// grpcPointsLedgerValidationPort reaches acme.loyalty.v1.PointsLedgerValidationService over gRPC.
type grpcPointsLedgerValidationPort struct {
	client loyaltyv1.PointsLedgerValidationServiceClient
}

// sandboxPointsLedgerValidationPort answers in the sandbox environment, where
// acme.loyalty.v1.PointsLedgerValidationService is not deployed.
type sandboxPointsLedgerValidationPort struct{ log *slog.Logger }

func (p grpcPointsLedgerValidationPort) ValidatePointsLedger(ctx context.Context, request *loyaltyv1.ValidatePointsLedgerRequest) (*loyaltyv1.ValidatePointsLedgerResponse, error) {
	return p.client.ValidatePointsLedger(ctx, request)
}

func (p sandboxPointsLedgerValidationPort) ValidatePointsLedger(ctx context.Context, request *loyaltyv1.ValidatePointsLedgerRequest) (*loyaltyv1.ValidatePointsLedgerResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidatePointsLedger")
	return &loyaltyv1.ValidatePointsLedgerResponse{}, nil
}

func newPointsLedgerValidationPort(deps platform.Deps) pointsLedgerValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPointsLedgerValidationPort{log: deps.Logger}
	}
	return grpcPointsLedgerValidationPort{client: loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn)}
}

// preferenceValidationPort is what the warehousing domain needs of acme.customers.v1.PreferenceValidationService.
type preferenceValidationPort interface {
	ExplainPreferenceDecision(ctx context.Context, request *customersv1.ExplainPreferenceDecisionRequest) (*customersv1.ExplainPreferenceDecisionResponse, error)
}

// grpcPreferenceValidationPort reaches acme.customers.v1.PreferenceValidationService over gRPC.
type grpcPreferenceValidationPort struct {
	client customersv1.PreferenceValidationServiceClient
}

// sandboxPreferenceValidationPort answers in the sandbox environment, where
// acme.customers.v1.PreferenceValidationService is not deployed.
type sandboxPreferenceValidationPort struct{ log *slog.Logger }

func (p grpcPreferenceValidationPort) ExplainPreferenceDecision(ctx context.Context, request *customersv1.ExplainPreferenceDecisionRequest) (*customersv1.ExplainPreferenceDecisionResponse, error) {
	return p.client.ExplainPreferenceDecision(ctx, request)
}

func (p sandboxPreferenceValidationPort) ExplainPreferenceDecision(ctx context.Context, request *customersv1.ExplainPreferenceDecisionRequest) (*customersv1.ExplainPreferenceDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainPreferenceDecision")
	return &customersv1.ExplainPreferenceDecisionResponse{}, nil
}

func newPreferenceValidationPort(deps platform.Deps) preferenceValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPreferenceValidationPort{log: deps.Logger}
	}
	return grpcPreferenceValidationPort{client: customersv1.NewPreferenceValidationServiceClient(deps.Conn)}
}

// telemetryReadingCalculationPort is what the warehousing domain needs of
// acme.telematics.v1.TelemetryReadingCalculationService.
type telemetryReadingCalculationPort interface {
	SimulateTelemetryReading(ctx context.Context, request *telematicsv1.SimulateTelemetryReadingRequest) (*telematicsv1.SimulateTelemetryReadingResponse, error)
}

// grpcTelemetryReadingCalculationPort reaches acme.telematics.v1.TelemetryReadingCalculationService over gRPC.
type grpcTelemetryReadingCalculationPort struct {
	client telematicsv1.TelemetryReadingCalculationServiceClient
}

// sandboxTelemetryReadingCalculationPort answers in the sandbox environment, where
// acme.telematics.v1.TelemetryReadingCalculationService is not deployed.
type sandboxTelemetryReadingCalculationPort struct{ log *slog.Logger }

func (p grpcTelemetryReadingCalculationPort) SimulateTelemetryReading(ctx context.Context, request *telematicsv1.SimulateTelemetryReadingRequest) (*telematicsv1.SimulateTelemetryReadingResponse, error) {
	return p.client.SimulateTelemetryReading(ctx, request)
}

func (p sandboxTelemetryReadingCalculationPort) SimulateTelemetryReading(ctx context.Context, request *telematicsv1.SimulateTelemetryReadingRequest) (*telematicsv1.SimulateTelemetryReadingResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SimulateTelemetryReading")
	return &telematicsv1.SimulateTelemetryReadingResponse{}, nil
}

func newTelemetryReadingCalculationPort(deps platform.Deps) telemetryReadingCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxTelemetryReadingCalculationPort{log: deps.Logger}
	}
	return grpcTelemetryReadingCalculationPort{client: telematicsv1.NewTelemetryReadingCalculationServiceClient(deps.Conn)}
}
