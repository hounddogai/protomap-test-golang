package checkout

import (
	"context"
	"log/slog"

	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// carePlanCalculationPort is what the checkout domain needs of acme.careplans.v1.CarePlanCalculationService.
type carePlanCalculationPort interface {
	ExplainCarePlanCalculation(ctx context.Context, request *careplansv1.ExplainCarePlanCalculationRequest) (*careplansv1.ExplainCarePlanCalculationResponse, error)
}

// grpcCarePlanCalculationPort reaches acme.careplans.v1.CarePlanCalculationService over gRPC.
type grpcCarePlanCalculationPort struct {
	client careplansv1.CarePlanCalculationServiceClient
}

// sandboxCarePlanCalculationPort answers in the sandbox environment, where acme.careplans.v1.CarePlanCalculationService
// is not deployed.
type sandboxCarePlanCalculationPort struct{ log *slog.Logger }

func (p grpcCarePlanCalculationPort) ExplainCarePlanCalculation(ctx context.Context, request *careplansv1.ExplainCarePlanCalculationRequest) (*careplansv1.ExplainCarePlanCalculationResponse, error) {
	return p.client.ExplainCarePlanCalculation(ctx, request)
}

func (p sandboxCarePlanCalculationPort) ExplainCarePlanCalculation(ctx context.Context, request *careplansv1.ExplainCarePlanCalculationRequest) (*careplansv1.ExplainCarePlanCalculationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainCarePlanCalculation")
	return &careplansv1.ExplainCarePlanCalculationResponse{}, nil
}

func newCarePlanCalculationPort(deps platform.Deps) carePlanCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCarePlanCalculationPort{log: deps.Logger}
	}
	return grpcCarePlanCalculationPort{client: careplansv1.NewCarePlanCalculationServiceClient(deps.Conn)}
}

// checkoutSessionApprovalPort is what the checkout domain needs of acme.checkout.v1.CheckoutSessionApprovalService.
type checkoutSessionApprovalPort interface {
	RecallCheckoutSession(ctx context.Context, request *checkoutv1.RecallCheckoutSessionRequest) (*checkoutv1.RecallCheckoutSessionResponse, error)
}

// grpcCheckoutSessionApprovalPort reaches acme.checkout.v1.CheckoutSessionApprovalService over gRPC.
type grpcCheckoutSessionApprovalPort struct {
	client checkoutv1.CheckoutSessionApprovalServiceClient
}

// sandboxCheckoutSessionApprovalPort answers in the sandbox environment, where
// acme.checkout.v1.CheckoutSessionApprovalService is not deployed.
type sandboxCheckoutSessionApprovalPort struct{ log *slog.Logger }

func (p grpcCheckoutSessionApprovalPort) RecallCheckoutSession(ctx context.Context, request *checkoutv1.RecallCheckoutSessionRequest) (*checkoutv1.RecallCheckoutSessionResponse, error) {
	return p.client.RecallCheckoutSession(ctx, request)
}

func (p sandboxCheckoutSessionApprovalPort) RecallCheckoutSession(ctx context.Context, request *checkoutv1.RecallCheckoutSessionRequest) (*checkoutv1.RecallCheckoutSessionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecallCheckoutSession")
	return &checkoutv1.RecallCheckoutSessionResponse{}, nil
}

func newCheckoutSessionApprovalPort(deps platform.Deps) checkoutSessionApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCheckoutSessionApprovalPort{log: deps.Logger}
	}
	return grpcCheckoutSessionApprovalPort{client: checkoutv1.NewCheckoutSessionApprovalServiceClient(deps.Conn)}
}

// householdValidationPort is what the checkout domain needs of acme.customers.v1.HouseholdValidationService.
type householdValidationPort interface {
	NormalizeHousehold(ctx context.Context, request *customersv1.NormalizeHouseholdRequest) (*customersv1.NormalizeHouseholdResponse, error)
}

// grpcHouseholdValidationPort reaches acme.customers.v1.HouseholdValidationService over gRPC.
type grpcHouseholdValidationPort struct {
	client customersv1.HouseholdValidationServiceClient
}

// sandboxHouseholdValidationPort answers in the sandbox environment, where acme.customers.v1.HouseholdValidationService
// is not deployed.
type sandboxHouseholdValidationPort struct{ log *slog.Logger }

func (p grpcHouseholdValidationPort) NormalizeHousehold(ctx context.Context, request *customersv1.NormalizeHouseholdRequest) (*customersv1.NormalizeHouseholdResponse, error) {
	return p.client.NormalizeHousehold(ctx, request)
}

func (p sandboxHouseholdValidationPort) NormalizeHousehold(ctx context.Context, request *customersv1.NormalizeHouseholdRequest) (*customersv1.NormalizeHouseholdResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizeHousehold")
	return &customersv1.NormalizeHouseholdResponse{}, nil
}

func newHouseholdValidationPort(deps platform.Deps) householdValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxHouseholdValidationPort{log: deps.Logger}
	}
	return grpcHouseholdValidationPort{client: customersv1.NewHouseholdValidationServiceClient(deps.Conn)}
}

// loyaltyMemberApprovalPort is what the checkout domain needs of acme.loyalty.v1.LoyaltyMemberApprovalService.
type loyaltyMemberApprovalPort interface {
	ApproveLoyaltyMember(ctx context.Context, request *loyaltyv1.ApproveLoyaltyMemberRequest) (*loyaltyv1.ApproveLoyaltyMemberResponse, error)
}

// grpcLoyaltyMemberApprovalPort reaches acme.loyalty.v1.LoyaltyMemberApprovalService over gRPC.
type grpcLoyaltyMemberApprovalPort struct {
	client loyaltyv1.LoyaltyMemberApprovalServiceClient
}

// sandboxLoyaltyMemberApprovalPort answers in the sandbox environment, where
// acme.loyalty.v1.LoyaltyMemberApprovalService is not deployed.
type sandboxLoyaltyMemberApprovalPort struct{ log *slog.Logger }

func (p grpcLoyaltyMemberApprovalPort) ApproveLoyaltyMember(ctx context.Context, request *loyaltyv1.ApproveLoyaltyMemberRequest) (*loyaltyv1.ApproveLoyaltyMemberResponse, error) {
	return p.client.ApproveLoyaltyMember(ctx, request)
}

func (p sandboxLoyaltyMemberApprovalPort) ApproveLoyaltyMember(ctx context.Context, request *loyaltyv1.ApproveLoyaltyMemberRequest) (*loyaltyv1.ApproveLoyaltyMemberResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ApproveLoyaltyMember")
	return &loyaltyv1.ApproveLoyaltyMemberResponse{}, nil
}

func newLoyaltyMemberApprovalPort(deps platform.Deps) loyaltyMemberApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxLoyaltyMemberApprovalPort{log: deps.Logger}
	}
	return grpcLoyaltyMemberApprovalPort{client: loyaltyv1.NewLoyaltyMemberApprovalServiceClient(deps.Conn)}
}

// paymentIntentValidationPort is what the checkout domain needs of acme.checkout.v1.PaymentIntentValidationService.
type paymentIntentValidationPort interface {
	ScorePaymentIntent(ctx context.Context, request *checkoutv1.ScorePaymentIntentRequest) (*checkoutv1.ScorePaymentIntentResponse, error)
}

// grpcPaymentIntentValidationPort reaches acme.checkout.v1.PaymentIntentValidationService over gRPC.
type grpcPaymentIntentValidationPort struct {
	client checkoutv1.PaymentIntentValidationServiceClient
}

// sandboxPaymentIntentValidationPort answers in the sandbox environment, where
// acme.checkout.v1.PaymentIntentValidationService is not deployed.
type sandboxPaymentIntentValidationPort struct{ log *slog.Logger }

func (p grpcPaymentIntentValidationPort) ScorePaymentIntent(ctx context.Context, request *checkoutv1.ScorePaymentIntentRequest) (*checkoutv1.ScorePaymentIntentResponse, error) {
	return p.client.ScorePaymentIntent(ctx, request)
}

func (p sandboxPaymentIntentValidationPort) ScorePaymentIntent(ctx context.Context, request *checkoutv1.ScorePaymentIntentRequest) (*checkoutv1.ScorePaymentIntentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScorePaymentIntent")
	return &checkoutv1.ScorePaymentIntentResponse{}, nil
}

func newPaymentIntentValidationPort(deps platform.Deps) paymentIntentValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPaymentIntentValidationPort{log: deps.Logger}
	}
	return grpcPaymentIntentValidationPort{client: checkoutv1.NewPaymentIntentValidationServiceClient(deps.Conn)}
}

// preferenceCalculationPort is what the checkout domain needs of acme.customers.v1.PreferenceCalculationService.
type preferenceCalculationPort interface {
	EstimatePreference(ctx context.Context, request *customersv1.EstimatePreferenceRequest) (*customersv1.EstimatePreferenceResponse, error)
}

// grpcPreferenceCalculationPort reaches acme.customers.v1.PreferenceCalculationService over gRPC.
type grpcPreferenceCalculationPort struct {
	client customersv1.PreferenceCalculationServiceClient
}

// sandboxPreferenceCalculationPort answers in the sandbox environment, where
// acme.customers.v1.PreferenceCalculationService is not deployed.
type sandboxPreferenceCalculationPort struct{ log *slog.Logger }

func (p grpcPreferenceCalculationPort) EstimatePreference(ctx context.Context, request *customersv1.EstimatePreferenceRequest) (*customersv1.EstimatePreferenceResponse, error) {
	return p.client.EstimatePreference(ctx, request)
}

func (p sandboxPreferenceCalculationPort) EstimatePreference(ctx context.Context, request *customersv1.EstimatePreferenceRequest) (*customersv1.EstimatePreferenceResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimatePreference")
	return &customersv1.EstimatePreferenceResponse{}, nil
}

func newPreferenceCalculationPort(deps platform.Deps) preferenceCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPreferenceCalculationPort{log: deps.Logger}
	}
	return grpcPreferenceCalculationPort{client: customersv1.NewPreferenceCalculationServiceClient(deps.Conn)}
}

// riskAssessmentCalculationPort is what the checkout domain needs of acme.fraud.v1.RiskAssessmentCalculationService.
type riskAssessmentCalculationPort interface {
	ExplainRiskAssessmentCalculation(ctx context.Context, request *fraudv1.ExplainRiskAssessmentCalculationRequest) (*fraudv1.ExplainRiskAssessmentCalculationResponse, error)
}

// grpcRiskAssessmentCalculationPort reaches acme.fraud.v1.RiskAssessmentCalculationService over gRPC.
type grpcRiskAssessmentCalculationPort struct {
	client fraudv1.RiskAssessmentCalculationServiceClient
}

// sandboxRiskAssessmentCalculationPort answers in the sandbox environment, where
// acme.fraud.v1.RiskAssessmentCalculationService is not deployed.
type sandboxRiskAssessmentCalculationPort struct{ log *slog.Logger }

func (p grpcRiskAssessmentCalculationPort) ExplainRiskAssessmentCalculation(ctx context.Context, request *fraudv1.ExplainRiskAssessmentCalculationRequest) (*fraudv1.ExplainRiskAssessmentCalculationResponse, error) {
	return p.client.ExplainRiskAssessmentCalculation(ctx, request)
}

func (p sandboxRiskAssessmentCalculationPort) ExplainRiskAssessmentCalculation(ctx context.Context, request *fraudv1.ExplainRiskAssessmentCalculationRequest) (*fraudv1.ExplainRiskAssessmentCalculationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainRiskAssessmentCalculation")
	return &fraudv1.ExplainRiskAssessmentCalculationResponse{}, nil
}

func newRiskAssessmentCalculationPort(deps platform.Deps) riskAssessmentCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRiskAssessmentCalculationPort{log: deps.Logger}
	}
	return grpcRiskAssessmentCalculationPort{client: fraudv1.NewRiskAssessmentCalculationServiceClient(deps.Conn)}
}
