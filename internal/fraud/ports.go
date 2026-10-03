package fraud

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// consentRecordValidationPort is what the fraud domain needs of acme.customers.v1.ConsentRecordValidationService.
type consentRecordValidationPort interface {
	NormalizeConsentRecord(ctx context.Context, request *customersv1.NormalizeConsentRecordRequest) (*customersv1.NormalizeConsentRecordResponse, error)
}

// grpcConsentRecordValidationPort reaches acme.customers.v1.ConsentRecordValidationService over gRPC.
type grpcConsentRecordValidationPort struct {
	client customersv1.ConsentRecordValidationServiceClient
}

// sandboxConsentRecordValidationPort answers in the sandbox environment, where
// acme.customers.v1.ConsentRecordValidationService is not deployed.
type sandboxConsentRecordValidationPort struct{ log *slog.Logger }

func (p grpcConsentRecordValidationPort) NormalizeConsentRecord(ctx context.Context, request *customersv1.NormalizeConsentRecordRequest) (*customersv1.NormalizeConsentRecordResponse, error) {
	return p.client.NormalizeConsentRecord(ctx, request)
}

func (p sandboxConsentRecordValidationPort) NormalizeConsentRecord(ctx context.Context, request *customersv1.NormalizeConsentRecordRequest) (*customersv1.NormalizeConsentRecordResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizeConsentRecord")
	return &customersv1.NormalizeConsentRecordResponse{}, nil
}

func newConsentRecordValidationPort(deps platform.Deps) consentRecordValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxConsentRecordValidationPort{log: deps.Logger}
	}
	return grpcConsentRecordValidationPort{client: customersv1.NewConsentRecordValidationServiceClient(deps.Conn)}
}

// customerSegmentValidationPort is what the fraud domain needs of acme.customers.v1.CustomerSegmentValidationService.
type customerSegmentValidationPort interface {
	ExplainCustomerSegmentDecision(ctx context.Context, request *customersv1.ExplainCustomerSegmentDecisionRequest) (*customersv1.ExplainCustomerSegmentDecisionResponse, error)
}

// grpcCustomerSegmentValidationPort reaches acme.customers.v1.CustomerSegmentValidationService over gRPC.
type grpcCustomerSegmentValidationPort struct {
	client customersv1.CustomerSegmentValidationServiceClient
}

// sandboxCustomerSegmentValidationPort answers in the sandbox environment, where
// acme.customers.v1.CustomerSegmentValidationService is not deployed.
type sandboxCustomerSegmentValidationPort struct{ log *slog.Logger }

func (p grpcCustomerSegmentValidationPort) ExplainCustomerSegmentDecision(ctx context.Context, request *customersv1.ExplainCustomerSegmentDecisionRequest) (*customersv1.ExplainCustomerSegmentDecisionResponse, error) {
	return p.client.ExplainCustomerSegmentDecision(ctx, request)
}

func (p sandboxCustomerSegmentValidationPort) ExplainCustomerSegmentDecision(ctx context.Context, request *customersv1.ExplainCustomerSegmentDecisionRequest) (*customersv1.ExplainCustomerSegmentDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainCustomerSegmentDecision")
	return &customersv1.ExplainCustomerSegmentDecisionResponse{}, nil
}

func newCustomerSegmentValidationPort(deps platform.Deps) customerSegmentValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerSegmentValidationPort{log: deps.Logger}
	}
	return grpcCustomerSegmentValidationPort{client: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn)}
}

// fraudRuleCalculationPort is what the fraud domain needs of acme.fraud.v1.FraudRuleCalculationService.
type fraudRuleCalculationPort interface {
	SimulateFraudRule(ctx context.Context, request *fraudv1.SimulateFraudRuleRequest) (*fraudv1.SimulateFraudRuleResponse, error)
}

// grpcFraudRuleCalculationPort reaches acme.fraud.v1.FraudRuleCalculationService over gRPC.
type grpcFraudRuleCalculationPort struct {
	client fraudv1.FraudRuleCalculationServiceClient
}

// sandboxFraudRuleCalculationPort answers in the sandbox environment, where acme.fraud.v1.FraudRuleCalculationService
// is not deployed.
type sandboxFraudRuleCalculationPort struct{ log *slog.Logger }

func (p grpcFraudRuleCalculationPort) SimulateFraudRule(ctx context.Context, request *fraudv1.SimulateFraudRuleRequest) (*fraudv1.SimulateFraudRuleResponse, error) {
	return p.client.SimulateFraudRule(ctx, request)
}

func (p sandboxFraudRuleCalculationPort) SimulateFraudRule(ctx context.Context, request *fraudv1.SimulateFraudRuleRequest) (*fraudv1.SimulateFraudRuleResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SimulateFraudRule")
	return &fraudv1.SimulateFraudRuleResponse{}, nil
}

func newFraudRuleCalculationPort(deps platform.Deps) fraudRuleCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFraudRuleCalculationPort{log: deps.Logger}
	}
	return grpcFraudRuleCalculationPort{client: fraudv1.NewFraudRuleCalculationServiceClient(deps.Conn)}
}

// fraudSignalQueryPort is what the fraud domain needs of acme.fraud.v1.FraudSignalQueryService.
type fraudSignalQueryPort interface {
	ResolveFraudSignals(ctx context.Context, request *fraudv1.ResolveFraudSignalsRequest) (*fraudv1.ResolveFraudSignalsResponse, error)
}

// grpcFraudSignalQueryPort reaches acme.fraud.v1.FraudSignalQueryService over gRPC.
type grpcFraudSignalQueryPort struct {
	client fraudv1.FraudSignalQueryServiceClient
}

// sandboxFraudSignalQueryPort answers in the sandbox environment, where acme.fraud.v1.FraudSignalQueryService is not
// deployed.
type sandboxFraudSignalQueryPort struct{ log *slog.Logger }

func (p grpcFraudSignalQueryPort) ResolveFraudSignals(ctx context.Context, request *fraudv1.ResolveFraudSignalsRequest) (*fraudv1.ResolveFraudSignalsResponse, error) {
	return p.client.ResolveFraudSignals(ctx, request)
}

func (p sandboxFraudSignalQueryPort) ResolveFraudSignals(ctx context.Context, request *fraudv1.ResolveFraudSignalsRequest) (*fraudv1.ResolveFraudSignalsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ResolveFraudSignals")
	return &fraudv1.ResolveFraudSignalsResponse{}, nil
}

func newFraudSignalQueryPort(deps platform.Deps) fraudSignalQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFraudSignalQueryPort{log: deps.Logger}
	}
	return grpcFraudSignalQueryPort{client: fraudv1.NewFraudSignalQueryServiceClient(deps.Conn)}
}

// fraudSignalValidationPort is what the fraud domain needs of acme.fraud.v1.FraudSignalValidationService.
type fraudSignalValidationPort interface {
	PreviewFraudSignal(ctx context.Context, request *fraudv1.PreviewFraudSignalRequest) (*fraudv1.PreviewFraudSignalResponse, error)
}

// grpcFraudSignalValidationPort reaches acme.fraud.v1.FraudSignalValidationService over gRPC.
type grpcFraudSignalValidationPort struct {
	client fraudv1.FraudSignalValidationServiceClient
}

// sandboxFraudSignalValidationPort answers in the sandbox environment, where acme.fraud.v1.FraudSignalValidationService
// is not deployed.
type sandboxFraudSignalValidationPort struct{ log *slog.Logger }

func (p grpcFraudSignalValidationPort) PreviewFraudSignal(ctx context.Context, request *fraudv1.PreviewFraudSignalRequest) (*fraudv1.PreviewFraudSignalResponse, error) {
	return p.client.PreviewFraudSignal(ctx, request)
}

func (p sandboxFraudSignalValidationPort) PreviewFraudSignal(ctx context.Context, request *fraudv1.PreviewFraudSignalRequest) (*fraudv1.PreviewFraudSignalResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewFraudSignal")
	return &fraudv1.PreviewFraudSignalResponse{}, nil
}

func newFraudSignalValidationPort(deps platform.Deps) fraudSignalValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFraudSignalValidationPort{log: deps.Logger}
	}
	return grpcFraudSignalValidationPort{client: fraudv1.NewFraudSignalValidationServiceClient(deps.Conn)}
}

// householdCalculationPort is what the fraud domain needs of acme.customers.v1.HouseholdCalculationService.
type householdCalculationPort interface {
	EstimateHousehold(ctx context.Context, request *customersv1.EstimateHouseholdRequest) (*customersv1.EstimateHouseholdResponse, error)
}

// grpcHouseholdCalculationPort reaches acme.customers.v1.HouseholdCalculationService over gRPC.
type grpcHouseholdCalculationPort struct {
	client customersv1.HouseholdCalculationServiceClient
}

// sandboxHouseholdCalculationPort answers in the sandbox environment, where
// acme.customers.v1.HouseholdCalculationService is not deployed.
type sandboxHouseholdCalculationPort struct{ log *slog.Logger }

func (p grpcHouseholdCalculationPort) EstimateHousehold(ctx context.Context, request *customersv1.EstimateHouseholdRequest) (*customersv1.EstimateHouseholdResponse, error) {
	return p.client.EstimateHousehold(ctx, request)
}

func (p sandboxHouseholdCalculationPort) EstimateHousehold(ctx context.Context, request *customersv1.EstimateHouseholdRequest) (*customersv1.EstimateHouseholdResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateHousehold")
	return &customersv1.EstimateHouseholdResponse{}, nil
}

func newHouseholdCalculationPort(deps platform.Deps) householdCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxHouseholdCalculationPort{log: deps.Logger}
	}
	return grpcHouseholdCalculationPort{client: customersv1.NewHouseholdCalculationServiceClient(deps.Conn)}
}

// ledgerAccountPort is what the fraud domain needs of acme.ledger.v1.LedgerAccountService.
type ledgerAccountPort interface {
	CreateLedgerAccount(ctx context.Context, request *ledgerv1.CreateLedgerAccountRequest) (*ledgerv1.CreateLedgerAccountResponse, error)
}

// grpcLedgerAccountPort reaches acme.ledger.v1.LedgerAccountService over gRPC.
type grpcLedgerAccountPort struct {
	client ledgerv1.LedgerAccountServiceClient
}

// sandboxLedgerAccountPort answers in the sandbox environment, where acme.ledger.v1.LedgerAccountService is not
// deployed.
type sandboxLedgerAccountPort struct{ log *slog.Logger }

func (p grpcLedgerAccountPort) CreateLedgerAccount(ctx context.Context, request *ledgerv1.CreateLedgerAccountRequest) (*ledgerv1.CreateLedgerAccountResponse, error) {
	return p.client.CreateLedgerAccount(ctx, request)
}

func (p sandboxLedgerAccountPort) CreateLedgerAccount(ctx context.Context, request *ledgerv1.CreateLedgerAccountRequest) (*ledgerv1.CreateLedgerAccountResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CreateLedgerAccount")
	return &ledgerv1.CreateLedgerAccountResponse{}, nil
}

func newLedgerAccountPort(deps platform.Deps) ledgerAccountPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxLedgerAccountPort{log: deps.Logger}
	}
	return grpcLedgerAccountPort{client: ledgerv1.NewLedgerAccountServiceClient(deps.Conn)}
}

// riskAssessmentQueryPort is what the fraud domain needs of acme.fraud.v1.RiskAssessmentQueryService.
type riskAssessmentQueryPort interface {
	ResolveRiskAssessments(ctx context.Context, request *fraudv1.ResolveRiskAssessmentsRequest) (*fraudv1.ResolveRiskAssessmentsResponse, error)
}

// grpcRiskAssessmentQueryPort reaches acme.fraud.v1.RiskAssessmentQueryService over gRPC.
type grpcRiskAssessmentQueryPort struct {
	client fraudv1.RiskAssessmentQueryServiceClient
}

// sandboxRiskAssessmentQueryPort answers in the sandbox environment, where acme.fraud.v1.RiskAssessmentQueryService is
// not deployed.
type sandboxRiskAssessmentQueryPort struct{ log *slog.Logger }

func (p grpcRiskAssessmentQueryPort) ResolveRiskAssessments(ctx context.Context, request *fraudv1.ResolveRiskAssessmentsRequest) (*fraudv1.ResolveRiskAssessmentsResponse, error) {
	return p.client.ResolveRiskAssessments(ctx, request)
}

func (p sandboxRiskAssessmentQueryPort) ResolveRiskAssessments(ctx context.Context, request *fraudv1.ResolveRiskAssessmentsRequest) (*fraudv1.ResolveRiskAssessmentsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ResolveRiskAssessments")
	return &fraudv1.ResolveRiskAssessmentsResponse{}, nil
}

func newRiskAssessmentQueryPort(deps platform.Deps) riskAssessmentQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRiskAssessmentQueryPort{log: deps.Logger}
	}
	return grpcRiskAssessmentQueryPort{client: fraudv1.NewRiskAssessmentQueryServiceClient(deps.Conn)}
}

// synonymExportPort is what the fraud domain needs of acme.search.v1.SynonymExportService.
type synonymExportPort interface {
	RetrySynonymExport(ctx context.Context, request *searchv1.RetrySynonymExportRequest) (*searchv1.RetrySynonymExportResponse, error)
}

// grpcSynonymExportPort reaches acme.search.v1.SynonymExportService over gRPC.
type grpcSynonymExportPort struct {
	client searchv1.SynonymExportServiceClient
}

// sandboxSynonymExportPort answers in the sandbox environment, where acme.search.v1.SynonymExportService is not
// deployed.
type sandboxSynonymExportPort struct{ log *slog.Logger }

func (p grpcSynonymExportPort) RetrySynonymExport(ctx context.Context, request *searchv1.RetrySynonymExportRequest) (*searchv1.RetrySynonymExportResponse, error) {
	return p.client.RetrySynonymExport(ctx, request)
}

func (p sandboxSynonymExportPort) RetrySynonymExport(ctx context.Context, request *searchv1.RetrySynonymExportRequest) (*searchv1.RetrySynonymExportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RetrySynonymExport")
	return &searchv1.RetrySynonymExportResponse{}, nil
}

func newSynonymExportPort(deps platform.Deps) synonymExportPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSynonymExportPort{log: deps.Logger}
	}
	return grpcSynonymExportPort{client: searchv1.NewSynonymExportServiceClient(deps.Conn)}
}
