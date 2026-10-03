package sellers

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// consentRecordCalculationPort is what the sellers domain needs of acme.customers.v1.ConsentRecordCalculationService.
type consentRecordCalculationPort interface {
	EstimateConsentRecord(ctx context.Context, request *customersv1.EstimateConsentRecordRequest) (*customersv1.EstimateConsentRecordResponse, error)
}

// grpcConsentRecordCalculationPort reaches acme.customers.v1.ConsentRecordCalculationService over gRPC.
type grpcConsentRecordCalculationPort struct {
	client customersv1.ConsentRecordCalculationServiceClient
}

// sandboxConsentRecordCalculationPort answers in the sandbox environment, where
// acme.customers.v1.ConsentRecordCalculationService is not deployed.
type sandboxConsentRecordCalculationPort struct{ log *slog.Logger }

func (p grpcConsentRecordCalculationPort) EstimateConsentRecord(ctx context.Context, request *customersv1.EstimateConsentRecordRequest) (*customersv1.EstimateConsentRecordResponse, error) {
	return p.client.EstimateConsentRecord(ctx, request)
}

func (p sandboxConsentRecordCalculationPort) EstimateConsentRecord(ctx context.Context, request *customersv1.EstimateConsentRecordRequest) (*customersv1.EstimateConsentRecordResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateConsentRecord")
	return &customersv1.EstimateConsentRecordResponse{}, nil
}

func newConsentRecordCalculationPort(deps platform.Deps) consentRecordCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxConsentRecordCalculationPort{log: deps.Logger}
	}
	return grpcConsentRecordCalculationPort{client: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn)}
}

// currencyRateCalculationPort is what the sellers domain needs of acme.pricing.v1.CurrencyRateCalculationService.
type currencyRateCalculationPort interface {
	QuoteCurrencyRate(ctx context.Context, request *pricingv1.QuoteCurrencyRateRequest) (*pricingv1.QuoteCurrencyRateResponse, error)
}

// grpcCurrencyRateCalculationPort reaches acme.pricing.v1.CurrencyRateCalculationService over gRPC.
type grpcCurrencyRateCalculationPort struct {
	client pricingv1.CurrencyRateCalculationServiceClient
}

// sandboxCurrencyRateCalculationPort answers in the sandbox environment, where
// acme.pricing.v1.CurrencyRateCalculationService is not deployed.
type sandboxCurrencyRateCalculationPort struct{ log *slog.Logger }

func (p grpcCurrencyRateCalculationPort) QuoteCurrencyRate(ctx context.Context, request *pricingv1.QuoteCurrencyRateRequest) (*pricingv1.QuoteCurrencyRateResponse, error) {
	return p.client.QuoteCurrencyRate(ctx, request)
}

func (p sandboxCurrencyRateCalculationPort) QuoteCurrencyRate(ctx context.Context, request *pricingv1.QuoteCurrencyRateRequest) (*pricingv1.QuoteCurrencyRateResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers QuoteCurrencyRate")
	return &pricingv1.QuoteCurrencyRateResponse{}, nil
}

func newCurrencyRateCalculationPort(deps platform.Deps) currencyRateCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCurrencyRateCalculationPort{log: deps.Logger}
	}
	return grpcCurrencyRateCalculationPort{client: pricingv1.NewCurrencyRateCalculationServiceClient(deps.Conn)}
}

// customerSegmentValidationPort is what the sellers domain needs of acme.customers.v1.CustomerSegmentValidationService.
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

// featureFlagValidationPort is what the sellers domain needs of acme.featureflags.v1.FeatureFlagValidationService.
type featureFlagValidationPort interface {
	ExplainFeatureFlagDecision(ctx context.Context, request *featureflagsv1.ExplainFeatureFlagDecisionRequest) (*featureflagsv1.ExplainFeatureFlagDecisionResponse, error)
}

// grpcFeatureFlagValidationPort reaches acme.featureflags.v1.FeatureFlagValidationService over gRPC.
type grpcFeatureFlagValidationPort struct {
	client featureflagsv1.FeatureFlagValidationServiceClient
}

// sandboxFeatureFlagValidationPort answers in the sandbox environment, where
// acme.featureflags.v1.FeatureFlagValidationService is not deployed.
type sandboxFeatureFlagValidationPort struct{ log *slog.Logger }

func (p grpcFeatureFlagValidationPort) ExplainFeatureFlagDecision(ctx context.Context, request *featureflagsv1.ExplainFeatureFlagDecisionRequest) (*featureflagsv1.ExplainFeatureFlagDecisionResponse, error) {
	return p.client.ExplainFeatureFlagDecision(ctx, request)
}

func (p sandboxFeatureFlagValidationPort) ExplainFeatureFlagDecision(ctx context.Context, request *featureflagsv1.ExplainFeatureFlagDecisionRequest) (*featureflagsv1.ExplainFeatureFlagDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainFeatureFlagDecision")
	return &featureflagsv1.ExplainFeatureFlagDecisionResponse{}, nil
}

func newFeatureFlagValidationPort(deps platform.Deps) featureFlagValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFeatureFlagValidationPort{log: deps.Logger}
	}
	return grpcFeatureFlagValidationPort{client: featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn)}
}

// giftCardBatchCalculationPort is what the sellers domain needs of acme.giftcards.v1.GiftCardBatchCalculationService.
type giftCardBatchCalculationPort interface {
	QuoteGiftCardBatch(ctx context.Context, request *giftcardsv1.QuoteGiftCardBatchRequest) (*giftcardsv1.QuoteGiftCardBatchResponse, error)
}

// grpcGiftCardBatchCalculationPort reaches acme.giftcards.v1.GiftCardBatchCalculationService over gRPC.
type grpcGiftCardBatchCalculationPort struct {
	client giftcardsv1.GiftCardBatchCalculationServiceClient
}

// sandboxGiftCardBatchCalculationPort answers in the sandbox environment, where
// acme.giftcards.v1.GiftCardBatchCalculationService is not deployed.
type sandboxGiftCardBatchCalculationPort struct{ log *slog.Logger }

func (p grpcGiftCardBatchCalculationPort) QuoteGiftCardBatch(ctx context.Context, request *giftcardsv1.QuoteGiftCardBatchRequest) (*giftcardsv1.QuoteGiftCardBatchResponse, error) {
	return p.client.QuoteGiftCardBatch(ctx, request)
}

func (p sandboxGiftCardBatchCalculationPort) QuoteGiftCardBatch(ctx context.Context, request *giftcardsv1.QuoteGiftCardBatchRequest) (*giftcardsv1.QuoteGiftCardBatchResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers QuoteGiftCardBatch")
	return &giftcardsv1.QuoteGiftCardBatchResponse{}, nil
}

func newGiftCardBatchCalculationPort(deps platform.Deps) giftCardBatchCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardBatchCalculationPort{log: deps.Logger}
	}
	return grpcGiftCardBatchCalculationPort{client: giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn)}
}

// giftCardCalculationPort is what the sellers domain needs of acme.giftcards.v1.GiftCardCalculationService.
type giftCardCalculationPort interface {
	RecalculateGiftCards(ctx context.Context, request *giftcardsv1.RecalculateGiftCardsRequest) (*giftcardsv1.RecalculateGiftCardsResponse, error)
}

// grpcGiftCardCalculationPort reaches acme.giftcards.v1.GiftCardCalculationService over gRPC.
type grpcGiftCardCalculationPort struct {
	client giftcardsv1.GiftCardCalculationServiceClient
}

// sandboxGiftCardCalculationPort answers in the sandbox environment, where acme.giftcards.v1.GiftCardCalculationService
// is not deployed.
type sandboxGiftCardCalculationPort struct{ log *slog.Logger }

func (p grpcGiftCardCalculationPort) RecalculateGiftCards(ctx context.Context, request *giftcardsv1.RecalculateGiftCardsRequest) (*giftcardsv1.RecalculateGiftCardsResponse, error) {
	return p.client.RecalculateGiftCards(ctx, request)
}

func (p sandboxGiftCardCalculationPort) RecalculateGiftCards(ctx context.Context, request *giftcardsv1.RecalculateGiftCardsRequest) (*giftcardsv1.RecalculateGiftCardsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecalculateGiftCards")
	return &giftcardsv1.RecalculateGiftCardsResponse{}, nil
}

func newGiftCardCalculationPort(deps platform.Deps) giftCardCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardCalculationPort{log: deps.Logger}
	}
	return grpcGiftCardCalculationPort{client: giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn)}
}

// giftCardQueryPort is what the sellers domain needs of acme.giftcards.v1.GiftCardQueryService.
type giftCardQueryPort interface {
	FindGiftCards(ctx context.Context, request *giftcardsv1.FindGiftCardsRequest) (*giftcardsv1.FindGiftCardsResponse, error)
}

// grpcGiftCardQueryPort reaches acme.giftcards.v1.GiftCardQueryService over gRPC.
type grpcGiftCardQueryPort struct {
	client giftcardsv1.GiftCardQueryServiceClient
}

// sandboxGiftCardQueryPort answers in the sandbox environment, where acme.giftcards.v1.GiftCardQueryService is not
// deployed.
type sandboxGiftCardQueryPort struct{ log *slog.Logger }

func (p grpcGiftCardQueryPort) FindGiftCards(ctx context.Context, request *giftcardsv1.FindGiftCardsRequest) (*giftcardsv1.FindGiftCardsResponse, error) {
	return p.client.FindGiftCards(ctx, request)
}

func (p sandboxGiftCardQueryPort) FindGiftCards(ctx context.Context, request *giftcardsv1.FindGiftCardsRequest) (*giftcardsv1.FindGiftCardsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers FindGiftCards")
	return &giftcardsv1.FindGiftCardsResponse{}, nil
}

func newGiftCardQueryPort(deps platform.Deps) giftCardQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardQueryPort{log: deps.Logger}
	}
	return grpcGiftCardQueryPort{client: giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn)}
}

// giftCardTransactionValidationPort is what the sellers domain needs of
// acme.giftcards.v1.GiftCardTransactionValidationService.
type giftCardTransactionValidationPort interface {
	NormalizeGiftCardTransaction(ctx context.Context, request *giftcardsv1.NormalizeGiftCardTransactionRequest) (*giftcardsv1.NormalizeGiftCardTransactionResponse, error)
}

// grpcGiftCardTransactionValidationPort reaches acme.giftcards.v1.GiftCardTransactionValidationService over gRPC.
type grpcGiftCardTransactionValidationPort struct {
	client giftcardsv1.GiftCardTransactionValidationServiceClient
}

// sandboxGiftCardTransactionValidationPort answers in the sandbox environment, where
// acme.giftcards.v1.GiftCardTransactionValidationService is not deployed.
type sandboxGiftCardTransactionValidationPort struct{ log *slog.Logger }

func (p grpcGiftCardTransactionValidationPort) NormalizeGiftCardTransaction(ctx context.Context, request *giftcardsv1.NormalizeGiftCardTransactionRequest) (*giftcardsv1.NormalizeGiftCardTransactionResponse, error) {
	return p.client.NormalizeGiftCardTransaction(ctx, request)
}

func (p sandboxGiftCardTransactionValidationPort) NormalizeGiftCardTransaction(ctx context.Context, request *giftcardsv1.NormalizeGiftCardTransactionRequest) (*giftcardsv1.NormalizeGiftCardTransactionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizeGiftCardTransaction")
	return &giftcardsv1.NormalizeGiftCardTransactionResponse{}, nil
}

func newGiftCardTransactionValidationPort(deps platform.Deps) giftCardTransactionValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardTransactionValidationPort{log: deps.Logger}
	}
	return grpcGiftCardTransactionValidationPort{client: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn)}
}

// giftCardWorkflowPort is what the sellers domain needs of acme.giftcards.v1.GiftCardWorkflowService.
type giftCardWorkflowPort interface {
	RetryGiftCardWorkflow(ctx context.Context, request *giftcardsv1.RetryGiftCardWorkflowRequest) (*giftcardsv1.RetryGiftCardWorkflowResponse, error)
}

// grpcGiftCardWorkflowPort reaches acme.giftcards.v1.GiftCardWorkflowService over gRPC.
type grpcGiftCardWorkflowPort struct {
	client giftcardsv1.GiftCardWorkflowServiceClient
}

// sandboxGiftCardWorkflowPort answers in the sandbox environment, where acme.giftcards.v1.GiftCardWorkflowService is
// not deployed.
type sandboxGiftCardWorkflowPort struct{ log *slog.Logger }

func (p grpcGiftCardWorkflowPort) RetryGiftCardWorkflow(ctx context.Context, request *giftcardsv1.RetryGiftCardWorkflowRequest) (*giftcardsv1.RetryGiftCardWorkflowResponse, error) {
	return p.client.RetryGiftCardWorkflow(ctx, request)
}

func (p sandboxGiftCardWorkflowPort) RetryGiftCardWorkflow(ctx context.Context, request *giftcardsv1.RetryGiftCardWorkflowRequest) (*giftcardsv1.RetryGiftCardWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RetryGiftCardWorkflow")
	return &giftcardsv1.RetryGiftCardWorkflowResponse{}, nil
}

func newGiftCardWorkflowPort(deps platform.Deps) giftCardWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardWorkflowPort{log: deps.Logger}
	}
	return grpcGiftCardWorkflowPort{client: giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn)}
}

// roleHistoryPort is what the sellers domain needs of acme.identity.v1.RoleHistoryService.
type roleHistoryPort interface {
	AnnotateRoleVersion(ctx context.Context, request *identityv1.AnnotateRoleVersionRequest) (*identityv1.AnnotateRoleVersionResponse, error)
	RecordRoleVersion(ctx context.Context, request *identityv1.RecordRoleVersionRequest) (*identityv1.RecordRoleVersionResponse, error)
}

// grpcRoleHistoryPort reaches acme.identity.v1.RoleHistoryService over gRPC.
type grpcRoleHistoryPort struct {
	client identityv1.RoleHistoryServiceClient
}

// sandboxRoleHistoryPort answers in the sandbox environment, where acme.identity.v1.RoleHistoryService is not deployed.
type sandboxRoleHistoryPort struct{ log *slog.Logger }

func (p grpcRoleHistoryPort) AnnotateRoleVersion(ctx context.Context, request *identityv1.AnnotateRoleVersionRequest) (*identityv1.AnnotateRoleVersionResponse, error) {
	return p.client.AnnotateRoleVersion(ctx, request)
}

func (p sandboxRoleHistoryPort) AnnotateRoleVersion(ctx context.Context, request *identityv1.AnnotateRoleVersionRequest) (*identityv1.AnnotateRoleVersionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers AnnotateRoleVersion")
	return &identityv1.AnnotateRoleVersionResponse{}, nil
}

func (p grpcRoleHistoryPort) RecordRoleVersion(ctx context.Context, request *identityv1.RecordRoleVersionRequest) (*identityv1.RecordRoleVersionResponse, error) {
	return p.client.RecordRoleVersion(ctx, request)
}

func (p sandboxRoleHistoryPort) RecordRoleVersion(ctx context.Context, request *identityv1.RecordRoleVersionRequest) (*identityv1.RecordRoleVersionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecordRoleVersion")
	return &identityv1.RecordRoleVersionResponse{}, nil
}

func newRoleHistoryPort(deps platform.Deps) roleHistoryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRoleHistoryPort{log: deps.Logger}
	}
	return grpcRoleHistoryPort{client: identityv1.NewRoleHistoryServiceClient(deps.Conn)}
}

// sellerApprovalPort is what the sellers domain needs of acme.sellers.v1.SellerApprovalService.
type sellerApprovalPort interface {
	EscalateSeller(ctx context.Context, request *sellersv1.EscalateSellerRequest) (*sellersv1.EscalateSellerResponse, error)
}

// grpcSellerApprovalPort reaches acme.sellers.v1.SellerApprovalService over gRPC.
type grpcSellerApprovalPort struct {
	client sellersv1.SellerApprovalServiceClient
}

// sandboxSellerApprovalPort answers in the sandbox environment, where acme.sellers.v1.SellerApprovalService is not
// deployed.
type sandboxSellerApprovalPort struct{ log *slog.Logger }

func (p grpcSellerApprovalPort) EscalateSeller(ctx context.Context, request *sellersv1.EscalateSellerRequest) (*sellersv1.EscalateSellerResponse, error) {
	return p.client.EscalateSeller(ctx, request)
}

func (p sandboxSellerApprovalPort) EscalateSeller(ctx context.Context, request *sellersv1.EscalateSellerRequest) (*sellersv1.EscalateSellerResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EscalateSeller")
	return &sellersv1.EscalateSellerResponse{}, nil
}

func newSellerApprovalPort(deps platform.Deps) sellerApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSellerApprovalPort{log: deps.Logger}
	}
	return grpcSellerApprovalPort{client: sellersv1.NewSellerApprovalServiceClient(deps.Conn)}
}

// sellerPayoutScheduleReportingPort is what the sellers domain needs of
// acme.sellers.v1.SellerPayoutScheduleReportingService.
type sellerPayoutScheduleReportingPort interface {
	CountSellerPayoutScheduleTotals(ctx context.Context, request *sellersv1.CountSellerPayoutScheduleTotalsRequest) (*sellersv1.CountSellerPayoutScheduleTotalsResponse, error)
}

// grpcSellerPayoutScheduleReportingPort reaches acme.sellers.v1.SellerPayoutScheduleReportingService over gRPC.
type grpcSellerPayoutScheduleReportingPort struct {
	client sellersv1.SellerPayoutScheduleReportingServiceClient
}

// sandboxSellerPayoutScheduleReportingPort answers in the sandbox environment, where
// acme.sellers.v1.SellerPayoutScheduleReportingService is not deployed.
type sandboxSellerPayoutScheduleReportingPort struct{ log *slog.Logger }

func (p grpcSellerPayoutScheduleReportingPort) CountSellerPayoutScheduleTotals(ctx context.Context, request *sellersv1.CountSellerPayoutScheduleTotalsRequest) (*sellersv1.CountSellerPayoutScheduleTotalsResponse, error) {
	return p.client.CountSellerPayoutScheduleTotals(ctx, request)
}

func (p sandboxSellerPayoutScheduleReportingPort) CountSellerPayoutScheduleTotals(ctx context.Context, request *sellersv1.CountSellerPayoutScheduleTotalsRequest) (*sellersv1.CountSellerPayoutScheduleTotalsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CountSellerPayoutScheduleTotals")
	return &sellersv1.CountSellerPayoutScheduleTotalsResponse{}, nil
}

func newSellerPayoutScheduleReportingPort(deps platform.Deps) sellerPayoutScheduleReportingPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSellerPayoutScheduleReportingPort{log: deps.Logger}
	}
	return grpcSellerPayoutScheduleReportingPort{client: sellersv1.NewSellerPayoutScheduleReportingServiceClient(deps.Conn)}
}

// sellerPort is what the sellers domain needs of acme.sellers.v1.SellerService.
type sellerPort interface {
	BatchGetSellers(ctx context.Context, request *sellersv1.BatchGetSellersRequest) (*sellersv1.BatchGetSellersResponse, error)
}

// grpcSellerPort reaches acme.sellers.v1.SellerService over gRPC.
type grpcSellerPort struct{ client sellersv1.SellerServiceClient }

// sandboxSellerPort answers in the sandbox environment, where acme.sellers.v1.SellerService is not deployed.
type sandboxSellerPort struct{ log *slog.Logger }

func (p grpcSellerPort) BatchGetSellers(ctx context.Context, request *sellersv1.BatchGetSellersRequest) (*sellersv1.BatchGetSellersResponse, error) {
	return p.client.BatchGetSellers(ctx, request)
}

func (p sandboxSellerPort) BatchGetSellers(ctx context.Context, request *sellersv1.BatchGetSellersRequest) (*sellersv1.BatchGetSellersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers BatchGetSellers")
	return &sellersv1.BatchGetSellersResponse{}, nil
}

func newSellerPort(deps platform.Deps) sellerPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSellerPort{log: deps.Logger}
	}
	return grpcSellerPort{client: sellersv1.NewSellerServiceClient(deps.Conn)}
}

// sellerQueryPort is what the sellers domain needs of acme.sellers.v1.SellerQueryService.
type sellerQueryPort interface {
	SuggestSellers(ctx context.Context, request *sellersv1.SuggestSellersRequest) (*sellersv1.SuggestSellersResponse, error)
}

// grpcSellerQueryPort reaches acme.sellers.v1.SellerQueryService over gRPC.
type grpcSellerQueryPort struct {
	client sellersv1.SellerQueryServiceClient
}

// sandboxSellerQueryPort answers in the sandbox environment, where acme.sellers.v1.SellerQueryService is not deployed.
type sandboxSellerQueryPort struct{ log *slog.Logger }

func (p grpcSellerQueryPort) SuggestSellers(ctx context.Context, request *sellersv1.SuggestSellersRequest) (*sellersv1.SuggestSellersResponse, error) {
	return p.client.SuggestSellers(ctx, request)
}

func (p sandboxSellerQueryPort) SuggestSellers(ctx context.Context, request *sellersv1.SuggestSellersRequest) (*sellersv1.SuggestSellersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SuggestSellers")
	return &sellersv1.SuggestSellersResponse{}, nil
}

func newSellerQueryPort(deps platform.Deps) sellerQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSellerQueryPort{log: deps.Logger}
	}
	return grpcSellerQueryPort{client: sellersv1.NewSellerQueryServiceClient(deps.Conn)}
}

// sellerScorecardValidationPort is what the sellers domain needs of acme.sellers.v1.SellerScorecardValidationService.
type sellerScorecardValidationPort interface {
	PreviewSellerScorecard(ctx context.Context, request *sellersv1.PreviewSellerScorecardRequest) (*sellersv1.PreviewSellerScorecardResponse, error)
}

// grpcSellerScorecardValidationPort reaches acme.sellers.v1.SellerScorecardValidationService over gRPC.
type grpcSellerScorecardValidationPort struct {
	client sellersv1.SellerScorecardValidationServiceClient
}

// sandboxSellerScorecardValidationPort answers in the sandbox environment, where
// acme.sellers.v1.SellerScorecardValidationService is not deployed.
type sandboxSellerScorecardValidationPort struct{ log *slog.Logger }

func (p grpcSellerScorecardValidationPort) PreviewSellerScorecard(ctx context.Context, request *sellersv1.PreviewSellerScorecardRequest) (*sellersv1.PreviewSellerScorecardResponse, error) {
	return p.client.PreviewSellerScorecard(ctx, request)
}

func (p sandboxSellerScorecardValidationPort) PreviewSellerScorecard(ctx context.Context, request *sellersv1.PreviewSellerScorecardRequest) (*sellersv1.PreviewSellerScorecardResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewSellerScorecard")
	return &sellersv1.PreviewSellerScorecardResponse{}, nil
}

func newSellerScorecardValidationPort(deps platform.Deps) sellerScorecardValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSellerScorecardValidationPort{log: deps.Logger}
	}
	return grpcSellerScorecardValidationPort{client: sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn)}
}

// sellerValidationPort is what the sellers domain needs of acme.sellers.v1.SellerValidationService.
type sellerValidationPort interface {
	ExplainSellerDecision(ctx context.Context, request *sellersv1.ExplainSellerDecisionRequest) (*sellersv1.ExplainSellerDecisionResponse, error)
}

// grpcSellerValidationPort reaches acme.sellers.v1.SellerValidationService over gRPC.
type grpcSellerValidationPort struct {
	client sellersv1.SellerValidationServiceClient
}

// sandboxSellerValidationPort answers in the sandbox environment, where acme.sellers.v1.SellerValidationService is not
// deployed.
type sandboxSellerValidationPort struct{ log *slog.Logger }

func (p grpcSellerValidationPort) ExplainSellerDecision(ctx context.Context, request *sellersv1.ExplainSellerDecisionRequest) (*sellersv1.ExplainSellerDecisionResponse, error) {
	return p.client.ExplainSellerDecision(ctx, request)
}

func (p sandboxSellerValidationPort) ExplainSellerDecision(ctx context.Context, request *sellersv1.ExplainSellerDecisionRequest) (*sellersv1.ExplainSellerDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainSellerDecision")
	return &sellersv1.ExplainSellerDecisionResponse{}, nil
}

func newSellerValidationPort(deps platform.Deps) sellerValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSellerValidationPort{log: deps.Logger}
	}
	return grpcSellerValidationPort{client: sellersv1.NewSellerValidationServiceClient(deps.Conn)}
}

// synonymExportPort is what the sellers domain needs of acme.search.v1.SynonymExportService.
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
