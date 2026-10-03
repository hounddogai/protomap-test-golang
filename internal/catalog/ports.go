package catalog

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// auditEventQueryPort is what the catalog domain needs of acme.audit.v1.AuditEventQueryService.
type auditEventQueryPort interface {
	BrowseAuditEvents(ctx context.Context, request *auditv1.BrowseAuditEventsRequest) (*auditv1.BrowseAuditEventsResponse, error)
}

// grpcAuditEventQueryPort reaches acme.audit.v1.AuditEventQueryService over gRPC.
type grpcAuditEventQueryPort struct {
	client auditv1.AuditEventQueryServiceClient
}

// sandboxAuditEventQueryPort answers in the sandbox environment, where acme.audit.v1.AuditEventQueryService is not
// deployed.
type sandboxAuditEventQueryPort struct{ log *slog.Logger }

func (p grpcAuditEventQueryPort) BrowseAuditEvents(ctx context.Context, request *auditv1.BrowseAuditEventsRequest) (*auditv1.BrowseAuditEventsResponse, error) {
	return p.client.BrowseAuditEvents(ctx, request)
}

func (p sandboxAuditEventQueryPort) BrowseAuditEvents(ctx context.Context, request *auditv1.BrowseAuditEventsRequest) (*auditv1.BrowseAuditEventsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers BrowseAuditEvents")
	return &auditv1.BrowseAuditEventsResponse{}, nil
}

func newAuditEventQueryPort(deps platform.Deps) auditEventQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxAuditEventQueryPort{log: deps.Logger}
	}
	return grpcAuditEventQueryPort{client: auditv1.NewAuditEventQueryServiceClient(deps.Conn)}
}

// brandLifecyclePort is what the catalog domain needs of acme.catalog.v1.BrandLifecycleService.
type brandLifecyclePort interface {
	ExpireBrand(ctx context.Context, request *catalogv1.ExpireBrandRequest) (*catalogv1.ExpireBrandResponse, error)
}

// grpcBrandLifecyclePort reaches acme.catalog.v1.BrandLifecycleService over gRPC.
type grpcBrandLifecyclePort struct {
	client catalogv1.BrandLifecycleServiceClient
}

// sandboxBrandLifecyclePort answers in the sandbox environment, where acme.catalog.v1.BrandLifecycleService is not
// deployed.
type sandboxBrandLifecyclePort struct{ log *slog.Logger }

func (p grpcBrandLifecyclePort) ExpireBrand(ctx context.Context, request *catalogv1.ExpireBrandRequest) (*catalogv1.ExpireBrandResponse, error) {
	return p.client.ExpireBrand(ctx, request)
}

func (p sandboxBrandLifecyclePort) ExpireBrand(ctx context.Context, request *catalogv1.ExpireBrandRequest) (*catalogv1.ExpireBrandResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExpireBrand")
	return &catalogv1.ExpireBrandResponse{}, nil
}

func newBrandLifecyclePort(deps platform.Deps) brandLifecyclePort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxBrandLifecyclePort{log: deps.Logger}
	}
	return grpcBrandLifecyclePort{client: catalogv1.NewBrandLifecycleServiceClient(deps.Conn)}
}

// brandValidationPort is what the catalog domain needs of acme.catalog.v1.BrandValidationService.
type brandValidationPort interface {
	LintBrands(ctx context.Context, request *catalogv1.LintBrandsRequest) (*catalogv1.LintBrandsResponse, error)
}

// grpcBrandValidationPort reaches acme.catalog.v1.BrandValidationService over gRPC.
type grpcBrandValidationPort struct {
	client catalogv1.BrandValidationServiceClient
}

// sandboxBrandValidationPort answers in the sandbox environment, where acme.catalog.v1.BrandValidationService is not
// deployed.
type sandboxBrandValidationPort struct{ log *slog.Logger }

func (p grpcBrandValidationPort) LintBrands(ctx context.Context, request *catalogv1.LintBrandsRequest) (*catalogv1.LintBrandsResponse, error) {
	return p.client.LintBrands(ctx, request)
}

func (p sandboxBrandValidationPort) LintBrands(ctx context.Context, request *catalogv1.LintBrandsRequest) (*catalogv1.LintBrandsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers LintBrands")
	return &catalogv1.LintBrandsResponse{}, nil
}

func newBrandValidationPort(deps platform.Deps) brandValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxBrandValidationPort{log: deps.Logger}
	}
	return grpcBrandValidationPort{client: catalogv1.NewBrandValidationServiceClient(deps.Conn)}
}

// cartRuleValidationPort is what the catalog domain needs of acme.cart.v1.CartRuleValidationService.
type cartRuleValidationPort interface {
	NormalizeCartRule(ctx context.Context, request *cartv1.NormalizeCartRuleRequest) (*cartv1.NormalizeCartRuleResponse, error)
}

// grpcCartRuleValidationPort reaches acme.cart.v1.CartRuleValidationService over gRPC.
type grpcCartRuleValidationPort struct {
	client cartv1.CartRuleValidationServiceClient
}

// sandboxCartRuleValidationPort answers in the sandbox environment, where acme.cart.v1.CartRuleValidationService is not
// deployed.
type sandboxCartRuleValidationPort struct{ log *slog.Logger }

func (p grpcCartRuleValidationPort) NormalizeCartRule(ctx context.Context, request *cartv1.NormalizeCartRuleRequest) (*cartv1.NormalizeCartRuleResponse, error) {
	return p.client.NormalizeCartRule(ctx, request)
}

func (p sandboxCartRuleValidationPort) NormalizeCartRule(ctx context.Context, request *cartv1.NormalizeCartRuleRequest) (*cartv1.NormalizeCartRuleResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizeCartRule")
	return &cartv1.NormalizeCartRuleResponse{}, nil
}

func newCartRuleValidationPort(deps platform.Deps) cartRuleValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCartRuleValidationPort{log: deps.Logger}
	}
	return grpcCartRuleValidationPort{client: cartv1.NewCartRuleValidationServiceClient(deps.Conn)}
}

// fulfillmentWorkflowPort is what the catalog domain needs of acme.fulfillment.v1.FulfillmentWorkflowService.
type fulfillmentWorkflowPort interface {
	CancelFulfillmentWorkflow(ctx context.Context, request *fulfillmentv1.CancelFulfillmentWorkflowRequest) (*fulfillmentv1.CancelFulfillmentWorkflowResponse, error)
}

// grpcFulfillmentWorkflowPort reaches acme.fulfillment.v1.FulfillmentWorkflowService over gRPC.
type grpcFulfillmentWorkflowPort struct {
	client fulfillmentv1.FulfillmentWorkflowServiceClient
}

// sandboxFulfillmentWorkflowPort answers in the sandbox environment, where
// acme.fulfillment.v1.FulfillmentWorkflowService is not deployed.
type sandboxFulfillmentWorkflowPort struct{ log *slog.Logger }

func (p grpcFulfillmentWorkflowPort) CancelFulfillmentWorkflow(ctx context.Context, request *fulfillmentv1.CancelFulfillmentWorkflowRequest) (*fulfillmentv1.CancelFulfillmentWorkflowResponse, error) {
	return p.client.CancelFulfillmentWorkflow(ctx, request)
}

func (p sandboxFulfillmentWorkflowPort) CancelFulfillmentWorkflow(ctx context.Context, request *fulfillmentv1.CancelFulfillmentWorkflowRequest) (*fulfillmentv1.CancelFulfillmentWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CancelFulfillmentWorkflow")
	return &fulfillmentv1.CancelFulfillmentWorkflowResponse{}, nil
}

func newFulfillmentWorkflowPort(deps platform.Deps) fulfillmentWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFulfillmentWorkflowPort{log: deps.Logger}
	}
	return grpcFulfillmentWorkflowPort{client: fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn)}
}

// giftCardBatchCalculationPort is what the catalog domain needs of acme.giftcards.v1.GiftCardBatchCalculationService.
type giftCardBatchCalculationPort interface {
	CalculateGiftCardBatch(ctx context.Context, request *giftcardsv1.CalculateGiftCardBatchRequest) (*giftcardsv1.CalculateGiftCardBatchResponse, error)
}

// grpcGiftCardBatchCalculationPort reaches acme.giftcards.v1.GiftCardBatchCalculationService over gRPC.
type grpcGiftCardBatchCalculationPort struct {
	client giftcardsv1.GiftCardBatchCalculationServiceClient
}

// sandboxGiftCardBatchCalculationPort answers in the sandbox environment, where
// acme.giftcards.v1.GiftCardBatchCalculationService is not deployed.
type sandboxGiftCardBatchCalculationPort struct{ log *slog.Logger }

func (p grpcGiftCardBatchCalculationPort) CalculateGiftCardBatch(ctx context.Context, request *giftcardsv1.CalculateGiftCardBatchRequest) (*giftcardsv1.CalculateGiftCardBatchResponse, error) {
	return p.client.CalculateGiftCardBatch(ctx, request)
}

func (p sandboxGiftCardBatchCalculationPort) CalculateGiftCardBatch(ctx context.Context, request *giftcardsv1.CalculateGiftCardBatchRequest) (*giftcardsv1.CalculateGiftCardBatchResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateGiftCardBatch")
	return &giftcardsv1.CalculateGiftCardBatchResponse{}, nil
}

func newGiftCardBatchCalculationPort(deps platform.Deps) giftCardBatchCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardBatchCalculationPort{log: deps.Logger}
	}
	return grpcGiftCardBatchCalculationPort{client: giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn)}
}

// paymentInstrumentApprovalPort is what the catalog domain needs of acme.payments.v1.PaymentInstrumentApprovalService.
type paymentInstrumentApprovalPort interface {
	RecallPaymentInstrument(ctx context.Context, request *paymentsv1.RecallPaymentInstrumentRequest) (*paymentsv1.RecallPaymentInstrumentResponse, error)
}

// grpcPaymentInstrumentApprovalPort reaches acme.payments.v1.PaymentInstrumentApprovalService over gRPC.
type grpcPaymentInstrumentApprovalPort struct {
	client paymentsv1.PaymentInstrumentApprovalServiceClient
}

// sandboxPaymentInstrumentApprovalPort answers in the sandbox environment, where
// acme.payments.v1.PaymentInstrumentApprovalService is not deployed.
type sandboxPaymentInstrumentApprovalPort struct{ log *slog.Logger }

func (p grpcPaymentInstrumentApprovalPort) RecallPaymentInstrument(ctx context.Context, request *paymentsv1.RecallPaymentInstrumentRequest) (*paymentsv1.RecallPaymentInstrumentResponse, error) {
	return p.client.RecallPaymentInstrument(ctx, request)
}

func (p sandboxPaymentInstrumentApprovalPort) RecallPaymentInstrument(ctx context.Context, request *paymentsv1.RecallPaymentInstrumentRequest) (*paymentsv1.RecallPaymentInstrumentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecallPaymentInstrument")
	return &paymentsv1.RecallPaymentInstrumentResponse{}, nil
}

func newPaymentInstrumentApprovalPort(deps platform.Deps) paymentInstrumentApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPaymentInstrumentApprovalPort{log: deps.Logger}
	}
	return grpcPaymentInstrumentApprovalPort{client: paymentsv1.NewPaymentInstrumentApprovalServiceClient(deps.Conn)}
}

// paymentInstrumentLifecyclePort is what the catalog domain needs of
// acme.payments.v1.PaymentInstrumentLifecycleService.
type paymentInstrumentLifecyclePort interface {
	SchedulePaymentInstrumentExpiry(ctx context.Context, request *paymentsv1.SchedulePaymentInstrumentExpiryRequest) (*paymentsv1.SchedulePaymentInstrumentExpiryResponse, error)
}

// grpcPaymentInstrumentLifecyclePort reaches acme.payments.v1.PaymentInstrumentLifecycleService over gRPC.
type grpcPaymentInstrumentLifecyclePort struct {
	client paymentsv1.PaymentInstrumentLifecycleServiceClient
}

// sandboxPaymentInstrumentLifecyclePort answers in the sandbox environment, where
// acme.payments.v1.PaymentInstrumentLifecycleService is not deployed.
type sandboxPaymentInstrumentLifecyclePort struct{ log *slog.Logger }

func (p grpcPaymentInstrumentLifecyclePort) SchedulePaymentInstrumentExpiry(ctx context.Context, request *paymentsv1.SchedulePaymentInstrumentExpiryRequest) (*paymentsv1.SchedulePaymentInstrumentExpiryResponse, error) {
	return p.client.SchedulePaymentInstrumentExpiry(ctx, request)
}

func (p sandboxPaymentInstrumentLifecyclePort) SchedulePaymentInstrumentExpiry(ctx context.Context, request *paymentsv1.SchedulePaymentInstrumentExpiryRequest) (*paymentsv1.SchedulePaymentInstrumentExpiryResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SchedulePaymentInstrumentExpiry")
	return &paymentsv1.SchedulePaymentInstrumentExpiryResponse{}, nil
}

func newPaymentInstrumentLifecyclePort(deps platform.Deps) paymentInstrumentLifecyclePort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPaymentInstrumentLifecyclePort{log: deps.Logger}
	}
	return grpcPaymentInstrumentLifecyclePort{client: paymentsv1.NewPaymentInstrumentLifecycleServiceClient(deps.Conn)}
}

// productApprovalPort is what the catalog domain needs of acme.catalog.v1.ProductApprovalService.
type productApprovalPort interface {
	RecallProduct(ctx context.Context, request *catalogv1.RecallProductRequest) (*catalogv1.RecallProductResponse, error)
}

// grpcProductApprovalPort reaches acme.catalog.v1.ProductApprovalService over gRPC.
type grpcProductApprovalPort struct {
	client catalogv1.ProductApprovalServiceClient
}

// sandboxProductApprovalPort answers in the sandbox environment, where acme.catalog.v1.ProductApprovalService is not
// deployed.
type sandboxProductApprovalPort struct{ log *slog.Logger }

func (p grpcProductApprovalPort) RecallProduct(ctx context.Context, request *catalogv1.RecallProductRequest) (*catalogv1.RecallProductResponse, error) {
	return p.client.RecallProduct(ctx, request)
}

func (p sandboxProductApprovalPort) RecallProduct(ctx context.Context, request *catalogv1.RecallProductRequest) (*catalogv1.RecallProductResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecallProduct")
	return &catalogv1.RecallProductResponse{}, nil
}

func newProductApprovalPort(deps platform.Deps) productApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxProductApprovalPort{log: deps.Logger}
	}
	return grpcProductApprovalPort{client: catalogv1.NewProductApprovalServiceClient(deps.Conn)}
}

// productCalculationPort is what the catalog domain needs of acme.catalog.v1.ProductCalculationService.
type productCalculationPort interface {
	EstimateProduct(ctx context.Context, request *catalogv1.EstimateProductRequest) (*catalogv1.EstimateProductResponse, error)
	QuoteProduct(ctx context.Context, request *catalogv1.QuoteProductRequest) (*catalogv1.QuoteProductResponse, error)
	SimulateProduct(ctx context.Context, request *catalogv1.SimulateProductRequest) (*catalogv1.SimulateProductResponse, error)
}

// grpcProductCalculationPort reaches acme.catalog.v1.ProductCalculationService over gRPC.
type grpcProductCalculationPort struct {
	client catalogv1.ProductCalculationServiceClient
}

// sandboxProductCalculationPort answers in the sandbox environment, where acme.catalog.v1.ProductCalculationService is
// not deployed.
type sandboxProductCalculationPort struct{ log *slog.Logger }

func (p grpcProductCalculationPort) EstimateProduct(ctx context.Context, request *catalogv1.EstimateProductRequest) (*catalogv1.EstimateProductResponse, error) {
	return p.client.EstimateProduct(ctx, request)
}

func (p sandboxProductCalculationPort) EstimateProduct(ctx context.Context, request *catalogv1.EstimateProductRequest) (*catalogv1.EstimateProductResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateProduct")
	return &catalogv1.EstimateProductResponse{}, nil
}

func (p grpcProductCalculationPort) QuoteProduct(ctx context.Context, request *catalogv1.QuoteProductRequest) (*catalogv1.QuoteProductResponse, error) {
	return p.client.QuoteProduct(ctx, request)
}

func (p sandboxProductCalculationPort) QuoteProduct(ctx context.Context, request *catalogv1.QuoteProductRequest) (*catalogv1.QuoteProductResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers QuoteProduct")
	return &catalogv1.QuoteProductResponse{}, nil
}

func (p grpcProductCalculationPort) SimulateProduct(ctx context.Context, request *catalogv1.SimulateProductRequest) (*catalogv1.SimulateProductResponse, error) {
	return p.client.SimulateProduct(ctx, request)
}

func (p sandboxProductCalculationPort) SimulateProduct(ctx context.Context, request *catalogv1.SimulateProductRequest) (*catalogv1.SimulateProductResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SimulateProduct")
	return &catalogv1.SimulateProductResponse{}, nil
}

func newProductCalculationPort(deps platform.Deps) productCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxProductCalculationPort{log: deps.Logger}
	}
	return grpcProductCalculationPort{client: catalogv1.NewProductCalculationServiceClient(deps.Conn)}
}

// productValidationPort is what the catalog domain needs of acme.catalog.v1.ProductValidationService.
type productValidationPort interface {
	ValidateProduct(ctx context.Context, request *catalogv1.ValidateProductRequest) (*catalogv1.ValidateProductResponse, error)
}

// grpcProductValidationPort reaches acme.catalog.v1.ProductValidationService over gRPC.
type grpcProductValidationPort struct {
	client catalogv1.ProductValidationServiceClient
}

// sandboxProductValidationPort answers in the sandbox environment, where acme.catalog.v1.ProductValidationService is
// not deployed.
type sandboxProductValidationPort struct{ log *slog.Logger }

func (p grpcProductValidationPort) ValidateProduct(ctx context.Context, request *catalogv1.ValidateProductRequest) (*catalogv1.ValidateProductResponse, error) {
	return p.client.ValidateProduct(ctx, request)
}

func (p sandboxProductValidationPort) ValidateProduct(ctx context.Context, request *catalogv1.ValidateProductRequest) (*catalogv1.ValidateProductResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidateProduct")
	return &catalogv1.ValidateProductResponse{}, nil
}

func newProductValidationPort(deps platform.Deps) productValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxProductValidationPort{log: deps.Logger}
	}
	return grpcProductValidationPort{client: catalogv1.NewProductValidationServiceClient(deps.Conn)}
}

// productVariantCalculationPort is what the catalog domain needs of acme.catalog.v1.ProductVariantCalculationService.
type productVariantCalculationPort interface {
	EstimateProductVariant(ctx context.Context, request *catalogv1.EstimateProductVariantRequest) (*catalogv1.EstimateProductVariantResponse, error)
}

// grpcProductVariantCalculationPort reaches acme.catalog.v1.ProductVariantCalculationService over gRPC.
type grpcProductVariantCalculationPort struct {
	client catalogv1.ProductVariantCalculationServiceClient
}

// sandboxProductVariantCalculationPort answers in the sandbox environment, where
// acme.catalog.v1.ProductVariantCalculationService is not deployed.
type sandboxProductVariantCalculationPort struct{ log *slog.Logger }

func (p grpcProductVariantCalculationPort) EstimateProductVariant(ctx context.Context, request *catalogv1.EstimateProductVariantRequest) (*catalogv1.EstimateProductVariantResponse, error) {
	return p.client.EstimateProductVariant(ctx, request)
}

func (p sandboxProductVariantCalculationPort) EstimateProductVariant(ctx context.Context, request *catalogv1.EstimateProductVariantRequest) (*catalogv1.EstimateProductVariantResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateProductVariant")
	return &catalogv1.EstimateProductVariantResponse{}, nil
}

func newProductVariantCalculationPort(deps platform.Deps) productVariantCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxProductVariantCalculationPort{log: deps.Logger}
	}
	return grpcProductVariantCalculationPort{client: catalogv1.NewProductVariantCalculationServiceClient(deps.Conn)}
}

// productVariantValidationPort is what the catalog domain needs of acme.catalog.v1.ProductVariantValidationService.
type productVariantValidationPort interface {
	LintProductVariants(ctx context.Context, request *catalogv1.LintProductVariantsRequest) (*catalogv1.LintProductVariantsResponse, error)
	PreviewProductVariant(ctx context.Context, request *catalogv1.PreviewProductVariantRequest) (*catalogv1.PreviewProductVariantResponse, error)
}

// grpcProductVariantValidationPort reaches acme.catalog.v1.ProductVariantValidationService over gRPC.
type grpcProductVariantValidationPort struct {
	client catalogv1.ProductVariantValidationServiceClient
}

// sandboxProductVariantValidationPort answers in the sandbox environment, where
// acme.catalog.v1.ProductVariantValidationService is not deployed.
type sandboxProductVariantValidationPort struct{ log *slog.Logger }

func (p grpcProductVariantValidationPort) LintProductVariants(ctx context.Context, request *catalogv1.LintProductVariantsRequest) (*catalogv1.LintProductVariantsResponse, error) {
	return p.client.LintProductVariants(ctx, request)
}

func (p sandboxProductVariantValidationPort) LintProductVariants(ctx context.Context, request *catalogv1.LintProductVariantsRequest) (*catalogv1.LintProductVariantsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers LintProductVariants")
	return &catalogv1.LintProductVariantsResponse{}, nil
}

func (p grpcProductVariantValidationPort) PreviewProductVariant(ctx context.Context, request *catalogv1.PreviewProductVariantRequest) (*catalogv1.PreviewProductVariantResponse, error) {
	return p.client.PreviewProductVariant(ctx, request)
}

func (p sandboxProductVariantValidationPort) PreviewProductVariant(ctx context.Context, request *catalogv1.PreviewProductVariantRequest) (*catalogv1.PreviewProductVariantResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewProductVariant")
	return &catalogv1.PreviewProductVariantResponse{}, nil
}

func newProductVariantValidationPort(deps platform.Deps) productVariantValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxProductVariantValidationPort{log: deps.Logger}
	}
	return grpcProductVariantValidationPort{client: catalogv1.NewProductVariantValidationServiceClient(deps.Conn)}
}

// searchIndexQueryPort is what the catalog domain needs of acme.search.v1.SearchIndexQueryService.
type searchIndexQueryPort interface {
	SuggestSearchIndexes(ctx context.Context, request *searchv1.SuggestSearchIndexesRequest) (*searchv1.SuggestSearchIndexesResponse, error)
}

// grpcSearchIndexQueryPort reaches acme.search.v1.SearchIndexQueryService over gRPC.
type grpcSearchIndexQueryPort struct {
	client searchv1.SearchIndexQueryServiceClient
}

// sandboxSearchIndexQueryPort answers in the sandbox environment, where acme.search.v1.SearchIndexQueryService is not
// deployed.
type sandboxSearchIndexQueryPort struct{ log *slog.Logger }

func (p grpcSearchIndexQueryPort) SuggestSearchIndexes(ctx context.Context, request *searchv1.SuggestSearchIndexesRequest) (*searchv1.SuggestSearchIndexesResponse, error) {
	return p.client.SuggestSearchIndexes(ctx, request)
}

func (p sandboxSearchIndexQueryPort) SuggestSearchIndexes(ctx context.Context, request *searchv1.SuggestSearchIndexesRequest) (*searchv1.SuggestSearchIndexesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SuggestSearchIndexes")
	return &searchv1.SuggestSearchIndexesResponse{}, nil
}

func newSearchIndexQueryPort(deps platform.Deps) searchIndexQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSearchIndexQueryPort{log: deps.Logger}
	}
	return grpcSearchIndexQueryPort{client: searchv1.NewSearchIndexQueryServiceClient(deps.Conn)}
}

// sessionValidationPort is what the catalog domain needs of acme.identity.v1.SessionValidationService.
type sessionValidationPort interface {
	ScoreSession(ctx context.Context, request *identityv1.ScoreSessionRequest) (*identityv1.ScoreSessionResponse, error)
}

// grpcSessionValidationPort reaches acme.identity.v1.SessionValidationService over gRPC.
type grpcSessionValidationPort struct {
	client identityv1.SessionValidationServiceClient
}

// sandboxSessionValidationPort answers in the sandbox environment, where acme.identity.v1.SessionValidationService is
// not deployed.
type sandboxSessionValidationPort struct{ log *slog.Logger }

func (p grpcSessionValidationPort) ScoreSession(ctx context.Context, request *identityv1.ScoreSessionRequest) (*identityv1.ScoreSessionResponse, error) {
	return p.client.ScoreSession(ctx, request)
}

func (p sandboxSessionValidationPort) ScoreSession(ctx context.Context, request *identityv1.ScoreSessionRequest) (*identityv1.ScoreSessionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScoreSession")
	return &identityv1.ScoreSessionResponse{}, nil
}

func newSessionValidationPort(deps platform.Deps) sessionValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSessionValidationPort{log: deps.Logger}
	}
	return grpcSessionValidationPort{client: identityv1.NewSessionValidationServiceClient(deps.Conn)}
}

// storageBinWorkflowPort is what the catalog domain needs of acme.warehousing.v1.StorageBinWorkflowService.
type storageBinWorkflowPort interface {
	StartStorageBinWorkflow(ctx context.Context, request *warehousingv1.StartStorageBinWorkflowRequest) (*warehousingv1.StartStorageBinWorkflowResponse, error)
}

// grpcStorageBinWorkflowPort reaches acme.warehousing.v1.StorageBinWorkflowService over gRPC.
type grpcStorageBinWorkflowPort struct {
	client warehousingv1.StorageBinWorkflowServiceClient
}

// sandboxStorageBinWorkflowPort answers in the sandbox environment, where acme.warehousing.v1.StorageBinWorkflowService
// is not deployed.
type sandboxStorageBinWorkflowPort struct{ log *slog.Logger }

func (p grpcStorageBinWorkflowPort) StartStorageBinWorkflow(ctx context.Context, request *warehousingv1.StartStorageBinWorkflowRequest) (*warehousingv1.StartStorageBinWorkflowResponse, error) {
	return p.client.StartStorageBinWorkflow(ctx, request)
}

func (p sandboxStorageBinWorkflowPort) StartStorageBinWorkflow(ctx context.Context, request *warehousingv1.StartStorageBinWorkflowRequest) (*warehousingv1.StartStorageBinWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers StartStorageBinWorkflow")
	return &warehousingv1.StartStorageBinWorkflowResponse{}, nil
}

func newStorageBinWorkflowPort(deps platform.Deps) storageBinWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxStorageBinWorkflowPort{log: deps.Logger}
	}
	return grpcStorageBinWorkflowPort{client: warehousingv1.NewStorageBinWorkflowServiceClient(deps.Conn)}
}
