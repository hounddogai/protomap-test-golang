package catalog

import (
	"context"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	showsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shows/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the catalog domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.cart.v1.CartRuleValidationService", cartv1.NewCartRuleValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.catalog.v1.AttributeSyncService", catalogv1.NewAttributeSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.catalog.v1.ProductVariantReportingService", catalogv1.NewProductVariantReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.catalog.v1.ProductVariantValidationService", catalogv1.NewProductVariantValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.featureflags.v1.FeatureFlagQueryService", featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.fulfillment.v1.PackingSlipApprovalService", fulfillmentv1.NewPackingSlipApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardQueryService", giftcardsv1.NewGiftCardQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.ledger.v1.LedgerAccountValidationService", ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.PointsLedgerApprovalService", loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.onboarding.v1.OnboardingPlanCalculationService", onboardingv1.NewOnboardingPlanCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.promotions.v1.CouponValidationService", promotionsv1.NewCouponValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.returns.v1.RestockDecisionApprovalService", returnsv1.NewRestockDecisionApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.ManifestValidationService", shippingv1.NewManifestValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.shows.v1.ShowCalculationService", showsv1.NewShowCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.SubscriptionApprovalService", subscriptionsv1.NewSubscriptionApprovalServiceClient(deps.Conn))
	catalogv1.RegisterProductServiceServer(server, NewProductServer(deps))
	catalogv1.RegisterCategoryQueryServiceServer(server, NewCategoryQueryServer(deps))
	catalogv1.RegisterProductVariantWorkflowServiceServer(server, NewProductVariantWorkflowServer(deps))
	catalogv1.RegisterBrandApprovalServiceServer(server, NewBrandApprovalServer(deps))
	catalogv1.RegisterAttributeValidationServiceServer(server, NewAttributeValidationServer(deps))
	catalogv1.RegisterProductQueryServiceServer(server, NewProductQueryServer(deps))
	catalogv1.RegisterCategoryWorkflowServiceServer(server, NewCategoryWorkflowServer(deps))
	catalogv1.RegisterProductVariantApprovalServiceServer(server, NewProductVariantApprovalServer(deps))
	catalogv1.RegisterBrandValidationServiceServer(server, NewBrandValidationServer(deps))
	catalogv1.RegisterAttributeCalculationServiceServer(server, NewAttributeCalculationServer(deps))
	catalogv1.RegisterProductWorkflowServiceServer(server, NewProductWorkflowServer(deps))
	categoryApprovalServer := NewCategoryApprovalServer(deps)
	server.RegisterService(&catalogv1.CategoryApprovalService_ServiceDesc, categoryApprovalServer)
	catalogv1.RegisterProductVariantValidationServiceServer(server, NewProductVariantValidationServer(deps))
	catalogv1.RegisterBrandCalculationServiceServer(server, NewBrandCalculationServer(deps))
	catalogv1.RegisterAttributeLifecycleServiceServer(server, NewAttributeLifecycleServer(deps))
	productApprovalServer := NewProductApprovalServer(deps)
	server.RegisterService(&catalogv1.ProductApprovalService_ServiceDesc, productApprovalServer)
	catalogv1.RegisterCategoryValidationServiceServer(server, NewCategoryValidationServer(deps))
	catalogv1.RegisterProductVariantCalculationServiceServer(server, NewProductVariantCalculationServer(deps))
	catalogv1.RegisterBrandLifecycleServiceServer(server, NewBrandLifecycleServer(deps))
	catalogv1.RegisterAttributeReportingServiceServer(server, NewAttributeReportingServer(deps))
	productValidationServer := NewProductValidationServer(deps)
	server.RegisterService(&catalogv1.ProductValidationService_ServiceDesc, productValidationServer)
	catalogv1.RegisterCategoryCalculationServiceServer(server, NewCategoryCalculationServer(deps))
	productVariantLifecycleServer := NewProductVariantLifecycleServer(deps)
	server.RegisterService(&catalogv1.ProductVariantLifecycleService_ServiceDesc, productVariantLifecycleServer)
	catalogv1.RegisterBrandReportingServiceServer(server, NewBrandReportingServer(deps))
	catalogv1.RegisterAttributeSyncServiceServer(server, NewAttributeSyncServer(deps))
	catalogv1.RegisterProductCalculationServiceServer(server, NewProductCalculationServer(deps))
	catalogv1.RegisterCategoryLifecycleServiceServer(server, NewCategoryLifecycleServer(deps))
	catalogv1.RegisterProductVariantReportingServiceServer(server, NewProductVariantReportingServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	previewRenewalScheduleCall          func(context.Context, *subscriptionsv1.PreviewRenewalScheduleRequest) (*subscriptionsv1.PreviewRenewalScheduleResponse, error)
	renewProductVariantCall             func(context.Context, *catalogv1.RenewProductVariantRequest, ...grpc.CallOption) (*catalogv1.RenewProductVariantResponse, error)
	previewSynonymCall                  func(context.Context, *searchv1.PreviewSynonymRequest, ...grpc.CallOption) (*searchv1.PreviewSynonymResponse, error)
	countBrandTotalsCall                func(context.Context, *catalogv1.CountBrandTotalsRequest) (*catalogv1.CountBrandTotalsResponse, error)
	startPickListWorkflowCall           func(context.Context, *warehousingv1.StartPickListWorkflowRequest, ...grpc.CallOption) (*warehousingv1.StartPickListWorkflowResponse, error)
	calculateLoyaltyMemberCall          func(context.Context, *loyaltyv1.CalculateLoyaltyMemberRequest) (*loyaltyv1.CalculateLoyaltyMemberResponse, error)
	rejectPayoutCall                    func(context.Context, *paymentsv1.RejectPayoutRequest) (*paymentsv1.RejectPayoutResponse, error)
	lintProductsCall                    func(context.Context, *catalogv1.LintProductsRequest) (*catalogv1.LintProductsResponse, error)
	previewMetricSeriesCall             func(context.Context, *metricsv1.PreviewMetricSeriesRequest, ...grpc.CallOption) (*metricsv1.PreviewMetricSeriesResponse, error)
	calculateCategoryCall               func(context.Context, *catalogv1.CalculateCategoryRequest, ...grpc.CallOption) (*catalogv1.CalculateCategoryResponse, error)
	quoteCategoryCall                   func(context.Context, *catalogv1.QuoteCategoryRequest) (*catalogv1.QuoteCategoryResponse, error)
	simulateProductCall                 func(context.Context, *catalogv1.SimulateProductRequest) (*catalogv1.SimulateProductResponse, error)
	lockTenantCall                      func(context.Context, *identityv1.LockTenantRequest, ...grpc.CallOption) (*identityv1.LockTenantResponse, error)
	signalPaymentInstrumentWorkflowCall func(context.Context, *paymentsv1.SignalPaymentInstrumentWorkflowRequest) (*paymentsv1.SignalPaymentInstrumentWorkflowResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	renewalScheduleValidationForPreviewRenewalSchedule := subscriptionsv1.NewRenewalScheduleValidationServiceClient(deps.Conn)
	previewRenewalScheduleCall = func(ctx context.Context, request *subscriptionsv1.PreviewRenewalScheduleRequest) (*subscriptionsv1.PreviewRenewalScheduleResponse, error) {
		return renewalScheduleValidationForPreviewRenewalSchedule.PreviewRenewalSchedule(ctx, request)
	}
	productVariantLifecycleForRenewProductVariant := catalogv1.NewProductVariantLifecycleServiceClient(deps.Conn)
	renewProductVariantCall = productVariantLifecycleForRenewProductVariant.RenewProductVariant
	synonymValidationForPreviewSynonym := searchv1.NewSynonymValidationServiceClient(deps.Conn)
	previewSynonymCall = synonymValidationForPreviewSynonym.PreviewSynonym
	brandReportingForCountBrandTotals := catalogv1.NewBrandReportingServiceClient(deps.Conn)
	countBrandTotalsCall = func(ctx context.Context, request *catalogv1.CountBrandTotalsRequest) (*catalogv1.CountBrandTotalsResponse, error) {
		return brandReportingForCountBrandTotals.CountBrandTotals(ctx, request)
	}
	pickListWorkflowForStartPickListWorkflow := warehousingv1.NewPickListWorkflowServiceClient(deps.Conn)
	startPickListWorkflowCall = pickListWorkflowForStartPickListWorkflow.StartPickListWorkflow
	loyaltyMemberCalculationForCalculateLoyaltyMember := loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn)
	calculateLoyaltyMemberCall = func(ctx context.Context, request *loyaltyv1.CalculateLoyaltyMemberRequest) (*loyaltyv1.CalculateLoyaltyMemberResponse, error) {
		return loyaltyMemberCalculationForCalculateLoyaltyMember.CalculateLoyaltyMember(ctx, request)
	}
	payoutApprovalForRejectPayout := paymentsv1.NewPayoutApprovalServiceClient(deps.Conn)
	rejectPayoutCall = func(ctx context.Context, request *paymentsv1.RejectPayoutRequest) (*paymentsv1.RejectPayoutResponse, error) {
		return payoutApprovalForRejectPayout.RejectPayout(ctx, request)
	}
	productValidationForLintProducts := catalogv1.NewProductValidationServiceClient(deps.Conn)
	lintProductsCall = func(ctx context.Context, request *catalogv1.LintProductsRequest) (*catalogv1.LintProductsResponse, error) {
		return productValidationForLintProducts.LintProducts(ctx, request)
	}
	metricSeriesValidationForPreviewMetricSeries := metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn)
	previewMetricSeriesCall = metricSeriesValidationForPreviewMetricSeries.PreviewMetricSeries
	categoryCalculationForCalculateCategory := catalogv1.NewCategoryCalculationServiceClient(deps.Conn)
	calculateCategoryCall = categoryCalculationForCalculateCategory.CalculateCategory
	categoryCalculationForQuoteCategory := catalogv1.NewCategoryCalculationServiceClient(deps.Conn)
	quoteCategoryCall = func(ctx context.Context, request *catalogv1.QuoteCategoryRequest) (*catalogv1.QuoteCategoryResponse, error) {
		return categoryCalculationForQuoteCategory.QuoteCategory(ctx, request)
	}
	productCalculationForSimulateProduct := catalogv1.NewProductCalculationServiceClient(deps.Conn)
	simulateProductCall = func(ctx context.Context, request *catalogv1.SimulateProductRequest) (*catalogv1.SimulateProductResponse, error) {
		return productCalculationForSimulateProduct.SimulateProduct(ctx, request)
	}
	tenantAdminForLockTenant := identityv1.NewTenantAdminServiceClient(deps.Conn)
	lockTenantCall = tenantAdminForLockTenant.LockTenant
	paymentInstrumentWorkflowForSignalPaymentInstrumentWorkflow := paymentsv1.NewPaymentInstrumentWorkflowServiceClient(deps.Conn)
	signalPaymentInstrumentWorkflowCall = func(ctx context.Context, request *paymentsv1.SignalPaymentInstrumentWorkflowRequest) (*paymentsv1.SignalPaymentInstrumentWorkflowResponse, error) {
		return paymentInstrumentWorkflowForSignalPaymentInstrumentWorkflow.SignalPaymentInstrumentWorkflow(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                             *slog.Logger
	productVariantValidationClient  catalogv1.ProductVariantValidationServiceClient
	productApprovalClient           catalogv1.ProductApprovalServiceClient
	paymentInstrumentApprovalClient paymentsv1.PaymentInstrumentApprovalServiceClient
	orderAmendmentValidationClient  ordersv1.OrderAmendmentValidationServiceClient
	brandReportingClient            catalogv1.BrandReportingServiceClient
	invoiceBatchCalculationClient   invoicingv1.InvoiceBatchCalculationServiceClient
	searchIndexHistoryClient        searchv1.SearchIndexHistoryServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                             deps.Logger,
		productVariantValidationClient:  catalogv1.NewProductVariantValidationServiceClient(deps.Conn),
		productApprovalClient:           catalogv1.NewProductApprovalServiceClient(deps.Conn),
		paymentInstrumentApprovalClient: paymentsv1.NewPaymentInstrumentApprovalServiceClient(deps.Conn),
		orderAmendmentValidationClient:  ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn),
		brandReportingClient:            catalogv1.NewBrandReportingServiceClient(deps.Conn),
		invoiceBatchCalculationClient:   invoicingv1.NewInvoiceBatchCalculationServiceClient(deps.Conn),
		searchIndexHistoryClient:        searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
	}
	go w.consume(ctx, "catalog.create_product.validate_product_variant", deps.Bus.Subscribe("catalog.create_product.validate_product_variant"), w.handleCatalogCreateProductValidateProductVariant)
	go w.consume(ctx, "catalog.start_product_variant_workflow.recall_product", deps.Bus.Subscribe("catalog.start_product_variant_workflow.recall_product"), w.handleCatalogStartProductVariantWorkflowRecallProduct)
	go w.consume(ctx, "catalog.pause_product_variant_workflow.recall_product", deps.Bus.Subscribe("catalog.pause_product_variant_workflow.recall_product"), w.handleCatalogPauseProductVariantWorkflowRecallProduct)
	go w.consume(ctx, "catalog.signal_category_workflow.escalate_payment_instrument", deps.Bus.Subscribe("catalog.signal_category_workflow.escalate_payment_instrument"), w.handleCatalogSignalCategoryWorkflowEscalatePaymentInstrument)
	go w.consume(ctx, "catalog.explain_brand_decision.normalize_order_amendment", deps.Bus.Subscribe("catalog.explain_brand_decision.normalize_order_amendment"), w.handleCatalogExplainBrandDecisionNormalizeOrderAmendment)
	go w.consume(ctx, "catalog.retry_product_workflow.share_brand_report", deps.Bus.Subscribe("catalog.retry_product_workflow.share_brand_report"), w.handleCatalogRetryProductWorkflowShareBrandReport)
	go w.consume(ctx, "catalog.validate_product_variant.calculate_invoice_batch", deps.Bus.Subscribe("catalog.validate_product_variant.calculate_invoice_batch"), w.handleCatalogValidateProductVariantCalculateInvoiceBatch)
	go w.consume(ctx, "catalog.simulate_product.list_search_index_versions", deps.Bus.Subscribe("catalog.simulate_product.list_search_index_versions"), w.handleCatalogSimulateProductListSearchIndexVersions)
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

// handleCatalogCreateProductValidateProductVariant makes the call the catalog.create_product.validate_product_variant
// event asks for.
func (w *workers) handleCatalogCreateProductValidateProductVariant(ctx context.Context, event eventbus.Event) error {
	_, err := w.productVariantValidationClient.ValidateProductVariant(ctx, &catalogv1.ValidateProductVariantRequest{
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				Email:  event.Attributes["product_variant.owner.email"],
				UserId: event.Attributes["product_variant.owner.user_id"],
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

// handleCatalogStartProductVariantWorkflowRecallProduct makes the call the
// catalog.start_product_variant_workflow.recall_product event asks for.
func (w *workers) handleCatalogStartProductVariantWorkflowRecallProduct(ctx context.Context, event eventbus.Event) error {
	_, err := w.productApprovalClient.RecallProduct(ctx, &catalogv1.RecallProductRequest{
		Details: &catalogv1.RecallProductRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: event.Attributes["details.approver.display_name"],
			},
		},
		Product: &catalogv1.Product{
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{Recipient: event.Attributes["product.address.recipient"]}},
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

// handleCatalogPauseProductVariantWorkflowRecallProduct makes the call the
// catalog.pause_product_variant_workflow.recall_product event asks for.
func (w *workers) handleCatalogPauseProductVariantWorkflowRecallProduct(ctx context.Context, event eventbus.Event) error {
	_, err := w.productApprovalClient.RecallProduct(ctx, &catalogv1.RecallProductRequest{
		Product: &catalogv1.Product{
			Audit:   &commonv1.AuditInfo{UpdatedBy: event.Attributes["product.audit.updated_by"]},
			Locator: &catalogv1.Product_Address{Address: &commonv1.PostalAddress{PostalCode: event.Attributes["product.address.postal_code"]}},
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

// handleCatalogSignalCategoryWorkflowEscalatePaymentInstrument makes the call the
// catalog.signal_category_workflow.escalate_payment_instrument event asks for.
func (w *workers) handleCatalogSignalCategoryWorkflowEscalatePaymentInstrument(ctx context.Context, event eventbus.Event) error {
	_, err := w.paymentInstrumentApprovalClient.EscalatePaymentInstrument(ctx, &paymentsv1.EscalatePaymentInstrumentRequest{
		Details: &paymentsv1.EscalatePaymentInstrumentRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: event.Attributes["details.approver.user_id"],
				Email:  event.Attributes["details.approver.email"],
			},
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{Phone: event.Attributes["payment_instrument.contact.phone"]},
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

// handleCatalogExplainBrandDecisionNormalizeOrderAmendment makes the call the
// catalog.explain_brand_decision.normalize_order_amendment event asks for.
func (w *workers) handleCatalogExplainBrandDecisionNormalizeOrderAmendment(ctx context.Context, event eventbus.Event) error {
	_, err := w.orderAmendmentValidationClient.NormalizeOrderAmendment(ctx, &ordersv1.NormalizeOrderAmendmentRequest{
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				Etag:      event.Attributes["order_amendment.audit.etag"],
				UpdatedBy: event.Attributes["order_amendment.audit.updated_by"],
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: event.Attributes["order_amendment.billing_address.locality"],
			},
		},
		Details: &ordersv1.NormalizeOrderAmendmentRequest_Details{
			Contact: &commonv1.ContactInfo{Email: event.Attributes["details.contact.email"]},
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

// handleCatalogRetryProductWorkflowShareBrandReport makes the call the
// catalog.retry_product_workflow.share_brand_report event asks for.
func (w *workers) handleCatalogRetryProductWorkflowShareBrandReport(ctx context.Context, event eventbus.Event) error {
	_, err := w.brandReportingClient.ShareBrandReport(ctx, &catalogv1.ShareBrandReportRequest{
		Details: &catalogv1.ShareBrandReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            event.Attributes["details.recipient.email"],
				Phone:            event.Attributes["details.recipient.phone"],
				PreferredChannel: event.Attributes["details.recipient.preferred_channel"],
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

// handleCatalogValidateProductVariantCalculateInvoiceBatch makes the call the
// catalog.validate_product_variant.calculate_invoice_batch event asks for.
func (w *workers) handleCatalogValidateProductVariantCalculateInvoiceBatch(ctx context.Context, event eventbus.Event) error {
	_, err := w.invoiceBatchCalculationClient.CalculateInvoiceBatch(ctx, &invoicingv1.CalculateInvoiceBatchRequest{
		Details: &invoicingv1.CalculateInvoiceBatchRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: event.Attributes["details.destination.country_code"],
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

// handleCatalogSimulateProductListSearchIndexVersions makes the call the
// catalog.simulate_product.list_search_index_versions event asks for.
func (w *workers) handleCatalogSimulateProductListSearchIndexVersions(ctx context.Context, event eventbus.Event) error {
	_, err := w.searchIndexHistoryClient.ListSearchIndexVersions(ctx, &searchv1.ListSearchIndexVersionsRequest{
		Filter: &searchv1.ListSearchIndexVersionsRequest_Filter{
			Region:  event.Attributes["filter.region"],
			OwnerId: event.Attributes["filter.owner_id"],
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
