package cart

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	experimentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the cart domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.audit.v1.AuditEventValidationService", auditv1.NewAuditEventValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.cart.v1.CartCalculationService", cartv1.NewCartCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.cart.v1.CartRuleValidationService", cartv1.NewCartRuleValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.cart.v1.CartValidationService", cartv1.NewCartValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.cart.v1.SavedListCalculationService", cartv1.NewSavedListCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.cart.v1.SavedListValidationService", cartv1.NewSavedListValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.notifications.v1.NotificationService", notificationsv1.NewNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.payments.v1.DisputeSyncService", paymentsv1.NewDisputeSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.payments.v1.PayoutApprovalService", paymentsv1.NewPayoutApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerListingValidationService", sellersv1.NewSellerListingValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerScorecardReportingService", sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.ManifestCalculationService", shippingv1.NewManifestCalculationServiceClient(deps.Conn))
	cartv1.RegisterCartServiceServer(server, NewCartServer(deps))
	cartv1.RegisterSavedListQueryServiceServer(server, NewSavedListQueryServer(deps))
	cartv1.RegisterCartRuleWorkflowServiceServer(server, NewCartRuleWorkflowServer(deps))
	cartv1.RegisterCartQueryServiceServer(server, NewCartQueryServer(deps))
	cartv1.RegisterSavedListWorkflowServiceServer(server, NewSavedListWorkflowServer(deps))
	cartv1.RegisterCartRuleApprovalServiceServer(server, NewCartRuleApprovalServer(deps))
	cartWorkflowServer := NewCartWorkflowServer(deps)
	server.RegisterService(&cartv1.CartWorkflowService_ServiceDesc, cartWorkflowServer)
	cartv1.RegisterSavedListApprovalServiceServer(server, NewSavedListApprovalServer(deps))
	cartRuleValidationServer := NewCartRuleValidationServer(deps)
	server.RegisterService(&cartv1.CartRuleValidationService_ServiceDesc, cartRuleValidationServer)
	cartv1.RegisterCartApprovalServiceServer(server, NewCartApprovalServer(deps))
	cartv1.RegisterSavedListValidationServiceServer(server, NewSavedListValidationServer(deps))
	cartv1.RegisterCartRuleCalculationServiceServer(server, NewCartRuleCalculationServer(deps))
	cartv1.RegisterCartValidationServiceServer(server, NewCartValidationServer(deps))
	cartv1.RegisterSavedListCalculationServiceServer(server, NewSavedListCalculationServer(deps))
	cartv1.RegisterCartRuleLifecycleServiceServer(server, NewCartRuleLifecycleServer(deps))
	cartv1.RegisterCartCalculationServiceServer(server, NewCartCalculationServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	listApiCredentialSubscriptionsCall func(context.Context, *identityv1.ListApiCredentialSubscriptionsRequest) (*identityv1.ListApiCredentialSubscriptionsResponse, error)
	explainFeatureFlagDecisionCall     func(context.Context, *featureflagsv1.ExplainFeatureFlagDecisionRequest) (*featureflagsv1.ExplainFeatureFlagDecisionResponse, error)
	summarizeTranslationKeysCall       func(context.Context, *localizationv1.SummarizeTranslationKeysRequest, ...grpc.CallOption) (*localizationv1.SummarizeTranslationKeysResponse, error)
	findSavedListsCall                 func(context.Context, *cartv1.FindSavedListsRequest) (*cartv1.FindSavedListsResponse, error)
	recallParcelCall                   func(context.Context, *shippingv1.RecallParcelRequest, ...grpc.CallOption) (*shippingv1.RecallParcelResponse, error)
	summarizeExperimentsCall           func(context.Context, *experimentsv1.SummarizeExperimentsRequest) (*experimentsv1.SummarizeExperimentsResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	apiCredentialNotificationForListApiCredentialSubscriptions := identityv1.NewApiCredentialNotificationServiceClient(deps.Conn)
	listApiCredentialSubscriptionsCall = func(ctx context.Context, request *identityv1.ListApiCredentialSubscriptionsRequest) (*identityv1.ListApiCredentialSubscriptionsResponse, error) {
		return apiCredentialNotificationForListApiCredentialSubscriptions.ListApiCredentialSubscriptions(ctx, request)
	}
	featureFlagValidationForExplainFeatureFlagDecision := featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn)
	explainFeatureFlagDecisionCall = func(ctx context.Context, request *featureflagsv1.ExplainFeatureFlagDecisionRequest) (*featureflagsv1.ExplainFeatureFlagDecisionResponse, error) {
		return featureFlagValidationForExplainFeatureFlagDecision.ExplainFeatureFlagDecision(ctx, request)
	}
	translationKeyQueryForSummarizeTranslationKeys := localizationv1.NewTranslationKeyQueryServiceClient(deps.Conn)
	summarizeTranslationKeysCall = translationKeyQueryForSummarizeTranslationKeys.SummarizeTranslationKeys
	savedListQueryForFindSavedLists := cartv1.NewSavedListQueryServiceClient(deps.Conn)
	findSavedListsCall = func(ctx context.Context, request *cartv1.FindSavedListsRequest) (*cartv1.FindSavedListsResponse, error) {
		return savedListQueryForFindSavedLists.FindSavedLists(ctx, request)
	}
	parcelApprovalForRecallParcel := shippingv1.NewParcelApprovalServiceClient(deps.Conn)
	recallParcelCall = parcelApprovalForRecallParcel.RecallParcel
	experimentQueryForSummarizeExperiments := experimentsv1.NewExperimentQueryServiceClient(deps.Conn)
	summarizeExperimentsCall = func(ctx context.Context, request *experimentsv1.SummarizeExperimentsRequest) (*experimentsv1.SummarizeExperimentsResponse, error) {
		return experimentQueryForSummarizeExperiments.SummarizeExperiments(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                         *slog.Logger
	cartWorkflowClient          cartv1.CartWorkflowServiceClient
	stockReservationQueryClient inventoryv1.StockReservationQueryServiceClient
	savedListQueryClient        cartv1.SavedListQueryServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                         deps.Logger,
		cartWorkflowClient:          cartv1.NewCartWorkflowServiceClient(deps.Conn),
		stockReservationQueryClient: inventoryv1.NewStockReservationQueryServiceClient(deps.Conn),
		savedListQueryClient:        cartv1.NewSavedListQueryServiceClient(deps.Conn),
	}
	go w.consume(ctx, "cart.cancel_cart_rule_workflow.retry_cart_workflow", deps.Bus.Subscribe("cart.cancel_cart_rule_workflow.retry_cart_workflow"), w.handleCartCancelCartRuleWorkflowRetryCartWorkflow)
	go w.consume(ctx, "cart.lint_cart_rules.summarize_stock_reservations", deps.Bus.Subscribe("cart.lint_cart_rules.summarize_stock_reservations"), w.handleCartLintCartRulesSummarizeStockReservations)
	go w.consume(ctx, "cart.escalate_cart.summarize_saved_lists", deps.Bus.Subscribe("cart.escalate_cart.summarize_saved_lists"), w.handleCartEscalateCartSummarizeSavedLists)
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

// handleCartCancelCartRuleWorkflowRetryCartWorkflow makes the call the
// cart.cancel_cart_rule_workflow.retry_cart_workflow event asks for.
func (w *workers) handleCartCancelCartRuleWorkflowRetryCartWorkflow(ctx context.Context, event eventbus.Event) error {
	_, err := w.cartWorkflowClient.RetryCartWorkflow(ctx, &cartv1.RetryCartWorkflowRequest{
		Details: &cartv1.RetryCartWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: event.Attributes["details.notify.phone"]},
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

// handleCartLintCartRulesSummarizeStockReservations makes the call the
// cart.lint_cart_rules.summarize_stock_reservations event asks for.
func (w *workers) handleCartLintCartRulesSummarizeStockReservations(ctx context.Context, event eventbus.Event) error {
	_, err := w.stockReservationQueryClient.SummarizeStockReservations(ctx, &inventoryv1.SummarizeStockReservationsRequest{
		Reason: event.Attributes["reason"],
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

// handleCartEscalateCartSummarizeSavedLists makes the call the cart.escalate_cart.summarize_saved_lists event asks for.
func (w *workers) handleCartEscalateCartSummarizeSavedLists(ctx context.Context, event eventbus.Event) error {
	_, err := w.savedListQueryClient.SummarizeSavedLists(ctx, &cartv1.SummarizeSavedListsRequest{
		SavedListId: event.Attributes["saved_list_id"],
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
