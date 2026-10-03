package cart

import (
	"context"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	yardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// cartCalculationPort is what the cart domain needs of acme.cart.v1.CartCalculationService.
type cartCalculationPort interface {
	CalculateCart(ctx context.Context, request *cartv1.CalculateCartRequest) (*cartv1.CalculateCartResponse, error)
}

// grpcCartCalculationPort reaches acme.cart.v1.CartCalculationService over gRPC.
type grpcCartCalculationPort struct {
	client cartv1.CartCalculationServiceClient
}

// sandboxCartCalculationPort answers in the sandbox environment, where acme.cart.v1.CartCalculationService is not
// deployed.
type sandboxCartCalculationPort struct{ log *slog.Logger }

func (p grpcCartCalculationPort) CalculateCart(ctx context.Context, request *cartv1.CalculateCartRequest) (*cartv1.CalculateCartResponse, error) {
	return p.client.CalculateCart(ctx, request)
}

func (p sandboxCartCalculationPort) CalculateCart(ctx context.Context, request *cartv1.CalculateCartRequest) (*cartv1.CalculateCartResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateCart")
	return &cartv1.CalculateCartResponse{}, nil
}

func newCartCalculationPort(deps platform.Deps) cartCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCartCalculationPort{log: deps.Logger}
	}
	return grpcCartCalculationPort{client: cartv1.NewCartCalculationServiceClient(deps.Conn)}
}

// cartValidationPort is what the cart domain needs of acme.cart.v1.CartValidationService.
type cartValidationPort interface {
	NormalizeCart(ctx context.Context, request *cartv1.NormalizeCartRequest) (*cartv1.NormalizeCartResponse, error)
	PreviewCart(ctx context.Context, request *cartv1.PreviewCartRequest) (*cartv1.PreviewCartResponse, error)
}

// grpcCartValidationPort reaches acme.cart.v1.CartValidationService over gRPC.
type grpcCartValidationPort struct {
	client cartv1.CartValidationServiceClient
}

// sandboxCartValidationPort answers in the sandbox environment, where acme.cart.v1.CartValidationService is not
// deployed.
type sandboxCartValidationPort struct{ log *slog.Logger }

func (p grpcCartValidationPort) NormalizeCart(ctx context.Context, request *cartv1.NormalizeCartRequest) (*cartv1.NormalizeCartResponse, error) {
	return p.client.NormalizeCart(ctx, request)
}

func (p sandboxCartValidationPort) NormalizeCart(ctx context.Context, request *cartv1.NormalizeCartRequest) (*cartv1.NormalizeCartResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizeCart")
	return &cartv1.NormalizeCartResponse{}, nil
}

func (p grpcCartValidationPort) PreviewCart(ctx context.Context, request *cartv1.PreviewCartRequest) (*cartv1.PreviewCartResponse, error) {
	return p.client.PreviewCart(ctx, request)
}

func (p sandboxCartValidationPort) PreviewCart(ctx context.Context, request *cartv1.PreviewCartRequest) (*cartv1.PreviewCartResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewCart")
	return &cartv1.PreviewCartResponse{}, nil
}

func newCartValidationPort(deps platform.Deps) cartValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCartValidationPort{log: deps.Logger}
	}
	return grpcCartValidationPort{client: cartv1.NewCartValidationServiceClient(deps.Conn)}
}

// disputeImportPort is what the cart domain needs of acme.payments.v1.DisputeImportService.
type disputeImportPort interface {
	MapDisputeImportColumns(ctx context.Context, request *paymentsv1.MapDisputeImportColumnsRequest) (*paymentsv1.MapDisputeImportColumnsResponse, error)
}

// grpcDisputeImportPort reaches acme.payments.v1.DisputeImportService over gRPC.
type grpcDisputeImportPort struct {
	client paymentsv1.DisputeImportServiceClient
}

// sandboxDisputeImportPort answers in the sandbox environment, where acme.payments.v1.DisputeImportService is not
// deployed.
type sandboxDisputeImportPort struct{ log *slog.Logger }

func (p grpcDisputeImportPort) MapDisputeImportColumns(ctx context.Context, request *paymentsv1.MapDisputeImportColumnsRequest) (*paymentsv1.MapDisputeImportColumnsResponse, error) {
	return p.client.MapDisputeImportColumns(ctx, request)
}

func (p sandboxDisputeImportPort) MapDisputeImportColumns(ctx context.Context, request *paymentsv1.MapDisputeImportColumnsRequest) (*paymentsv1.MapDisputeImportColumnsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers MapDisputeImportColumns")
	return &paymentsv1.MapDisputeImportColumnsResponse{}, nil
}

func newDisputeImportPort(deps platform.Deps) disputeImportPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxDisputeImportPort{log: deps.Logger}
	}
	return grpcDisputeImportPort{client: paymentsv1.NewDisputeImportServiceClient(deps.Conn)}
}

// fraudRuleWorkflowPort is what the cart domain needs of acme.fraud.v1.FraudRuleWorkflowService.
type fraudRuleWorkflowPort interface {
	AdvanceFraudRuleWorkflow(ctx context.Context, request *fraudv1.AdvanceFraudRuleWorkflowRequest) (*fraudv1.AdvanceFraudRuleWorkflowResponse, error)
}

// grpcFraudRuleWorkflowPort reaches acme.fraud.v1.FraudRuleWorkflowService over gRPC.
type grpcFraudRuleWorkflowPort struct {
	client fraudv1.FraudRuleWorkflowServiceClient
}

// sandboxFraudRuleWorkflowPort answers in the sandbox environment, where acme.fraud.v1.FraudRuleWorkflowService is not
// deployed.
type sandboxFraudRuleWorkflowPort struct{ log *slog.Logger }

func (p grpcFraudRuleWorkflowPort) AdvanceFraudRuleWorkflow(ctx context.Context, request *fraudv1.AdvanceFraudRuleWorkflowRequest) (*fraudv1.AdvanceFraudRuleWorkflowResponse, error) {
	return p.client.AdvanceFraudRuleWorkflow(ctx, request)
}

func (p sandboxFraudRuleWorkflowPort) AdvanceFraudRuleWorkflow(ctx context.Context, request *fraudv1.AdvanceFraudRuleWorkflowRequest) (*fraudv1.AdvanceFraudRuleWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers AdvanceFraudRuleWorkflow")
	return &fraudv1.AdvanceFraudRuleWorkflowResponse{}, nil
}

func newFraudRuleWorkflowPort(deps platform.Deps) fraudRuleWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFraudRuleWorkflowPort{log: deps.Logger}
	}
	return grpcFraudRuleWorkflowPort{client: fraudv1.NewFraudRuleWorkflowServiceClient(deps.Conn)}
}

// notificationPort is what the cart domain needs of acme.notifications.v1.NotificationService.
type notificationPort interface {
	BatchGetNotifications(ctx context.Context, request *notificationsv1.BatchGetNotificationsRequest) (*notificationsv1.BatchGetNotificationsResponse, error)
}

// grpcNotificationPort reaches acme.notifications.v1.NotificationService over gRPC.
type grpcNotificationPort struct {
	client notificationsv1.NotificationServiceClient
}

// sandboxNotificationPort answers in the sandbox environment, where acme.notifications.v1.NotificationService is not
// deployed.
type sandboxNotificationPort struct{ log *slog.Logger }

func (p grpcNotificationPort) BatchGetNotifications(ctx context.Context, request *notificationsv1.BatchGetNotificationsRequest) (*notificationsv1.BatchGetNotificationsResponse, error) {
	return p.client.BatchGetNotifications(ctx, request)
}

func (p sandboxNotificationPort) BatchGetNotifications(ctx context.Context, request *notificationsv1.BatchGetNotificationsRequest) (*notificationsv1.BatchGetNotificationsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers BatchGetNotifications")
	return &notificationsv1.BatchGetNotificationsResponse{}, nil
}

func newNotificationPort(deps platform.Deps) notificationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxNotificationPort{log: deps.Logger}
	}
	return grpcNotificationPort{client: notificationsv1.NewNotificationServiceClient(deps.Conn)}
}

// paymentValidationPort is what the cart domain needs of acme.payments.v1.PaymentValidationService.
type paymentValidationPort interface {
	ExplainPaymentDecision(ctx context.Context, request *paymentsv1.ExplainPaymentDecisionRequest) (*paymentsv1.ExplainPaymentDecisionResponse, error)
}

// grpcPaymentValidationPort reaches acme.payments.v1.PaymentValidationService over gRPC.
type grpcPaymentValidationPort struct {
	client paymentsv1.PaymentValidationServiceClient
}

// sandboxPaymentValidationPort answers in the sandbox environment, where acme.payments.v1.PaymentValidationService is
// not deployed.
type sandboxPaymentValidationPort struct{ log *slog.Logger }

func (p grpcPaymentValidationPort) ExplainPaymentDecision(ctx context.Context, request *paymentsv1.ExplainPaymentDecisionRequest) (*paymentsv1.ExplainPaymentDecisionResponse, error) {
	return p.client.ExplainPaymentDecision(ctx, request)
}

func (p sandboxPaymentValidationPort) ExplainPaymentDecision(ctx context.Context, request *paymentsv1.ExplainPaymentDecisionRequest) (*paymentsv1.ExplainPaymentDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainPaymentDecision")
	return &paymentsv1.ExplainPaymentDecisionResponse{}, nil
}

func newPaymentValidationPort(deps platform.Deps) paymentValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPaymentValidationPort{log: deps.Logger}
	}
	return grpcPaymentValidationPort{client: paymentsv1.NewPaymentValidationServiceClient(deps.Conn)}
}

// returnLabelValidationPort is what the cart domain needs of acme.returns.v1.ReturnLabelValidationService.
type returnLabelValidationPort interface {
	ValidateReturnLabel(ctx context.Context, request *returnsv1.ValidateReturnLabelRequest) (*returnsv1.ValidateReturnLabelResponse, error)
}

// grpcReturnLabelValidationPort reaches acme.returns.v1.ReturnLabelValidationService over gRPC.
type grpcReturnLabelValidationPort struct {
	client returnsv1.ReturnLabelValidationServiceClient
}

// sandboxReturnLabelValidationPort answers in the sandbox environment, where
// acme.returns.v1.ReturnLabelValidationService is not deployed.
type sandboxReturnLabelValidationPort struct{ log *slog.Logger }

func (p grpcReturnLabelValidationPort) ValidateReturnLabel(ctx context.Context, request *returnsv1.ValidateReturnLabelRequest) (*returnsv1.ValidateReturnLabelResponse, error) {
	return p.client.ValidateReturnLabel(ctx, request)
}

func (p sandboxReturnLabelValidationPort) ValidateReturnLabel(ctx context.Context, request *returnsv1.ValidateReturnLabelRequest) (*returnsv1.ValidateReturnLabelResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidateReturnLabel")
	return &returnsv1.ValidateReturnLabelResponse{}, nil
}

func newReturnLabelValidationPort(deps platform.Deps) returnLabelValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReturnLabelValidationPort{log: deps.Logger}
	}
	return grpcReturnLabelValidationPort{client: returnsv1.NewReturnLabelValidationServiceClient(deps.Conn)}
}

// savedListCalculationPort is what the cart domain needs of acme.cart.v1.SavedListCalculationService.
type savedListCalculationPort interface {
	RecalculateSavedLists(ctx context.Context, request *cartv1.RecalculateSavedListsRequest) (*cartv1.RecalculateSavedListsResponse, error)
}

// grpcSavedListCalculationPort reaches acme.cart.v1.SavedListCalculationService over gRPC.
type grpcSavedListCalculationPort struct {
	client cartv1.SavedListCalculationServiceClient
}

// sandboxSavedListCalculationPort answers in the sandbox environment, where acme.cart.v1.SavedListCalculationService is
// not deployed.
type sandboxSavedListCalculationPort struct{ log *slog.Logger }

func (p grpcSavedListCalculationPort) RecalculateSavedLists(ctx context.Context, request *cartv1.RecalculateSavedListsRequest) (*cartv1.RecalculateSavedListsResponse, error) {
	return p.client.RecalculateSavedLists(ctx, request)
}

func (p sandboxSavedListCalculationPort) RecalculateSavedLists(ctx context.Context, request *cartv1.RecalculateSavedListsRequest) (*cartv1.RecalculateSavedListsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecalculateSavedLists")
	return &cartv1.RecalculateSavedListsResponse{}, nil
}

func newSavedListCalculationPort(deps platform.Deps) savedListCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSavedListCalculationPort{log: deps.Logger}
	}
	return grpcSavedListCalculationPort{client: cartv1.NewSavedListCalculationServiceClient(deps.Conn)}
}

// yardCalculationPort is what the cart domain needs of acme.yards.v1.YardCalculationService.
type yardCalculationPort interface {
	ExplainYardCalculation(ctx context.Context, request *yardsv1.ExplainYardCalculationRequest) (*yardsv1.ExplainYardCalculationResponse, error)
}

// grpcYardCalculationPort reaches acme.yards.v1.YardCalculationService over gRPC.
type grpcYardCalculationPort struct {
	client yardsv1.YardCalculationServiceClient
}

// sandboxYardCalculationPort answers in the sandbox environment, where acme.yards.v1.YardCalculationService is not
// deployed.
type sandboxYardCalculationPort struct{ log *slog.Logger }

func (p grpcYardCalculationPort) ExplainYardCalculation(ctx context.Context, request *yardsv1.ExplainYardCalculationRequest) (*yardsv1.ExplainYardCalculationResponse, error) {
	return p.client.ExplainYardCalculation(ctx, request)
}

func (p sandboxYardCalculationPort) ExplainYardCalculation(ctx context.Context, request *yardsv1.ExplainYardCalculationRequest) (*yardsv1.ExplainYardCalculationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainYardCalculation")
	return &yardsv1.ExplainYardCalculationResponse{}, nil
}

func newYardCalculationPort(deps platform.Deps) yardCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxYardCalculationPort{log: deps.Logger}
	}
	return grpcYardCalculationPort{client: yardsv1.NewYardCalculationServiceClient(deps.Conn)}
}
