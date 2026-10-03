package returns

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	treasuryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/treasury/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// cashPositionCalculationPort is what the returns domain needs of acme.treasury.v1.CashPositionCalculationService.
type cashPositionCalculationPort interface {
	QuoteCashPosition(ctx context.Context, request *treasuryv1.QuoteCashPositionRequest) (*treasuryv1.QuoteCashPositionResponse, error)
}

// grpcCashPositionCalculationPort reaches acme.treasury.v1.CashPositionCalculationService over gRPC.
type grpcCashPositionCalculationPort struct {
	client treasuryv1.CashPositionCalculationServiceClient
}

// sandboxCashPositionCalculationPort answers in the sandbox environment, where
// acme.treasury.v1.CashPositionCalculationService is not deployed.
type sandboxCashPositionCalculationPort struct{ log *slog.Logger }

func (p grpcCashPositionCalculationPort) QuoteCashPosition(ctx context.Context, request *treasuryv1.QuoteCashPositionRequest) (*treasuryv1.QuoteCashPositionResponse, error) {
	return p.client.QuoteCashPosition(ctx, request)
}

func (p sandboxCashPositionCalculationPort) QuoteCashPosition(ctx context.Context, request *treasuryv1.QuoteCashPositionRequest) (*treasuryv1.QuoteCashPositionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers QuoteCashPosition")
	return &treasuryv1.QuoteCashPositionResponse{}, nil
}

func newCashPositionCalculationPort(deps platform.Deps) cashPositionCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCashPositionCalculationPort{log: deps.Logger}
	}
	return grpcCashPositionCalculationPort{client: treasuryv1.NewCashPositionCalculationServiceClient(deps.Conn)}
}

// householdCalculationPort is what the returns domain needs of acme.customers.v1.HouseholdCalculationService.
type householdCalculationPort interface {
	QuoteHousehold(ctx context.Context, request *customersv1.QuoteHouseholdRequest) (*customersv1.QuoteHouseholdResponse, error)
}

// grpcHouseholdCalculationPort reaches acme.customers.v1.HouseholdCalculationService over gRPC.
type grpcHouseholdCalculationPort struct {
	client customersv1.HouseholdCalculationServiceClient
}

// sandboxHouseholdCalculationPort answers in the sandbox environment, where
// acme.customers.v1.HouseholdCalculationService is not deployed.
type sandboxHouseholdCalculationPort struct{ log *slog.Logger }

func (p grpcHouseholdCalculationPort) QuoteHousehold(ctx context.Context, request *customersv1.QuoteHouseholdRequest) (*customersv1.QuoteHouseholdResponse, error) {
	return p.client.QuoteHousehold(ctx, request)
}

func (p sandboxHouseholdCalculationPort) QuoteHousehold(ctx context.Context, request *customersv1.QuoteHouseholdRequest) (*customersv1.QuoteHouseholdResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers QuoteHousehold")
	return &customersv1.QuoteHouseholdResponse{}, nil
}

func newHouseholdCalculationPort(deps platform.Deps) householdCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxHouseholdCalculationPort{log: deps.Logger}
	}
	return grpcHouseholdCalculationPort{client: customersv1.NewHouseholdCalculationServiceClient(deps.Conn)}
}

// inspectionCalculationPort is what the returns domain needs of acme.returns.v1.InspectionCalculationService.
type inspectionCalculationPort interface {
	CompareInspectionOptions(ctx context.Context, request *returnsv1.CompareInspectionOptionsRequest) (*returnsv1.CompareInspectionOptionsResponse, error)
}

// grpcInspectionCalculationPort reaches acme.returns.v1.InspectionCalculationService over gRPC.
type grpcInspectionCalculationPort struct {
	client returnsv1.InspectionCalculationServiceClient
}

// sandboxInspectionCalculationPort answers in the sandbox environment, where
// acme.returns.v1.InspectionCalculationService is not deployed.
type sandboxInspectionCalculationPort struct{ log *slog.Logger }

func (p grpcInspectionCalculationPort) CompareInspectionOptions(ctx context.Context, request *returnsv1.CompareInspectionOptionsRequest) (*returnsv1.CompareInspectionOptionsResponse, error) {
	return p.client.CompareInspectionOptions(ctx, request)
}

func (p sandboxInspectionCalculationPort) CompareInspectionOptions(ctx context.Context, request *returnsv1.CompareInspectionOptionsRequest) (*returnsv1.CompareInspectionOptionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CompareInspectionOptions")
	return &returnsv1.CompareInspectionOptionsResponse{}, nil
}

func newInspectionCalculationPort(deps platform.Deps) inspectionCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxInspectionCalculationPort{log: deps.Logger}
	}
	return grpcInspectionCalculationPort{client: returnsv1.NewInspectionCalculationServiceClient(deps.Conn)}
}

// notificationQueryPort is what the returns domain needs of acme.notifications.v1.NotificationQueryService.
type notificationQueryPort interface {
	BrowseNotifications(ctx context.Context, request *notificationsv1.BrowseNotificationsRequest) (*notificationsv1.BrowseNotificationsResponse, error)
}

// grpcNotificationQueryPort reaches acme.notifications.v1.NotificationQueryService over gRPC.
type grpcNotificationQueryPort struct {
	client notificationsv1.NotificationQueryServiceClient
}

// sandboxNotificationQueryPort answers in the sandbox environment, where acme.notifications.v1.NotificationQueryService
// is not deployed.
type sandboxNotificationQueryPort struct{ log *slog.Logger }

func (p grpcNotificationQueryPort) BrowseNotifications(ctx context.Context, request *notificationsv1.BrowseNotificationsRequest) (*notificationsv1.BrowseNotificationsResponse, error) {
	return p.client.BrowseNotifications(ctx, request)
}

func (p sandboxNotificationQueryPort) BrowseNotifications(ctx context.Context, request *notificationsv1.BrowseNotificationsRequest) (*notificationsv1.BrowseNotificationsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers BrowseNotifications")
	return &notificationsv1.BrowseNotificationsResponse{}, nil
}

func newNotificationQueryPort(deps platform.Deps) notificationQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxNotificationQueryPort{log: deps.Logger}
	}
	return grpcNotificationQueryPort{client: notificationsv1.NewNotificationQueryServiceClient(deps.Conn)}
}

// policyPort is what the returns domain needs of acme.insurance.v1.PolicyService.
type policyPort interface {
	DeletePolicy(ctx context.Context, request *insurancev1.DeletePolicyRequest) (*insurancev1.DeletePolicyResponse, error)
}

// grpcPolicyPort reaches acme.insurance.v1.PolicyService over gRPC.
type grpcPolicyPort struct {
	client insurancev1.PolicyServiceClient
}

// sandboxPolicyPort answers in the sandbox environment, where acme.insurance.v1.PolicyService is not deployed.
type sandboxPolicyPort struct{ log *slog.Logger }

func (p grpcPolicyPort) DeletePolicy(ctx context.Context, request *insurancev1.DeletePolicyRequest) (*insurancev1.DeletePolicyResponse, error) {
	return p.client.DeletePolicy(ctx, request)
}

func (p sandboxPolicyPort) DeletePolicy(ctx context.Context, request *insurancev1.DeletePolicyRequest) (*insurancev1.DeletePolicyResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers DeletePolicy")
	return &insurancev1.DeletePolicyResponse{}, nil
}

func newPolicyPort(deps platform.Deps) policyPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPolicyPort{log: deps.Logger}
	}
	return grpcPolicyPort{client: insurancev1.NewPolicyServiceClient(deps.Conn)}
}

// restockDecisionCalculationPort is what the returns domain needs of acme.returns.v1.RestockDecisionCalculationService.
type restockDecisionCalculationPort interface {
	ExplainRestockDecisionCalculation(ctx context.Context, request *returnsv1.ExplainRestockDecisionCalculationRequest) (*returnsv1.ExplainRestockDecisionCalculationResponse, error)
}

// grpcRestockDecisionCalculationPort reaches acme.returns.v1.RestockDecisionCalculationService over gRPC.
type grpcRestockDecisionCalculationPort struct {
	client returnsv1.RestockDecisionCalculationServiceClient
}

// sandboxRestockDecisionCalculationPort answers in the sandbox environment, where
// acme.returns.v1.RestockDecisionCalculationService is not deployed.
type sandboxRestockDecisionCalculationPort struct{ log *slog.Logger }

func (p grpcRestockDecisionCalculationPort) ExplainRestockDecisionCalculation(ctx context.Context, request *returnsv1.ExplainRestockDecisionCalculationRequest) (*returnsv1.ExplainRestockDecisionCalculationResponse, error) {
	return p.client.ExplainRestockDecisionCalculation(ctx, request)
}

func (p sandboxRestockDecisionCalculationPort) ExplainRestockDecisionCalculation(ctx context.Context, request *returnsv1.ExplainRestockDecisionCalculationRequest) (*returnsv1.ExplainRestockDecisionCalculationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainRestockDecisionCalculation")
	return &returnsv1.ExplainRestockDecisionCalculationResponse{}, nil
}

func newRestockDecisionCalculationPort(deps platform.Deps) restockDecisionCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRestockDecisionCalculationPort{log: deps.Logger}
	}
	return grpcRestockDecisionCalculationPort{client: returnsv1.NewRestockDecisionCalculationServiceClient(deps.Conn)}
}

// restockDecisionReportingPort is what the returns domain needs of acme.returns.v1.RestockDecisionReportingService.
type restockDecisionReportingPort interface {
	DeleteRestockDecisionReport(ctx context.Context, request *returnsv1.DeleteRestockDecisionReportRequest) (*returnsv1.DeleteRestockDecisionReportResponse, error)
}

// grpcRestockDecisionReportingPort reaches acme.returns.v1.RestockDecisionReportingService over gRPC.
type grpcRestockDecisionReportingPort struct {
	client returnsv1.RestockDecisionReportingServiceClient
}

// sandboxRestockDecisionReportingPort answers in the sandbox environment, where
// acme.returns.v1.RestockDecisionReportingService is not deployed.
type sandboxRestockDecisionReportingPort struct{ log *slog.Logger }

func (p grpcRestockDecisionReportingPort) DeleteRestockDecisionReport(ctx context.Context, request *returnsv1.DeleteRestockDecisionReportRequest) (*returnsv1.DeleteRestockDecisionReportResponse, error) {
	return p.client.DeleteRestockDecisionReport(ctx, request)
}

func (p sandboxRestockDecisionReportingPort) DeleteRestockDecisionReport(ctx context.Context, request *returnsv1.DeleteRestockDecisionReportRequest) (*returnsv1.DeleteRestockDecisionReportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers DeleteRestockDecisionReport")
	return &returnsv1.DeleteRestockDecisionReportResponse{}, nil
}

func newRestockDecisionReportingPort(deps platform.Deps) restockDecisionReportingPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRestockDecisionReportingPort{log: deps.Logger}
	}
	return grpcRestockDecisionReportingPort{client: returnsv1.NewRestockDecisionReportingServiceClient(deps.Conn)}
}

// returnLabelCalculationPort is what the returns domain needs of acme.returns.v1.ReturnLabelCalculationService.
type returnLabelCalculationPort interface {
	CompareReturnLabelOptions(ctx context.Context, request *returnsv1.CompareReturnLabelOptionsRequest) (*returnsv1.CompareReturnLabelOptionsResponse, error)
	SimulateReturnLabel(ctx context.Context, request *returnsv1.SimulateReturnLabelRequest) (*returnsv1.SimulateReturnLabelResponse, error)
}

// grpcReturnLabelCalculationPort reaches acme.returns.v1.ReturnLabelCalculationService over gRPC.
type grpcReturnLabelCalculationPort struct {
	client returnsv1.ReturnLabelCalculationServiceClient
}

// sandboxReturnLabelCalculationPort answers in the sandbox environment, where
// acme.returns.v1.ReturnLabelCalculationService is not deployed.
type sandboxReturnLabelCalculationPort struct{ log *slog.Logger }

func (p grpcReturnLabelCalculationPort) CompareReturnLabelOptions(ctx context.Context, request *returnsv1.CompareReturnLabelOptionsRequest) (*returnsv1.CompareReturnLabelOptionsResponse, error) {
	return p.client.CompareReturnLabelOptions(ctx, request)
}

func (p sandboxReturnLabelCalculationPort) CompareReturnLabelOptions(ctx context.Context, request *returnsv1.CompareReturnLabelOptionsRequest) (*returnsv1.CompareReturnLabelOptionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CompareReturnLabelOptions")
	return &returnsv1.CompareReturnLabelOptionsResponse{}, nil
}

func (p grpcReturnLabelCalculationPort) SimulateReturnLabel(ctx context.Context, request *returnsv1.SimulateReturnLabelRequest) (*returnsv1.SimulateReturnLabelResponse, error) {
	return p.client.SimulateReturnLabel(ctx, request)
}

func (p sandboxReturnLabelCalculationPort) SimulateReturnLabel(ctx context.Context, request *returnsv1.SimulateReturnLabelRequest) (*returnsv1.SimulateReturnLabelResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SimulateReturnLabel")
	return &returnsv1.SimulateReturnLabelResponse{}, nil
}

func newReturnLabelCalculationPort(deps platform.Deps) returnLabelCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReturnLabelCalculationPort{log: deps.Logger}
	}
	return grpcReturnLabelCalculationPort{client: returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn)}
}

// returnLabelValidationPort is what the returns domain needs of acme.returns.v1.ReturnLabelValidationService.
type returnLabelValidationPort interface {
	PreviewReturnLabel(ctx context.Context, request *returnsv1.PreviewReturnLabelRequest) (*returnsv1.PreviewReturnLabelResponse, error)
}

// grpcReturnLabelValidationPort reaches acme.returns.v1.ReturnLabelValidationService over gRPC.
type grpcReturnLabelValidationPort struct {
	client returnsv1.ReturnLabelValidationServiceClient
}

// sandboxReturnLabelValidationPort answers in the sandbox environment, where
// acme.returns.v1.ReturnLabelValidationService is not deployed.
type sandboxReturnLabelValidationPort struct{ log *slog.Logger }

func (p grpcReturnLabelValidationPort) PreviewReturnLabel(ctx context.Context, request *returnsv1.PreviewReturnLabelRequest) (*returnsv1.PreviewReturnLabelResponse, error) {
	return p.client.PreviewReturnLabel(ctx, request)
}

func (p sandboxReturnLabelValidationPort) PreviewReturnLabel(ctx context.Context, request *returnsv1.PreviewReturnLabelRequest) (*returnsv1.PreviewReturnLabelResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewReturnLabel")
	return &returnsv1.PreviewReturnLabelResponse{}, nil
}

func newReturnLabelValidationPort(deps platform.Deps) returnLabelValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReturnLabelValidationPort{log: deps.Logger}
	}
	return grpcReturnLabelValidationPort{client: returnsv1.NewReturnLabelValidationServiceClient(deps.Conn)}
}

// returnRequestCalculationPort is what the returns domain needs of acme.returns.v1.ReturnRequestCalculationService.
type returnRequestCalculationPort interface {
	CompareReturnRequestOptions(ctx context.Context, request *returnsv1.CompareReturnRequestOptionsRequest) (*returnsv1.CompareReturnRequestOptionsResponse, error)
}

// grpcReturnRequestCalculationPort reaches acme.returns.v1.ReturnRequestCalculationService over gRPC.
type grpcReturnRequestCalculationPort struct {
	client returnsv1.ReturnRequestCalculationServiceClient
}

// sandboxReturnRequestCalculationPort answers in the sandbox environment, where
// acme.returns.v1.ReturnRequestCalculationService is not deployed.
type sandboxReturnRequestCalculationPort struct{ log *slog.Logger }

func (p grpcReturnRequestCalculationPort) CompareReturnRequestOptions(ctx context.Context, request *returnsv1.CompareReturnRequestOptionsRequest) (*returnsv1.CompareReturnRequestOptionsResponse, error) {
	return p.client.CompareReturnRequestOptions(ctx, request)
}

func (p sandboxReturnRequestCalculationPort) CompareReturnRequestOptions(ctx context.Context, request *returnsv1.CompareReturnRequestOptionsRequest) (*returnsv1.CompareReturnRequestOptionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CompareReturnRequestOptions")
	return &returnsv1.CompareReturnRequestOptionsResponse{}, nil
}

func newReturnRequestCalculationPort(deps platform.Deps) returnRequestCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReturnRequestCalculationPort{log: deps.Logger}
	}
	return grpcReturnRequestCalculationPort{client: returnsv1.NewReturnRequestCalculationServiceClient(deps.Conn)}
}

// returnRequestPort is what the returns domain needs of acme.returns.v1.ReturnRequestService.
type returnRequestPort interface {
	UpdateReturnRequest(ctx context.Context, request *returnsv1.UpdateReturnRequestRequest) (*returnsv1.UpdateReturnRequestResponse, error)
}

// grpcReturnRequestPort reaches acme.returns.v1.ReturnRequestService over gRPC.
type grpcReturnRequestPort struct {
	client returnsv1.ReturnRequestServiceClient
}

// sandboxReturnRequestPort answers in the sandbox environment, where acme.returns.v1.ReturnRequestService is not
// deployed.
type sandboxReturnRequestPort struct{ log *slog.Logger }

func (p grpcReturnRequestPort) UpdateReturnRequest(ctx context.Context, request *returnsv1.UpdateReturnRequestRequest) (*returnsv1.UpdateReturnRequestResponse, error) {
	return p.client.UpdateReturnRequest(ctx, request)
}

func (p sandboxReturnRequestPort) UpdateReturnRequest(ctx context.Context, request *returnsv1.UpdateReturnRequestRequest) (*returnsv1.UpdateReturnRequestResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers UpdateReturnRequest")
	return &returnsv1.UpdateReturnRequestResponse{}, nil
}

func newReturnRequestPort(deps platform.Deps) returnRequestPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReturnRequestPort{log: deps.Logger}
	}
	return grpcReturnRequestPort{client: returnsv1.NewReturnRequestServiceClient(deps.Conn)}
}

// searchQueryExportPort is what the returns domain needs of acme.search.v1.SearchQueryExportService.
type searchQueryExportPort interface {
	ListSearchQueryExports(ctx context.Context, request *searchv1.ListSearchQueryExportsRequest) (*searchv1.ListSearchQueryExportsResponse, error)
}

// grpcSearchQueryExportPort reaches acme.search.v1.SearchQueryExportService over gRPC.
type grpcSearchQueryExportPort struct {
	client searchv1.SearchQueryExportServiceClient
}

// sandboxSearchQueryExportPort answers in the sandbox environment, where acme.search.v1.SearchQueryExportService is not
// deployed.
type sandboxSearchQueryExportPort struct{ log *slog.Logger }

func (p grpcSearchQueryExportPort) ListSearchQueryExports(ctx context.Context, request *searchv1.ListSearchQueryExportsRequest) (*searchv1.ListSearchQueryExportsResponse, error) {
	return p.client.ListSearchQueryExports(ctx, request)
}

func (p sandboxSearchQueryExportPort) ListSearchQueryExports(ctx context.Context, request *searchv1.ListSearchQueryExportsRequest) (*searchv1.ListSearchQueryExportsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListSearchQueryExports")
	return &searchv1.ListSearchQueryExportsResponse{}, nil
}

func newSearchQueryExportPort(deps platform.Deps) searchQueryExportPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSearchQueryExportPort{log: deps.Logger}
	}
	return grpcSearchQueryExportPort{client: searchv1.NewSearchQueryExportServiceClient(deps.Conn)}
}

// subscriptionPlanApprovalPort is what the returns domain needs of
// acme.subscriptions.v1.SubscriptionPlanApprovalService.
type subscriptionPlanApprovalPort interface {
	ApproveSubscriptionPlan(ctx context.Context, request *subscriptionsv1.ApproveSubscriptionPlanRequest) (*subscriptionsv1.ApproveSubscriptionPlanResponse, error)
}

// grpcSubscriptionPlanApprovalPort reaches acme.subscriptions.v1.SubscriptionPlanApprovalService over gRPC.
type grpcSubscriptionPlanApprovalPort struct {
	client subscriptionsv1.SubscriptionPlanApprovalServiceClient
}

// sandboxSubscriptionPlanApprovalPort answers in the sandbox environment, where
// acme.subscriptions.v1.SubscriptionPlanApprovalService is not deployed.
type sandboxSubscriptionPlanApprovalPort struct{ log *slog.Logger }

func (p grpcSubscriptionPlanApprovalPort) ApproveSubscriptionPlan(ctx context.Context, request *subscriptionsv1.ApproveSubscriptionPlanRequest) (*subscriptionsv1.ApproveSubscriptionPlanResponse, error) {
	return p.client.ApproveSubscriptionPlan(ctx, request)
}

func (p sandboxSubscriptionPlanApprovalPort) ApproveSubscriptionPlan(ctx context.Context, request *subscriptionsv1.ApproveSubscriptionPlanRequest) (*subscriptionsv1.ApproveSubscriptionPlanResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ApproveSubscriptionPlan")
	return &subscriptionsv1.ApproveSubscriptionPlanResponse{}, nil
}

func newSubscriptionPlanApprovalPort(deps platform.Deps) subscriptionPlanApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSubscriptionPlanApprovalPort{log: deps.Logger}
	}
	return grpcSubscriptionPlanApprovalPort{client: subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn)}
}

// subscriptionPlanValidationPort is what the returns domain needs of
// acme.subscriptions.v1.SubscriptionPlanValidationService.
type subscriptionPlanValidationPort interface {
	CheckSubscriptionPlanEligibility(ctx context.Context, request *subscriptionsv1.CheckSubscriptionPlanEligibilityRequest) (*subscriptionsv1.CheckSubscriptionPlanEligibilityResponse, error)
}

// grpcSubscriptionPlanValidationPort reaches acme.subscriptions.v1.SubscriptionPlanValidationService over gRPC.
type grpcSubscriptionPlanValidationPort struct {
	client subscriptionsv1.SubscriptionPlanValidationServiceClient
}

// sandboxSubscriptionPlanValidationPort answers in the sandbox environment, where
// acme.subscriptions.v1.SubscriptionPlanValidationService is not deployed.
type sandboxSubscriptionPlanValidationPort struct{ log *slog.Logger }

func (p grpcSubscriptionPlanValidationPort) CheckSubscriptionPlanEligibility(ctx context.Context, request *subscriptionsv1.CheckSubscriptionPlanEligibilityRequest) (*subscriptionsv1.CheckSubscriptionPlanEligibilityResponse, error) {
	return p.client.CheckSubscriptionPlanEligibility(ctx, request)
}

func (p sandboxSubscriptionPlanValidationPort) CheckSubscriptionPlanEligibility(ctx context.Context, request *subscriptionsv1.CheckSubscriptionPlanEligibilityRequest) (*subscriptionsv1.CheckSubscriptionPlanEligibilityResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CheckSubscriptionPlanEligibility")
	return &subscriptionsv1.CheckSubscriptionPlanEligibilityResponse{}, nil
}

func newSubscriptionPlanValidationPort(deps platform.Deps) subscriptionPlanValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSubscriptionPlanValidationPort{log: deps.Logger}
	}
	return grpcSubscriptionPlanValidationPort{client: subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn)}
}

// timesheetQueryPort is what the returns domain needs of acme.timekeeping.v1.TimesheetQueryService.
type timesheetQueryPort interface {
	SummarizeTimesheets(ctx context.Context, request *timekeepingv1.SummarizeTimesheetsRequest) (*timekeepingv1.SummarizeTimesheetsResponse, error)
}

// grpcTimesheetQueryPort reaches acme.timekeeping.v1.TimesheetQueryService over gRPC.
type grpcTimesheetQueryPort struct {
	client timekeepingv1.TimesheetQueryServiceClient
}

// sandboxTimesheetQueryPort answers in the sandbox environment, where acme.timekeeping.v1.TimesheetQueryService is not
// deployed.
type sandboxTimesheetQueryPort struct{ log *slog.Logger }

func (p grpcTimesheetQueryPort) SummarizeTimesheets(ctx context.Context, request *timekeepingv1.SummarizeTimesheetsRequest) (*timekeepingv1.SummarizeTimesheetsResponse, error) {
	return p.client.SummarizeTimesheets(ctx, request)
}

func (p sandboxTimesheetQueryPort) SummarizeTimesheets(ctx context.Context, request *timekeepingv1.SummarizeTimesheetsRequest) (*timekeepingv1.SummarizeTimesheetsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SummarizeTimesheets")
	return &timekeepingv1.SummarizeTimesheetsResponse{}, nil
}

func newTimesheetQueryPort(deps platform.Deps) timesheetQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxTimesheetQueryPort{log: deps.Logger}
	}
	return grpcTimesheetQueryPort{client: timekeepingv1.NewTimesheetQueryServiceClient(deps.Conn)}
}
