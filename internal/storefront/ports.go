package storefront

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	edgegatewayv1 "github.com/hounddogai/protomap-test-golang/gen/acme/edgegateway/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// consentRecordWorkflowPort is what the storefront domain needs of acme.customers.v1.ConsentRecordWorkflowService.
type consentRecordWorkflowPort interface {
	AdvanceConsentRecordWorkflow(ctx context.Context, request *customersv1.AdvanceConsentRecordWorkflowRequest) (*customersv1.AdvanceConsentRecordWorkflowResponse, error)
}

// grpcConsentRecordWorkflowPort reaches acme.customers.v1.ConsentRecordWorkflowService over gRPC.
type grpcConsentRecordWorkflowPort struct {
	client customersv1.ConsentRecordWorkflowServiceClient
}

// sandboxConsentRecordWorkflowPort answers in the sandbox environment, where
// acme.customers.v1.ConsentRecordWorkflowService is not deployed.
type sandboxConsentRecordWorkflowPort struct{ log *slog.Logger }

func (p grpcConsentRecordWorkflowPort) AdvanceConsentRecordWorkflow(ctx context.Context, request *customersv1.AdvanceConsentRecordWorkflowRequest) (*customersv1.AdvanceConsentRecordWorkflowResponse, error) {
	return p.client.AdvanceConsentRecordWorkflow(ctx, request)
}

func (p sandboxConsentRecordWorkflowPort) AdvanceConsentRecordWorkflow(ctx context.Context, request *customersv1.AdvanceConsentRecordWorkflowRequest) (*customersv1.AdvanceConsentRecordWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers AdvanceConsentRecordWorkflow")
	return &customersv1.AdvanceConsentRecordWorkflowResponse{}, nil
}

func newConsentRecordWorkflowPort(deps platform.Deps) consentRecordWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxConsentRecordWorkflowPort{log: deps.Logger}
	}
	return grpcConsentRecordWorkflowPort{client: customersv1.NewConsentRecordWorkflowServiceClient(deps.Conn)}
}

// householdApprovalPort is what the storefront domain needs of acme.customers.v1.HouseholdApprovalService.
type householdApprovalPort interface {
	RejectHousehold(ctx context.Context, request *customersv1.RejectHouseholdRequest) (*customersv1.RejectHouseholdResponse, error)
}

// grpcHouseholdApprovalPort reaches acme.customers.v1.HouseholdApprovalService over gRPC.
type grpcHouseholdApprovalPort struct {
	client customersv1.HouseholdApprovalServiceClient
}

// sandboxHouseholdApprovalPort answers in the sandbox environment, where acme.customers.v1.HouseholdApprovalService is
// not deployed.
type sandboxHouseholdApprovalPort struct{ log *slog.Logger }

func (p grpcHouseholdApprovalPort) RejectHousehold(ctx context.Context, request *customersv1.RejectHouseholdRequest) (*customersv1.RejectHouseholdResponse, error) {
	return p.client.RejectHousehold(ctx, request)
}

func (p sandboxHouseholdApprovalPort) RejectHousehold(ctx context.Context, request *customersv1.RejectHouseholdRequest) (*customersv1.RejectHouseholdResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RejectHousehold")
	return &customersv1.RejectHouseholdResponse{}, nil
}

func newHouseholdApprovalPort(deps platform.Deps) householdApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxHouseholdApprovalPort{log: deps.Logger}
	}
	return grpcHouseholdApprovalPort{client: customersv1.NewHouseholdApprovalServiceClient(deps.Conn)}
}

// landingPageIntegrationPort is what the storefront domain needs of acme.storefront.v1.LandingPageIntegrationService.
type landingPageIntegrationPort interface {
	ReplayLandingPageEvents(ctx context.Context, request *storefrontv1.ReplayLandingPageEventsRequest) (*storefrontv1.ReplayLandingPageEventsResponse, error)
}

// grpcLandingPageIntegrationPort reaches acme.storefront.v1.LandingPageIntegrationService over gRPC.
type grpcLandingPageIntegrationPort struct {
	client storefrontv1.LandingPageIntegrationServiceClient
}

// sandboxLandingPageIntegrationPort answers in the sandbox environment, where
// acme.storefront.v1.LandingPageIntegrationService is not deployed.
type sandboxLandingPageIntegrationPort struct{ log *slog.Logger }

func (p grpcLandingPageIntegrationPort) ReplayLandingPageEvents(ctx context.Context, request *storefrontv1.ReplayLandingPageEventsRequest) (*storefrontv1.ReplayLandingPageEventsResponse, error) {
	return p.client.ReplayLandingPageEvents(ctx, request)
}

func (p sandboxLandingPageIntegrationPort) ReplayLandingPageEvents(ctx context.Context, request *storefrontv1.ReplayLandingPageEventsRequest) (*storefrontv1.ReplayLandingPageEventsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ReplayLandingPageEvents")
	return &storefrontv1.ReplayLandingPageEventsResponse{}, nil
}

func newLandingPageIntegrationPort(deps platform.Deps) landingPageIntegrationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxLandingPageIntegrationPort{log: deps.Logger}
	}
	return grpcLandingPageIntegrationPort{client: storefrontv1.NewLandingPageIntegrationServiceClient(deps.Conn)}
}

// partnerIntegrationGatewayPort is what the storefront domain needs of
// acme.partnerapi.v1.PartnerIntegrationGatewayService.
type partnerIntegrationGatewayPort interface {
	ComposePartnerIntegrationPage(ctx context.Context, request *partnerapiv1.ComposePartnerIntegrationPageRequest) (*partnerapiv1.ComposePartnerIntegrationPageResponse, error)
}

// grpcPartnerIntegrationGatewayPort reaches acme.partnerapi.v1.PartnerIntegrationGatewayService over gRPC.
type grpcPartnerIntegrationGatewayPort struct {
	client partnerapiv1.PartnerIntegrationGatewayServiceClient
}

// sandboxPartnerIntegrationGatewayPort answers in the sandbox environment, where
// acme.partnerapi.v1.PartnerIntegrationGatewayService is not deployed.
type sandboxPartnerIntegrationGatewayPort struct{ log *slog.Logger }

func (p grpcPartnerIntegrationGatewayPort) ComposePartnerIntegrationPage(ctx context.Context, request *partnerapiv1.ComposePartnerIntegrationPageRequest) (*partnerapiv1.ComposePartnerIntegrationPageResponse, error) {
	return p.client.ComposePartnerIntegrationPage(ctx, request)
}

func (p sandboxPartnerIntegrationGatewayPort) ComposePartnerIntegrationPage(ctx context.Context, request *partnerapiv1.ComposePartnerIntegrationPageRequest) (*partnerapiv1.ComposePartnerIntegrationPageResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ComposePartnerIntegrationPage")
	return &partnerapiv1.ComposePartnerIntegrationPageResponse{}, nil
}

func newPartnerIntegrationGatewayPort(deps platform.Deps) partnerIntegrationGatewayPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPartnerIntegrationGatewayPort{log: deps.Logger}
	}
	return grpcPartnerIntegrationGatewayPort{client: partnerapiv1.NewPartnerIntegrationGatewayServiceClient(deps.Conn)}
}

// partnerOrderQueryPort is what the storefront domain needs of acme.partnerapi.v1.PartnerOrderQueryService.
type partnerOrderQueryPort interface {
	SummarizePartnerOrders(ctx context.Context, request *partnerapiv1.SummarizePartnerOrdersRequest) (*partnerapiv1.SummarizePartnerOrdersResponse, error)
}

// grpcPartnerOrderQueryPort reaches acme.partnerapi.v1.PartnerOrderQueryService over gRPC.
type grpcPartnerOrderQueryPort struct {
	client partnerapiv1.PartnerOrderQueryServiceClient
}

// sandboxPartnerOrderQueryPort answers in the sandbox environment, where acme.partnerapi.v1.PartnerOrderQueryService is
// not deployed.
type sandboxPartnerOrderQueryPort struct{ log *slog.Logger }

func (p grpcPartnerOrderQueryPort) SummarizePartnerOrders(ctx context.Context, request *partnerapiv1.SummarizePartnerOrdersRequest) (*partnerapiv1.SummarizePartnerOrdersResponse, error) {
	return p.client.SummarizePartnerOrders(ctx, request)
}

func (p sandboxPartnerOrderQueryPort) SummarizePartnerOrders(ctx context.Context, request *partnerapiv1.SummarizePartnerOrdersRequest) (*partnerapiv1.SummarizePartnerOrdersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SummarizePartnerOrders")
	return &partnerapiv1.SummarizePartnerOrdersResponse{}, nil
}

func newPartnerOrderQueryPort(deps platform.Deps) partnerOrderQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPartnerOrderQueryPort{log: deps.Logger}
	}
	return grpcPartnerOrderQueryPort{client: partnerapiv1.NewPartnerOrderQueryServiceClient(deps.Conn)}
}

// partnerWebhookValidationPort is what the storefront domain needs of
// acme.partnerapi.v1.PartnerWebhookValidationService.
type partnerWebhookValidationPort interface {
	PreviewPartnerWebhook(ctx context.Context, request *partnerapiv1.PreviewPartnerWebhookRequest) (*partnerapiv1.PreviewPartnerWebhookResponse, error)
}

// grpcPartnerWebhookValidationPort reaches acme.partnerapi.v1.PartnerWebhookValidationService over gRPC.
type grpcPartnerWebhookValidationPort struct {
	client partnerapiv1.PartnerWebhookValidationServiceClient
}

// sandboxPartnerWebhookValidationPort answers in the sandbox environment, where
// acme.partnerapi.v1.PartnerWebhookValidationService is not deployed.
type sandboxPartnerWebhookValidationPort struct{ log *slog.Logger }

func (p grpcPartnerWebhookValidationPort) PreviewPartnerWebhook(ctx context.Context, request *partnerapiv1.PreviewPartnerWebhookRequest) (*partnerapiv1.PreviewPartnerWebhookResponse, error) {
	return p.client.PreviewPartnerWebhook(ctx, request)
}

func (p sandboxPartnerWebhookValidationPort) PreviewPartnerWebhook(ctx context.Context, request *partnerapiv1.PreviewPartnerWebhookRequest) (*partnerapiv1.PreviewPartnerWebhookResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewPartnerWebhook")
	return &partnerapiv1.PreviewPartnerWebhookResponse{}, nil
}

func newPartnerWebhookValidationPort(deps platform.Deps) partnerWebhookValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPartnerWebhookValidationPort{log: deps.Logger}
	}
	return grpcPartnerWebhookValidationPort{client: partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn)}
}

// routeBindingPort is what the storefront domain needs of acme.edgegateway.v1.RouteBindingService.
type routeBindingPort interface {
	CreateRouteBinding(ctx context.Context, request *edgegatewayv1.CreateRouteBindingRequest) (*edgegatewayv1.CreateRouteBindingResponse, error)
}

// grpcRouteBindingPort reaches acme.edgegateway.v1.RouteBindingService over gRPC.
type grpcRouteBindingPort struct {
	client edgegatewayv1.RouteBindingServiceClient
}

// sandboxRouteBindingPort answers in the sandbox environment, where acme.edgegateway.v1.RouteBindingService is not
// deployed.
type sandboxRouteBindingPort struct{ log *slog.Logger }

func (p grpcRouteBindingPort) CreateRouteBinding(ctx context.Context, request *edgegatewayv1.CreateRouteBindingRequest) (*edgegatewayv1.CreateRouteBindingResponse, error) {
	return p.client.CreateRouteBinding(ctx, request)
}

func (p sandboxRouteBindingPort) CreateRouteBinding(ctx context.Context, request *edgegatewayv1.CreateRouteBindingRequest) (*edgegatewayv1.CreateRouteBindingResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CreateRouteBinding")
	return &edgegatewayv1.CreateRouteBindingResponse{}, nil
}

func newRouteBindingPort(deps platform.Deps) routeBindingPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRouteBindingPort{log: deps.Logger}
	}
	return grpcRouteBindingPort{client: edgegatewayv1.NewRouteBindingServiceClient(deps.Conn)}
}

// searchQueryExportPort is what the storefront domain needs of acme.search.v1.SearchQueryExportService.
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

// sessionQueryPort is what the storefront domain needs of acme.identity.v1.SessionQueryService.
type sessionQueryPort interface {
	CountSessions(ctx context.Context, request *identityv1.CountSessionsRequest) (*identityv1.CountSessionsResponse, error)
}

// grpcSessionQueryPort reaches acme.identity.v1.SessionQueryService over gRPC.
type grpcSessionQueryPort struct {
	client identityv1.SessionQueryServiceClient
}

// sandboxSessionQueryPort answers in the sandbox environment, where acme.identity.v1.SessionQueryService is not
// deployed.
type sandboxSessionQueryPort struct{ log *slog.Logger }

func (p grpcSessionQueryPort) CountSessions(ctx context.Context, request *identityv1.CountSessionsRequest) (*identityv1.CountSessionsResponse, error) {
	return p.client.CountSessions(ctx, request)
}

func (p sandboxSessionQueryPort) CountSessions(ctx context.Context, request *identityv1.CountSessionsRequest) (*identityv1.CountSessionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CountSessions")
	return &identityv1.CountSessionsResponse{}, nil
}

func newSessionQueryPort(deps platform.Deps) sessionQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSessionQueryPort{log: deps.Logger}
	}
	return grpcSessionQueryPort{client: identityv1.NewSessionQueryServiceClient(deps.Conn)}
}

// storefrontSessionIntegrationPort is what the storefront domain needs of
// acme.storefront.v1.StorefrontSessionIntegrationService.
type storefrontSessionIntegrationPort interface {
	AcknowledgeStorefrontSessionEvent(ctx context.Context, request *storefrontv1.AcknowledgeStorefrontSessionEventRequest) (*storefrontv1.AcknowledgeStorefrontSessionEventResponse, error)
}

// grpcStorefrontSessionIntegrationPort reaches acme.storefront.v1.StorefrontSessionIntegrationService over gRPC.
type grpcStorefrontSessionIntegrationPort struct {
	client storefrontv1.StorefrontSessionIntegrationServiceClient
}

// sandboxStorefrontSessionIntegrationPort answers in the sandbox environment, where
// acme.storefront.v1.StorefrontSessionIntegrationService is not deployed.
type sandboxStorefrontSessionIntegrationPort struct{ log *slog.Logger }

func (p grpcStorefrontSessionIntegrationPort) AcknowledgeStorefrontSessionEvent(ctx context.Context, request *storefrontv1.AcknowledgeStorefrontSessionEventRequest) (*storefrontv1.AcknowledgeStorefrontSessionEventResponse, error) {
	return p.client.AcknowledgeStorefrontSessionEvent(ctx, request)
}

func (p sandboxStorefrontSessionIntegrationPort) AcknowledgeStorefrontSessionEvent(ctx context.Context, request *storefrontv1.AcknowledgeStorefrontSessionEventRequest) (*storefrontv1.AcknowledgeStorefrontSessionEventResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers AcknowledgeStorefrontSessionEvent")
	return &storefrontv1.AcknowledgeStorefrontSessionEventResponse{}, nil
}

func newStorefrontSessionIntegrationPort(deps platform.Deps) storefrontSessionIntegrationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxStorefrontSessionIntegrationPort{log: deps.Logger}
	}
	return grpcStorefrontSessionIntegrationPort{client: storefrontv1.NewStorefrontSessionIntegrationServiceClient(deps.Conn)}
}

// storefrontSessionWorkflowPort is what the storefront domain needs of
// acme.storefront.v1.StorefrontSessionWorkflowService.
type storefrontSessionWorkflowPort interface {
	CancelStorefrontSessionWorkflow(ctx context.Context, request *storefrontv1.CancelStorefrontSessionWorkflowRequest) (*storefrontv1.CancelStorefrontSessionWorkflowResponse, error)
	RetryStorefrontSessionWorkflow(ctx context.Context, request *storefrontv1.RetryStorefrontSessionWorkflowRequest) (*storefrontv1.RetryStorefrontSessionWorkflowResponse, error)
}

// grpcStorefrontSessionWorkflowPort reaches acme.storefront.v1.StorefrontSessionWorkflowService over gRPC.
type grpcStorefrontSessionWorkflowPort struct {
	client storefrontv1.StorefrontSessionWorkflowServiceClient
}

// sandboxStorefrontSessionWorkflowPort answers in the sandbox environment, where
// acme.storefront.v1.StorefrontSessionWorkflowService is not deployed.
type sandboxStorefrontSessionWorkflowPort struct{ log *slog.Logger }

func (p grpcStorefrontSessionWorkflowPort) CancelStorefrontSessionWorkflow(ctx context.Context, request *storefrontv1.CancelStorefrontSessionWorkflowRequest) (*storefrontv1.CancelStorefrontSessionWorkflowResponse, error) {
	return p.client.CancelStorefrontSessionWorkflow(ctx, request)
}

func (p sandboxStorefrontSessionWorkflowPort) CancelStorefrontSessionWorkflow(ctx context.Context, request *storefrontv1.CancelStorefrontSessionWorkflowRequest) (*storefrontv1.CancelStorefrontSessionWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CancelStorefrontSessionWorkflow")
	return &storefrontv1.CancelStorefrontSessionWorkflowResponse{}, nil
}

func (p grpcStorefrontSessionWorkflowPort) RetryStorefrontSessionWorkflow(ctx context.Context, request *storefrontv1.RetryStorefrontSessionWorkflowRequest) (*storefrontv1.RetryStorefrontSessionWorkflowResponse, error) {
	return p.client.RetryStorefrontSessionWorkflow(ctx, request)
}

func (p sandboxStorefrontSessionWorkflowPort) RetryStorefrontSessionWorkflow(ctx context.Context, request *storefrontv1.RetryStorefrontSessionWorkflowRequest) (*storefrontv1.RetryStorefrontSessionWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RetryStorefrontSessionWorkflow")
	return &storefrontv1.RetryStorefrontSessionWorkflowResponse{}, nil
}

func newStorefrontSessionWorkflowPort(deps platform.Deps) storefrontSessionWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxStorefrontSessionWorkflowPort{log: deps.Logger}
	}
	return grpcStorefrontSessionWorkflowPort{client: storefrontv1.NewStorefrontSessionWorkflowServiceClient(deps.Conn)}
}
