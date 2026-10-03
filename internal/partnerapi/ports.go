package partnerapi

import (
	"context"
	"log/slog"

	employeeportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/employeeportal/v1"
	fanportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fanportal/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// fanSessionPort is what the partnerapi domain needs of acme.fanportal.v1.FanSessionService.
type fanSessionPort interface {
	CreateFanSession(ctx context.Context, request *fanportalv1.CreateFanSessionRequest) (*fanportalv1.CreateFanSessionResponse, error)
}

// grpcFanSessionPort reaches acme.fanportal.v1.FanSessionService over gRPC.
type grpcFanSessionPort struct {
	client fanportalv1.FanSessionServiceClient
}

// sandboxFanSessionPort answers in the sandbox environment, where acme.fanportal.v1.FanSessionService is not deployed.
type sandboxFanSessionPort struct{ log *slog.Logger }

func (p grpcFanSessionPort) CreateFanSession(ctx context.Context, request *fanportalv1.CreateFanSessionRequest) (*fanportalv1.CreateFanSessionResponse, error) {
	return p.client.CreateFanSession(ctx, request)
}

func (p sandboxFanSessionPort) CreateFanSession(ctx context.Context, request *fanportalv1.CreateFanSessionRequest) (*fanportalv1.CreateFanSessionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CreateFanSession")
	return &fanportalv1.CreateFanSessionResponse{}, nil
}

func newFanSessionPort(deps platform.Deps) fanSessionPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFanSessionPort{log: deps.Logger}
	}
	return grpcFanSessionPort{client: fanportalv1.NewFanSessionServiceClient(deps.Conn)}
}

// partnerIntegrationIntegrationPort is what the partnerapi domain needs of
// acme.partnerapi.v1.PartnerIntegrationIntegrationService.
type partnerIntegrationIntegrationPort interface {
	ListPartnerIntegrationSources(ctx context.Context, request *partnerapiv1.ListPartnerIntegrationSourcesRequest) (*partnerapiv1.ListPartnerIntegrationSourcesResponse, error)
}

// grpcPartnerIntegrationIntegrationPort reaches acme.partnerapi.v1.PartnerIntegrationIntegrationService over gRPC.
type grpcPartnerIntegrationIntegrationPort struct {
	client partnerapiv1.PartnerIntegrationIntegrationServiceClient
}

// sandboxPartnerIntegrationIntegrationPort answers in the sandbox environment, where
// acme.partnerapi.v1.PartnerIntegrationIntegrationService is not deployed.
type sandboxPartnerIntegrationIntegrationPort struct{ log *slog.Logger }

func (p grpcPartnerIntegrationIntegrationPort) ListPartnerIntegrationSources(ctx context.Context, request *partnerapiv1.ListPartnerIntegrationSourcesRequest) (*partnerapiv1.ListPartnerIntegrationSourcesResponse, error) {
	return p.client.ListPartnerIntegrationSources(ctx, request)
}

func (p sandboxPartnerIntegrationIntegrationPort) ListPartnerIntegrationSources(ctx context.Context, request *partnerapiv1.ListPartnerIntegrationSourcesRequest) (*partnerapiv1.ListPartnerIntegrationSourcesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListPartnerIntegrationSources")
	return &partnerapiv1.ListPartnerIntegrationSourcesResponse{}, nil
}

func newPartnerIntegrationIntegrationPort(deps platform.Deps) partnerIntegrationIntegrationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPartnerIntegrationIntegrationPort{log: deps.Logger}
	}
	return grpcPartnerIntegrationIntegrationPort{client: partnerapiv1.NewPartnerIntegrationIntegrationServiceClient(deps.Conn)}
}

// partnerIntegrationNotificationPort is what the partnerapi domain needs of
// acme.partnerapi.v1.PartnerIntegrationNotificationService.
type partnerIntegrationNotificationPort interface {
	UnsubscribePartnerIntegrationUpdates(ctx context.Context, request *partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest) (*partnerapiv1.UnsubscribePartnerIntegrationUpdatesResponse, error)
}

// grpcPartnerIntegrationNotificationPort reaches acme.partnerapi.v1.PartnerIntegrationNotificationService over gRPC.
type grpcPartnerIntegrationNotificationPort struct {
	client partnerapiv1.PartnerIntegrationNotificationServiceClient
}

// sandboxPartnerIntegrationNotificationPort answers in the sandbox environment, where
// acme.partnerapi.v1.PartnerIntegrationNotificationService is not deployed.
type sandboxPartnerIntegrationNotificationPort struct{ log *slog.Logger }

func (p grpcPartnerIntegrationNotificationPort) UnsubscribePartnerIntegrationUpdates(ctx context.Context, request *partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest) (*partnerapiv1.UnsubscribePartnerIntegrationUpdatesResponse, error) {
	return p.client.UnsubscribePartnerIntegrationUpdates(ctx, request)
}

func (p sandboxPartnerIntegrationNotificationPort) UnsubscribePartnerIntegrationUpdates(ctx context.Context, request *partnerapiv1.UnsubscribePartnerIntegrationUpdatesRequest) (*partnerapiv1.UnsubscribePartnerIntegrationUpdatesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers UnsubscribePartnerIntegrationUpdates")
	return &partnerapiv1.UnsubscribePartnerIntegrationUpdatesResponse{}, nil
}

func newPartnerIntegrationNotificationPort(deps platform.Deps) partnerIntegrationNotificationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPartnerIntegrationNotificationPort{log: deps.Logger}
	}
	return grpcPartnerIntegrationNotificationPort{client: partnerapiv1.NewPartnerIntegrationNotificationServiceClient(deps.Conn)}
}

// partnerIntegrationReportingPort is what the partnerapi domain needs of
// acme.partnerapi.v1.PartnerIntegrationReportingService.
type partnerIntegrationReportingPort interface {
	DeletePartnerIntegrationReport(ctx context.Context, request *partnerapiv1.DeletePartnerIntegrationReportRequest) (*partnerapiv1.DeletePartnerIntegrationReportResponse, error)
	GeneratePartnerIntegrationReport(ctx context.Context, request *partnerapiv1.GeneratePartnerIntegrationReportRequest) (*partnerapiv1.GeneratePartnerIntegrationReportResponse, error)
}

// grpcPartnerIntegrationReportingPort reaches acme.partnerapi.v1.PartnerIntegrationReportingService over gRPC.
type grpcPartnerIntegrationReportingPort struct {
	client partnerapiv1.PartnerIntegrationReportingServiceClient
}

// sandboxPartnerIntegrationReportingPort answers in the sandbox environment, where
// acme.partnerapi.v1.PartnerIntegrationReportingService is not deployed.
type sandboxPartnerIntegrationReportingPort struct{ log *slog.Logger }

func (p grpcPartnerIntegrationReportingPort) DeletePartnerIntegrationReport(ctx context.Context, request *partnerapiv1.DeletePartnerIntegrationReportRequest) (*partnerapiv1.DeletePartnerIntegrationReportResponse, error) {
	return p.client.DeletePartnerIntegrationReport(ctx, request)
}

func (p sandboxPartnerIntegrationReportingPort) DeletePartnerIntegrationReport(ctx context.Context, request *partnerapiv1.DeletePartnerIntegrationReportRequest) (*partnerapiv1.DeletePartnerIntegrationReportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers DeletePartnerIntegrationReport")
	return &partnerapiv1.DeletePartnerIntegrationReportResponse{}, nil
}

func (p grpcPartnerIntegrationReportingPort) GeneratePartnerIntegrationReport(ctx context.Context, request *partnerapiv1.GeneratePartnerIntegrationReportRequest) (*partnerapiv1.GeneratePartnerIntegrationReportResponse, error) {
	return p.client.GeneratePartnerIntegrationReport(ctx, request)
}

func (p sandboxPartnerIntegrationReportingPort) GeneratePartnerIntegrationReport(ctx context.Context, request *partnerapiv1.GeneratePartnerIntegrationReportRequest) (*partnerapiv1.GeneratePartnerIntegrationReportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GeneratePartnerIntegrationReport")
	return &partnerapiv1.GeneratePartnerIntegrationReportResponse{}, nil
}

func newPartnerIntegrationReportingPort(deps platform.Deps) partnerIntegrationReportingPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPartnerIntegrationReportingPort{log: deps.Logger}
	}
	return grpcPartnerIntegrationReportingPort{client: partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn)}
}

// partnerOrderApprovalPort is what the partnerapi domain needs of acme.partnerapi.v1.PartnerOrderApprovalService.
type partnerOrderApprovalPort interface {
	ListPendingPartnerOrders(ctx context.Context, request *partnerapiv1.ListPendingPartnerOrdersRequest) (*partnerapiv1.ListPendingPartnerOrdersResponse, error)
	RecallPartnerOrder(ctx context.Context, request *partnerapiv1.RecallPartnerOrderRequest) (*partnerapiv1.RecallPartnerOrderResponse, error)
}

// grpcPartnerOrderApprovalPort reaches acme.partnerapi.v1.PartnerOrderApprovalService over gRPC.
type grpcPartnerOrderApprovalPort struct {
	client partnerapiv1.PartnerOrderApprovalServiceClient
}

// sandboxPartnerOrderApprovalPort answers in the sandbox environment, where
// acme.partnerapi.v1.PartnerOrderApprovalService is not deployed.
type sandboxPartnerOrderApprovalPort struct{ log *slog.Logger }

func (p grpcPartnerOrderApprovalPort) ListPendingPartnerOrders(ctx context.Context, request *partnerapiv1.ListPendingPartnerOrdersRequest) (*partnerapiv1.ListPendingPartnerOrdersResponse, error) {
	return p.client.ListPendingPartnerOrders(ctx, request)
}

func (p sandboxPartnerOrderApprovalPort) ListPendingPartnerOrders(ctx context.Context, request *partnerapiv1.ListPendingPartnerOrdersRequest) (*partnerapiv1.ListPendingPartnerOrdersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListPendingPartnerOrders")
	return &partnerapiv1.ListPendingPartnerOrdersResponse{}, nil
}

func (p grpcPartnerOrderApprovalPort) RecallPartnerOrder(ctx context.Context, request *partnerapiv1.RecallPartnerOrderRequest) (*partnerapiv1.RecallPartnerOrderResponse, error) {
	return p.client.RecallPartnerOrder(ctx, request)
}

func (p sandboxPartnerOrderApprovalPort) RecallPartnerOrder(ctx context.Context, request *partnerapiv1.RecallPartnerOrderRequest) (*partnerapiv1.RecallPartnerOrderResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecallPartnerOrder")
	return &partnerapiv1.RecallPartnerOrderResponse{}, nil
}

func newPartnerOrderApprovalPort(deps platform.Deps) partnerOrderApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPartnerOrderApprovalPort{log: deps.Logger}
	}
	return grpcPartnerOrderApprovalPort{client: partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn)}
}

// partnerWebhookValidationPort is what the partnerapi domain needs of
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

// portalHomePort is what the partnerapi domain needs of acme.employeeportal.v1.PortalHomeService.
type portalHomePort interface {
	ClonePortalHome(ctx context.Context, request *employeeportalv1.ClonePortalHomeRequest) (*employeeportalv1.ClonePortalHomeResponse, error)
}

// grpcPortalHomePort reaches acme.employeeportal.v1.PortalHomeService over gRPC.
type grpcPortalHomePort struct {
	client employeeportalv1.PortalHomeServiceClient
}

// sandboxPortalHomePort answers in the sandbox environment, where acme.employeeportal.v1.PortalHomeService is not
// deployed.
type sandboxPortalHomePort struct{ log *slog.Logger }

func (p grpcPortalHomePort) ClonePortalHome(ctx context.Context, request *employeeportalv1.ClonePortalHomeRequest) (*employeeportalv1.ClonePortalHomeResponse, error) {
	return p.client.ClonePortalHome(ctx, request)
}

func (p sandboxPortalHomePort) ClonePortalHome(ctx context.Context, request *employeeportalv1.ClonePortalHomeRequest) (*employeeportalv1.ClonePortalHomeResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ClonePortalHome")
	return &employeeportalv1.ClonePortalHomeResponse{}, nil
}

func newPortalHomePort(deps platform.Deps) portalHomePort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPortalHomePort{log: deps.Logger}
	}
	return grpcPortalHomePort{client: employeeportalv1.NewPortalHomeServiceClient(deps.Conn)}
}

// reviewVoteWorkflowPort is what the partnerapi domain needs of acme.reviews.v1.ReviewVoteWorkflowService.
type reviewVoteWorkflowPort interface {
	PauseReviewVoteWorkflow(ctx context.Context, request *reviewsv1.PauseReviewVoteWorkflowRequest) (*reviewsv1.PauseReviewVoteWorkflowResponse, error)
}

// grpcReviewVoteWorkflowPort reaches acme.reviews.v1.ReviewVoteWorkflowService over gRPC.
type grpcReviewVoteWorkflowPort struct {
	client reviewsv1.ReviewVoteWorkflowServiceClient
}

// sandboxReviewVoteWorkflowPort answers in the sandbox environment, where acme.reviews.v1.ReviewVoteWorkflowService is
// not deployed.
type sandboxReviewVoteWorkflowPort struct{ log *slog.Logger }

func (p grpcReviewVoteWorkflowPort) PauseReviewVoteWorkflow(ctx context.Context, request *reviewsv1.PauseReviewVoteWorkflowRequest) (*reviewsv1.PauseReviewVoteWorkflowResponse, error) {
	return p.client.PauseReviewVoteWorkflow(ctx, request)
}

func (p sandboxReviewVoteWorkflowPort) PauseReviewVoteWorkflow(ctx context.Context, request *reviewsv1.PauseReviewVoteWorkflowRequest) (*reviewsv1.PauseReviewVoteWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PauseReviewVoteWorkflow")
	return &reviewsv1.PauseReviewVoteWorkflowResponse{}, nil
}

func newReviewVoteWorkflowPort(deps platform.Deps) reviewVoteWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReviewVoteWorkflowPort{log: deps.Logger}
	}
	return grpcReviewVoteWorkflowPort{client: reviewsv1.NewReviewVoteWorkflowServiceClient(deps.Conn)}
}

// searchQueryHistoryPort is what the partnerapi domain needs of acme.search.v1.SearchQueryHistoryService.
type searchQueryHistoryPort interface {
	RevertSearchQuery(ctx context.Context, request *searchv1.RevertSearchQueryRequest) (*searchv1.RevertSearchQueryResponse, error)
}

// grpcSearchQueryHistoryPort reaches acme.search.v1.SearchQueryHistoryService over gRPC.
type grpcSearchQueryHistoryPort struct {
	client searchv1.SearchQueryHistoryServiceClient
}

// sandboxSearchQueryHistoryPort answers in the sandbox environment, where acme.search.v1.SearchQueryHistoryService is
// not deployed.
type sandboxSearchQueryHistoryPort struct{ log *slog.Logger }

func (p grpcSearchQueryHistoryPort) RevertSearchQuery(ctx context.Context, request *searchv1.RevertSearchQueryRequest) (*searchv1.RevertSearchQueryResponse, error) {
	return p.client.RevertSearchQuery(ctx, request)
}

func (p sandboxSearchQueryHistoryPort) RevertSearchQuery(ctx context.Context, request *searchv1.RevertSearchQueryRequest) (*searchv1.RevertSearchQueryResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RevertSearchQuery")
	return &searchv1.RevertSearchQueryResponse{}, nil
}

func newSearchQueryHistoryPort(deps platform.Deps) searchQueryHistoryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSearchQueryHistoryPort{log: deps.Logger}
	}
	return grpcSearchQueryHistoryPort{client: searchv1.NewSearchQueryHistoryServiceClient(deps.Conn)}
}

// synonymValidationPort is what the partnerapi domain needs of acme.search.v1.SynonymValidationService.
type synonymValidationPort interface {
	ValidateSynonym(ctx context.Context, request *searchv1.ValidateSynonymRequest) (*searchv1.ValidateSynonymResponse, error)
}

// grpcSynonymValidationPort reaches acme.search.v1.SynonymValidationService over gRPC.
type grpcSynonymValidationPort struct {
	client searchv1.SynonymValidationServiceClient
}

// sandboxSynonymValidationPort answers in the sandbox environment, where acme.search.v1.SynonymValidationService is not
// deployed.
type sandboxSynonymValidationPort struct{ log *slog.Logger }

func (p grpcSynonymValidationPort) ValidateSynonym(ctx context.Context, request *searchv1.ValidateSynonymRequest) (*searchv1.ValidateSynonymResponse, error) {
	return p.client.ValidateSynonym(ctx, request)
}

func (p sandboxSynonymValidationPort) ValidateSynonym(ctx context.Context, request *searchv1.ValidateSynonymRequest) (*searchv1.ValidateSynonymResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidateSynonym")
	return &searchv1.ValidateSynonymResponse{}, nil
}

func newSynonymValidationPort(deps platform.Deps) synonymValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSynonymValidationPort{log: deps.Logger}
	}
	return grpcSynonymValidationPort{client: searchv1.NewSynonymValidationServiceClient(deps.Conn)}
}
