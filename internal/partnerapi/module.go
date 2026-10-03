package partnerapi

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fanportalv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fanportal/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	telehealthv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telehealth/v1"
	webbffv1 "github.com/hounddogai/protomap-test-golang/gen/acme/webbff/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the partnerapi domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.authorization.v1.AccessPolicyQueryService", authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.cart.v1.SavedListWorkflowService", cartv1.NewSavedListWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.ApiCredentialNotificationService", identityv1.NewApiCredentialNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.UserQueryService", identityv1.NewUserQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.partnerapi.v1.PartnerIntegrationReportingService", partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.partnerapi.v1.PartnerOrderApprovalService", partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.partnerapi.v1.PartnerOrderIntegrationService", partnerapiv1.NewPartnerOrderIntegrationServiceClient(deps.Conn))
	deps.Clients.Put("acme.partnerapi.v1.PartnerOrderWorkflowService", partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.partnerapi.v1.PartnerWebhookApprovalService", partnerapiv1.NewPartnerWebhookApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.RenewalScheduleWorkflowService", subscriptionsv1.NewRenewalScheduleWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.telehealth.v1.VideoVisitQueryService", telehealthv1.NewVideoVisitQueryServiceClient(deps.Conn))
	partnerapiv1.RegisterPartnerIntegrationGatewayServiceServer(server, NewPartnerIntegrationGatewayServer(deps))
	partnerapiv1.RegisterPartnerOrderServiceServer(server, NewPartnerOrderServer(deps))
	partnerapiv1.RegisterPartnerWebhookQueryServiceServer(server, NewPartnerWebhookQueryServer(deps))
	partnerapiv1.RegisterPartnerIntegrationServiceServer(server, NewPartnerIntegrationServer(deps))
	partnerapiv1.RegisterPartnerOrderQueryServiceServer(server, NewPartnerOrderQueryServer(deps))
	partnerapiv1.RegisterPartnerWebhookNotificationServiceServer(server, NewPartnerWebhookNotificationServer(deps))
	partnerapiv1.RegisterPartnerIntegrationQueryServiceServer(server, NewPartnerIntegrationQueryServer(deps))
	partnerapiv1.RegisterPartnerOrderNotificationServiceServer(server, NewPartnerOrderNotificationServer(deps))
	partnerapiv1.RegisterPartnerWebhookIntegrationServiceServer(server, NewPartnerWebhookIntegrationServer(deps))
	partnerIntegrationNotificationServer := NewPartnerIntegrationNotificationServer(deps)
	server.RegisterService(&partnerapiv1.PartnerIntegrationNotificationService_ServiceDesc, partnerIntegrationNotificationServer)
	partnerOrderIntegrationServer := NewPartnerOrderIntegrationServer(deps)
	server.RegisterService(&partnerapiv1.PartnerOrderIntegrationService_ServiceDesc, partnerOrderIntegrationServer)
	partnerapiv1.RegisterPartnerWebhookWorkflowServiceServer(server, NewPartnerWebhookWorkflowServer(deps))
	partnerapiv1.RegisterPartnerIntegrationIntegrationServiceServer(server, NewPartnerIntegrationIntegrationServer(deps))
	partnerapiv1.RegisterPartnerOrderWorkflowServiceServer(server, NewPartnerOrderWorkflowServer(deps))
	partnerWebhookReportingServer := NewPartnerWebhookReportingServer(deps)
	server.RegisterService(&partnerapiv1.PartnerWebhookReportingService_ServiceDesc, partnerWebhookReportingServer)
	partnerapiv1.RegisterPartnerIntegrationWorkflowServiceServer(server, NewPartnerIntegrationWorkflowServer(deps))
	partnerapiv1.RegisterPartnerOrderReportingServiceServer(server, NewPartnerOrderReportingServer(deps))
	partnerWebhookApprovalServer := NewPartnerWebhookApprovalServer(deps)
	server.RegisterService(&partnerapiv1.PartnerWebhookApprovalService_ServiceDesc, partnerWebhookApprovalServer)
	partnerapiv1.RegisterPartnerIntegrationReportingServiceServer(server, NewPartnerIntegrationReportingServer(deps))
	partnerapiv1.RegisterPartnerOrderApprovalServiceServer(server, NewPartnerOrderApprovalServer(deps))
	partnerapiv1.RegisterPartnerWebhookValidationServiceServer(server, NewPartnerWebhookValidationServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	listPartnerWebhookWorkflowsCall      func(context.Context, *partnerapiv1.ListPartnerWebhookWorkflowsRequest) (*partnerapiv1.ListPartnerWebhookWorkflowsResponse, error)
	scorePartnerWebhookCall              func(context.Context, *partnerapiv1.ScorePartnerWebhookRequest, ...grpc.CallOption) (*partnerapiv1.ScorePartnerWebhookResponse, error)
	countHouseholdTotalsCall             func(context.Context, *customersv1.CountHouseholdTotalsRequest, ...grpc.CallOption) (*customersv1.CountHouseholdTotalsResponse, error)
	explainPartnerWebhookDecisionCall    func(context.Context, *partnerapiv1.ExplainPartnerWebhookDecisionRequest) (*partnerapiv1.ExplainPartnerWebhookDecisionResponse, error)
	pauseCouponWorkflowCall              func(context.Context, *promotionsv1.PauseCouponWorkflowRequest) (*promotionsv1.PauseCouponWorkflowResponse, error)
	resumePartnerIntegrationWorkflowCall func(context.Context, *partnerapiv1.ResumePartnerIntegrationWorkflowRequest, ...grpc.CallOption) (*partnerapiv1.ResumePartnerIntegrationWorkflowResponse, error)
	resumePlacementWorkflowCall          func(context.Context, *merchandisingv1.ResumePlacementWorkflowRequest, ...grpc.CallOption) (*merchandisingv1.ResumePlacementWorkflowResponse, error)
	createPageCompositionCall            func(context.Context, *webbffv1.CreatePageCompositionRequest) (*webbffv1.CreatePageCompositionResponse, error)
	resolveFanSessionsCall               func(context.Context, *fanportalv1.ResolveFanSessionsRequest) (*fanportalv1.ResolveFanSessionsResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	partnerWebhookWorkflowForListPartnerWebhookWorkflows := partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn)
	listPartnerWebhookWorkflowsCall = func(ctx context.Context, request *partnerapiv1.ListPartnerWebhookWorkflowsRequest) (*partnerapiv1.ListPartnerWebhookWorkflowsResponse, error) {
		return partnerWebhookWorkflowForListPartnerWebhookWorkflows.ListPartnerWebhookWorkflows(ctx, request)
	}
	partnerWebhookValidationForScorePartnerWebhook := partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn)
	scorePartnerWebhookCall = partnerWebhookValidationForScorePartnerWebhook.ScorePartnerWebhook
	householdReportingForCountHouseholdTotals := customersv1.NewHouseholdReportingServiceClient(deps.Conn)
	countHouseholdTotalsCall = householdReportingForCountHouseholdTotals.CountHouseholdTotals
	partnerWebhookValidationForExplainPartnerWebhookDecision := partnerapiv1.NewPartnerWebhookValidationServiceClient(deps.Conn)
	explainPartnerWebhookDecisionCall = func(ctx context.Context, request *partnerapiv1.ExplainPartnerWebhookDecisionRequest) (*partnerapiv1.ExplainPartnerWebhookDecisionResponse, error) {
		return partnerWebhookValidationForExplainPartnerWebhookDecision.ExplainPartnerWebhookDecision(ctx, request)
	}
	couponWorkflowForPauseCouponWorkflow := promotionsv1.NewCouponWorkflowServiceClient(deps.Conn)
	pauseCouponWorkflowCall = func(ctx context.Context, request *promotionsv1.PauseCouponWorkflowRequest) (*promotionsv1.PauseCouponWorkflowResponse, error) {
		return couponWorkflowForPauseCouponWorkflow.PauseCouponWorkflow(ctx, request)
	}
	partnerIntegrationWorkflowForResumePartnerIntegrationWorkflow := partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn)
	resumePartnerIntegrationWorkflowCall = partnerIntegrationWorkflowForResumePartnerIntegrationWorkflow.ResumePartnerIntegrationWorkflow
	placementWorkflowForResumePlacementWorkflow := merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn)
	resumePlacementWorkflowCall = placementWorkflowForResumePlacementWorkflow.ResumePlacementWorkflow
	pageCompositionForCreatePageComposition := webbffv1.NewPageCompositionServiceClient(deps.Conn)
	createPageCompositionCall = func(ctx context.Context, request *webbffv1.CreatePageCompositionRequest) (*webbffv1.CreatePageCompositionResponse, error) {
		return pageCompositionForCreatePageComposition.CreatePageComposition(ctx, request)
	}
	fanSessionQueryForResolveFanSessions := fanportalv1.NewFanSessionQueryServiceClient(deps.Conn)
	resolveFanSessionsCall = func(ctx context.Context, request *fanportalv1.ResolveFanSessionsRequest) (*fanportalv1.ResolveFanSessionsResponse, error) {
		return fanSessionQueryForResolveFanSessions.ResolveFanSessions(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                              *slog.Logger
	householdReportingClient         customersv1.HouseholdReportingServiceClient
	partnerIntegrationWorkflowClient partnerapiv1.PartnerIntegrationWorkflowServiceClient
	sellerListingReportingClient     sellersv1.SellerListingReportingServiceClient
	partnerOrderApprovalClient       partnerapiv1.PartnerOrderApprovalServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                              deps.Logger,
		householdReportingClient:         customersv1.NewHouseholdReportingServiceClient(deps.Conn),
		partnerIntegrationWorkflowClient: partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn),
		sellerListingReportingClient:     sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		partnerOrderApprovalClient:       partnerapiv1.NewPartnerOrderApprovalServiceClient(deps.Conn),
	}
	go w.consume(ctx, "partnerapi.browse_partner_webhooks.list_household_reports", deps.Bus.Subscribe("partnerapi.browse_partner_webhooks.list_household_reports"), w.handlePartnerapiBrowsePartnerWebhooksListHouseholdReports)
	go w.consume(ctx, "partnerapi.connect_partner_webhook_source.resume_partner_integration_workflow", deps.Bus.Subscribe("partnerapi.connect_partner_webhook_source.resume_partner_integration_workflow"), w.handlePartnerapiConnectPartnerWebhookSourceResumePartnerIntegrationWorkflow)
	go w.consume(ctx, "partnerapi.replay_partner_order_events.list_seller_listing_reports", deps.Bus.Subscribe("partnerapi.replay_partner_order_events.list_seller_listing_reports"), w.handlePartnerapiReplayPartnerOrderEventsListSellerListingReports)
	go w.consume(ctx, "partnerapi.share_partner_integration_report.approve_partner_order", deps.Bus.Subscribe("partnerapi.share_partner_integration_report.approve_partner_order"), w.handlePartnerapiSharePartnerIntegrationReportApprovePartnerOrder)
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

// handlePartnerapiBrowsePartnerWebhooksListHouseholdReports makes the call the
// partnerapi.browse_partner_webhooks.list_household_reports event asks for.
func (w *workers) handlePartnerapiBrowsePartnerWebhooksListHouseholdReports(ctx context.Context, event eventbus.Event) error {
	_, err := w.householdReportingClient.ListHouseholdReports(ctx, &customersv1.ListHouseholdReportsRequest{
		Filter: &customersv1.ListHouseholdReportsRequest_Filter{Region: event.Attributes["filter.region"]},
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

// handlePartnerapiConnectPartnerWebhookSourceResumePartnerIntegrationWorkflow makes the call the
// partnerapi.connect_partner_webhook_source.resume_partner_integration_workflow event asks for.
func (w *workers) handlePartnerapiConnectPartnerWebhookSourceResumePartnerIntegrationWorkflow(ctx context.Context, event eventbus.Event) error {
	_, err := w.partnerIntegrationWorkflowClient.ResumePartnerIntegrationWorkflow(ctx, &partnerapiv1.ResumePartnerIntegrationWorkflowRequest{
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Contact: &commonv1.ContactInfo{
				Email: event.Attributes["partner_integration.contact.email"],
			},
			Limits: []*partnerapiv1.PartnerIntegrationLimit{&partnerapiv1.PartnerIntegrationLimit{Name: event.Attributes["partner_integration.limits.name"]}},
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

// handlePartnerapiReplayPartnerOrderEventsListSellerListingReports makes the call the
// partnerapi.replay_partner_order_events.list_seller_listing_reports event asks for.
func (w *workers) handlePartnerapiReplayPartnerOrderEventsListSellerListingReports(ctx context.Context, event eventbus.Event) error {
	_, err := w.sellerListingReportingClient.ListSellerListingReports(ctx, &sellersv1.ListSellerListingReportsRequest{
		Filter: &sellersv1.ListSellerListingReportsRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
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

// handlePartnerapiSharePartnerIntegrationReportApprovePartnerOrder makes the call the
// partnerapi.share_partner_integration_report.approve_partner_order event asks for.
func (w *workers) handlePartnerapiSharePartnerIntegrationReportApprovePartnerOrder(ctx context.Context, event eventbus.Event) error {
	_, err := w.partnerOrderApprovalClient.ApprovePartnerOrder(ctx, &partnerapiv1.ApprovePartnerOrderRequest{
		PartnerOrder: &partnerapiv1.PartnerOrder{
			Counterparty: &sellersv1.SellerRef{Email: event.Attributes["partner_order.counterparty.email"]},
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
