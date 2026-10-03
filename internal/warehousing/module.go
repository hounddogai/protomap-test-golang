package warehousing

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	onboardingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/onboarding/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the warehousing domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.identity.v1.ApiCredentialValidationService", identityv1.NewApiCredentialValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.merchandising.v1.MerchandisingRuleWorkflowService", merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.onboarding.v1.OnboardingPlanService", onboardingv1.NewOnboardingPlanServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SynonymValidationService", searchv1.NewSynonymValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerScorecardValidationService", sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.subscriptions.v1.UsageRecordCalculationService", subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.warehousing.v1.PickListValidationService", warehousingv1.NewPickListValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.warehousing.v1.StorageBinQueryService", warehousingv1.NewStorageBinQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.warehousing.v1.WarehouseQueryService", warehousingv1.NewWarehouseQueryServiceClient(deps.Conn))
	warehousingv1.RegisterWarehouseServiceServer(server, NewWarehouseServer(deps))
	warehousingv1.RegisterStorageBinQueryServiceServer(server, NewStorageBinQueryServer(deps))
	warehousingv1.RegisterPickListWorkflowServiceServer(server, NewPickListWorkflowServer(deps))
	warehousingv1.RegisterPutawayTaskApprovalServiceServer(server, NewPutawayTaskApprovalServer(deps))
	warehouseQueryServer := NewWarehouseQueryServer(deps)
	server.RegisterService(&warehousingv1.WarehouseQueryService_ServiceDesc, warehouseQueryServer)
	warehousingv1.RegisterStorageBinWorkflowServiceServer(server, NewStorageBinWorkflowServer(deps))
	warehousingv1.RegisterPickListApprovalServiceServer(server, NewPickListApprovalServer(deps))
	warehousingv1.RegisterPutawayTaskValidationServiceServer(server, NewPutawayTaskValidationServer(deps))
	warehouseWorkflowServer := NewWarehouseWorkflowServer(deps)
	server.RegisterService(&warehousingv1.WarehouseWorkflowService_ServiceDesc, warehouseWorkflowServer)
	warehousingv1.RegisterStorageBinApprovalServiceServer(server, NewStorageBinApprovalServer(deps))
	warehousingv1.RegisterPickListValidationServiceServer(server, NewPickListValidationServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	peekWarehouseCall                  func(context.Context, *warehousingv1.PeekWarehouseRequest, ...grpc.CallOption) (*warehousingv1.PeekWarehouseResponse, error)
	listAuditEventsCall                func(context.Context, *auditv1.ListAuditEventsRequest) (*auditv1.ListAuditEventsResponse, error)
	escalateRewardCall                 func(context.Context, *loyaltyv1.EscalateRewardRequest) (*loyaltyv1.EscalateRewardResponse, error)
	resolveStorageBinsCall             func(context.Context, *warehousingv1.ResolveStorageBinsRequest, ...grpc.CallOption) (*warehousingv1.ResolveStorageBinsResponse, error)
	pauseMerchandisingRuleWorkflowCall func(context.Context, *merchandisingv1.PauseMerchandisingRuleWorkflowRequest) (*merchandisingv1.PauseMerchandisingRuleWorkflowResponse, error)
	updateCustomerCall                 func(context.Context, *customersv1.UpdateCustomerRequest, ...grpc.CallOption) (*customersv1.UpdateCustomerResponse, error)
	findSearchQueriesCall              func(context.Context, *searchv1.FindSearchQueriesRequest) (*searchv1.FindSearchQueriesResponse, error)
	browseWarehousesCall               func(context.Context, *warehousingv1.BrowseWarehousesRequest, ...grpc.CallOption) (*warehousingv1.BrowseWarehousesResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	warehouseQueryForPeekWarehouse := warehousingv1.NewWarehouseQueryServiceClient(deps.Conn)
	peekWarehouseCall = warehouseQueryForPeekWarehouse.PeekWarehouse
	auditEventForListAuditEvents := auditv1.NewAuditEventServiceClient(deps.Conn)
	listAuditEventsCall = func(ctx context.Context, request *auditv1.ListAuditEventsRequest) (*auditv1.ListAuditEventsResponse, error) {
		return auditEventForListAuditEvents.ListAuditEvents(ctx, request)
	}
	rewardApprovalForEscalateReward := loyaltyv1.NewRewardApprovalServiceClient(deps.Conn)
	escalateRewardCall = func(ctx context.Context, request *loyaltyv1.EscalateRewardRequest) (*loyaltyv1.EscalateRewardResponse, error) {
		return rewardApprovalForEscalateReward.EscalateReward(ctx, request)
	}
	storageBinQueryForResolveStorageBins := warehousingv1.NewStorageBinQueryServiceClient(deps.Conn)
	resolveStorageBinsCall = storageBinQueryForResolveStorageBins.ResolveStorageBins
	merchandisingRuleWorkflowForPauseMerchandisingRuleWorkflow := merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn)
	pauseMerchandisingRuleWorkflowCall = func(ctx context.Context, request *merchandisingv1.PauseMerchandisingRuleWorkflowRequest) (*merchandisingv1.PauseMerchandisingRuleWorkflowResponse, error) {
		return merchandisingRuleWorkflowForPauseMerchandisingRuleWorkflow.PauseMerchandisingRuleWorkflow(ctx, request)
	}
	customerForUpdateCustomer := customersv1.NewCustomerServiceClient(deps.Conn)
	updateCustomerCall = customerForUpdateCustomer.UpdateCustomer
	searchQueryQueryForFindSearchQueries := searchv1.NewSearchQueryQueryServiceClient(deps.Conn)
	findSearchQueriesCall = func(ctx context.Context, request *searchv1.FindSearchQueriesRequest) (*searchv1.FindSearchQueriesResponse, error) {
		return searchQueryQueryForFindSearchQueries.FindSearchQueries(ctx, request)
	}
	warehouseQueryForBrowseWarehouses := warehousingv1.NewWarehouseQueryServiceClient(deps.Conn)
	browseWarehousesCall = warehouseQueryForBrowseWarehouses.BrowseWarehouses
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                      *slog.Logger
	warehouseQueryClient     warehousingv1.WarehouseQueryServiceClient
	pickListValidationClient warehousingv1.PickListValidationServiceClient
	venueQueryClient         venuesv1.VenueQueryServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                      deps.Logger,
		warehouseQueryClient:     warehousingv1.NewWarehouseQueryServiceClient(deps.Conn),
		pickListValidationClient: warehousingv1.NewPickListValidationServiceClient(deps.Conn),
		venueQueryClient:         venuesv1.NewVenueQueryServiceClient(deps.Conn),
	}
	go w.consume(ctx, "warehousing.get_warehouse.browse_warehouses", deps.Bus.Subscribe("warehousing.get_warehouse.browse_warehouses"), w.handleWarehousingGetWarehouseBrowseWarehouses)
	go w.consume(ctx, "warehousing.submit_putaway_task.lint_pick_lists", deps.Bus.Subscribe("warehousing.submit_putaway_task.lint_pick_lists"), w.handleWarehousingSubmitPutawayTaskLintPickLists)
	go w.consume(ctx, "warehousing.score_putaway_task.validate_pick_list", deps.Bus.Subscribe("warehousing.score_putaway_task.validate_pick_list"), w.handleWarehousingScorePutawayTaskValidatePickList)
	go w.consume(ctx, "warehousing.get_putaway_task_rules.find_venues", deps.Bus.Subscribe("warehousing.get_putaway_task_rules.find_venues"), w.handleWarehousingGetPutawayTaskRulesFindVenues)
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

// handleWarehousingGetWarehouseBrowseWarehouses makes the call the warehousing.get_warehouse.browse_warehouses event
// asks for.
func (w *workers) handleWarehousingGetWarehouseBrowseWarehouses(ctx context.Context, event eventbus.Event) error {
	_, err := w.warehouseQueryClient.BrowseWarehouses(ctx, &warehousingv1.BrowseWarehousesRequest{
		Filter: &warehousingv1.BrowseWarehousesRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
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

// handleWarehousingSubmitPutawayTaskLintPickLists makes the call the warehousing.submit_putaway_task.lint_pick_lists
// event asks for.
func (w *workers) handleWarehousingSubmitPutawayTaskLintPickLists(ctx context.Context, event eventbus.Event) error {
	_, err := w.pickListValidationClient.LintPickLists(ctx, &warehousingv1.LintPickListsRequest{
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{Etag: event.Attributes["pick_list.audit.etag"]},
			BillingAddress: &commonv1.PostalAddress{
				Recipient: event.Attributes["pick_list.billing_address.recipient"],
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: event.Attributes["pick_list.counterparty.display_name"],
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

// handleWarehousingScorePutawayTaskValidatePickList makes the call the
// warehousing.score_putaway_task.validate_pick_list event asks for.
func (w *workers) handleWarehousingScorePutawayTaskValidatePickList(ctx context.Context, event eventbus.Event) error {
	_, err := w.pickListValidationClient.ValidatePickList(ctx, &warehousingv1.ValidatePickListRequest{
		Details: &warehousingv1.ValidatePickListRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: event.Attributes["details.contact.phone"]},
		},
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{CreatedBy: event.Attributes["pick_list.audit.created_by"]},
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

// handleWarehousingGetPutawayTaskRulesFindVenues makes the call the warehousing.get_putaway_task_rules.find_venues
// event asks for.
func (w *workers) handleWarehousingGetPutawayTaskRulesFindVenues(ctx context.Context, event eventbus.Event) error {
	_, err := w.venueQueryClient.FindVenues(ctx, &venuesv1.FindVenuesRequest{
		Filter: &venuesv1.FindVenuesRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
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
