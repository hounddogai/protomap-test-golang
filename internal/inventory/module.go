package inventory

import (
	"context"
	"log/slog"

	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the inventory domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.appointments.v1.AppointmentService", appointmentsv1.NewAppointmentServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.ConsentRecordCalculationService", customersv1.NewConsentRecordCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerValidationService", customersv1.NewCustomerValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.HouseholdQueryService", customersv1.NewHouseholdQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.inventory.v1.ReplenishmentPlanValidationService", inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.inventory.v1.StockMovementValidationService", inventoryv1.NewStockMovementValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.inventory.v1.StockReservationQueryService", inventoryv1.NewStockReservationQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchQueryNotificationService", searchv1.NewSearchQueryNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerListingWorkflowService", sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.ParcelQueryService", shippingv1.NewParcelQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.warehousing.v1.PutawayTaskValidationService", warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn))
	inventoryv1.RegisterStockItemServiceServer(server, NewStockItemServer(deps))
	inventoryv1.RegisterStockReservationQueryServiceServer(server, NewStockReservationQueryServer(deps))
	inventoryv1.RegisterStockMovementWorkflowServiceServer(server, NewStockMovementWorkflowServer(deps))
	inventoryv1.RegisterReplenishmentPlanApprovalServiceServer(server, NewReplenishmentPlanApprovalServer(deps))
	inventoryv1.RegisterStockItemQueryServiceServer(server, NewStockItemQueryServer(deps))
	inventoryv1.RegisterStockReservationWorkflowServiceServer(server, NewStockReservationWorkflowServer(deps))
	inventoryv1.RegisterStockMovementApprovalServiceServer(server, NewStockMovementApprovalServer(deps))
	inventoryv1.RegisterReplenishmentPlanValidationServiceServer(server, NewReplenishmentPlanValidationServer(deps))
	inventoryv1.RegisterStockItemWorkflowServiceServer(server, NewStockItemWorkflowServer(deps))
	stockReservationApprovalServer := NewStockReservationApprovalServer(deps)
	server.RegisterService(&inventoryv1.StockReservationApprovalService_ServiceDesc, stockReservationApprovalServer)
	inventoryv1.RegisterStockMovementValidationServiceServer(server, NewStockMovementValidationServer(deps))
	replenishmentPlanCalculationServer := NewReplenishmentPlanCalculationServer(deps)
	server.RegisterService(&inventoryv1.ReplenishmentPlanCalculationService_ServiceDesc, replenishmentPlanCalculationServer)
	inventoryv1.RegisterStockItemApprovalServiceServer(server, NewStockItemApprovalServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	explainReplenishmentPlanDecisionCall func(context.Context, *inventoryv1.ExplainReplenishmentPlanDecisionRequest) (*inventoryv1.ExplainReplenishmentPlanDecisionResponse, error)
	getReplenishmentPlanRulesCall        func(context.Context, *inventoryv1.GetReplenishmentPlanRulesRequest, ...grpc.CallOption) (*inventoryv1.GetReplenishmentPlanRulesResponse, error)
	recalculateReplenishmentPlansCall    func(context.Context, *inventoryv1.RecalculateReplenishmentPlansRequest, ...grpc.CallOption) (*inventoryv1.RecalculateReplenishmentPlansResponse, error)
	calculateReplenishmentPlanCall       func(context.Context, *inventoryv1.CalculateReplenishmentPlanRequest, ...grpc.CallOption) (*inventoryv1.CalculateReplenishmentPlanResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	replenishmentPlanValidationForExplainReplenishmentPlanDecision := inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn)
	explainReplenishmentPlanDecisionCall = func(ctx context.Context, request *inventoryv1.ExplainReplenishmentPlanDecisionRequest) (*inventoryv1.ExplainReplenishmentPlanDecisionResponse, error) {
		return replenishmentPlanValidationForExplainReplenishmentPlanDecision.ExplainReplenishmentPlanDecision(ctx, request)
	}
	replenishmentPlanValidationForGetReplenishmentPlanRules := inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn)
	getReplenishmentPlanRulesCall = replenishmentPlanValidationForGetReplenishmentPlanRules.GetReplenishmentPlanRules
	replenishmentPlanCalculationForRecalculateReplenishmentPlans := inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn)
	recalculateReplenishmentPlansCall = replenishmentPlanCalculationForRecalculateReplenishmentPlans.RecalculateReplenishmentPlans
	replenishmentPlanCalculationForCalculateReplenishmentPlan := inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn)
	calculateReplenishmentPlanCall = replenishmentPlanCalculationForCalculateReplenishmentPlan.CalculateReplenishmentPlan
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                                *slog.Logger
	stockReservationApprovalClient     inventoryv1.StockReservationApprovalServiceClient
	replenishmentPlanCalculationClient inventoryv1.ReplenishmentPlanCalculationServiceClient
	subscriptionWorkflowClient         subscriptionsv1.SubscriptionWorkflowServiceClient
	customerQueryClient                customersv1.CustomerQueryServiceClient
	replenishmentPlanValidationClient  inventoryv1.ReplenishmentPlanValidationServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                                deps.Logger,
		stockReservationApprovalClient:     inventoryv1.NewStockReservationApprovalServiceClient(deps.Conn),
		replenishmentPlanCalculationClient: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		subscriptionWorkflowClient:         subscriptionsv1.NewSubscriptionWorkflowServiceClient(deps.Conn),
		customerQueryClient:                customersv1.NewCustomerQueryServiceClient(deps.Conn),
		replenishmentPlanValidationClient:  inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn),
	}
	go w.consume(ctx, "inventory.recall_replenishment_plan.submit_stock_reservation", deps.Bus.Subscribe("inventory.recall_replenishment_plan.submit_stock_reservation"), w.handleInventoryRecallReplenishmentPlanSubmitStockReservation)
	go w.consume(ctx, "inventory.normalize_replenishment_plan.calculate_replenishment_plan", deps.Bus.Subscribe("inventory.normalize_replenishment_plan.calculate_replenishment_plan"), w.handleInventoryNormalizeReplenishmentPlanCalculateReplenishmentPlan)
	go w.consume(ctx, "inventory.pause_stock_item_workflow.advance_subscription_workflow", deps.Bus.Subscribe("inventory.pause_stock_item_workflow.advance_subscription_workflow"), w.handleInventoryPauseStockItemWorkflowAdvanceSubscriptionWorkflow)
	go w.consume(ctx, "inventory.list_stock_item_workflows.find_customers", deps.Bus.Subscribe("inventory.list_stock_item_workflows.find_customers"), w.handleInventoryListStockItemWorkflowsFindCustomers)
	go w.consume(ctx, "inventory.submit_stock_item.score_replenishment_plan", deps.Bus.Subscribe("inventory.submit_stock_item.score_replenishment_plan"), w.handleInventorySubmitStockItemScoreReplenishmentPlan)
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

// handleInventoryRecallReplenishmentPlanSubmitStockReservation makes the call the
// inventory.recall_replenishment_plan.submit_stock_reservation event asks for.
func (w *workers) handleInventoryRecallReplenishmentPlanSubmitStockReservation(ctx context.Context, event eventbus.Event) error {
	_, err := w.stockReservationApprovalClient.SubmitStockReservation(ctx, &inventoryv1.SubmitStockReservationRequest{
		StockReservation: &inventoryv1.StockReservation{
			Audit: &commonv1.AuditInfo{
				CreatedBy: event.Attributes["stock_reservation.audit.created_by"],
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

// handleInventoryNormalizeReplenishmentPlanCalculateReplenishmentPlan makes the call the
// inventory.normalize_replenishment_plan.calculate_replenishment_plan event asks for.
func (w *workers) handleInventoryNormalizeReplenishmentPlanCalculateReplenishmentPlan(ctx context.Context, event eventbus.Event) error {
	_, err := w.replenishmentPlanCalculationClient.CalculateReplenishmentPlan(ctx, &inventoryv1.CalculateReplenishmentPlanRequest{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: &inventoryv1.StockReservationRef{
				StockReservationId: event.Attributes["replenishment_plan.counterparty.stock_reservation_id"],
				DisplayName:        event.Attributes["replenishment_plan.counterparty.display_name"],
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

// handleInventoryPauseStockItemWorkflowAdvanceSubscriptionWorkflow makes the call the
// inventory.pause_stock_item_workflow.advance_subscription_workflow event asks for.
func (w *workers) handleInventoryPauseStockItemWorkflowAdvanceSubscriptionWorkflow(ctx context.Context, event eventbus.Event) error {
	_, err := w.subscriptionWorkflowClient.AdvanceSubscriptionWorkflow(ctx, &subscriptionsv1.AdvanceSubscriptionWorkflowRequest{
		Subscription: &subscriptionsv1.Subscription{
			Holder: &customersv1.CustomerRef{Email: event.Attributes["subscription.holder.email"]},
			Audit:  &commonv1.AuditInfo{Etag: event.Attributes["subscription.audit.etag"]},
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

// handleInventoryListStockItemWorkflowsFindCustomers makes the call the
// inventory.list_stock_item_workflows.find_customers event asks for.
func (w *workers) handleInventoryListStockItemWorkflowsFindCustomers(ctx context.Context, event eventbus.Event) error {
	_, err := w.customerQueryClient.FindCustomers(ctx, &customersv1.FindCustomersRequest{
		Page:   &commonv1.PageRequest{PageToken: event.Attributes["page.page_token"]},
		Filter: &customersv1.FindCustomersRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
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

// handleInventorySubmitStockItemScoreReplenishmentPlan makes the call the
// inventory.submit_stock_item.score_replenishment_plan event asks for.
func (w *workers) handleInventorySubmitStockItemScoreReplenishmentPlan(ctx context.Context, event eventbus.Event) error {
	_, err := w.replenishmentPlanValidationClient.ScoreReplenishmentPlan(ctx, &inventoryv1.ScoreReplenishmentPlanRequest{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: &inventoryv1.StockReservationRef{
				DisplayName: event.Attributes["replenishment_plan.counterparty.display_name"],
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: event.Attributes["replenishment_plan.billing_address.postal_code"],
			},
			Audit: &commonv1.AuditInfo{Etag: event.Attributes["replenishment_plan.audit.etag"]},
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
