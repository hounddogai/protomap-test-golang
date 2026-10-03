package fulfillment

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the fulfillment domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.authorization.v1.AccessPolicyQueryService", authorizationv1.NewAccessPolicyQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerValidationService", customersv1.NewCustomerValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.fulfillment.v1.PackingSlipQueryService", fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.RoleNotificationService", identityv1.NewRoleNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.RewardApprovalService", loyaltyv1.NewRewardApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.ShipmentCalculationService", shippingv1.NewShipmentCalculationServiceClient(deps.Conn))
	fulfillmentv1.RegisterFulfillmentServiceServer(server, NewFulfillmentServer(deps))
	fulfillmentv1.RegisterPackingSlipQueryServiceServer(server, NewPackingSlipQueryServer(deps))
	fulfillmentv1.RegisterFulfillmentExceptionWorkflowServiceServer(server, NewFulfillmentExceptionWorkflowServer(deps))
	fulfillmentv1.RegisterFulfillmentQueryServiceServer(server, NewFulfillmentQueryServer(deps))
	fulfillmentv1.RegisterPackingSlipWorkflowServiceServer(server, NewPackingSlipWorkflowServer(deps))
	fulfillmentExceptionApprovalServer := NewFulfillmentExceptionApprovalServer(deps)
	server.RegisterService(&fulfillmentv1.FulfillmentExceptionApprovalService_ServiceDesc, fulfillmentExceptionApprovalServer)
	fulfillmentv1.RegisterFulfillmentWorkflowServiceServer(server, NewFulfillmentWorkflowServer(deps))
	fulfillmentv1.RegisterPackingSlipApprovalServiceServer(server, NewPackingSlipApprovalServer(deps))
	fulfillmentv1.RegisterFulfillmentExceptionValidationServiceServer(server, NewFulfillmentExceptionValidationServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	simulatePreferenceCall func(context.Context, *customersv1.SimulatePreferenceRequest, ...grpc.CallOption) (*customersv1.SimulatePreferenceResponse, error)
	browseDocumentsCall    func(context.Context, *documentsv1.BrowseDocumentsRequest) (*documentsv1.BrowseDocumentsResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	preferenceCalculationForSimulatePreference := customersv1.NewPreferenceCalculationServiceClient(deps.Conn)
	simulatePreferenceCall = preferenceCalculationForSimulatePreference.SimulatePreference
	documentQueryForBrowseDocuments := documentsv1.NewDocumentQueryServiceClient(deps.Conn)
	browseDocumentsCall = func(ctx context.Context, request *documentsv1.BrowseDocumentsRequest) (*documentsv1.BrowseDocumentsResponse, error) {
		return documentQueryForBrowseDocuments.BrowseDocuments(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                       *slog.Logger
	roleHistoryClient         identityv1.RoleHistoryServiceClient
	fulfillmentWorkflowClient fulfillmentv1.FulfillmentWorkflowServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                       deps.Logger,
		roleHistoryClient:         identityv1.NewRoleHistoryServiceClient(deps.Conn),
		fulfillmentWorkflowClient: fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn),
	}
	go w.consume(ctx, "fulfillment.update_fulfillment.record_role_version", deps.Bus.Subscribe("fulfillment.update_fulfillment.record_role_version"), w.handleFulfillmentUpdateFulfillmentRecordRoleVersion)
	go w.consume(ctx, "fulfillment.escalate_fulfillment_exception.signal_fulfillment_workflow", deps.Bus.Subscribe("fulfillment.escalate_fulfillment_exception.signal_fulfillment_workflow"), w.handleFulfillmentEscalateFulfillmentExceptionSignalFulfillmentWorkflow)
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

// handleFulfillmentUpdateFulfillmentRecordRoleVersion makes the call the
// fulfillment.update_fulfillment.record_role_version event asks for.
func (w *workers) handleFulfillmentUpdateFulfillmentRecordRoleVersion(ctx context.Context, event eventbus.Event) error {
	_, err := w.roleHistoryClient.RecordRoleVersion(ctx, &identityv1.RecordRoleVersionRequest{
		Role: &identityv1.Role{
			Audit: &commonv1.AuditInfo{
				Etag:      event.Attributes["role.audit.etag"],
				CreatedBy: event.Attributes["role.audit.created_by"],
			},
			Owner: &identityv1.UserRef{Email: event.Attributes["role.owner.email"]},
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

// handleFulfillmentEscalateFulfillmentExceptionSignalFulfillmentWorkflow makes the call the
// fulfillment.escalate_fulfillment_exception.signal_fulfillment_workflow event asks for.
func (w *workers) handleFulfillmentEscalateFulfillmentExceptionSignalFulfillmentWorkflow(ctx context.Context, event eventbus.Event) error {
	_, err := w.fulfillmentWorkflowClient.SignalFulfillmentWorkflow(ctx, &fulfillmentv1.SignalFulfillmentWorkflowRequest{
		Fulfillment: &fulfillmentv1.Fulfillment{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: event.Attributes["fulfillment.counterparty.display_name"],
				Email:       event.Attributes["fulfillment.counterparty.email"],
			},
			Audit: &commonv1.AuditInfo{Etag: event.Attributes["fulfillment.audit.etag"]},
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
