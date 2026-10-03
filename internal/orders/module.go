package orders

import (
	"context"
	"log/slog"

	boxofficev1 "github.com/hounddogai/protomap-test-golang/gen/acme/boxoffice/v1"
	collectionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	formsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forms/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	performancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the orders domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.boxoffice.v1.BoxOfficeSaleCalculationService", boxofficev1.NewBoxOfficeSaleCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerCalculationService", customersv1.NewCustomerCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.HouseholdQueryService", customersv1.NewHouseholdQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.featureflags.v1.FeatureFlagValidationService", featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.fulfillment.v1.FulfillmentExceptionWorkflowService", fulfillmentv1.NewFulfillmentExceptionWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.SessionNotificationService", identityv1.NewSessionNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.SessionValidationService", identityv1.NewSessionValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.TenantNotificationService", identityv1.NewTenantNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.UserQueryService", identityv1.NewUserQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.RewardApprovalService", loyaltyv1.NewRewardApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.merchandising.v1.MerchandisingRuleWorkflowService", merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.notifications.v1.NotificationService", notificationsv1.NewNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.BackorderLifecycleService", ordersv1.NewBackorderLifecycleServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.BackorderReportingService", ordersv1.NewBackorderReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.BackorderSyncService", ordersv1.NewBackorderSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.OrderAmendmentQueryService", ordersv1.NewOrderAmendmentQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.OrderAmendmentReportingService", ordersv1.NewOrderAmendmentReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.OrderAmendmentValidationService", ordersv1.NewOrderAmendmentValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.OrderCalculationService", ordersv1.NewOrderCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.OrderNoteCalculationService", ordersv1.NewOrderNoteCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.OrderNoteValidationService", ordersv1.NewOrderNoteValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.OrderQueryService", ordersv1.NewOrderQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.orders.v1.OrderReportingService", ordersv1.NewOrderReportingServiceClient(deps.Conn))
	deps.Clients.Put("acme.returns.v1.InspectionCalculationService", returnsv1.NewInspectionCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.warehousing.v1.StorageBinQueryService", warehousingv1.NewStorageBinQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.warehousing.v1.WarehouseQueryService", warehousingv1.NewWarehouseQueryServiceClient(deps.Conn))
	ordersv1.RegisterOrderServiceServer(server, NewOrderServer(deps))
	ordersv1.RegisterOrderAmendmentQueryServiceServer(server, NewOrderAmendmentQueryServer(deps))
	backorderWorkflowServer := NewBackorderWorkflowServer(deps)
	server.RegisterService(&ordersv1.BackorderWorkflowService_ServiceDesc, backorderWorkflowServer)
	orderNoteApprovalServer := NewOrderNoteApprovalServer(deps)
	server.RegisterService(&ordersv1.OrderNoteApprovalService_ServiceDesc, orderNoteApprovalServer)
	orderQueryServer := NewOrderQueryServer(deps)
	server.RegisterService(&ordersv1.OrderQueryService_ServiceDesc, orderQueryServer)
	ordersv1.RegisterOrderAmendmentWorkflowServiceServer(server, NewOrderAmendmentWorkflowServer(deps))
	ordersv1.RegisterBackorderApprovalServiceServer(server, NewBackorderApprovalServer(deps))
	ordersv1.RegisterOrderNoteValidationServiceServer(server, NewOrderNoteValidationServer(deps))
	orderWorkflowServer := NewOrderWorkflowServer(deps)
	server.RegisterService(&ordersv1.OrderWorkflowService_ServiceDesc, orderWorkflowServer)
	ordersv1.RegisterOrderAmendmentApprovalServiceServer(server, NewOrderAmendmentApprovalServer(deps))
	ordersv1.RegisterBackorderValidationServiceServer(server, NewBackorderValidationServer(deps))
	orderNoteCalculationServer := NewOrderNoteCalculationServer(deps)
	server.RegisterService(&ordersv1.OrderNoteCalculationService_ServiceDesc, orderNoteCalculationServer)
	ordersv1.RegisterOrderApprovalServiceServer(server, NewOrderApprovalServer(deps))
	ordersv1.RegisterOrderAmendmentValidationServiceServer(server, NewOrderAmendmentValidationServer(deps))
	ordersv1.RegisterBackorderCalculationServiceServer(server, NewBackorderCalculationServer(deps))
	ordersv1.RegisterOrderNoteLifecycleServiceServer(server, NewOrderNoteLifecycleServer(deps))
	ordersv1.RegisterOrderValidationServiceServer(server, NewOrderValidationServer(deps))
	ordersv1.RegisterOrderAmendmentCalculationServiceServer(server, NewOrderAmendmentCalculationServer(deps))
	ordersv1.RegisterBackorderLifecycleServiceServer(server, NewBackorderLifecycleServer(deps))
	ordersv1.RegisterOrderNoteReportingServiceServer(server, NewOrderNoteReportingServer(deps))
	ordersv1.RegisterOrderCalculationServiceServer(server, NewOrderCalculationServer(deps))
	ordersv1.RegisterOrderAmendmentLifecycleServiceServer(server, NewOrderAmendmentLifecycleServer(deps))
	backorderReportingServer := NewBackorderReportingServer(deps)
	server.RegisterService(&ordersv1.BackorderReportingService_ServiceDesc, backorderReportingServer)
	ordersv1.RegisterOrderNoteSyncServiceServer(server, NewOrderNoteSyncServer(deps))
	ordersv1.RegisterOrderLifecycleServiceServer(server, NewOrderLifecycleServer(deps))
	ordersv1.RegisterOrderAmendmentReportingServiceServer(server, NewOrderAmendmentReportingServer(deps))
	ordersv1.RegisterBackorderSyncServiceServer(server, NewBackorderSyncServer(deps))
	ordersv1.RegisterOrderNoteImportServiceServer(server, NewOrderNoteImportServer(deps))
	orderReportingServer := NewOrderReportingServer(deps)
	server.RegisterService(&ordersv1.OrderReportingService_ServiceDesc, orderReportingServer)
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	scheduleOrderAmendmentReportCall func(context.Context, *ordersv1.ScheduleOrderAmendmentReportRequest, ...grpc.CallOption) (*ordersv1.ScheduleOrderAmendmentReportResponse, error)
	getBackorderRulesCall            func(context.Context, *ordersv1.GetBackorderRulesRequest, ...grpc.CallOption) (*ordersv1.GetBackorderRulesResponse, error)
	reconcileBackordersCall          func(context.Context, *ordersv1.ReconcileBackordersRequest) (*ordersv1.ReconcileBackordersResponse, error)
	listSearchQueryVersionsCall      func(context.Context, *searchv1.ListSearchQueryVersionsRequest, ...grpc.CallOption) (*searchv1.ListSearchQueryVersionsResponse, error)
	searchMetricSeriesesCall         func(context.Context, *metricsv1.SearchMetricSeriesesRequest, ...grpc.CallOption) (*metricsv1.SearchMetricSeriesesResponse, error)
	explainBackorderCalculationCall  func(context.Context, *ordersv1.ExplainBackorderCalculationRequest, ...grpc.CallOption) (*ordersv1.ExplainBackorderCalculationResponse, error)
	browseOrderAmendmentsCall        func(context.Context, *ordersv1.BrowseOrderAmendmentsRequest, ...grpc.CallOption) (*ordersv1.BrowseOrderAmendmentsResponse, error)
	lintOrdersCall                   func(context.Context, *ordersv1.LintOrdersRequest) (*ordersv1.LintOrdersResponse, error)
	sendRoleTestNoticeCall           func(context.Context, *identityv1.SendRoleTestNoticeRequest) (*identityv1.SendRoleTestNoticeResponse, error)
	lintOrderNotesCall               func(context.Context, *ordersv1.LintOrderNotesRequest, ...grpc.CallOption) (*ordersv1.LintOrderNotesResponse, error)
	browseReviewsCall                func(context.Context, *performancev1.BrowseReviewsRequest, ...grpc.CallOption) (*performancev1.BrowseReviewsResponse, error)
	createCustomerCall               func(context.Context, *customersv1.CreateCustomerRequest) (*customersv1.CreateCustomerResponse, error)
	browseCollectionCasesCall        func(context.Context, *collectionsv1.BrowseCollectionCasesRequest, ...grpc.CallOption) (*collectionsv1.BrowseCollectionCasesResponse, error)
	countFeatureFlagsCall            func(context.Context, *featureflagsv1.CountFeatureFlagsRequest, ...grpc.CallOption) (*featureflagsv1.CountFeatureFlagsResponse, error)
	updateSearchIndexCall            func(context.Context, *searchv1.UpdateSearchIndexRequest) (*searchv1.UpdateSearchIndexResponse, error)
	resolveOrderAmendmentsCall       func(context.Context, *ordersv1.ResolveOrderAmendmentsRequest, ...grpc.CallOption) (*ordersv1.ResolveOrderAmendmentsResponse, error)
	batchGetFeatureFlagsCall         func(context.Context, *featureflagsv1.BatchGetFeatureFlagsRequest, ...grpc.CallOption) (*featureflagsv1.BatchGetFeatureFlagsResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	orderAmendmentReportingForScheduleOrderAmendmentReport := ordersv1.NewOrderAmendmentReportingServiceClient(deps.Conn)
	scheduleOrderAmendmentReportCall = orderAmendmentReportingForScheduleOrderAmendmentReport.ScheduleOrderAmendmentReport
	backorderValidationForGetBackorderRules := ordersv1.NewBackorderValidationServiceClient(deps.Conn)
	getBackorderRulesCall = backorderValidationForGetBackorderRules.GetBackorderRules
	backorderSyncForReconcileBackorders := ordersv1.NewBackorderSyncServiceClient(deps.Conn)
	reconcileBackordersCall = func(ctx context.Context, request *ordersv1.ReconcileBackordersRequest) (*ordersv1.ReconcileBackordersResponse, error) {
		return backorderSyncForReconcileBackorders.ReconcileBackorders(ctx, request)
	}
	searchQueryHistoryForListSearchQueryVersions := searchv1.NewSearchQueryHistoryServiceClient(deps.Conn)
	listSearchQueryVersionsCall = searchQueryHistoryForListSearchQueryVersions.ListSearchQueryVersions
	metricSeriesForSearchMetricSerieses := metricsv1.NewMetricSeriesServiceClient(deps.Conn)
	searchMetricSeriesesCall = metricSeriesForSearchMetricSerieses.SearchMetricSerieses
	backorderCalculationForExplainBackorderCalculation := ordersv1.NewBackorderCalculationServiceClient(deps.Conn)
	explainBackorderCalculationCall = backorderCalculationForExplainBackorderCalculation.ExplainBackorderCalculation
	orderAmendmentQueryForBrowseOrderAmendments := ordersv1.NewOrderAmendmentQueryServiceClient(deps.Conn)
	browseOrderAmendmentsCall = orderAmendmentQueryForBrowseOrderAmendments.BrowseOrderAmendments
	orderValidationForLintOrders := ordersv1.NewOrderValidationServiceClient(deps.Conn)
	lintOrdersCall = func(ctx context.Context, request *ordersv1.LintOrdersRequest) (*ordersv1.LintOrdersResponse, error) {
		return orderValidationForLintOrders.LintOrders(ctx, request)
	}
	roleNotificationForSendRoleTestNotice := identityv1.NewRoleNotificationServiceClient(deps.Conn)
	sendRoleTestNoticeCall = func(ctx context.Context, request *identityv1.SendRoleTestNoticeRequest) (*identityv1.SendRoleTestNoticeResponse, error) {
		return roleNotificationForSendRoleTestNotice.SendRoleTestNotice(ctx, request)
	}
	orderNoteValidationForLintOrderNotes := ordersv1.NewOrderNoteValidationServiceClient(deps.Conn)
	lintOrderNotesCall = orderNoteValidationForLintOrderNotes.LintOrderNotes
	reviewQueryForBrowseReviews := performancev1.NewReviewQueryServiceClient(deps.Conn)
	browseReviewsCall = reviewQueryForBrowseReviews.BrowseReviews
	customerForCreateCustomer := customersv1.NewCustomerServiceClient(deps.Conn)
	createCustomerCall = func(ctx context.Context, request *customersv1.CreateCustomerRequest) (*customersv1.CreateCustomerResponse, error) {
		return customerForCreateCustomer.CreateCustomer(ctx, request)
	}
	collectionCaseQueryForBrowseCollectionCases := collectionsv1.NewCollectionCaseQueryServiceClient(deps.Conn)
	browseCollectionCasesCall = collectionCaseQueryForBrowseCollectionCases.BrowseCollectionCases
	featureFlagQueryForCountFeatureFlags := featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn)
	countFeatureFlagsCall = featureFlagQueryForCountFeatureFlags.CountFeatureFlags
	searchIndexForUpdateSearchIndex := searchv1.NewSearchIndexServiceClient(deps.Conn)
	updateSearchIndexCall = func(ctx context.Context, request *searchv1.UpdateSearchIndexRequest) (*searchv1.UpdateSearchIndexResponse, error) {
		return searchIndexForUpdateSearchIndex.UpdateSearchIndex(ctx, request)
	}
	orderAmendmentQueryForResolveOrderAmendments := ordersv1.NewOrderAmendmentQueryServiceClient(deps.Conn)
	resolveOrderAmendmentsCall = orderAmendmentQueryForResolveOrderAmendments.ResolveOrderAmendments
	featureFlagForBatchGetFeatureFlags := featureflagsv1.NewFeatureFlagServiceClient(deps.Conn)
	batchGetFeatureFlagsCall = featureFlagForBatchGetFeatureFlags.BatchGetFeatureFlags
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                             *slog.Logger
	synonymValidationClient         searchv1.SynonymValidationServiceClient
	orderAmendmentCalculationClient ordersv1.OrderAmendmentCalculationServiceClient
	fulfillmentWorkflowClient       fulfillmentv1.FulfillmentWorkflowServiceClient
	formCalculationClient           formsv1.FormCalculationServiceClient
	orderAmendmentQueryClient       ordersv1.OrderAmendmentQueryServiceClient
	backorderValidationClient       ordersv1.BackorderValidationServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                             deps.Logger,
		synonymValidationClient:         searchv1.NewSynonymValidationServiceClient(deps.Conn),
		orderAmendmentCalculationClient: ordersv1.NewOrderAmendmentCalculationServiceClient(deps.Conn),
		fulfillmentWorkflowClient:       fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn),
		formCalculationClient:           formsv1.NewFormCalculationServiceClient(deps.Conn),
		orderAmendmentQueryClient:       ordersv1.NewOrderAmendmentQueryServiceClient(deps.Conn),
		backorderValidationClient:       ordersv1.NewBackorderValidationServiceClient(deps.Conn),
	}
	go w.consume(ctx, "orders.preview_backorder.validate_synonym", deps.Bus.Subscribe("orders.preview_backorder.validate_synonym"), w.handleOrdersPreviewBackorderValidateSynonym)
	go w.consume(ctx, "orders.estimate_order_note.estimate_order_amendment", deps.Bus.Subscribe("orders.estimate_order_note.estimate_order_amendment"), w.handleOrdersEstimateOrderNoteEstimateOrderAmendment)
	go w.consume(ctx, "orders.delegate_order_approval.signal_fulfillment_workflow", deps.Bus.Subscribe("orders.delegate_order_approval.signal_fulfillment_workflow"), w.handleOrdersDelegateOrderApprovalSignalFulfillmentWorkflow)
	go w.consume(ctx, "orders.get_order_note_lifecycle.quote_form", deps.Bus.Subscribe("orders.get_order_note_lifecycle.quote_form"), w.handleOrdersGetOrderNoteLifecycleQuoteForm)
	go w.consume(ctx, "orders.count_order_note_totals.count_order_amendments", deps.Bus.Subscribe("orders.count_order_note_totals.count_order_amendments"), w.handleOrdersCountOrderNoteTotalsCountOrderAmendments)
	go w.consume(ctx, "orders.fetch_order_amendment_report.validate_backorder", deps.Bus.Subscribe("orders.fetch_order_amendment_report.validate_backorder"), w.handleOrdersFetchOrderAmendmentReportValidateBackorder)
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

// handleOrdersPreviewBackorderValidateSynonym makes the call the orders.preview_backorder.validate_synonym event asks
// for.
func (w *workers) handleOrdersPreviewBackorderValidateSynonym(ctx context.Context, event eventbus.Event) error {
	_, err := w.synonymValidationClient.ValidateSynonym(ctx, &searchv1.ValidateSynonymRequest{
		Details: &searchv1.ValidateSynonymRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone:            event.Attributes["details.contact.phone"],
				PreferredChannel: event.Attributes["details.contact.preferred_channel"],
				Email:            event.Attributes["details.contact.email"],
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

// handleOrdersEstimateOrderNoteEstimateOrderAmendment makes the call the
// orders.estimate_order_note.estimate_order_amendment event asks for.
func (w *workers) handleOrdersEstimateOrderNoteEstimateOrderAmendment(ctx context.Context, event eventbus.Event) error {
	_, err := w.orderAmendmentCalculationClient.EstimateOrderAmendment(ctx, &ordersv1.EstimateOrderAmendmentRequest{
		OrderAmendment: &ordersv1.OrderAmendment{
			Settlement: &ordersv1.OrderAmendment_Card{Card: &ordersv1.OrderAmendmentCardSettlement{Last4: event.Attributes["order_amendment.card.last4"]}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: event.Attributes["order_amendment.audit.created_by"],
			},
		},
		Details: &ordersv1.EstimateOrderAmendmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: event.Attributes["details.destination.recipient"],
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

// handleOrdersDelegateOrderApprovalSignalFulfillmentWorkflow makes the call the
// orders.delegate_order_approval.signal_fulfillment_workflow event asks for.
func (w *workers) handleOrdersDelegateOrderApprovalSignalFulfillmentWorkflow(ctx context.Context, event eventbus.Event) error {
	_, err := w.fulfillmentWorkflowClient.SignalFulfillmentWorkflow(ctx, &fulfillmentv1.SignalFulfillmentWorkflowRequest{
		Fulfillment: &fulfillmentv1.Fulfillment{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: event.Attributes["fulfillment.counterparty.customer_id"],
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

// handleOrdersGetOrderNoteLifecycleQuoteForm makes the call the orders.get_order_note_lifecycle.quote_form event asks
// for.
func (w *workers) handleOrdersGetOrderNoteLifecycleQuoteForm(ctx context.Context, event eventbus.Event) error {
	_, err := w.formCalculationClient.QuoteForm(ctx, &formsv1.QuoteFormRequest{
		Form: &formsv1.Form{
			Counterparty: &customersv1.CustomerRef{Email: event.Attributes["form.counterparty.email"]},
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

// handleOrdersCountOrderNoteTotalsCountOrderAmendments makes the call the
// orders.count_order_note_totals.count_order_amendments event asks for.
func (w *workers) handleOrdersCountOrderNoteTotalsCountOrderAmendments(ctx context.Context, event eventbus.Event) error {
	_, err := w.orderAmendmentQueryClient.CountOrderAmendments(ctx, &ordersv1.CountOrderAmendmentsRequest{
		Page:   &commonv1.PageRequest{PageToken: event.Attributes["page.page_token"]},
		Filter: &ordersv1.CountOrderAmendmentsRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
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

// handleOrdersFetchOrderAmendmentReportValidateBackorder makes the call the
// orders.fetch_order_amendment_report.validate_backorder event asks for.
func (w *workers) handleOrdersFetchOrderAmendmentReportValidateBackorder(ctx context.Context, event eventbus.Event) error {
	_, err := w.backorderValidationClient.ValidateBackorder(ctx, &ordersv1.ValidateBackorderRequest{
		Backorder: &ordersv1.Backorder{
			Origin:   &ordersv1.Backorder_Customer{Customer: &customersv1.CustomerRef{Email: event.Attributes["backorder.customer.email"]}},
			Assignee: &identityv1.UserRef{UserId: event.Attributes["backorder.assignee.user_id"]},
		},
		Details: &ordersv1.ValidateBackorderRequest_Details{Locale: event.Attributes["details.locale"]},
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
