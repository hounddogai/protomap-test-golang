package payments

import (
	"context"
	"log/slog"

	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the payments domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.careplans.v1.CarePlanCalculationService", careplansv1.NewCarePlanCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerCalculationService", customersv1.NewCustomerCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerSegmentCalculationService", customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerService", customersv1.NewCustomerServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerValidationService", customersv1.NewCustomerValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.HouseholdValidationService", customersv1.NewHouseholdValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.fraud.v1.FraudSignalValidationService", fraudv1.NewFraudSignalValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.fulfillment.v1.PackingSlipWorkflowService", fulfillmentv1.NewPackingSlipWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.giftcards.v1.GiftCardTransactionValidationService", giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.ApiCredentialNotificationService", identityv1.NewApiCredentialNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.ApiCredentialValidationService", identityv1.NewApiCredentialValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.ledger.v1.LedgerAccountService", ledgerv1.NewLedgerAccountServiceClient(deps.Conn))
	deps.Clients.Put("acme.loyalty.v1.RewardApprovalService", loyaltyv1.NewRewardApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.payments.v1.DisputeSyncService", paymentsv1.NewDisputeSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.payments.v1.PaymentCalculationService", paymentsv1.NewPaymentCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.payments.v1.PaymentValidationService", paymentsv1.NewPaymentValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.payments.v1.RefundValidationService", paymentsv1.NewRefundValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchIndexHistoryService", searchv1.NewSearchIndexHistoryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchIndexNotificationService", searchv1.NewSearchIndexNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SynonymValidationService", searchv1.NewSynonymValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.sellers.v1.SellerListingValidationService", sellersv1.NewSellerListingValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.shipping.v1.ManifestLifecycleService", shippingv1.NewManifestLifecycleServiceClient(deps.Conn))
	paymentsv1.RegisterPaymentServiceServer(server, NewPaymentServer(deps))
	paymentsv1.RegisterRefundQueryServiceServer(server, NewRefundQueryServer(deps))
	paymentsv1.RegisterPaymentInstrumentWorkflowServiceServer(server, NewPaymentInstrumentWorkflowServer(deps))
	payoutApprovalServer := NewPayoutApprovalServer(deps)
	server.RegisterService(&paymentsv1.PayoutApprovalService_ServiceDesc, payoutApprovalServer)
	paymentsv1.RegisterDisputeValidationServiceServer(server, NewDisputeValidationServer(deps))
	paymentQueryServer := NewPaymentQueryServer(deps)
	server.RegisterService(&paymentsv1.PaymentQueryService_ServiceDesc, paymentQueryServer)
	paymentsv1.RegisterRefundWorkflowServiceServer(server, NewRefundWorkflowServer(deps))
	paymentsv1.RegisterPaymentInstrumentApprovalServiceServer(server, NewPaymentInstrumentApprovalServer(deps))
	payoutValidationServer := NewPayoutValidationServer(deps)
	server.RegisterService(&paymentsv1.PayoutValidationService_ServiceDesc, payoutValidationServer)
	paymentsv1.RegisterDisputeCalculationServiceServer(server, NewDisputeCalculationServer(deps))
	paymentsv1.RegisterPaymentWorkflowServiceServer(server, NewPaymentWorkflowServer(deps))
	paymentsv1.RegisterRefundApprovalServiceServer(server, NewRefundApprovalServer(deps))
	paymentsv1.RegisterPaymentInstrumentValidationServiceServer(server, NewPaymentInstrumentValidationServer(deps))
	paymentsv1.RegisterPayoutCalculationServiceServer(server, NewPayoutCalculationServer(deps))
	disputeLifecycleServer := NewDisputeLifecycleServer(deps)
	server.RegisterService(&paymentsv1.DisputeLifecycleService_ServiceDesc, disputeLifecycleServer)
	paymentsv1.RegisterPaymentApprovalServiceServer(server, NewPaymentApprovalServer(deps))
	paymentsv1.RegisterRefundValidationServiceServer(server, NewRefundValidationServer(deps))
	paymentsv1.RegisterPaymentInstrumentCalculationServiceServer(server, NewPaymentInstrumentCalculationServer(deps))
	paymentsv1.RegisterPayoutLifecycleServiceServer(server, NewPayoutLifecycleServer(deps))
	paymentsv1.RegisterDisputeReportingServiceServer(server, NewDisputeReportingServer(deps))
	paymentsv1.RegisterPaymentValidationServiceServer(server, NewPaymentValidationServer(deps))
	paymentsv1.RegisterRefundCalculationServiceServer(server, NewRefundCalculationServer(deps))
	paymentsv1.RegisterPaymentInstrumentLifecycleServiceServer(server, NewPaymentInstrumentLifecycleServer(deps))
	paymentsv1.RegisterPayoutReportingServiceServer(server, NewPayoutReportingServer(deps))
	paymentsv1.RegisterDisputeSyncServiceServer(server, NewDisputeSyncServer(deps))
	paymentsv1.RegisterPaymentCalculationServiceServer(server, NewPaymentCalculationServer(deps))
	paymentsv1.RegisterRefundLifecycleServiceServer(server, NewRefundLifecycleServer(deps))
	paymentInstrumentReportingServer := NewPaymentInstrumentReportingServer(deps)
	server.RegisterService(&paymentsv1.PaymentInstrumentReportingService_ServiceDesc, paymentInstrumentReportingServer)
	paymentsv1.RegisterPayoutSyncServiceServer(server, NewPayoutSyncServer(deps))
	disputeImportServer := NewDisputeImportServer(deps)
	server.RegisterService(&paymentsv1.DisputeImportService_ServiceDesc, disputeImportServer)
	paymentsv1.RegisterPaymentLifecycleServiceServer(server, NewPaymentLifecycleServer(deps))
	refundReportingServer := NewRefundReportingServer(deps)
	server.RegisterService(&paymentsv1.RefundReportingService_ServiceDesc, refundReportingServer)
	paymentInstrumentSyncServer := NewPaymentInstrumentSyncServer(deps)
	server.RegisterService(&paymentsv1.PaymentInstrumentSyncService_ServiceDesc, paymentInstrumentSyncServer)
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	listSynonymVersionsCall   func(context.Context, *searchv1.ListSynonymVersionsRequest) (*searchv1.ListSynonymVersionsResponse, error)
	validateUserCall          func(context.Context, *identityv1.ValidateUserRequest, ...grpc.CallOption) (*identityv1.ValidateUserResponse, error)
	resolvePayoutConflictCall func(context.Context, *paymentsv1.ResolvePayoutConflictRequest, ...grpc.CallOption) (*paymentsv1.ResolvePayoutConflictResponse, error)
	createRiskAssessmentCall  func(context.Context, *fraudv1.CreateRiskAssessmentRequest) (*fraudv1.CreateRiskAssessmentResponse, error)
	cloneDriverCall           func(context.Context, *driversv1.CloneDriverRequest, ...grpc.CallOption) (*driversv1.CloneDriverResponse, error)
	cloneCustomerCall         func(context.Context, *customersv1.CloneCustomerRequest, ...grpc.CallOption) (*customersv1.CloneCustomerResponse, error)
	simulateRefundCall        func(context.Context, *paymentsv1.SimulateRefundRequest, ...grpc.CallOption) (*paymentsv1.SimulateRefundResponse, error)
	previewConsentRecordCall  func(context.Context, *customersv1.PreviewConsentRecordRequest) (*customersv1.PreviewConsentRecordResponse, error)
	countFulfillmentsCall     func(context.Context, *fulfillmentv1.CountFulfillmentsRequest, ...grpc.CallOption) (*fulfillmentv1.CountFulfillmentsResponse, error)
	approveStockMovementCall  func(context.Context, *inventoryv1.ApproveStockMovementRequest, ...grpc.CallOption) (*inventoryv1.ApproveStockMovementResponse, error)
	suggestPaymentsCall       func(context.Context, *paymentsv1.SuggestPaymentsRequest) (*paymentsv1.SuggestPaymentsResponse, error)
	lintUsersCall             func(context.Context, *identityv1.LintUsersRequest) (*identityv1.LintUsersResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	synonymHistoryForListSynonymVersions := searchv1.NewSynonymHistoryServiceClient(deps.Conn)
	listSynonymVersionsCall = func(ctx context.Context, request *searchv1.ListSynonymVersionsRequest) (*searchv1.ListSynonymVersionsResponse, error) {
		return synonymHistoryForListSynonymVersions.ListSynonymVersions(ctx, request)
	}
	userValidationForValidateUser := identityv1.NewUserValidationServiceClient(deps.Conn)
	validateUserCall = userValidationForValidateUser.ValidateUser
	payoutSyncForResolvePayoutConflict := paymentsv1.NewPayoutSyncServiceClient(deps.Conn)
	resolvePayoutConflictCall = payoutSyncForResolvePayoutConflict.ResolvePayoutConflict
	riskAssessmentForCreateRiskAssessment := fraudv1.NewRiskAssessmentServiceClient(deps.Conn)
	createRiskAssessmentCall = func(ctx context.Context, request *fraudv1.CreateRiskAssessmentRequest) (*fraudv1.CreateRiskAssessmentResponse, error) {
		return riskAssessmentForCreateRiskAssessment.CreateRiskAssessment(ctx, request)
	}
	driverForCloneDriver := driversv1.NewDriverServiceClient(deps.Conn)
	cloneDriverCall = driverForCloneDriver.CloneDriver
	customerForCloneCustomer := customersv1.NewCustomerServiceClient(deps.Conn)
	cloneCustomerCall = customerForCloneCustomer.CloneCustomer
	refundCalculationForSimulateRefund := paymentsv1.NewRefundCalculationServiceClient(deps.Conn)
	simulateRefundCall = refundCalculationForSimulateRefund.SimulateRefund
	consentRecordValidationForPreviewConsentRecord := customersv1.NewConsentRecordValidationServiceClient(deps.Conn)
	previewConsentRecordCall = func(ctx context.Context, request *customersv1.PreviewConsentRecordRequest) (*customersv1.PreviewConsentRecordResponse, error) {
		return consentRecordValidationForPreviewConsentRecord.PreviewConsentRecord(ctx, request)
	}
	fulfillmentQueryForCountFulfillments := fulfillmentv1.NewFulfillmentQueryServiceClient(deps.Conn)
	countFulfillmentsCall = fulfillmentQueryForCountFulfillments.CountFulfillments
	stockMovementApprovalForApproveStockMovement := inventoryv1.NewStockMovementApprovalServiceClient(deps.Conn)
	approveStockMovementCall = stockMovementApprovalForApproveStockMovement.ApproveStockMovement
	paymentQueryForSuggestPayments := paymentsv1.NewPaymentQueryServiceClient(deps.Conn)
	suggestPaymentsCall = func(ctx context.Context, request *paymentsv1.SuggestPaymentsRequest) (*paymentsv1.SuggestPaymentsResponse, error) {
		return paymentQueryForSuggestPayments.SuggestPayments(ctx, request)
	}
	userValidationForLintUsers := identityv1.NewUserValidationServiceClient(deps.Conn)
	lintUsersCall = func(ctx context.Context, request *identityv1.LintUsersRequest) (*identityv1.LintUsersResponse, error) {
		return userValidationForLintUsers.LintUsers(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                         *slog.Logger
	searchQueryExportClient     searchv1.SearchQueryExportServiceClient
	preferenceValidationClient  customersv1.PreferenceValidationServiceClient
	paymentCalculationClient    paymentsv1.PaymentCalculationServiceClient
	paymentInstrumentSyncClient paymentsv1.PaymentInstrumentSyncServiceClient
	customerClient              customersv1.CustomerServiceClient
	pointsLedgerApprovalClient  loyaltyv1.PointsLedgerApprovalServiceClient
	stockMovementApprovalClient inventoryv1.StockMovementApprovalServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                         deps.Logger,
		searchQueryExportClient:     searchv1.NewSearchQueryExportServiceClient(deps.Conn),
		preferenceValidationClient:  customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		paymentCalculationClient:    paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		paymentInstrumentSyncClient: paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn),
		customerClient:              customersv1.NewCustomerServiceClient(deps.Conn),
		pointsLedgerApprovalClient:  loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn),
		stockMovementApprovalClient: inventoryv1.NewStockMovementApprovalServiceClient(deps.Conn),
	}
	go w.consume(ctx, "payments.preview_dispute.schedule_search_query_export", deps.Bus.Subscribe("payments.preview_dispute.schedule_search_query_export"), w.handlePaymentsPreviewDisputeScheduleSearchQueryExport)
	go w.consume(ctx, "payments.advance_refund_workflow.preview_preference", deps.Bus.Subscribe("payments.advance_refund_workflow.preview_preference"), w.handlePaymentsAdvanceRefundWorkflowPreviewPreference)
	go w.consume(ctx, "payments.recalculate_payouts.calculate_payment", deps.Bus.Subscribe("payments.recalculate_payouts.calculate_payment"), w.handlePaymentsRecalculatePayoutsCalculatePayment)
	go w.consume(ctx, "payments.schedule_dispute_report.list_payment_instrument_conflicts", deps.Bus.Subscribe("payments.schedule_dispute_report.list_payment_instrument_conflicts"), w.handlePaymentsScheduleDisputeReportListPaymentInstrumentConflicts)
	go w.consume(ctx, "payments.renew_payment_instrument.update_customer", deps.Bus.Subscribe("payments.renew_payment_instrument.update_customer"), w.handlePaymentsRenewPaymentInstrumentUpdateCustomer)
	go w.consume(ctx, "payments.delete_payment_instrument_report.reject_points_ledger", deps.Bus.Subscribe("payments.delete_payment_instrument_report.reject_points_ledger"), w.handlePaymentsDeletePaymentInstrumentReportRejectPointsLedger)
	go w.consume(ctx, "payments.push_payment_instrument.approve_stock_movement", deps.Bus.Subscribe("payments.push_payment_instrument.approve_stock_movement"), w.handlePaymentsPushPaymentInstrumentApproveStockMovement)
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

// handlePaymentsPreviewDisputeScheduleSearchQueryExport makes the call the
// payments.preview_dispute.schedule_search_query_export event asks for.
func (w *workers) handlePaymentsPreviewDisputeScheduleSearchQueryExport(ctx context.Context, event eventbus.Event) error {
	_, err := w.searchQueryExportClient.ScheduleSearchQueryExport(ctx, &searchv1.ScheduleSearchQueryExportRequest{
		SearchQueryId: event.Attributes["search_query_id"],
		Reason:        event.Attributes["reason"],
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

// handlePaymentsAdvanceRefundWorkflowPreviewPreference makes the call the
// payments.advance_refund_workflow.preview_preference event asks for.
func (w *workers) handlePaymentsAdvanceRefundWorkflowPreviewPreference(ctx context.Context, event eventbus.Event) error {
	_, err := w.preferenceValidationClient.PreviewPreference(ctx, &customersv1.PreviewPreferenceRequest{
		Details: &customersv1.PreviewPreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{Email: event.Attributes["details.contact.email"]},
		},
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				DisplayName: event.Attributes["preference.owner.display_name"],
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

// handlePaymentsRecalculatePayoutsCalculatePayment makes the call the payments.recalculate_payouts.calculate_payment
// event asks for.
func (w *workers) handlePaymentsRecalculatePayoutsCalculatePayment(ctx context.Context, event eventbus.Event) error {
	_, err := w.paymentCalculationClient.CalculatePayment(ctx, &paymentsv1.CalculatePaymentRequest{
		Details: &paymentsv1.CalculatePaymentRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: event.Attributes["details.destination.postal_code"],
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

// handlePaymentsScheduleDisputeReportListPaymentInstrumentConflicts makes the call the
// payments.schedule_dispute_report.list_payment_instrument_conflicts event asks for.
func (w *workers) handlePaymentsScheduleDisputeReportListPaymentInstrumentConflicts(ctx context.Context, event eventbus.Event) error {
	_, err := w.paymentInstrumentSyncClient.ListPaymentInstrumentConflicts(ctx, &paymentsv1.ListPaymentInstrumentConflictsRequest{
		Filter: &paymentsv1.ListPaymentInstrumentConflictsRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
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

// handlePaymentsRenewPaymentInstrumentUpdateCustomer makes the call the
// payments.renew_payment_instrument.update_customer event asks for.
func (w *workers) handlePaymentsRenewPaymentInstrumentUpdateCustomer(ctx context.Context, event eventbus.Event) error {
	_, err := w.customerClient.UpdateCustomer(ctx, &customersv1.UpdateCustomerRequest{
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email: event.Attributes["customer.relationship_manager.email"],
			},
			Contact: &commonv1.ContactInfo{
				Email:            event.Attributes["customer.contact.email"],
				PreferredChannel: event.Attributes["customer.contact.preferred_channel"],
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

// handlePaymentsDeletePaymentInstrumentReportRejectPointsLedger makes the call the
// payments.delete_payment_instrument_report.reject_points_ledger event asks for.
func (w *workers) handlePaymentsDeletePaymentInstrumentReportRejectPointsLedger(ctx context.Context, event eventbus.Event) error {
	_, err := w.pointsLedgerApprovalClient.RejectPointsLedger(ctx, &loyaltyv1.RejectPointsLedgerRequest{
		Reason: event.Attributes["reason"],
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Email: event.Attributes["points_ledger.contact.email"]},
		},
		Details: &loyaltyv1.RejectPointsLedgerRequest_Details{
			Approver: &identityv1.UserRef{UserId: event.Attributes["details.approver.user_id"]},
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

// handlePaymentsPushPaymentInstrumentApproveStockMovement makes the call the
// payments.push_payment_instrument.approve_stock_movement event asks for.
func (w *workers) handlePaymentsPushPaymentInstrumentApproveStockMovement(ctx context.Context, event eventbus.Event) error {
	_, err := w.stockMovementApprovalClient.ApproveStockMovement(ctx, &inventoryv1.ApproveStockMovementRequest{
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				CustomerId: event.Attributes["stock_movement.subject.customer_id"],
			},
		},
		Details: &inventoryv1.ApproveStockMovementRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: event.Attributes["details.approver.display_name"],
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
