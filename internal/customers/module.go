package customers

import (
	"context"
	"log/slog"

	assetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/assets/v1"
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	benefitsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/benefits/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	recommendationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recommendations/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	webhooksv1 "github.com/hounddogai/protomap-test-golang/gen/acme/webhooks/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the customers domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.assets.v1.FixedAssetCalculationService", assetsv1.NewFixedAssetCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.benefits.v1.BenefitPlanService", benefitsv1.NewBenefitPlanServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.ConsentRecordCalculationService", customersv1.NewConsentRecordCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.ConsentRecordSyncService", customersv1.NewConsentRecordSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerCalculationService", customersv1.NewCustomerCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerQueryService", customersv1.NewCustomerQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerSegmentSyncService", customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerSegmentValidationService", customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerWorkflowService", customersv1.NewCustomerWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.HouseholdCalculationService", customersv1.NewHouseholdCalculationServiceClient(deps.Conn))
	deps.Clients.Put("acme.featureflags.v1.FeatureFlagValidationService", featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.ApiCredentialNotificationService", identityv1.NewApiCredentialNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.RoleHistoryService", identityv1.NewRoleHistoryServiceClient(deps.Conn))
	deps.Clients.Put("acme.insurance.v1.PolicyService", insurancev1.NewPolicyServiceClient(deps.Conn))
	deps.Clients.Put("acme.media.v1.MediaAssetService", mediav1.NewMediaAssetServiceClient(deps.Conn))
	deps.Clients.Put("acme.recommendations.v1.RecommendationSetService", recommendationsv1.NewRecommendationSetServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchIndexQueryService", searchv1.NewSearchIndexQueryServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SearchQueryExportService", searchv1.NewSearchQueryExportServiceClient(deps.Conn))
	deps.Clients.Put("acme.webhooks.v1.WebhookEndpointQueryService", webhooksv1.NewWebhookEndpointQueryServiceClient(deps.Conn))
	customersv1.RegisterCustomerServiceServer(server, NewCustomerServer(deps))
	customersv1.RegisterHouseholdQueryServiceServer(server, NewHouseholdQueryServer(deps))
	customersv1.RegisterConsentRecordWorkflowServiceServer(server, NewConsentRecordWorkflowServer(deps))
	customerSegmentApprovalServer := NewCustomerSegmentApprovalServer(deps)
	server.RegisterService(&customersv1.CustomerSegmentApprovalService_ServiceDesc, customerSegmentApprovalServer)
	customersv1.RegisterPreferenceValidationServiceServer(server, NewPreferenceValidationServer(deps))
	customersv1.RegisterCustomerQueryServiceServer(server, NewCustomerQueryServer(deps))
	customersv1.RegisterHouseholdWorkflowServiceServer(server, NewHouseholdWorkflowServer(deps))
	customersv1.RegisterConsentRecordApprovalServiceServer(server, NewConsentRecordApprovalServer(deps))
	customersv1.RegisterCustomerSegmentValidationServiceServer(server, NewCustomerSegmentValidationServer(deps))
	customersv1.RegisterPreferenceCalculationServiceServer(server, NewPreferenceCalculationServer(deps))
	customersv1.RegisterCustomerWorkflowServiceServer(server, NewCustomerWorkflowServer(deps))
	householdApprovalServer := NewHouseholdApprovalServer(deps)
	server.RegisterService(&customersv1.HouseholdApprovalService_ServiceDesc, householdApprovalServer)
	customersv1.RegisterConsentRecordValidationServiceServer(server, NewConsentRecordValidationServer(deps))
	customersv1.RegisterCustomerSegmentCalculationServiceServer(server, NewCustomerSegmentCalculationServer(deps))
	customersv1.RegisterPreferenceLifecycleServiceServer(server, NewPreferenceLifecycleServer(deps))
	customersv1.RegisterCustomerApprovalServiceServer(server, NewCustomerApprovalServer(deps))
	householdValidationServer := NewHouseholdValidationServer(deps)
	server.RegisterService(&customersv1.HouseholdValidationService_ServiceDesc, householdValidationServer)
	customersv1.RegisterConsentRecordCalculationServiceServer(server, NewConsentRecordCalculationServer(deps))
	customersv1.RegisterCustomerSegmentLifecycleServiceServer(server, NewCustomerSegmentLifecycleServer(deps))
	customersv1.RegisterPreferenceReportingServiceServer(server, NewPreferenceReportingServer(deps))
	customersv1.RegisterCustomerValidationServiceServer(server, NewCustomerValidationServer(deps))
	customersv1.RegisterHouseholdCalculationServiceServer(server, NewHouseholdCalculationServer(deps))
	consentRecordLifecycleServer := NewConsentRecordLifecycleServer(deps)
	server.RegisterService(&customersv1.ConsentRecordLifecycleService_ServiceDesc, consentRecordLifecycleServer)
	customersv1.RegisterCustomerSegmentReportingServiceServer(server, NewCustomerSegmentReportingServer(deps))
	customersv1.RegisterPreferenceSyncServiceServer(server, NewPreferenceSyncServer(deps))
	customersv1.RegisterCustomerCalculationServiceServer(server, NewCustomerCalculationServer(deps))
	customersv1.RegisterHouseholdLifecycleServiceServer(server, NewHouseholdLifecycleServer(deps))
	consentRecordReportingServer := NewConsentRecordReportingServer(deps)
	server.RegisterService(&customersv1.ConsentRecordReportingService_ServiceDesc, consentRecordReportingServer)
	customersv1.RegisterCustomerSegmentSyncServiceServer(server, NewCustomerSegmentSyncServer(deps))
	customersv1.RegisterPreferenceImportServiceServer(server, NewPreferenceImportServer(deps))
	customersv1.RegisterCustomerLifecycleServiceServer(server, NewCustomerLifecycleServer(deps))
	customersv1.RegisterHouseholdReportingServiceServer(server, NewHouseholdReportingServer(deps))
	customersv1.RegisterConsentRecordSyncServiceServer(server, NewConsentRecordSyncServer(deps))
	customersv1.RegisterCustomerSegmentImportServiceServer(server, NewCustomerSegmentImportServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	countMetricSeriesesCall          func(context.Context, *metricsv1.CountMetricSeriesesRequest) (*metricsv1.CountMetricSeriesesResponse, error)
	previewBrandCall                 func(context.Context, *catalogv1.PreviewBrandRequest) (*catalogv1.PreviewBrandResponse, error)
	rejectRiskAssessmentCall         func(context.Context, *fraudv1.RejectRiskAssessmentRequest) (*fraudv1.RejectRiskAssessmentResponse, error)
	repairSearchIndexCall            func(context.Context, *searchv1.RepairSearchIndexRequest, ...grpc.CallOption) (*searchv1.RepairSearchIndexResponse, error)
	lintSearchQueriesCall            func(context.Context, *searchv1.LintSearchQueriesRequest, ...grpc.CallOption) (*searchv1.LintSearchQueriesResponse, error)
	lintApiCredentialsCall           func(context.Context, *identityv1.LintApiCredentialsRequest, ...grpc.CallOption) (*identityv1.LintApiCredentialsResponse, error)
	createLedgerAccountCall          func(context.Context, *ledgerv1.CreateLedgerAccountRequest, ...grpc.CallOption) (*ledgerv1.CreateLedgerAccountResponse, error)
	recallSubscriptionPlanCall       func(context.Context, *subscriptionsv1.RecallSubscriptionPlanRequest, ...grpc.CallOption) (*subscriptionsv1.RecallSubscriptionPlanResponse, error)
	listCustomerSegmentImportsCall   func(context.Context, *customersv1.ListCustomerSegmentImportsRequest) (*customersv1.ListCustomerSegmentImportsResponse, error)
	resolveConsentRecordConflictCall func(context.Context, *customersv1.ResolveConsentRecordConflictRequest) (*customersv1.ResolveConsentRecordConflictResponse, error)
	listDriversCall                  func(context.Context, *driversv1.ListDriversRequest, ...grpc.CallOption) (*driversv1.ListDriversResponse, error)
	checkCustomerEligibilityCall     func(context.Context, *customersv1.CheckCustomerEligibilityRequest) (*customersv1.CheckCustomerEligibilityResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	metricSeriesQueryForCountMetricSerieses := metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn)
	countMetricSeriesesCall = func(ctx context.Context, request *metricsv1.CountMetricSeriesesRequest) (*metricsv1.CountMetricSeriesesResponse, error) {
		return metricSeriesQueryForCountMetricSerieses.CountMetricSerieses(ctx, request)
	}
	brandValidationForPreviewBrand := catalogv1.NewBrandValidationServiceClient(deps.Conn)
	previewBrandCall = func(ctx context.Context, request *catalogv1.PreviewBrandRequest) (*catalogv1.PreviewBrandResponse, error) {
		return brandValidationForPreviewBrand.PreviewBrand(ctx, request)
	}
	riskAssessmentApprovalForRejectRiskAssessment := fraudv1.NewRiskAssessmentApprovalServiceClient(deps.Conn)
	rejectRiskAssessmentCall = func(ctx context.Context, request *fraudv1.RejectRiskAssessmentRequest) (*fraudv1.RejectRiskAssessmentResponse, error) {
		return riskAssessmentApprovalForRejectRiskAssessment.RejectRiskAssessment(ctx, request)
	}
	searchIndexAdminForRepairSearchIndex := searchv1.NewSearchIndexAdminServiceClient(deps.Conn)
	repairSearchIndexCall = searchIndexAdminForRepairSearchIndex.RepairSearchIndex
	searchQueryValidationForLintSearchQueries := searchv1.NewSearchQueryValidationServiceClient(deps.Conn)
	lintSearchQueriesCall = searchQueryValidationForLintSearchQueries.LintSearchQueries
	apiCredentialValidationForLintApiCredentials := identityv1.NewApiCredentialValidationServiceClient(deps.Conn)
	lintApiCredentialsCall = apiCredentialValidationForLintApiCredentials.LintApiCredentials
	ledgerAccountForCreateLedgerAccount := ledgerv1.NewLedgerAccountServiceClient(deps.Conn)
	createLedgerAccountCall = ledgerAccountForCreateLedgerAccount.CreateLedgerAccount
	subscriptionPlanApprovalForRecallSubscriptionPlan := subscriptionsv1.NewSubscriptionPlanApprovalServiceClient(deps.Conn)
	recallSubscriptionPlanCall = subscriptionPlanApprovalForRecallSubscriptionPlan.RecallSubscriptionPlan
	customerSegmentImportForListCustomerSegmentImports := customersv1.NewCustomerSegmentImportServiceClient(deps.Conn)
	listCustomerSegmentImportsCall = func(ctx context.Context, request *customersv1.ListCustomerSegmentImportsRequest) (*customersv1.ListCustomerSegmentImportsResponse, error) {
		return customerSegmentImportForListCustomerSegmentImports.ListCustomerSegmentImports(ctx, request)
	}
	consentRecordSyncForResolveConsentRecordConflict := customersv1.NewConsentRecordSyncServiceClient(deps.Conn)
	resolveConsentRecordConflictCall = func(ctx context.Context, request *customersv1.ResolveConsentRecordConflictRequest) (*customersv1.ResolveConsentRecordConflictResponse, error) {
		return consentRecordSyncForResolveConsentRecordConflict.ResolveConsentRecordConflict(ctx, request)
	}
	driverForListDrivers := driversv1.NewDriverServiceClient(deps.Conn)
	listDriversCall = driverForListDrivers.ListDrivers
	customerValidationForCheckCustomerEligibility := customersv1.NewCustomerValidationServiceClient(deps.Conn)
	checkCustomerEligibilityCall = func(ctx context.Context, request *customersv1.CheckCustomerEligibilityRequest) (*customersv1.CheckCustomerEligibilityResponse, error) {
		return customerValidationForCheckCustomerEligibility.CheckCustomerEligibility(ctx, request)
	}
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                            *slog.Logger
	householdCalculationClient     customersv1.HouseholdCalculationServiceClient
	customerCalculationClient      customersv1.CustomerCalculationServiceClient
	auditEventQueryClient          auditv1.AuditEventQueryServiceClient
	collectionWorkflowClient       merchandisingv1.CollectionWorkflowServiceClient
	customerValidationClient       customersv1.CustomerValidationServiceClient
	customerSegmentSyncClient      customersv1.CustomerSegmentSyncServiceClient
	orderAmendmentLifecycleClient  ordersv1.OrderAmendmentLifecycleServiceClient
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
	customerClient                 customersv1.CustomerServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                            deps.Logger,
		householdCalculationClient:     customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		customerCalculationClient:      customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		auditEventQueryClient:          auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		collectionWorkflowClient:       merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		customerValidationClient:       customersv1.NewCustomerValidationServiceClient(deps.Conn),
		customerSegmentSyncClient:      customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
		orderAmendmentLifecycleClient:  ordersv1.NewOrderAmendmentLifecycleServiceClient(deps.Conn),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		customerClient:                 customersv1.NewCustomerServiceClient(deps.Conn),
	}
	go w.consume(ctx, "customers.explain_customer_segment_decision.recalculate_households", deps.Bus.Subscribe("customers.explain_customer_segment_decision.recalculate_households"), w.handleCustomersExplainCustomerSegmentDecisionRecalculateHouseholds)
	go w.consume(ctx, "customers.simulate_preference.quote_customer", deps.Bus.Subscribe("customers.simulate_preference.quote_customer"), w.handleCustomersSimulatePreferenceQuoteCustomer)
	go w.consume(ctx, "customers.approve_household.count_audit_events", deps.Bus.Subscribe("customers.approve_household.count_audit_events"), w.handleCustomersApproveHouseholdCountAuditEvents)
	go w.consume(ctx, "customers.get_household_approval_status.advance_collection_workflow", deps.Bus.Subscribe("customers.get_household_approval_status.advance_collection_workflow"), w.handleCustomersGetHouseholdApprovalStatusAdvanceCollectionWorkflow)
	go w.consume(ctx, "customers.preview_household.check_customer_eligibility", deps.Bus.Subscribe("customers.preview_household.check_customer_eligibility"), w.handleCustomersPreviewHouseholdCheckCustomerEligibility)
	go w.consume(ctx, "customers.explain_household_calculation.simulate_customer", deps.Bus.Subscribe("customers.explain_household_calculation.simulate_customer"), w.handleCustomersExplainHouseholdCalculationSimulateCustomer)
	go w.consume(ctx, "customers.activate_consent_record.list_customer_segment_conflicts", deps.Bus.Subscribe("customers.activate_consent_record.list_customer_segment_conflicts"), w.handleCustomersActivateConsentRecordListCustomerSegmentConflicts)
	go w.consume(ctx, "customers.cancel_preference_import.schedule_order_amendment_expiry", deps.Bus.Subscribe("customers.cancel_preference_import.schedule_order_amendment_expiry"), w.handleCustomersCancelPreferenceImportScheduleOrderAmendmentExpiry)
	go w.consume(ctx, "customers.activate_customer.get_customer_rules", deps.Bus.Subscribe("customers.activate_customer.get_customer_rules"), w.handleCustomersActivateCustomerGetCustomerRules)
	go w.consume(ctx, "customers.pull_consent_records.quote_consent_record", deps.Bus.Subscribe("customers.pull_consent_records.quote_consent_record"), w.handleCustomersPullConsentRecordsQuoteConsentRecord)
	go w.consume(ctx, "customers.reconcile_consent_records.list_customers", deps.Bus.Subscribe("customers.reconcile_consent_records.list_customers"), w.handleCustomersReconcileConsentRecordsListCustomers)
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

// handleCustomersExplainCustomerSegmentDecisionRecalculateHouseholds makes the call the
// customers.explain_customer_segment_decision.recalculate_households event asks for.
func (w *workers) handleCustomersExplainCustomerSegmentDecisionRecalculateHouseholds(ctx context.Context, event eventbus.Event) error {
	_, err := w.householdCalculationClient.RecalculateHouseholds(ctx, &customersv1.RecalculateHouseholdsRequest{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Recipient: event.Attributes["household.children.children.location.recipient"],
					},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: event.Attributes["household.children.children.audit.updated_by"],
					},
					Manager: &identityv1.UserRef{
						Email: event.Attributes["household.children.children.manager.email"],
					},
				}},
			}},
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

// handleCustomersSimulatePreferenceQuoteCustomer makes the call the customers.simulate_preference.quote_customer event
// asks for.
func (w *workers) handleCustomersSimulatePreferenceQuoteCustomer(ctx context.Context, event eventbus.Event) error {
	_, err := w.customerCalculationClient.QuoteCustomer(ctx, &customersv1.QuoteCustomerRequest{
		Customer: &customersv1.Customer{
			Audit:   &commonv1.AuditInfo{CreatedBy: event.Attributes["customer.audit.created_by"]},
			Contact: &commonv1.ContactInfo{Email: event.Attributes["customer.contact.email"]},
			RelationshipManager: &identityv1.UserRef{
				UserId:      event.Attributes["customer.relationship_manager.user_id"],
				DisplayName: event.Attributes["customer.relationship_manager.display_name"],
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

// handleCustomersApproveHouseholdCountAuditEvents makes the call the customers.approve_household.count_audit_events
// event asks for.
func (w *workers) handleCustomersApproveHouseholdCountAuditEvents(ctx context.Context, event eventbus.Event) error {
	_, err := w.auditEventQueryClient.CountAuditEvents(ctx, &auditv1.CountAuditEventsRequest{
		Filter: &auditv1.CountAuditEventsRequest_Filter{
			Region:  event.Attributes["filter.region"],
			OwnerId: event.Attributes["filter.owner_id"],
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

// handleCustomersGetHouseholdApprovalStatusAdvanceCollectionWorkflow makes the call the
// customers.get_household_approval_status.advance_collection_workflow event asks for.
func (w *workers) handleCustomersGetHouseholdApprovalStatusAdvanceCollectionWorkflow(ctx context.Context, event eventbus.Event) error {
	_, err := w.collectionWorkflowClient.AdvanceCollectionWorkflow(ctx, &merchandisingv1.AdvanceCollectionWorkflowRequest{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Manager: &identityv1.UserRef{
					UserId: event.Attributes["collection.children.manager.user_id"],
				},
			}},
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

// handleCustomersPreviewHouseholdCheckCustomerEligibility makes the call the
// customers.preview_household.check_customer_eligibility event asks for.
func (w *workers) handleCustomersPreviewHouseholdCheckCustomerEligibility(ctx context.Context, event eventbus.Event) error {
	_, err := w.customerValidationClient.CheckCustomerEligibility(ctx, &customersv1.CheckCustomerEligibilityRequest{
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				Email:       event.Attributes["customer.relationship_manager.email"],
				DisplayName: event.Attributes["customer.relationship_manager.display_name"],
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

// handleCustomersExplainHouseholdCalculationSimulateCustomer makes the call the
// customers.explain_household_calculation.simulate_customer event asks for.
func (w *workers) handleCustomersExplainHouseholdCalculationSimulateCustomer(ctx context.Context, event eventbus.Event) error {
	_, err := w.customerCalculationClient.SimulateCustomer(ctx, &customersv1.SimulateCustomerRequest{
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{Etag: event.Attributes["customer.audit.etag"]},
			IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{
				IssuingCountry: event.Attributes["customer.passport.issuing_country"],
			}},
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

// handleCustomersActivateConsentRecordListCustomerSegmentConflicts makes the call the
// customers.activate_consent_record.list_customer_segment_conflicts event asks for.
func (w *workers) handleCustomersActivateConsentRecordListCustomerSegmentConflicts(ctx context.Context, event eventbus.Event) error {
	_, err := w.customerSegmentSyncClient.ListCustomerSegmentConflicts(ctx, &customersv1.ListCustomerSegmentConflictsRequest{
		Filter: &customersv1.ListCustomerSegmentConflictsRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
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

// handleCustomersCancelPreferenceImportScheduleOrderAmendmentExpiry makes the call the
// customers.cancel_preference_import.schedule_order_amendment_expiry event asks for.
func (w *workers) handleCustomersCancelPreferenceImportScheduleOrderAmendmentExpiry(ctx context.Context, event eventbus.Event) error {
	_, err := w.orderAmendmentLifecycleClient.ScheduleOrderAmendmentExpiry(ctx, &ordersv1.ScheduleOrderAmendmentExpiryRequest{
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: event.Attributes["order_amendment.counterparty.display_name"],
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: event.Attributes["order_amendment.billing_address.locality"],
			},
		},
		Details: &ordersv1.ScheduleOrderAmendmentExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Email: event.Attributes["details.notify.email"]},
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

// handleCustomersActivateCustomerGetCustomerRules makes the call the customers.activate_customer.get_customer_rules
// event asks for.
func (w *workers) handleCustomersActivateCustomerGetCustomerRules(ctx context.Context, event eventbus.Event) error {
	_, err := w.customerValidationClient.GetCustomerRules(ctx, &customersv1.GetCustomerRulesRequest{
		CustomerId: event.Attributes["customer_id"],
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

// handleCustomersPullConsentRecordsQuoteConsentRecord makes the call the
// customers.pull_consent_records.quote_consent_record event asks for.
func (w *workers) handleCustomersPullConsentRecordsQuoteConsentRecord(ctx context.Context, event eventbus.Event) error {
	_, err := w.consentRecordCalculationClient.QuoteConsentRecord(ctx, &customersv1.QuoteConsentRecordRequest{
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				HouseholdId: event.Attributes["consent_record.subject.household_id"],
				DisplayName: event.Attributes["consent_record.subject.display_name"],
				Path:        event.Attributes["consent_record.subject.path"],
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

// handleCustomersReconcileConsentRecordsListCustomers makes the call the
// customers.reconcile_consent_records.list_customers event asks for.
func (w *workers) handleCustomersReconcileConsentRecordsListCustomers(ctx context.Context, event eventbus.Event) error {
	_, err := w.customerClient.ListCustomers(ctx, &customersv1.ListCustomersRequest{
		Filter: &customersv1.ListCustomersRequest_Filter{OwnerId: event.Attributes["filter.owner_id"]},
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
