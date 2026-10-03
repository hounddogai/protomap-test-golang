package storefront

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	patientappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patientapp/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the storefront domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	wireCalls(deps)
	deps.Clients.Put("acme.audit.v1.AuditEventService", auditv1.NewAuditEventServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerSegmentApprovalService", customersv1.NewCustomerSegmentApprovalServiceClient(deps.Conn))
	deps.Clients.Put("acme.customers.v1.CustomerSegmentSyncService", customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.TenantNotificationService", identityv1.NewTenantNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.identity.v1.UserHistoryService", identityv1.NewUserHistoryServiceClient(deps.Conn))
	deps.Clients.Put("acme.partnerapi.v1.PartnerIntegrationWorkflowService", partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.partnerapi.v1.PartnerWebhookIntegrationService", partnerapiv1.NewPartnerWebhookIntegrationServiceClient(deps.Conn))
	deps.Clients.Put("acme.partnerapi.v1.PartnerWebhookWorkflowService", partnerapiv1.NewPartnerWebhookWorkflowServiceClient(deps.Conn))
	deps.Clients.Put("acme.search.v1.SynonymNotificationService", searchv1.NewSynonymNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.storefront.v1.StorefrontSessionIntegrationService", storefrontv1.NewStorefrontSessionIntegrationServiceClient(deps.Conn))
	deps.Clients.Put("acme.storefront.v1.StorefrontSessionNotificationService", storefrontv1.NewStorefrontSessionNotificationServiceClient(deps.Conn))
	deps.Clients.Put("acme.storefront.v1.StorefrontSessionWorkflowService", storefrontv1.NewStorefrontSessionWorkflowServiceClient(deps.Conn))
	storefrontv1.RegisterStorefrontSessionGatewayServiceServer(server, NewStorefrontSessionGatewayServer(deps))
	storefrontv1.RegisterShopperViewServiceServer(server, NewShopperViewServer(deps))
	storefrontv1.RegisterLandingPageQueryServiceServer(server, NewLandingPageQueryServer(deps))
	storefrontSessionServer := NewStorefrontSessionServer(deps)
	server.RegisterService(&storefrontv1.StorefrontSessionService_ServiceDesc, storefrontSessionServer)
	storefrontv1.RegisterShopperViewQueryServiceServer(server, NewShopperViewQueryServer(deps))
	storefrontv1.RegisterLandingPageNotificationServiceServer(server, NewLandingPageNotificationServer(deps))
	storefrontv1.RegisterStorefrontSessionQueryServiceServer(server, NewStorefrontSessionQueryServer(deps))
	storefrontv1.RegisterShopperViewNotificationServiceServer(server, NewShopperViewNotificationServer(deps))
	storefrontv1.RegisterLandingPageIntegrationServiceServer(server, NewLandingPageIntegrationServer(deps))
	storefrontv1.RegisterStorefrontSessionNotificationServiceServer(server, NewStorefrontSessionNotificationServer(deps))
	storefrontv1.RegisterShopperViewIntegrationServiceServer(server, NewShopperViewIntegrationServer(deps))
	storefrontv1.RegisterLandingPageWorkflowServiceServer(server, NewLandingPageWorkflowServer(deps))
	storefrontv1.RegisterStorefrontSessionIntegrationServiceServer(server, NewStorefrontSessionIntegrationServer(deps))
	storefrontv1.RegisterShopperViewWorkflowServiceServer(server, NewShopperViewWorkflowServer(deps))
	storefrontv1.RegisterLandingPageReportingServiceServer(server, NewLandingPageReportingServer(deps))
	storefrontv1.RegisterStorefrontSessionWorkflowServiceServer(server, NewStorefrontSessionWorkflowServer(deps))
}

// The functions that call other services, which wireCalls sets when the module registers.
var (
	disconnectPartnerWebhookSourceCall func(context.Context, *partnerapiv1.DisconnectPartnerWebhookSourceRequest, ...grpc.CallOption) (*partnerapiv1.DisconnectPartnerWebhookSourceResponse, error)
	findAuditEventsCall                func(context.Context, *auditv1.FindAuditEventsRequest, ...grpc.CallOption) (*auditv1.FindAuditEventsResponse, error)
	schedulePreferenceExpiryCall       func(context.Context, *customersv1.SchedulePreferenceExpiryRequest) (*customersv1.SchedulePreferenceExpiryResponse, error)
	pushPreferenceCall                 func(context.Context, *customersv1.PushPreferenceRequest) (*customersv1.PushPreferenceResponse, error)
	listCustomerSegmentImportsCall     func(context.Context, *customersv1.ListCustomerSegmentImportsRequest, ...grpc.CallOption) (*customersv1.ListCustomerSegmentImportsResponse, error)
	resumeLandingPageWorkflowCall      func(context.Context, *storefrontv1.ResumeLandingPageWorkflowRequest) (*storefrontv1.ResumeLandingPageWorkflowResponse, error)
	submitConsentRecordCall            func(context.Context, *customersv1.SubmitConsentRecordRequest) (*customersv1.SubmitConsentRecordResponse, error)
	advanceLandingPageWorkflowCall     func(context.Context, *storefrontv1.AdvanceLandingPageWorkflowRequest) (*storefrontv1.AdvanceLandingPageWorkflowResponse, error)
	validateCustomerSegmentImportCall  func(context.Context, *customersv1.ValidateCustomerSegmentImportRequest) (*customersv1.ValidateCustomerSegmentImportResponse, error)
	escalateCustomerSegmentCall        func(context.Context, *customersv1.EscalateCustomerSegmentRequest, ...grpc.CallOption) (*customersv1.EscalateCustomerSegmentResponse, error)
)

// wireCalls points the package's call functions at the services they reach.
func wireCalls(deps platform.Deps) {
	partnerWebhookIntegrationForDisconnectPartnerWebhookSource := partnerapiv1.NewPartnerWebhookIntegrationServiceClient(deps.Conn)
	disconnectPartnerWebhookSourceCall = partnerWebhookIntegrationForDisconnectPartnerWebhookSource.DisconnectPartnerWebhookSource
	auditEventQueryForFindAuditEvents := auditv1.NewAuditEventQueryServiceClient(deps.Conn)
	findAuditEventsCall = auditEventQueryForFindAuditEvents.FindAuditEvents
	preferenceLifecycleForSchedulePreferenceExpiry := customersv1.NewPreferenceLifecycleServiceClient(deps.Conn)
	schedulePreferenceExpiryCall = func(ctx context.Context, request *customersv1.SchedulePreferenceExpiryRequest) (*customersv1.SchedulePreferenceExpiryResponse, error) {
		return preferenceLifecycleForSchedulePreferenceExpiry.SchedulePreferenceExpiry(ctx, request)
	}
	preferenceSyncForPushPreference := customersv1.NewPreferenceSyncServiceClient(deps.Conn)
	pushPreferenceCall = func(ctx context.Context, request *customersv1.PushPreferenceRequest) (*customersv1.PushPreferenceResponse, error) {
		return preferenceSyncForPushPreference.PushPreference(ctx, request)
	}
	customerSegmentImportForListCustomerSegmentImports := customersv1.NewCustomerSegmentImportServiceClient(deps.Conn)
	listCustomerSegmentImportsCall = customerSegmentImportForListCustomerSegmentImports.ListCustomerSegmentImports
	landingPageWorkflowForResumeLandingPageWorkflow := storefrontv1.NewLandingPageWorkflowServiceClient(deps.Conn)
	resumeLandingPageWorkflowCall = func(ctx context.Context, request *storefrontv1.ResumeLandingPageWorkflowRequest) (*storefrontv1.ResumeLandingPageWorkflowResponse, error) {
		return landingPageWorkflowForResumeLandingPageWorkflow.ResumeLandingPageWorkflow(ctx, request)
	}
	consentRecordApprovalForSubmitConsentRecord := customersv1.NewConsentRecordApprovalServiceClient(deps.Conn)
	submitConsentRecordCall = func(ctx context.Context, request *customersv1.SubmitConsentRecordRequest) (*customersv1.SubmitConsentRecordResponse, error) {
		return consentRecordApprovalForSubmitConsentRecord.SubmitConsentRecord(ctx, request)
	}
	landingPageWorkflowForAdvanceLandingPageWorkflow := storefrontv1.NewLandingPageWorkflowServiceClient(deps.Conn)
	advanceLandingPageWorkflowCall = func(ctx context.Context, request *storefrontv1.AdvanceLandingPageWorkflowRequest) (*storefrontv1.AdvanceLandingPageWorkflowResponse, error) {
		return landingPageWorkflowForAdvanceLandingPageWorkflow.AdvanceLandingPageWorkflow(ctx, request)
	}
	customerSegmentImportForValidateCustomerSegmentImport := customersv1.NewCustomerSegmentImportServiceClient(deps.Conn)
	validateCustomerSegmentImportCall = func(ctx context.Context, request *customersv1.ValidateCustomerSegmentImportRequest) (*customersv1.ValidateCustomerSegmentImportResponse, error) {
		return customerSegmentImportForValidateCustomerSegmentImport.ValidateCustomerSegmentImport(ctx, request)
	}
	customerSegmentApprovalForEscalateCustomerSegment := customersv1.NewCustomerSegmentApprovalServiceClient(deps.Conn)
	escalateCustomerSegmentCall = customerSegmentApprovalForEscalateCustomerSegment.EscalateCustomerSegment
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                                *slog.Logger
	storefrontSessionIntegrationClient storefrontv1.StorefrontSessionIntegrationServiceClient
	shopperViewWorkflowClient          storefrontv1.ShopperViewWorkflowServiceClient
	synonymSyncClient                  searchv1.SynonymSyncServiceClient
	patientHomeClient                  patientappv1.PatientHomeServiceClient
	landingPageReportingClient         storefrontv1.LandingPageReportingServiceClient
	accessPolicyClient                 authorizationv1.AccessPolicyServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                                deps.Logger,
		storefrontSessionIntegrationClient: storefrontv1.NewStorefrontSessionIntegrationServiceClient(deps.Conn),
		shopperViewWorkflowClient:          storefrontv1.NewShopperViewWorkflowServiceClient(deps.Conn),
		synonymSyncClient:                  searchv1.NewSynonymSyncServiceClient(deps.Conn),
		patientHomeClient:                  patientappv1.NewPatientHomeServiceClient(deps.Conn),
		landingPageReportingClient:         storefrontv1.NewLandingPageReportingServiceClient(deps.Conn),
		accessPolicyClient:                 authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
	}
	go w.consume(ctx, "storefront.resolve_landing_pages.test_storefront_session_connection", deps.Bus.Subscribe("storefront.resolve_landing_pages.test_storefront_session_connection"), w.handleStorefrontResolveLandingPagesTestStorefrontSessionConnection)
	go w.consume(ctx, "storefront.update_storefront_session.signal_shopper_view_workflow", deps.Bus.Subscribe("storefront.update_storefront_session.signal_shopper_view_workflow"), w.handleStorefrontUpdateStorefrontSessionSignalShopperViewWorkflow)
	go w.consume(ctx, "storefront.summarize_shopper_views.sync_synonyms", deps.Bus.Subscribe("storefront.summarize_shopper_views.sync_synonyms"), w.handleStorefrontSummarizeShopperViewsSyncSynonyms)
	go w.consume(ctx, "storefront.retry_landing_page_workflow.create_patient_home", deps.Bus.Subscribe("storefront.retry_landing_page_workflow.create_patient_home"), w.handleStorefrontRetryLandingPageWorkflowCreatePatientHome)
	go w.consume(ctx, "storefront.retry_landing_page_workflow.list_storefront_session_sources", deps.Bus.Subscribe("storefront.retry_landing_page_workflow.list_storefront_session_sources"), w.handleStorefrontRetryLandingPageWorkflowListStorefrontSessionSources)
	go w.consume(ctx, "storefront.start_shopper_view_workflow.list_patient_homes", deps.Bus.Subscribe("storefront.start_shopper_view_workflow.list_patient_homes"), w.handleStorefrontStartShopperViewWorkflowListPatientHomes)
	go w.consume(ctx, "storefront.cancel_shopper_view_workflow.generate_landing_page_report", deps.Bus.Subscribe("storefront.cancel_shopper_view_workflow.generate_landing_page_report"), w.handleStorefrontCancelShopperViewWorkflowGenerateLandingPageReport)
	go w.consume(ctx, "storefront.signal_storefront_session_workflow.update_access_policy", deps.Bus.Subscribe("storefront.signal_storefront_session_workflow.update_access_policy"), w.handleStorefrontSignalStorefrontSessionWorkflowUpdateAccessPolicy)
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

// handleStorefrontResolveLandingPagesTestStorefrontSessionConnection makes the call the
// storefront.resolve_landing_pages.test_storefront_session_connection event asks for.
func (w *workers) handleStorefrontResolveLandingPagesTestStorefrontSessionConnection(ctx context.Context, event eventbus.Event) error {
	_, err := w.storefrontSessionIntegrationClient.TestStorefrontSessionConnection(ctx, &storefrontv1.TestStorefrontSessionConnectionRequest{
		Details: &storefrontv1.TestStorefrontSessionConnectionRequest_Details{MessageId: event.Attributes["details.message_id"]},
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

// handleStorefrontUpdateStorefrontSessionSignalShopperViewWorkflow makes the call the
// storefront.update_storefront_session.signal_shopper_view_workflow event asks for.
func (w *workers) handleStorefrontUpdateStorefrontSessionSignalShopperViewWorkflow(ctx context.Context, event eventbus.Event) error {
	_, err := w.shopperViewWorkflowClient.SignalShopperViewWorkflow(ctx, &storefrontv1.SignalShopperViewWorkflowRequest{
		Details: &storefrontv1.SignalShopperViewWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: event.Attributes["details.notify.phone"]},
		},
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				DisplayName: event.Attributes["shopper_view.owner.display_name"],
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

// handleStorefrontSummarizeShopperViewsSyncSynonyms makes the call the storefront.summarize_shopper_views.sync_synonyms
// event asks for.
func (w *workers) handleStorefrontSummarizeShopperViewsSyncSynonyms(ctx context.Context, event eventbus.Event) error {
	_, err := w.synonymSyncClient.SyncSynonyms(ctx, &searchv1.SyncSynonymsRequest{
		Synonym: &searchv1.Synonym{
			Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{Reason: event.Attributes["synonym.block.reason"]}},
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

// handleStorefrontRetryLandingPageWorkflowCreatePatientHome makes the call the
// storefront.retry_landing_page_workflow.create_patient_home event asks for.
func (w *workers) handleStorefrontRetryLandingPageWorkflowCreatePatientHome(ctx context.Context, event eventbus.Event) error {
	_, err := w.patientHomeClient.CreatePatientHome(ctx, &patientappv1.CreatePatientHomeRequest{
		PatientHome: &patientappv1.PatientHome{
			Requester: &commonv1.ContactInfo{Phone: event.Attributes["patient_home.requester.phone"]},
			Assignee: &identityv1.UserRef{
				DisplayName: event.Attributes["patient_home.assignee.display_name"],
				Email:       event.Attributes["patient_home.assignee.email"],
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

// handleStorefrontRetryLandingPageWorkflowListStorefrontSessionSources makes the call the
// storefront.retry_landing_page_workflow.list_storefront_session_sources event asks for.
func (w *workers) handleStorefrontRetryLandingPageWorkflowListStorefrontSessionSources(ctx context.Context, event eventbus.Event) error {
	_, err := w.storefrontSessionIntegrationClient.ListStorefrontSessionSources(ctx, &storefrontv1.ListStorefrontSessionSourcesRequest{
		Filter: &storefrontv1.ListStorefrontSessionSourcesRequest_Filter{Region: event.Attributes["filter.region"]},
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

// handleStorefrontStartShopperViewWorkflowListPatientHomes makes the call the
// storefront.start_shopper_view_workflow.list_patient_homes event asks for.
func (w *workers) handleStorefrontStartShopperViewWorkflowListPatientHomes(ctx context.Context, event eventbus.Event) error {
	_, err := w.patientHomeClient.ListPatientHomes(ctx, &patientappv1.ListPatientHomesRequest{
		Filter: &patientappv1.ListPatientHomesRequest_Filter{
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

// handleStorefrontCancelShopperViewWorkflowGenerateLandingPageReport makes the call the
// storefront.cancel_shopper_view_workflow.generate_landing_page_report event asks for.
func (w *workers) handleStorefrontCancelShopperViewWorkflowGenerateLandingPageReport(ctx context.Context, event eventbus.Event) error {
	_, err := w.landingPageReportingClient.GenerateLandingPageReport(ctx, &storefrontv1.GenerateLandingPageReportRequest{
		Reason: event.Attributes["reason"],
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

// handleStorefrontSignalStorefrontSessionWorkflowUpdateAccessPolicy makes the call the
// storefront.signal_storefront_session_workflow.update_access_policy event asks for.
func (w *workers) handleStorefrontSignalStorefrontSessionWorkflowUpdateAccessPolicy(ctx context.Context, event eventbus.Event) error {
	_, err := w.accessPolicyClient.UpdateAccessPolicy(ctx, &authorizationv1.UpdateAccessPolicyRequest{
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{Email: event.Attributes["access_policy.owner.email"]},
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
