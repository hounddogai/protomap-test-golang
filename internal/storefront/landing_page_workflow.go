package storefront

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	datahubv1 "github.com/hounddogai/protomap-test-golang/gen/acme/datahub/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// LandingPageWorkflowServer implements `acme.storefront.v1.LandingPageWorkflowService`.
// Runs the long-lived workflow that takes a landing page from start to finish.
type LandingPageWorkflowServer struct {
	storefrontv1.UnimplementedLandingPageWorkflowServiceServer
	store                                *Store
	log                                  *slog.Logger
	landingPageReportingClient           storefrontv1.LandingPageReportingServiceClient
	householdApprovalClient              customersv1.HouseholdApprovalServiceClient
	auditEventClient                     auditv1.AuditEventServiceClient
	clients                              *registry.Registry
	featureFlagQueryClient               featureflagsv1.FeatureFlagQueryServiceClient
	conn                                 *grpc.ClientConn
	searchIndexQueryClient               searchv1.SearchIndexQueryServiceClient
	partnerIntegrationNotificationClient partnerapiv1.PartnerIntegrationNotificationServiceClient
	customerWorkflowClient               customersv1.CustomerWorkflowServiceClient
	bus                                  *eventbus.Bus
	workspaceClient                      datahubv1.WorkspaceServiceClient
	customerClient                       customersv1.CustomerServiceClient
	storefrontSessionIntegrationPort     storefrontSessionIntegrationPort
	storefrontSessionIntegrationClient   storefrontv1.StorefrontSessionIntegrationServiceClient
	partnerOrderIntegrationClient        partnerapiv1.PartnerOrderIntegrationServiceClient
	rejectCustomerFn                     func(context.Context, *customersv1.RejectCustomerRequest, ...grpc.CallOption) (*customersv1.RejectCustomerResponse, error)
}

// NewLandingPageWorkflowServer wires a LandingPageWorkflowServer.
func NewLandingPageWorkflowServer(deps platform.Deps) *LandingPageWorkflowServer {
	customerApproval := customersv1.NewCustomerApprovalServiceClient(deps.Conn)
	return &LandingPageWorkflowServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.storefront.v1.LandingPageWorkflowService"),
		landingPageReportingClient:           storefrontv1.NewLandingPageReportingServiceClient(deps.Conn),
		householdApprovalClient:              customersv1.NewHouseholdApprovalServiceClient(deps.Conn),
		auditEventClient:                     auditv1.NewAuditEventServiceClient(deps.Conn),
		clients:                              deps.Clients,
		featureFlagQueryClient:               featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		searchIndexQueryClient:               searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		partnerIntegrationNotificationClient: partnerapiv1.NewPartnerIntegrationNotificationServiceClient(deps.Conn),
		customerWorkflowClient:               customersv1.NewCustomerWorkflowServiceClient(deps.Conn),
		bus:                                  deps.Bus,
		workspaceClient:                      datahubv1.NewWorkspaceServiceClient(deps.Conn),
		customerClient:                       customersv1.NewCustomerServiceClient(deps.Conn),
		storefrontSessionIntegrationPort:     newStorefrontSessionIntegrationPort(deps),
		storefrontSessionIntegrationClient:   storefrontv1.NewStorefrontSessionIntegrationServiceClient(deps.Conn),
		partnerOrderIntegrationClient:        partnerapiv1.NewPartnerOrderIntegrationServiceClient(deps.Conn),
		rejectCustomerFn:                     customerApproval.RejectCustomer,
	}
}

// StartLandingPageWorkflow starts a landing page.
func (s *LandingPageWorkflowServer) StartLandingPageWorkflow(ctx context.Context, req *storefrontv1.StartLandingPageWorkflowRequest) (*storefrontv1.StartLandingPageWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLandingPage(ctx, req.GetContext().GetTenantId(), req.GetLandingPage()); err != nil {
		return nil, grpcx.Wrap(err, "save landing page")
	}
	s.log.InfoContext(ctx, "start landing page workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLandingPage().GetAudit().GetEtag())
	countLandingPageTotalsRequest := &storefrontv1.CountLandingPageTotalsRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.CountLandingPageTotalsRequest_Filter{
			Region:       req.GetLandingPage().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetLandingPage().GetAudit().GetUpdatedAt(),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
	countLandingPageTotalsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*storefrontv1.CountLandingPageTotalsResponse, error) {
		return s.landingPageReportingClient.CountLandingPageTotals(ctx, countLandingPageTotalsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/CountLandingPageTotals")
	}
	cloned := proto.Clone(req.GetLandingPage()).(*storefrontv1.LandingPage)
	ctx = tenancy.With(ctx, "locality", req.GetLandingPage().GetBillingAddress().GetLocality())
	delegateHouseholdApprovalRequest := &customersv1.DelegateHouseholdApprovalRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						CreatedBy: cloned.GetAudit().GetCreatedBy(),
						UpdatedAt: req.GetLandingPage().GetAudit().GetUpdatedAt(),
					},
					Location: &commonv1.PostalAddress{Locality: tenancy.Value(ctx, "locality")},
				}},
			}},
		},
	}
	send := s.delegateHouseholdApprovalSender(delegateHouseholdApprovalRequest)
	delegateHouseholdApprovalReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdApprovalService/DelegateHouseholdApproval")
	}
	searchAuditEventsReply, err := s.auditEventClient.SearchAuditEvents(ctx, &auditv1.SearchAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.SearchAuditEventsRequest_Filter{
			Region:       delegateHouseholdApprovalReply.GetHousehold().GetLocation().GetRegion(),
			CreatedAfter: delegateHouseholdApprovalReply.GetHousehold().GetAudit().GetUpdatedAt(),
			Labels:       delegateHouseholdApprovalReply.GetHousehold().GetAttributes(),
			OwnerId:      countLandingPageTotalsReply.GetResult().GetCorrelationId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/SearchAuditEvents")
	}
	_ = searchAuditEventsReply
	return &storefrontv1.StartLandingPageWorkflowResponse{
		Result: &commonv1.OperationResult{Status: countLandingPageTotalsReply.GetResult().GetStatus()},
		Outcome: &storefrontv1.StartLandingPageWorkflowResponse_Outcome{
			Message: countLandingPageTotalsReply.GetResult().GetMessage(),
		},
		LandingPage: &storefrontv1.LandingPage{
			BillingAddress: &commonv1.PostalAddress{
				Locality: delegateHouseholdApprovalReply.GetHousehold().GetLocation().GetLocality(),
			},
			LandingPageId: req.GetLandingPageId(),
			IssuedAt:      req.GetLandingPage().GetIssuedAt(),
		},
	}, nil
}

// AdvanceLandingPageWorkflow advances a landing page.
func (s *LandingPageWorkflowServer) AdvanceLandingPageWorkflow(ctx context.Context, req *storefrontv1.AdvanceLandingPageWorkflowRequest) (*storefrontv1.AdvanceLandingPageWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceLandingPageWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveLandingPage(ctx, req.GetContext().GetTenantId(), req.GetLandingPage()); err != nil {
		return nil, grpcx.Wrap(err, "save landing page")
	}
	s.log.InfoContext(ctx, "advance landing page workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLandingPage().GetAudit().GetEtag())
	partnerIntegrationWorkflow, err := registry.Lookup[partnerapiv1.PartnerIntegrationWorkflowServiceClient](s.clients, "acme.partnerapi.v1.PartnerIntegrationWorkflowService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	source := req.GetDetails().GetNotify().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	var holder struct{ Phone string }
	if record := req.GetDetails().GetNotify(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Phone"); err != nil {
			s.log.WarnContext(ctx, "copy phone", "error", err)
		}
	}
	signalPartnerIntegrationWorkflowRequest := &partnerapiv1.SignalPartnerIntegrationWorkflowRequest{
		Context: req.GetContext(),
		PartnerIntegration: &partnerapiv1.PartnerIntegration{
			Funding: &partnerapiv1.PartnerIntegration_Card{Card: &partnerapiv1.PartnerIntegrationCardFunding{HolderName: req.GetLandingPage().GetCard().GetHolderName()}},
		},
		Details: &partnerapiv1.SignalPartnerIntegrationWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotify().GetName(),
				Email: email,
				Phone: holder.Phone,
			},
		},
	}
	signalPartnerIntegrationWorkflowReply, err := partnerIntegrationWorkflow.SignalPartnerIntegrationWorkflow(ctx, signalPartnerIntegrationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/SignalPartnerIntegrationWorkflow")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := landingPageWorkflowServerBrowseFeatureFlagsSteps[mode]
	browseFeatureFlagsRequest := &featureflagsv1.BrowseFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.BrowseFeatureFlagsRequest_Filter{
			Labels:       signalPartnerIntegrationWorkflowReply.GetPartnerIntegration().GetLabels(),
			Region:       req.GetLandingPage().GetBillingAddress().GetRegion(),
			CreatedAfter: signalPartnerIntegrationWorkflowReply.GetPartnerIntegration().GetAudit().GetCreatedAt(),
			OwnerId:      signalPartnerIntegrationWorkflowReply.GetPartnerIntegration().GetHolder().GetPartnerWebhookId(),
		},
	}
	browseFeatureFlagsReply, err := run(s, ctx, browseFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags")
	}
	packed, err := anypb.New(req.GetLandingPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	testStorefrontSessionConnectionRequest := &storefrontv1.TestStorefrontSessionConnectionRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details: &storefrontv1.TestStorefrontSessionConnectionRequest_Details{
			MessageId: signalPartnerIntegrationWorkflowReply.GetOutcome().GetMessage(),
			Envelope:  packed,
		},
	}
	testStorefrontSessionConnectionReply := new(storefrontv1.TestStorefrontSessionConnectionResponse)
	if err := grpcx.Invoke(ctx, s.conn, storefrontv1.StorefrontSessionIntegrationService_TestStorefrontSessionConnection_FullMethodName, testStorefrontSessionConnectionRequest, testStorefrontSessionConnectionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/TestStorefrontSessionConnection")
	}
	return &storefrontv1.AdvanceLandingPageWorkflowResponse{
		Outcome: &storefrontv1.AdvanceLandingPageWorkflowResponse_Outcome{
			ReferenceId: signalPartnerIntegrationWorkflowReply.GetOutcome().GetReferenceId(),
			Status:      browseFeatureFlagsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Status: testStorefrontSessionConnectionReply.GetResult().GetStatus(),
		},
		LandingPage: &storefrontv1.LandingPage{
			LandingPageId: req.GetLandingPageId(),
			IssuedAt:      req.GetLandingPage().GetIssuedAt(),
		},
	}, nil
}

// CancelLandingPageWorkflow cancels a landing page.
func (s *LandingPageWorkflowServer) CancelLandingPageWorkflow(ctx context.Context, req *storefrontv1.CancelLandingPageWorkflowRequest) (*storefrontv1.CancelLandingPageWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLandingPage(ctx, req.GetContext().GetTenantId(), req.GetLandingPage()); err != nil {
		return nil, grpcx.Wrap(err, "save landing page")
	}
	s.log.InfoContext(ctx, "cancel landing page workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLandingPage().GetAudit().GetEtag())
	exportSearchIndexesStream, err := s.searchIndexQueryClient.ExportSearchIndexes(ctx, &searchv1.ExportSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchIndexesRequest_Filter{
			Region:       req.GetLandingPage().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetLandingPage().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetLandingPage().GetLandingPageId(),
			Labels:       req.GetAnnotations(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
	}
	for {
		received, err := exportSearchIndexesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
		}
		_ = received
	}
	subscribePartnerIntegrationUpdatesRequest := &partnerapiv1.SubscribePartnerIntegrationUpdatesRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SubscribePartnerIntegrationUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: req.GetLandingPage().GetCounterparty().GetEmail(),
				Phone: req.GetDetails().GetNotify().GetPhone(),
			},
		},
	}
	subscribePartnerIntegrationUpdatesReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*partnerapiv1.SubscribePartnerIntegrationUpdatesResponse, error) {
		return s.partnerIntegrationNotificationClient.SubscribePartnerIntegrationUpdates(ctx, subscribePartnerIntegrationUpdatesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/SubscribePartnerIntegrationUpdates")
	}
	listCustomerWorkflowsRequest := &customersv1.ListCustomerWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListCustomerWorkflowsRequest_Filter{
			Region: req.GetLandingPage().GetBillingAddress().GetRegion(),
		},
	}
	var listCustomerWorkflowsReply *customersv1.ListCustomerWorkflowsResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.CustomerWorkflowService/ListCustomerWorkflows", func(ctx context.Context) error {
		answer, err := s.customerWorkflowClient.ListCustomerWorkflows(ctx, listCustomerWorkflowsRequest)
		listCustomerWorkflowsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerWorkflowService/ListCustomerWorkflows")
	}
	return &storefrontv1.CancelLandingPageWorkflowResponse{
		LandingPage: &storefrontv1.LandingPage{
			Counterparty: &customersv1.CustomerRef{
				Email: subscribePartnerIntegrationUpdatesReply.GetPartnerIntegration().GetContact().GetEmail(),
			},
			LandingPageId: req.GetLandingPageId(),
			Status:        req.GetLandingPage().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: listCustomerWorkflowsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// RetryLandingPageWorkflow retrys a landing page.
func (s *LandingPageWorkflowServer) RetryLandingPageWorkflow(ctx context.Context, req *storefrontv1.RetryLandingPageWorkflowRequest) (*storefrontv1.RetryLandingPageWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryLandingPageWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveLandingPage(ctx, req.GetContext().GetTenantId(), req.GetLandingPage()); err != nil {
		return nil, grpcx.Wrap(err, "save landing page")
	}
	s.log.InfoContext(ctx, "retry landing page workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLandingPage().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "storefront.retry_landing_page_workflow.create_patient_home",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"patient_home.requester.phone":       req.GetDetails().GetNotify().GetPhone(),
			"patient_home.assignee.display_name": req.GetLandingPage().GetCounterparty().GetDisplayName(),
			"patient_home.assignee.email":        req.GetDetails().GetNotify().GetEmail(),
			"context.tenant_id":                  req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	createWorkspaceRequest := &datahubv1.CreateWorkspaceRequest{
		Context: req.GetContext(),
		Workspace: &datahubv1.Workspace{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetLandingPage().GetAudit().GetCreatedAt(),
				CreatedBy: req.GetLandingPage().GetAudit().GetCreatedBy(),
			},
		},
	}
	createWorkspaceReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*datahubv1.CreateWorkspaceResponse, error) {
		return s.workspaceClient.CreateWorkspace(ctx, createWorkspaceRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.datahub.v1.WorkspaceService/CreateWorkspace")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "storefront.retry_landing_page_workflow.list_storefront_session_sources",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     req.GetLandingPage().GetBillingAddress().GetRegion(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &storefrontv1.RetryLandingPageWorkflowResponse{
		LandingPage: &storefrontv1.LandingPage{
			Audit: &commonv1.AuditInfo{
				Etag: createWorkspaceReply.GetWorkspace().GetAudit().GetEtag(),
			},
			Counterparty: &customersv1.CustomerRef{
				DisplayName: createWorkspaceReply.GetWorkspace().GetAssignee().GetDisplayName(),
			},
			LandingPageId: req.GetLandingPageId(),
			Status:        req.GetLandingPage().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseLandingPageWorkflow pauses a landing page.
func (s *LandingPageWorkflowServer) PauseLandingPageWorkflow(ctx context.Context, req *storefrontv1.PauseLandingPageWorkflowRequest) (*storefrontv1.PauseLandingPageWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseLandingPageWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveLandingPage(ctx, req.GetContext().GetTenantId(), req.GetLandingPage()); err != nil {
		return nil, grpcx.Wrap(err, "save landing page")
	}
	s.log.InfoContext(ctx, "pause landing page workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLandingPage().GetAudit().GetEtag())
	streamLandingPageMetricsStream, err := s.landingPageReportingClient.StreamLandingPageMetrics(ctx, &storefrontv1.StreamLandingPageMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone:            req.GetDetails().GetNotify().GetPhone(),
			Name:             req.GetDetails().GetNotify().GetName(),
			PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/StreamLandingPageMetrics")
	}
	for {
		received, err := streamLandingPageMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/StreamLandingPageMetrics")
		}
		_ = received
	}
	batchGetCustomersRequest := buildBatchGetCustomersRequestForPauseLandingPageWorkflow(req)
	batchGetCustomersReply, err := s.customerClient.BatchGetCustomers(ctx, batchGetCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchGetCustomers")
	}
	return &storefrontv1.PauseLandingPageWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: batchGetCustomersReply.GetResult().GetDetails(),
			Status:  batchGetCustomersReply.GetResult().GetStatus(),
		},
		LandingPage: &storefrontv1.LandingPage{
			LandingPageId: req.GetLandingPageId(),
			Number:        req.GetLandingPage().GetNumber(),
		},
	}, nil
}

// ResumeLandingPageWorkflow resumes a landing page.
func (s *LandingPageWorkflowServer) ResumeLandingPageWorkflow(ctx context.Context, req *storefrontv1.ResumeLandingPageWorkflowRequest) (*storefrontv1.ResumeLandingPageWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLandingPage(ctx, req.GetContext().GetTenantId(), req.GetLandingPage()); err != nil {
		return nil, grpcx.Wrap(err, "save landing page")
	}
	s.log.InfoContext(ctx, "resume landing page workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLandingPage().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	acknowledgeStorefrontSessionEventRequest := &storefrontv1.AcknowledgeStorefrontSessionEventRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details: &storefrontv1.AcknowledgeStorefrontSessionEventRequest_Details{
			ReceivedAt: req.GetLandingPage().GetAudit().GetUpdatedAt(),
			MessageId:  req.GetLandingPage().GetCounterparty().GetCustomerId(),
		},
	}
	acknowledgeStorefrontSessionEventReply, err := s.storefrontSessionIntegrationPort.AcknowledgeStorefrontSessionEvent(ctx, acknowledgeStorefrontSessionEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/AcknowledgeStorefrontSessionEvent")
	}
	step := &landingPageWorkflowServerResumeLandingPageWorkflowDisconnectStorefrontSessionSourceStep{request: &storefrontv1.DisconnectStorefrontSessionSourceRequest{
		Context:             req.GetContext(),
		StorefrontSessionId: acknowledgeStorefrontSessionEventReply.GetStorefrontSession().GetStorefrontSessionId(),
	}}
	if err := s.runResumeLandingPageWorkflowDisconnectStorefrontSessionSource(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/DisconnectStorefrontSessionSource")
	}
	disconnectStorefrontSessionSourceReply := step.reply
	return &storefrontv1.ResumeLandingPageWorkflowResponse{
		LandingPage: &storefrontv1.LandingPage{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: acknowledgeStorefrontSessionEventReply.GetStorefrontSession().GetAssignee().GetDisplayName(),
			},
			LandingPageId: req.GetLandingPageId(),
			Status:        req.GetLandingPage().GetStatus(),
		},
		Outcome: &storefrontv1.ResumeLandingPageWorkflowResponse_Outcome{
			EffectiveAt: disconnectStorefrontSessionSourceReply.GetOutcome().GetEffectiveAt(),
			Message:     disconnectStorefrontSessionSourceReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListLandingPageWorkflows lists landing pages a page at a time.
func (s *LandingPageWorkflowServer) ListLandingPageWorkflows(ctx context.Context, req *storefrontv1.ListLandingPageWorkflowsRequest) (*storefrontv1.ListLandingPageWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListLandingPageWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list landing page workflows", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	disconnectStorefrontSessionSourceRequest := &storefrontv1.DisconnectStorefrontSessionSourceRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.DisconnectStorefrontSessionSourceRequest_Details{
			ReceivedAt: req.GetFilter().GetCreatedAfter(),
			Envelope:   packed,
		},
	}
	disconnectStorefrontSessionSourceReply := new(storefrontv1.DisconnectStorefrontSessionSourceResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", storefrontStorefrontSessionIntegrationServiceName, "DisconnectStorefrontSessionSource"), disconnectStorefrontSessionSourceRequest, disconnectStorefrontSessionSourceReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/DisconnectStorefrontSessionSource")
	}
	return &storefrontv1.ListLandingPageWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Status: disconnectStorefrontSessionSourceReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// SignalLandingPageWorkflow signals a landing page.
func (s *LandingPageWorkflowServer) SignalLandingPageWorkflow(ctx context.Context, req *storefrontv1.SignalLandingPageWorkflowRequest) (*storefrontv1.SignalLandingPageWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveLandingPage(ctx, req.GetContext().GetTenantId(), req.GetLandingPage()); err != nil {
		return nil, grpcx.Wrap(err, "save landing page")
	}
	s.log.InfoContext(ctx, "signal landing page workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLandingPage().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetLandingPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	replayStorefrontSessionEventsReply, err := s.storefrontSessionIntegrationClient.ReplayStorefrontSessionEvents(ctx, &storefrontv1.ReplayStorefrontSessionEventsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &storefrontv1.ReplayStorefrontSessionEventsRequest_Details{Envelope: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/ReplayStorefrontSessionEvents")
	}
	annotations2 := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations2[key2] = value2
	}
	packed2, err := anypb.New(req.GetLandingPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	disconnectPartnerOrderSourceReply, err := s.partnerOrderIntegrationClient.DisconnectPartnerOrderSource(ctx, &partnerapiv1.DisconnectPartnerOrderSourceRequest{
		Context:     req.GetContext(),
		Annotations: annotations2,
		Details: &partnerapiv1.DisconnectPartnerOrderSourceRequest_Details{
			MessageId: replayStorefrontSessionEventsReply.GetResult().GetMessage(),
			Envelope:  packed2,
		},
		Reason: req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderIntegrationService/DisconnectPartnerOrderSource")
	}
	for line := range iterx.Each(req.GetLandingPage().GetLines(), func(candidate *storefrontv1.LandingPageLine) bool { return candidate != nil }) {
		if _, err := s.storefrontSessionIntegrationClient.DisconnectStorefrontSessionSource(ctx, &storefrontv1.DisconnectStorefrontSessionSourceRequest{
			Context:             req.GetContext(),
			StorefrontSessionId: replayStorefrontSessionEventsReply.GetStorefrontSession().GetStorefrontSessionId(),
			Details:             &storefrontv1.DisconnectStorefrontSessionSourceRequest_Details{MessageId: line.GetLineId()},
			Reason:              req.GetReason(),
			Annotations:         req.GetAnnotations(),
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/DisconnectStorefrontSessionSource")
		}
	}
	rejectCustomerRequest := &customersv1.RejectCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: disconnectPartnerOrderSourceReply.GetPartnerOrder().GetAudit().GetCreatedAt(),
			},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetLandingPage().GetCounterparty().GetDisplayName(),
			},
		},
	}
	rejectCustomerReply, err := s.rejectCustomerFn(ctx, rejectCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerApprovalService/RejectCustomer")
	}
	return &storefrontv1.SignalLandingPageWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: replayStorefrontSessionEventsReply.GetOutcome().GetStatus(),
		},
		LandingPage: &storefrontv1.LandingPage{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: replayStorefrontSessionEventsReply.GetStorefrontSession().GetAudit().GetUpdatedAt(),
				Etag:      disconnectPartnerOrderSourceReply.GetPartnerOrder().GetAudit().GetEtag(),
			},
			Counterparty: &customersv1.CustomerRef{
				Email: rejectCustomerReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			LandingPageId:  req.GetLandingPageId(),
			BillingAddress: req.GetLandingPage().GetBillingAddress(),
		},
	}, nil
}

// delegateHouseholdApprovalSender returns a function that sends one request to
// acme.customers.v1.HouseholdApprovalService/DelegateHouseholdApproval.
func (s *LandingPageWorkflowServer) delegateHouseholdApprovalSender(request *customersv1.DelegateHouseholdApprovalRequest) func(context.Context) (*customersv1.DelegateHouseholdApprovalResponse, error) {
	return func(ctx context.Context) (*customersv1.DelegateHouseholdApprovalResponse, error) {
		return s.householdApprovalClient.DelegateHouseholdApproval(ctx, request)
	}
}

// landingPageWorkflowServerBrowseFeatureFlagsSteps are the ways to run
// acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags, by name.
var landingPageWorkflowServerBrowseFeatureFlagsSteps = map[string]func(*LandingPageWorkflowServer, context.Context, *featureflagsv1.BrowseFeatureFlagsRequest) (*featureflagsv1.BrowseFeatureFlagsResponse, error){
	"default": (*LandingPageWorkflowServer).browseFeatureFlagsStep,
	"audited": (*LandingPageWorkflowServer).browseFeatureFlagsAuditedStep,
}

func (s *LandingPageWorkflowServer) browseFeatureFlagsStep(ctx context.Context, request *featureflagsv1.BrowseFeatureFlagsRequest) (*featureflagsv1.BrowseFeatureFlagsResponse, error) {
	return s.featureFlagQueryClient.BrowseFeatureFlags(ctx, request)
}

func (s *LandingPageWorkflowServer) browseFeatureFlagsAuditedStep(ctx context.Context, request *featureflagsv1.BrowseFeatureFlagsRequest) (*featureflagsv1.BrowseFeatureFlagsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags")
	return s.featureFlagQueryClient.BrowseFeatureFlags(ctx, request)
}

// landingPageWorkflowServerResumeLandingPageWorkflowDisconnectStorefrontSessionSourceStep carries the request of one
// step of ResumeLandingPageWorkflow and, once it ran, its reply.
type landingPageWorkflowServerResumeLandingPageWorkflowDisconnectStorefrontSessionSourceStep struct {
	request *storefrontv1.DisconnectStorefrontSessionSourceRequest
	reply   *storefrontv1.DisconnectStorefrontSessionSourceResponse
}

func (s *LandingPageWorkflowServer) runResumeLandingPageWorkflowDisconnectStorefrontSessionSource(ctx context.Context, step *landingPageWorkflowServerResumeLandingPageWorkflowDisconnectStorefrontSessionSourceStep) error {
	reply, err := s.storefrontSessionIntegrationClient.DisconnectStorefrontSessionSource(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
