package storefront

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShopperViewIntegrationServer implements `acme.storefront.v1.ShopperViewIntegrationService`.
// Connects external systems that send shopper view events.
type ShopperViewIntegrationServer struct {
	storefrontv1.UnimplementedShopperViewIntegrationServiceServer
	store                               *Store
	log                                 *slog.Logger
	conn                                *grpc.ClientConn
	apiCredentialHistoryClient          identityv1.ApiCredentialHistoryServiceClient
	clients                             *registry.Registry
	partnerOrderQueryClient             partnerapiv1.PartnerOrderQueryServiceClient
	shopperViewWorkflowClient           storefrontv1.ShopperViewWorkflowServiceClient
	partnerIntegrationIntegrationClient partnerapiv1.PartnerIntegrationIntegrationServiceClient
	landingPageReportingClient          storefrontv1.LandingPageReportingServiceClient
	partnerIntegrationReportingClient   partnerapiv1.PartnerIntegrationReportingServiceClient
	disconnectStorefrontSessionSourceFn func(context.Context, *storefrontv1.DisconnectStorefrontSessionSourceRequest, ...grpc.CallOption) (*storefrontv1.DisconnectStorefrontSessionSourceResponse, error)
	consentRecordWorkflowClient         customersv1.ConsentRecordWorkflowServiceClient
	householdApprovalClient             customersv1.HouseholdApprovalServiceClient
	searchIndexNotificationClient       searchv1.SearchIndexNotificationServiceClient
	meteredShopperViewWorkflow          meteredShopperViewWorkflow
	customerSegmentImportClient         customersv1.CustomerSegmentImportServiceClient
}

// NewShopperViewIntegrationServer wires a ShopperViewIntegrationServer.
func NewShopperViewIntegrationServer(deps platform.Deps) *ShopperViewIntegrationServer {
	storefrontSessionIntegration := storefrontv1.NewStorefrontSessionIntegrationServiceClient(deps.Conn)
	return &ShopperViewIntegrationServer{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.storefront.v1.ShopperViewIntegrationService"),
		conn:                                deps.Conn,
		apiCredentialHistoryClient:          identityv1.NewApiCredentialHistoryServiceClient(deps.Conn),
		clients:                             deps.Clients,
		partnerOrderQueryClient:             partnerapiv1.NewPartnerOrderQueryServiceClient(deps.Conn),
		shopperViewWorkflowClient:           storefrontv1.NewShopperViewWorkflowServiceClient(deps.Conn),
		partnerIntegrationIntegrationClient: partnerapiv1.NewPartnerIntegrationIntegrationServiceClient(deps.Conn),
		landingPageReportingClient:          storefrontv1.NewLandingPageReportingServiceClient(deps.Conn),
		partnerIntegrationReportingClient:   partnerapiv1.NewPartnerIntegrationReportingServiceClient(deps.Conn),
		disconnectStorefrontSessionSourceFn: storefrontSessionIntegration.DisconnectStorefrontSessionSource,
		consentRecordWorkflowClient:         customersv1.NewConsentRecordWorkflowServiceClient(deps.Conn),
		householdApprovalClient:             customersv1.NewHouseholdApprovalServiceClient(deps.Conn),
		searchIndexNotificationClient:       searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		meteredShopperViewWorkflow:          meteredShopperViewWorkflow{ShopperViewWorkflowServiceClient: storefrontv1.NewShopperViewWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		customerSegmentImportClient:         customersv1.NewCustomerSegmentImportServiceClient(deps.Conn),
	}
}

// ConnectShopperViewSource connects a shopper view.
func (s *ShopperViewIntegrationServer) ConnectShopperViewSource(ctx context.Context, req *storefrontv1.ConnectShopperViewSourceRequest) (*storefrontv1.ConnectShopperViewSourceResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "connect_shopper_view_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "connect shopper view source", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	validateCustomerSegmentImportRequest := &customersv1.ValidateCustomerSegmentImportRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: tenancy.Value(ctx, "reason")}},
			Owner:  &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
		},
		Details: &customersv1.ValidateCustomerSegmentImportRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cachedContext.GetActor().GetEmail()},
		},
		Annotations: req.GetAnnotations(),
	}
	validateCustomerSegmentImportReply, err := validateCustomerSegmentImportCall(ctx, validateCustomerSegmentImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/ValidateCustomerSegmentImport")
	}
	listFeatureFlagsRequest := &featureflagsv1.ListFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter:  &featureflagsv1.ListFeatureFlagsRequest_Filter{CreatedAfter: req.GetDetails().GetReceivedAt()},
	}
	listFeatureFlagsReply, err := grpcx.NewClient(s.conn, featureflagsv1.NewFeatureFlagServiceClient).ListFeatureFlags(ctx, listFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/ListFeatureFlags")
	}
	return &storefrontv1.ConnectShopperViewSourceResponse{
		Outcome: &storefrontv1.ConnectShopperViewSourceResponse_Outcome{
			Message: validateCustomerSegmentImportReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: validateCustomerSegmentImportReply.GetOutcome().GetMessage(),
			Details: listFeatureFlagsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		ShopperView: &storefrontv1.ShopperView{ShopperViewId: req.GetShopperViewId()},
	}, nil
}

// DisconnectShopperViewSource disconnects a shopper view.
func (s *ShopperViewIntegrationServer) DisconnectShopperViewSource(ctx context.Context, req *storefrontv1.DisconnectShopperViewSourceRequest) (*storefrontv1.DisconnectShopperViewSourceResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "disconnect_shopper_view_source"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "disconnect shopper view source", "tenant", req.GetContext().GetTenantId())
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listApiCredentialVersionsReply, err := s.apiCredentialHistoryClient.ListApiCredentialVersions(ctx, &identityv1.ListApiCredentialVersionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.ListApiCredentialVersionsRequest_Filter{CreatedAfter: req.GetDetails().GetReceivedAt()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/ListApiCredentialVersions")
	}
	partnerWebhookWorkflow, err := registry.Lookup[partnerapiv1.PartnerWebhookWorkflowServiceClient](s.clients, "acme.partnerapi.v1.PartnerWebhookWorkflowService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pausePartnerWebhookWorkflowRequest := &partnerapiv1.PausePartnerWebhookWorkflowRequest{
		Context: req.GetContext(),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{
			CorrelationId: listApiCredentialVersionsReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		PartnerWebhookId: envelope2.GetDisplayName(),
	}
	pausePartnerWebhookWorkflowReply, err := partnerWebhookWorkflow.PausePartnerWebhookWorkflow(ctx, pausePartnerWebhookWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookWorkflowService/PausePartnerWebhookWorkflow")
	}
	var envelope3 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	exportPartnerOrdersStream, err := s.partnerOrderQueryClient.ExportPartnerOrders(ctx, &partnerapiv1.ExportPartnerOrdersRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ExportPartnerOrdersRequest_Filter{
			CreatedAfter: pausePartnerWebhookWorkflowReply.GetOutcome().GetEffectiveAt(),
			Query:        envelope3.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderQueryService/ExportPartnerOrders")
	}
	for {
		received, err := exportPartnerOrdersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderQueryService/ExportPartnerOrders")
		}
		_ = received
	}
	return &storefrontv1.DisconnectShopperViewSourceResponse{
		Outcome: &storefrontv1.DisconnectShopperViewSourceResponse_Outcome{
			Status: listApiCredentialVersionsReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Status:        pausePartnerWebhookWorkflowReply.GetOutcome().GetStatus(),
			CorrelationId: pausePartnerWebhookWorkflowReply.GetPartnerWebhook().GetCorrelationId(),
		},
		ShopperView: &storefrontv1.ShopperView{ShopperViewId: req.GetShopperViewId()},
	}, nil
}

// IngestShopperViewEvents takes a stream of shopper views and answers once at the end.
func (s *ShopperViewIntegrationServer) IngestShopperViewEvents(stream grpc.ClientStreamingServer[storefrontv1.IngestShopperViewEventsRequest, storefrontv1.IngestShopperViewEventsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "ingest shopper view events", "tenant", msg.GetContext().GetTenantId())
		retryShopperViewWorkflowRequest := &storefrontv1.RetryShopperViewWorkflowRequest{}
		retryShopperViewWorkflowRequest.Context = msg.GetContext()
		retryShopperViewWorkflowRequest.ShopperView = &storefrontv1.ShopperView{
			Dimensions: &storefrontv1.ShopperViewDimensions{Length: msg.GetRecord().GetDimensions().GetLength()},
			Owner:      &identityv1.UserRef{Email: msg.GetHeader().GetSubmittedBy().GetEmail()},
		}
		if _, err := s.shopperViewWorkflowClient.RetryShopperViewWorkflow(ctx, retryShopperViewWorkflowRequest); err != nil {
			return grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/RetryShopperViewWorkflow")
		}
		if _, err := s.partnerIntegrationIntegrationClient.ListPartnerIntegrationSources(ctx, &partnerapiv1.ListPartnerIntegrationSourcesRequest{
			Context: msg.GetContext(),
			Filter: &partnerapiv1.ListPartnerIntegrationSourcesRequest_Filter{
				Region:       msg.GetRecord().GetAddress().GetRegion(),
				CreatedAfter: msg.GetRecord().GetAudit().GetUpdatedAt(),
			},
		}); err != nil {
			return grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationIntegrationService/ListPartnerIntegrationSources")
		}
		listLandingPageReportsRequest := &storefrontv1.ListLandingPageReportsRequest{
			Context: msg.GetContext(),
			Filter:  &storefrontv1.ListLandingPageReportsRequest_Filter{Region: msg.GetRecord().GetAddress().GetRegion()},
		}
		if _, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*storefrontv1.ListLandingPageReportsResponse, error) {
			return s.landingPageReportingClient.ListLandingPageReports(ctx, listLandingPageReportsRequest)
		}); err != nil {
			return grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/ListLandingPageReports")
		}
		accepted++
	}
	return stream.SendAndClose(&storefrontv1.IngestShopperViewEventsResponse{Accepted: accepted})
}

// ReplayShopperViewEvents replays a shopper view.
func (s *ShopperViewIntegrationServer) ReplayShopperViewEvents(ctx context.Context, req *storefrontv1.ReplayShopperViewEventsRequest) (*storefrontv1.ReplayShopperViewEventsResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "replay_shopper_view_events"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "replay shopper view events", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listPartnerIntegrationReportsRequest := &partnerapiv1.ListPartnerIntegrationReportsRequest{
		Context: req.GetContext(),
		Filter: &partnerapiv1.ListPartnerIntegrationReportsRequest_Filter{
			CreatedAfter: req.GetDetails().GetReceivedAt(),
			Labels:       annotations,
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listPartnerIntegrationReportsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*partnerapiv1.ListPartnerIntegrationReportsResponse, error) {
		return s.partnerIntegrationReportingClient.ListPartnerIntegrationReports(ctx, listPartnerIntegrationReportsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationReportingService/ListPartnerIntegrationReports")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope2 storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	disconnectStorefrontSessionSourceRequest := &storefrontv1.DisconnectStorefrontSessionSourceRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.DisconnectStorefrontSessionSourceRequest_Details{
			ReceivedAt: req.GetDetails().GetReceivedAt(),
			Envelope:   packed,
		},
		StorefrontSessionId: envelope2.GetDisplayName(),
	}
	disconnectStorefrontSessionSourceReply, err := s.disconnectStorefrontSessionSourceFn(ctx, disconnectStorefrontSessionSourceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/DisconnectStorefrontSessionSource")
	}
	startConsentRecordWorkflowRequest := buildStartConsentRecordWorkflowRequestForReplayShopperViewEvents(req, disconnectStorefrontSessionSourceReply)
	startConsentRecordWorkflowReply, err := s.consentRecordWorkflowClient.StartConsentRecordWorkflow(ctx, startConsentRecordWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordWorkflowService/StartConsentRecordWorkflow")
	}
	return &storefrontv1.ReplayShopperViewEventsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listPartnerIntegrationReportsReply.GetResult().GetCorrelationId(),
			Details:       startConsentRecordWorkflowReply.GetResult().GetDetails(),
			Status:        startConsentRecordWorkflowReply.GetResult().GetStatus(),
		},
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				Email:       disconnectStorefrontSessionSourceReply.GetStorefrontSession().GetAssignee().GetEmail(),
				DisplayName: disconnectStorefrontSessionSourceReply.GetStorefrontSession().GetCustomer().GetDisplayName(),
			},
			ShopperViewId: req.GetShopperViewId(),
		},
	}, nil
}

// AcknowledgeShopperViewEvent acknowledges a shopper view.
func (s *ShopperViewIntegrationServer) AcknowledgeShopperViewEvent(ctx context.Context, req *storefrontv1.AcknowledgeShopperViewEventRequest) (*storefrontv1.AcknowledgeShopperViewEventResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "acknowledge_shopper_view_event"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "acknowledge shopper view event", "tenant", req.GetContext().GetTenantId())
	inspectShopperViewWorkflowRequest := newInspectShopperViewWorkflowRequest(withInspectShopperViewWorkflowContext(req.GetContext()), withInspectShopperViewWorkflowShopperViewId(req.GetShopperViewId()))
	inspectShopperViewWorkflowReply, err := s.shopperViewWorkflowClient.InspectShopperViewWorkflow(ctx, inspectShopperViewWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/InspectShopperViewWorkflow")
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cancelConsentRecordWorkflowRequest := &customersv1.CancelConsentRecordWorkflowRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: inspectShopperViewWorkflowReply.GetShopperView().GetOwner().GetDisplayName(),
			},
		},
		ConsentRecordId: envelope.GetDisplayName(),
	}
	cancelConsentRecordWorkflowReply := new(customersv1.CancelConsentRecordWorkflowResponse)
	if err := grpc.Invoke(ctx, "/acme.customers.v1.ConsentRecordWorkflowService/CancelConsentRecordWorkflow", cancelConsentRecordWorkflowRequest, cancelConsentRecordWorkflowReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordWorkflowService/CancelConsentRecordWorkflow")
	}
	escalateHouseholdRequest := &customersv1.EscalateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						UserId: inspectShopperViewWorkflowReply.GetShopperView().GetOwner().GetUserId(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedBy: inspectShopperViewWorkflowReply.GetShopperView().GetAudit().GetCreatedBy(),
					},
					Location: &commonv1.PostalAddress{
						CountryCode: inspectShopperViewWorkflowReply.GetShopperView().GetAddress().GetCountryCode(),
					},
				}},
			}},
		},
	}
	escalateHouseholdReply, err := grpcx.Call(ctx, s.householdApprovalClient.EscalateHousehold, escalateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdApprovalService/EscalateHousehold")
	}
	return &storefrontv1.AcknowledgeShopperViewEventResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: inspectShopperViewWorkflowReply.GetResult().GetCorrelationId(),
			Message:       escalateHouseholdReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				UserId:      inspectShopperViewWorkflowReply.GetShopperView().GetOwner().GetUserId(),
				DisplayName: cancelConsentRecordWorkflowReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			ShopperViewId: req.GetShopperViewId(),
		},
		Outcome: &storefrontv1.AcknowledgeShopperViewEventResponse_Outcome{
			ReferenceId: cancelConsentRecordWorkflowReply.GetOutcome().GetReferenceId(),
			Message:     escalateHouseholdReply.GetOutcome().GetMessage(),
		},
	}, nil
}

// RelayShopperViewMessages exchanges changes of shopper views with a peer.
func (s *ShopperViewIntegrationServer) RelayShopperViewMessages(stream grpc.BidiStreamingServer[storefrontv1.RelayShopperViewMessagesRequest, storefrontv1.RelayShopperViewMessagesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *storefrontv1.RelayShopperViewMessagesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "relay shopper view messages", "tenant", msg.GetContext().GetTenantId())
		subscribeSearchIndexUpdatesRequest := &searchv1.SubscribeSearchIndexUpdatesRequest{
			Context: msg.GetContext(),
			Details: &searchv1.SubscribeSearchIndexUpdatesRequest_Details{
				Recipient: &commonv1.ContactInfo{Email: msg.GetChange().GetOwner().GetEmail()},
				Variables: msg.GetChange().GetSpecs(),
			},
			Annotations: msg.GetContext().GetBaggage(),
		}
		if _, err := grpcx.Call(ctx, s.searchIndexNotificationClient.SubscribeSearchIndexUpdates, subscribeSearchIndexUpdatesRequest); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SubscribeSearchIndexUpdates")
		}
		advanceShopperViewWorkflowRequest := &storefrontv1.AdvanceShopperViewWorkflowRequest{Context: msg.GetContext()}
		if _, err := s.meteredShopperViewWorkflow.AdvanceShopperViewWorkflow(ctx, advanceShopperViewWorkflowRequest); err != nil {
			return grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/AdvanceShopperViewWorkflow")
		}
		if err := stream.Send(&storefrontv1.RelayShopperViewMessagesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ListShopperViewSources lists shopper views a page at a time.
func (s *ShopperViewIntegrationServer) ListShopperViewSources(ctx context.Context, req *storefrontv1.ListShopperViewSourcesRequest) (*storefrontv1.ListShopperViewSourcesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListShopperViewSources", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list shopper view sources", "tenant", req.GetContext().GetTenantId())
	return &storefrontv1.ListShopperViewSourcesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// TestShopperViewConnection tests a shopper view.
func (s *ShopperViewIntegrationServer) TestShopperViewConnection(ctx context.Context, req *storefrontv1.TestShopperViewConnectionRequest) (*storefrontv1.TestShopperViewConnectionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "test_shopper_view_connection"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "test shopper view connection", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := shopperViewIntegrationServerCompareApiCredentialVersionsSteps[mode]
	compareApiCredentialVersionsRequest := &identityv1.CompareApiCredentialVersionsRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations: req.GetAnnotations(),
	}
	compareApiCredentialVersionsReply, err := run(s, ctx, compareApiCredentialVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialHistoryService/CompareApiCredentialVersions")
	}
	var envelope storefrontv1.StorefrontSessionRef
	if err := req.GetDetails().GetEnvelope().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countLandingPageTotalsReply, err := s.landingPageReportingClient.CountLandingPageTotals(ctx, &storefrontv1.CountLandingPageTotalsRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.CountLandingPageTotalsRequest_Filter{
			Labels: compareApiCredentialVersionsReply.GetApiCredential().GetLabels(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.LandingPageReportingService/CountLandingPageTotals")
	}
	listCustomerSegmentImportsRequest := &customersv1.ListCustomerSegmentImportsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListCustomerSegmentImportsRequest_Filter{
			Labels:       compareApiCredentialVersionsReply.GetApiCredential().GetLabels(),
			CreatedAfter: req.GetDetails().GetReceivedAt(),
			OwnerId:      compareApiCredentialVersionsReply.GetApiCredential().GetHolder().GetSessionId(),
		},
	}
	listCustomerSegmentImportsReply, err := grpcx.Call(ctx, s.customerSegmentImportClient.ListCustomerSegmentImports, listCustomerSegmentImportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/ListCustomerSegmentImports")
	}
	_ = listCustomerSegmentImportsReply
	return &storefrontv1.TestShopperViewConnectionResponse{
		Result: &commonv1.OperationResult{
			Details:       compareApiCredentialVersionsReply.GetResult().GetDetails(),
			ReferenceIds:  compareApiCredentialVersionsReply.GetResult().GetReferenceIds(),
			CorrelationId: countLandingPageTotalsReply.GetResult().GetCorrelationId(),
			Status:        countLandingPageTotalsReply.GetResult().GetStatus(),
		},
		ShopperView: &storefrontv1.ShopperView{ShopperViewId: req.GetShopperViewId()},
	}, nil
}

// shopperViewIntegrationServerCompareApiCredentialVersionsSteps are the ways to run
// acme.identity.v1.ApiCredentialHistoryService/CompareApiCredentialVersions, by name.
var shopperViewIntegrationServerCompareApiCredentialVersionsSteps = map[string]func(*ShopperViewIntegrationServer, context.Context, *identityv1.CompareApiCredentialVersionsRequest) (*identityv1.CompareApiCredentialVersionsResponse, error){
	"default": (*ShopperViewIntegrationServer).compareApiCredentialVersionsStep,
	"audited": (*ShopperViewIntegrationServer).compareApiCredentialVersionsAuditedStep,
}

func (s *ShopperViewIntegrationServer) compareApiCredentialVersionsStep(ctx context.Context, request *identityv1.CompareApiCredentialVersionsRequest) (*identityv1.CompareApiCredentialVersionsResponse, error) {
	return s.apiCredentialHistoryClient.CompareApiCredentialVersions(ctx, request)
}

func (s *ShopperViewIntegrationServer) compareApiCredentialVersionsAuditedStep(ctx context.Context, request *identityv1.CompareApiCredentialVersionsRequest) (*identityv1.CompareApiCredentialVersionsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.ApiCredentialHistoryService/CompareApiCredentialVersions")
	return s.apiCredentialHistoryClient.CompareApiCredentialVersions(ctx, request)
}
