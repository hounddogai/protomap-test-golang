package storefront

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	driverappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/driverapp/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShopperViewWorkflowServer implements `acme.storefront.v1.ShopperViewWorkflowService`.
// Runs the long-lived workflow that takes a shopper view from start to finish.
type ShopperViewWorkflowServer struct {
	storefrontv1.UnimplementedShopperViewWorkflowServiceServer
	store                            *Store
	log                              *slog.Logger
	storefrontSessionWorkflowClient  storefrontv1.StorefrontSessionWorkflowServiceClient
	bus                              *eventbus.Bus
	conn                             *grpc.ClientConn
	storefrontSessionWorkflowOnce    sync.Once
	storefrontSessionWorkflowCached  storefrontv1.StorefrontSessionWorkflowServiceClient
	customerApprovalClient           customersv1.CustomerApprovalServiceClient
	partnerWebhookIntegrationClient  partnerapiv1.PartnerWebhookIntegrationServiceClient
	partnerWebhookApprovalClient     partnerapiv1.PartnerWebhookApprovalServiceClient
	clients                          *registry.Registry
	customerWorkflowClient           customersv1.CustomerWorkflowServiceClient
	notificationValidationClient     notificationsv1.NotificationValidationServiceClient
	metricSeriesValidationClient     metricsv1.MetricSeriesValidationServiceClient
	storefrontSessionWorkflowPort    storefrontSessionWorkflowPort
	partnerIntegrationWorkflowClient partnerapiv1.PartnerIntegrationWorkflowServiceClient
	tenantAdminClient                identityv1.TenantAdminServiceClient
	driverHomeClient                 driverappv1.DriverHomeServiceClient
	driverHomeClientFallback         driverappv1.DriverHomeServiceClient
	consentRecordReportingClient     customersv1.ConsentRecordReportingServiceClient
	partnerOrderNotificationClient   partnerapiv1.PartnerOrderNotificationServiceClient
}

// NewShopperViewWorkflowServer wires a ShopperViewWorkflowServer.
func NewShopperViewWorkflowServer(deps platform.Deps) *ShopperViewWorkflowServer {
	return &ShopperViewWorkflowServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.storefront.v1.ShopperViewWorkflowService"),
		storefrontSessionWorkflowClient:  storefrontv1.NewStorefrontSessionWorkflowServiceClient(deps.Conn),
		bus:                              deps.Bus,
		conn:                             deps.Conn,
		customerApprovalClient:           customersv1.NewCustomerApprovalServiceClient(deps.Conn),
		partnerWebhookIntegrationClient:  partnerapiv1.NewPartnerWebhookIntegrationServiceClient(deps.Conn),
		partnerWebhookApprovalClient:     partnerapiv1.NewPartnerWebhookApprovalServiceClient(deps.Conn),
		clients:                          deps.Clients,
		customerWorkflowClient:           customersv1.NewCustomerWorkflowServiceClient(deps.Conn),
		notificationValidationClient:     notificationsv1.NewNotificationValidationServiceClient(deps.Conn),
		metricSeriesValidationClient:     metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		storefrontSessionWorkflowPort:    newStorefrontSessionWorkflowPort(deps),
		partnerIntegrationWorkflowClient: partnerapiv1.NewPartnerIntegrationWorkflowServiceClient(deps.Conn),
		tenantAdminClient:                identityv1.NewTenantAdminServiceClient(deps.Conn),
		driverHomeClient:                 driverappv1.NewDriverHomeServiceClient(deps.Conn),
		driverHomeClientFallback:         driverappv1.NewDriverHomeServiceClient(deps.Conn),
		consentRecordReportingClient:     customersv1.NewConsentRecordReportingServiceClient(deps.Conn),
		partnerOrderNotificationClient:   partnerapiv1.NewPartnerOrderNotificationServiceClient(deps.Conn),
	}
}

// StartShopperViewWorkflow starts a shopper view.
func (s *ShopperViewWorkflowServer) StartShopperViewWorkflow(ctx context.Context, req *storefrontv1.StartShopperViewWorkflowRequest) (*storefrontv1.StartShopperViewWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveShopperView(ctx, req.GetContext().GetTenantId(), req.GetShopperView()); err != nil {
		return nil, grpcx.Wrap(err, "save shopper view")
	}
	s.log.InfoContext(ctx, "start shopper view workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShopperView().GetAudit().GetEtag())
	startStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowClient.StartStorefrontSessionWorkflow(ctx, &storefrontv1.StartStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetShopperView().GetAudit().GetEtag(),
				CreatedAt: req.GetShopperView().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetShopperView().GetOwner().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/StartStorefrontSessionWorkflow")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "storefront.start_shopper_view_workflow.list_patient_homes",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     req.GetShopperView().GetAddress().GetRegion(),
			"filter.owner_id":   startStorefrontSessionWorkflowReply.GetStorefrontSession().GetAssignee().GetUserId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	listStorefrontSessionWorkflowsRequest := &storefrontv1.ListStorefrontSessionWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &storefrontv1.ListStorefrontSessionWorkflowsRequest_Filter{
			Region:       req.GetShopperView().GetAddress().GetRegion(),
			CreatedAfter: req.GetShopperView().GetAudit().GetUpdatedAt(),
		},
	}
	listStorefrontSessionWorkflowsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*storefrontv1.ListStorefrontSessionWorkflowsResponse, error) {
		return s.storefrontSessionWorkflowClient.ListStorefrontSessionWorkflows(ctx, listStorefrontSessionWorkflowsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/ListStorefrontSessionWorkflows")
	}
	return &storefrontv1.StartShopperViewWorkflowResponse{
		ShopperView: &storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				CreatedBy: startStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetCreatedBy(),
				UpdatedBy: startStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetUpdatedBy(),
			},
			ShopperViewId: req.GetShopperViewId(),
			Specs:         req.GetShopperView().GetSpecs(),
		},
		Result: &commonv1.OperationResult{
			Message: listStorefrontSessionWorkflowsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// AdvanceShopperViewWorkflow advances a shopper view.
func (s *ShopperViewWorkflowServer) AdvanceShopperViewWorkflow(ctx context.Context, req *storefrontv1.AdvanceShopperViewWorkflowRequest) (*storefrontv1.AdvanceShopperViewWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceShopperViewWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveShopperView(ctx, req.GetContext().GetTenantId(), req.GetShopperView()); err != nil {
		return nil, grpcx.Wrap(err, "save shopper view")
	}
	s.log.InfoContext(ctx, "advance shopper view workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShopperView().GetAudit().GetEtag())
	cancelStorefrontSessionWorkflowRequest := &storefrontv1.CancelStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{Email: req.GetShopperView().GetOwner().GetEmail()},
		},
	}
	cancelStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowLazy().CancelStorefrontSessionWorkflow(ctx, cancelStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/CancelStorefrontSessionWorkflow")
	}
	retryStorefrontSessionWorkflowRequest := &storefrontv1.RetryStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Assignee: &identityv1.UserRef{
				Email:  cancelStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetEmail(),
				UserId: req.GetShopperView().GetOwner().GetUserId(),
			},
			Sla: &storefrontv1.StorefrontSessionSla{
				PolicyId: cancelStorefrontSessionWorkflowReply.GetStorefrontSession().GetSla().GetPolicyId(),
			},
		},
	}
	retryStorefrontSessionWorkflowReply, err := grpcx.NewClient(s.conn, storefrontv1.NewStorefrontSessionWorkflowServiceClient).RetryStorefrontSessionWorkflow(ctx, retryStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/RetryStorefrontSessionWorkflow")
	}
	shopperViewCache := cache.New[string, *storefrontv1.ShopperView](1)
	shopperViewCache.Put(req.GetContext().GetTraceId(), req.GetShopperView())
	cachedShopperView, _ := shopperViewCache.Get(req.GetContext().GetTraceId())
	step := &shopperViewWorkflowServerAdvanceShopperViewWorkflowCommentCustomerApprovalStep{request: &customersv1.CommentCustomerApprovalRequest{
		Context: req.GetContext(),
		Details: &customersv1.CommentCustomerApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: req.GetShopperView().GetOwner().GetUserId(),
				Email:  cachedShopperView.GetOwner().GetEmail(),
			},
		},
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				Etag: retryStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetEtag(),
			},
			Contact: &commonv1.ContactInfo{
				Email: cancelStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetEmail(),
			},
		},
	}}
	if err := s.runAdvanceShopperViewWorkflowCommentCustomerApproval(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerApprovalService/CommentCustomerApproval")
	}
	commentCustomerApprovalReply := step.reply
	return &storefrontv1.AdvanceShopperViewWorkflowResponse{
		ShopperView: &storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cancelStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetCreatedAt(),
				Etag:      commentCustomerApprovalReply.GetCustomer().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				Email: cancelStorefrontSessionWorkflowReply.GetStorefrontSession().GetRequester().GetEmail(),
			},
			ShopperViewId: req.GetShopperViewId(),
			Name:          req.GetShopperView().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelShopperViewWorkflow cancels a shopper view.
func (s *ShopperViewWorkflowServer) CancelShopperViewWorkflow(ctx context.Context, req *storefrontv1.CancelShopperViewWorkflowRequest) (*storefrontv1.CancelShopperViewWorkflowResponse, error) {
	if err := s.store.SaveShopperView(ctx, req.GetContext().GetTenantId(), req.GetShopperView()); err != nil {
		return nil, grpcx.Wrap(err, "save shopper view")
	}
	s.log.InfoContext(ctx, "cancel shopper view workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShopperView().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetShopperView())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	connectPartnerWebhookSourceRequest := &partnerapiv1.ConnectPartnerWebhookSourceRequest{}
	connectPartnerWebhookSourceRequest.Context = req.GetContext()
	connectPartnerWebhookSourceRequest.Annotations = annotations
	connectPartnerWebhookSourceRequest.Reason = req.GetReason()
	connectPartnerWebhookSourceRequest.Details = &partnerapiv1.ConnectPartnerWebhookSourceRequest_Details{
		MessageId: req.GetShopperView().GetOwner().GetUserId(),
		Envelope:  packed,
	}
	connectPartnerWebhookSourceReply, err := s.partnerWebhookIntegrationClient.ConnectPartnerWebhookSource(ctx, connectPartnerWebhookSourceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookIntegrationService/ConnectPartnerWebhookSource")
	}
	ctx = tenancy.With(ctx, "email", req.GetShopperView().GetOwner().GetEmail())
	retryStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowClient.RetryStorefrontSessionWorkflow(ctx, &storefrontv1.RetryStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetShopperView().GetOwner().GetDisplayName(),
				Email:       tenancy.Value(ctx, "email"),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/RetryStorefrontSessionWorkflow")
	}
	resumeStorefrontSessionWorkflowRequest := buildResumeStorefrontSessionWorkflowRequestForCancelShopperViewWorkflow(req, retryStorefrontSessionWorkflowReply)
	resumeStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowClient.ResumeStorefrontSessionWorkflow(ctx, resumeStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/ResumeStorefrontSessionWorkflow")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "storefront.cancel_shopper_view_workflow.generate_landing_page_report",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"reason":            req.GetReason(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &storefrontv1.CancelShopperViewWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:       connectPartnerWebhookSourceReply.GetResult().GetStatus(),
			ReferenceIds: retryStorefrontSessionWorkflowReply.GetResult().GetReferenceIds(),
		},
		Outcome: &storefrontv1.CancelShopperViewWorkflowResponse_Outcome{
			ReferenceId: resumeStorefrontSessionWorkflowReply.GetOutcome().GetReferenceId(),
			Status:      resumeStorefrontSessionWorkflowReply.GetOutcome().GetStatus(),
		},
		ShopperView: &storefrontv1.ShopperView{
			ShopperViewId: req.GetShopperViewId(),
			Owner:         req.GetShopperView().GetOwner(),
		},
	}, nil
}

// RetryShopperViewWorkflow retrys a shopper view.
func (s *ShopperViewWorkflowServer) RetryShopperViewWorkflow(ctx context.Context, req *storefrontv1.RetryShopperViewWorkflowRequest) (*storefrontv1.RetryShopperViewWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveShopperView(ctx, req.GetContext().GetTenantId(), req.GetShopperView()); err != nil {
		return nil, grpcx.Wrap(err, "save shopper view")
	}
	s.log.InfoContext(ctx, "retry shopper view workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShopperView().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	packed, err := anypb.New(req.GetShopperView())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	approvePartnerWebhookRequest := &partnerapiv1.ApprovePartnerWebhookRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.ApprovePartnerWebhookRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       req.GetShopperView().GetOwner().GetEmail(),
				DisplayName: req.GetShopperView().GetOwner().GetDisplayName(),
				UserId:      req.GetShopperView().GetOwner().GetUserId(),
			},
		},
		Reason:         tenancy.Value(ctx, "reason"),
		PartnerWebhook: &partnerapiv1.PartnerWebhook{Detail: packed},
	}
	send := s.approvePartnerWebhookSender(approvePartnerWebhookRequest)
	approvePartnerWebhookReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookApprovalService/ApprovePartnerWebhook")
	}
	partnerWebhookIntegration, err := registry.Lookup[partnerapiv1.PartnerWebhookIntegrationServiceClient](s.clients, "acme.partnerapi.v1.PartnerWebhookIntegrationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed2, err := anypb.New(req.GetShopperView())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	testPartnerWebhookConnectionRequest := &partnerapiv1.TestPartnerWebhookConnectionRequest{
		Context:          req.GetContext(),
		PartnerWebhookId: approvePartnerWebhookReply.GetPartnerWebhook().GetPartnerWebhookId(),
		Reason:           req.GetReason(),
		Details: &partnerapiv1.TestPartnerWebhookConnectionRequest_Details{
			MessageId: approvePartnerWebhookReply.GetResult().GetMessage(),
			Envelope:  packed2,
		},
	}
	testPartnerWebhookConnectionReply, err := partnerWebhookIntegration.TestPartnerWebhookConnection(ctx, testPartnerWebhookConnectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerWebhookIntegrationService/TestPartnerWebhookConnection")
	}
	_ = testPartnerWebhookConnectionReply
	return &storefrontv1.RetryShopperViewWorkflowResponse{
		Outcome: &storefrontv1.RetryShopperViewWorkflowResponse_Outcome{
			Message: approvePartnerWebhookReply.GetOutcome().GetMessage(),
		},
		ShopperView: &storefrontv1.ShopperView{
			ShopperViewId: req.GetShopperViewId(),
			Capacity:      req.GetShopperView().GetCapacity(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseShopperViewWorkflow pauses a shopper view.
func (s *ShopperViewWorkflowServer) PauseShopperViewWorkflow(ctx context.Context, req *storefrontv1.PauseShopperViewWorkflowRequest) (*storefrontv1.PauseShopperViewWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseShopperViewWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveShopperView(ctx, req.GetContext().GetTenantId(), req.GetShopperView()); err != nil {
		return nil, grpcx.Wrap(err, "save shopper view")
	}
	s.log.InfoContext(ctx, "pause shopper view workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShopperView().GetAudit().GetEtag())
	listCustomerWorkflowsRequest := &customersv1.ListCustomerWorkflowsRequest{}
	listCustomerWorkflowsRequest.Context = req.GetContext()
	listCustomerWorkflowsRequest.Filter = &customersv1.ListCustomerWorkflowsRequest_Filter{Region: req.GetShopperView().GetAddress().GetRegion()}
	listCustomerWorkflowsReply, err := s.customerWorkflowClient.ListCustomerWorkflows(ctx, listCustomerWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerWorkflowService/ListCustomerWorkflows")
	}
	storefrontSessionWorkflow, ok := s.clients.Get("acme.storefront.v1.StorefrontSessionWorkflowService").(storefrontv1.StorefrontSessionWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.storefront.v1.StorefrontSessionWorkflowService")
	}
	source := req.GetShopperView().GetOwner().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	ctx = tenancy.With(ctx, "code", req.GetShopperView().GetCode())
	startStorefrontSessionWorkflowRequest := &storefrontv1.StartStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		Details: &storefrontv1.StartStorefrontSessionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotify().GetPhone(),
				Email: email,
			},
		},
		StorefrontSession: &storefrontv1.StorefrontSession{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
			Origin: &storefrontv1.StorefrontSession_Customer{Customer: &storefrontv1.ShopperViewRef{Code: tenancy.Value(ctx, "code")}},
		},
	}
	startStorefrontSessionWorkflowReply, err := storefrontSessionWorkflow.StartStorefrontSessionWorkflow(ctx, startStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/StartStorefrontSessionWorkflow")
	}
	return &storefrontv1.PauseShopperViewWorkflowResponse{
		Outcome: &storefrontv1.PauseShopperViewWorkflowResponse_Outcome{
			Message: listCustomerWorkflowsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message: startStorefrontSessionWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		ShopperView: &storefrontv1.ShopperView{
			ShopperViewId: req.GetShopperViewId(),
			Capacity:      req.GetShopperView().GetCapacity(),
		},
	}, nil
}

// ResumeShopperViewWorkflow resumes a shopper view.
func (s *ShopperViewWorkflowServer) ResumeShopperViewWorkflow(ctx context.Context, req *storefrontv1.ResumeShopperViewWorkflowRequest) (*storefrontv1.ResumeShopperViewWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeShopperViewWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveShopperView(ctx, req.GetContext().GetTenantId(), req.GetShopperView()); err != nil {
		return nil, grpcx.Wrap(err, "save shopper view")
	}
	s.log.InfoContext(ctx, "resume shopper view workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShopperView().GetAudit().GetEtag())
	previewNotificationRequest := &notificationsv1.PreviewNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Requester: &commonv1.ContactInfo{Email: req.GetShopperView().GetOwner().GetEmail()},
		},
	}
	previewNotificationReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*notificationsv1.PreviewNotificationResponse, error) {
		return s.notificationValidationClient.PreviewNotification(ctx, previewNotificationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/PreviewNotification")
	}
	cancelStorefrontSessionWorkflowRequest := &storefrontv1.CancelStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: previewNotificationReply.GetNotification().GetAudit().GetUpdatedAt(),
				Etag:      req.GetShopperView().GetAudit().GetEtag(),
			},
		},
	}
	cancelStorefrontSessionWorkflowReply := new(storefrontv1.CancelStorefrontSessionWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", storefrontStorefrontSessionWorkflowServiceName, "CancelStorefrontSessionWorkflow"), cancelStorefrontSessionWorkflowRequest, cancelStorefrontSessionWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/CancelStorefrontSessionWorkflow")
	}
	validateMetricSeriesReply, err := s.metricSeriesValidationClient.ValidateMetricSeries(ctx, &metricsv1.ValidateMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: previewNotificationReply.GetNotification().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/ValidateMetricSeries")
	}
	retryStorefrontSessionWorkflowRequest := &storefrontv1.RetryStorefrontSessionWorkflowRequest{
		Context: req.GetContext(),
		StorefrontSession: &storefrontv1.StorefrontSession{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cancelStorefrontSessionWorkflowReply.GetStorefrontSession().GetAudit().GetCreatedBy(),
				Etag:      previewNotificationReply.GetNotification().GetAudit().GetEtag(),
			},
		},
	}
	retryStorefrontSessionWorkflowReply, err := s.storefrontSessionWorkflowPort.RetryStorefrontSessionWorkflow(ctx, retryStorefrontSessionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionWorkflowService/RetryStorefrontSessionWorkflow")
	}
	return &storefrontv1.ResumeShopperViewWorkflowResponse{
		ShopperView: &storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: previewNotificationReply.GetNotification().GetAudit().GetUpdatedBy(),
				CreatedAt: validateMetricSeriesReply.GetMetricSeries().GetAudit().GetCreatedAt(),
			},
			Code:          validateMetricSeriesReply.GetMetricSeries().GetCode(),
			ShopperViewId: req.GetShopperViewId(),
			Name:          req.GetShopperView().GetName(),
		},
		Outcome: &storefrontv1.ResumeShopperViewWorkflowResponse_Outcome{
			Status:      previewNotificationReply.GetOutcome().GetStatus(),
			EffectiveAt: retryStorefrontSessionWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: cancelStorefrontSessionWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// InspectShopperViewWorkflow returns a shopper view.
func (s *ShopperViewWorkflowServer) InspectShopperViewWorkflow(ctx context.Context, req *storefrontv1.InspectShopperViewWorkflowRequest) (*storefrontv1.InspectShopperViewWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "inspect_shopper_view_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect shopper view workflow", "tenant", req.GetContext().GetTenantId())
	advancePartnerIntegrationWorkflowRequest := buildAdvancePartnerIntegrationWorkflowRequestForInspectShopperViewWorkflow(req)
	advancePartnerIntegrationWorkflowReply, err := s.partnerIntegrationWorkflowClient.AdvancePartnerIntegrationWorkflow(ctx, advancePartnerIntegrationWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationWorkflowService/AdvancePartnerIntegrationWorkflow")
	}
	return &storefrontv1.InspectShopperViewWorkflowResponse{
		ShopperView: &storefrontv1.ShopperView{
			Owner: &identityv1.UserRef{
				DisplayName: advancePartnerIntegrationWorkflowReply.GetPartnerIntegration().GetHolder().GetDisplayName(),
			},
			ShopperViewId: req.GetShopperViewId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: advancePartnerIntegrationWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ListShopperViewWorkflows lists shopper views a page at a time.
func (s *ShopperViewWorkflowServer) ListShopperViewWorkflows(ctx context.Context, req *storefrontv1.ListShopperViewWorkflowsRequest) (*storefrontv1.ListShopperViewWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list shopper view workflows", "tenant", req.GetContext().GetTenantId())
	archiveTenantRequest := buildArchiveTenantRequestForListShopperViewWorkflows(req)
	archiveTenantReply, err := s.tenantAdminClient.ArchiveTenant(ctx, archiveTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/ArchiveTenant")
	}
	routes := map[string]func(context.Context, *driverappv1.CloneDriverHomeRequest, ...grpc.CallOption) (*driverappv1.CloneDriverHomeResponse, error){
		"primary":  s.driverHomeClient.CloneDriverHome,
		"fallback": s.driverHomeClientFallback.CloneDriverHome,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloneDriverHomeRequest := &driverappv1.CloneDriverHomeRequest{
		Context: req.GetContext(),
		DriverHome: &driverappv1.DriverHome{
			Assignee: &identityv1.UserRef{
				DisplayName: archiveTenantReply.GetTenant().GetManager().GetDisplayName(),
			},
			Origin: &driverappv1.DriverHome_Customer{Customer: &customersv1.CustomerRef{
				Email: archiveTenantReply.GetTenant().GetManager().GetEmail(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: archiveTenantReply.GetTenant().GetAudit().GetUpdatedAt(),
			},
		},
	}
	cloneDriverHomeReply, err := route(ctx, cloneDriverHomeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.driverapp.v1.DriverHomeService/CloneDriverHome")
	}
	return &storefrontv1.ListShopperViewWorkflowsResponse{
		ShopperViews: []*storefrontv1.ShopperView{&storefrontv1.ShopperView{Name: archiveTenantReply.GetTenant().GetName()}},
		Result: &commonv1.OperationResult{
			CorrelationId: cloneDriverHomeReply.GetResult().GetCorrelationId(),
			ReferenceIds:  cloneDriverHomeReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
	}, nil
}

// SignalShopperViewWorkflow signals a shopper view.
func (s *ShopperViewWorkflowServer) SignalShopperViewWorkflow(ctx context.Context, req *storefrontv1.SignalShopperViewWorkflowRequest) (*storefrontv1.SignalShopperViewWorkflowResponse, error) {
	if err := s.store.SaveShopperView(ctx, req.GetContext().GetTenantId(), req.GetShopperView()); err != nil {
		return nil, grpcx.Wrap(err, "save shopper view")
	}
	s.log.InfoContext(ctx, "signal shopper view workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetShopperView().GetAudit().GetEtag())
	countConsentRecordTotalsReply, err := s.consentRecordReportingClient.CountConsentRecordTotals(ctx, &customersv1.CountConsentRecordTotalsRequest{
		Context: req.GetContext(),
		Filter:  &customersv1.CountConsentRecordTotalsRequest_Filter{Region: req.GetShopperView().GetAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordReportingService/CountConsentRecordTotals")
	}
	streamPartnerOrderNoticesStream, err := s.partnerOrderNotificationClient.StreamPartnerOrderNotices(ctx, &partnerapiv1.StreamPartnerOrderNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderNotificationService/StreamPartnerOrderNotices")
	}
	for {
		received, err := streamPartnerOrderNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderNotificationService/StreamPartnerOrderNotices")
		}
		_ = received
	}
	return &storefrontv1.SignalShopperViewWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: countConsentRecordTotalsReply.GetResult().GetStatus(),
		},
		Outcome: &storefrontv1.SignalShopperViewWorkflowResponse_Outcome{
			Message: countConsentRecordTotalsReply.GetResult().GetMessage(),
		},
		ShopperView: &storefrontv1.ShopperView{
			ShopperViewId: req.GetShopperViewId(),
			Specs:         req.GetShopperView().GetSpecs(),
		},
	}, nil
}

// storefrontSessionWorkflowLazy dials acme.storefront.v1.StorefrontSessionWorkflowService the first time a handler
// needs it.
func (s *ShopperViewWorkflowServer) storefrontSessionWorkflowLazy() storefrontv1.StorefrontSessionWorkflowServiceClient {
	s.storefrontSessionWorkflowOnce.Do(func() {
		s.storefrontSessionWorkflowCached = storefrontv1.NewStorefrontSessionWorkflowServiceClient(s.conn)
	})
	return s.storefrontSessionWorkflowCached
}

// shopperViewWorkflowServerAdvanceShopperViewWorkflowCommentCustomerApprovalStep carries the request of one step of
// AdvanceShopperViewWorkflow and, once it ran, its reply.
type shopperViewWorkflowServerAdvanceShopperViewWorkflowCommentCustomerApprovalStep struct {
	request *customersv1.CommentCustomerApprovalRequest
	reply   *customersv1.CommentCustomerApprovalResponse
}

func (s *ShopperViewWorkflowServer) runAdvanceShopperViewWorkflowCommentCustomerApproval(ctx context.Context, step *shopperViewWorkflowServerAdvanceShopperViewWorkflowCommentCustomerApprovalStep) error {
	reply, err := s.customerApprovalClient.CommentCustomerApproval(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// approvePartnerWebhookSender returns a function that sends one request to
// acme.partnerapi.v1.PartnerWebhookApprovalService/ApprovePartnerWebhook.
func (s *ShopperViewWorkflowServer) approvePartnerWebhookSender(request *partnerapiv1.ApprovePartnerWebhookRequest) func(context.Context) (*partnerapiv1.ApprovePartnerWebhookResponse, error) {
	return func(ctx context.Context) (*partnerapiv1.ApprovePartnerWebhookResponse, error) {
		return s.partnerWebhookApprovalClient.ApprovePartnerWebhook(ctx, request)
	}
}
