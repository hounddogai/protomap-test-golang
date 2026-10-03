package storefront

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ShopperViewQueryServer implements `acme.storefront.v1.ShopperViewQueryService`.
// Answers read-only questions about shopper views, for screens and reports.
type ShopperViewQueryServer struct {
	storefrontv1.UnimplementedShopperViewQueryServiceServer
	store                                *Store
	log                                  *slog.Logger
	auditEventQueryClient                auditv1.AuditEventQueryServiceClient
	userValidationClient                 identityv1.UserValidationServiceClient
	shopperViewWorkflowClient            storefrontv1.ShopperViewWorkflowServiceClient
	partnerOrderWorkflowClient           partnerapiv1.PartnerOrderWorkflowServiceClient
	customerSegmentSyncClient            customersv1.CustomerSegmentSyncServiceClient
	conn                                 *grpc.ClientConn
	bus                                  *eventbus.Bus
	partnerIntegrationNotificationClient partnerapiv1.PartnerIntegrationNotificationServiceClient
	customerLifecycleClient              customersv1.CustomerLifecycleServiceClient
	customerSegmentLifecycleClient       customersv1.CustomerSegmentLifecycleServiceClient
}

// NewShopperViewQueryServer wires a ShopperViewQueryServer.
func NewShopperViewQueryServer(deps platform.Deps) *ShopperViewQueryServer {
	return &ShopperViewQueryServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.storefront.v1.ShopperViewQueryService"),
		auditEventQueryClient:                auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		userValidationClient:                 identityv1.NewUserValidationServiceClient(deps.Conn),
		shopperViewWorkflowClient:            storefrontv1.NewShopperViewWorkflowServiceClient(deps.Conn),
		partnerOrderWorkflowClient:           partnerapiv1.NewPartnerOrderWorkflowServiceClient(deps.Conn),
		customerSegmentSyncClient:            customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		bus:                                  deps.Bus,
		partnerIntegrationNotificationClient: partnerapiv1.NewPartnerIntegrationNotificationServiceClient(deps.Conn),
		customerLifecycleClient:              customersv1.NewCustomerLifecycleServiceClient(deps.Conn),
		customerSegmentLifecycleClient:       customersv1.NewCustomerSegmentLifecycleServiceClient(deps.Conn),
	}
}

// LookupShopperView returns a shopper view.
func (s *ShopperViewQueryServer) LookupShopperView(ctx context.Context, req *storefrontv1.LookupShopperViewRequest) (*storefrontv1.LookupShopperViewResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "lookup_shopper_view"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup shopper view", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	countAuditEventsRequest := &auditv1.CountAuditEventsRequest{}
	countAuditEventsRequest.Context = req.GetContext()
	countAuditEventsRequest.Filter = &auditv1.CountAuditEventsRequest_Filter{
		CreatedAfter: (&decoded).GetDeadline(),
		OwnerId:      req.GetShopperViewId(),
	}
	countAuditEventsReply, err := s.auditEventQueryClient.CountAuditEvents(ctx, countAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/CountAuditEvents")
	}
	ctx = tenancy.With(ctx, "locale", req.GetContext().GetLocale())
	checkUserEligibilityReply, err := s.userValidationClient.CheckUserEligibility(ctx, &identityv1.CheckUserEligibilityRequest{
		Context: req.GetContext(),
		Details: &identityv1.CheckUserEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  tenancy.Value(ctx, "locale"),
		},
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Attributes:          countAuditEventsReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	return &storefrontv1.LookupShopperViewResponse{
		Result: &commonv1.OperationResult{
			Message: countAuditEventsReply.GetResult().GetMessage(),
			Status:  checkUserEligibilityReply.GetResult().GetStatus(),
		},
		ShopperView: &storefrontv1.ShopperView{ShopperViewId: req.GetShopperViewId()},
	}, nil
}

// BrowseShopperViews lists shopper views a page at a time.
func (s *ShopperViewQueryServer) BrowseShopperViews(ctx context.Context, req *storefrontv1.BrowseShopperViewsRequest) (*storefrontv1.BrowseShopperViewsResponse, error) {
	s.log.InfoContext(ctx, "browse shopper views", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	cancelShopperViewWorkflowReply, err := s.shopperViewWorkflowClient.CancelShopperViewWorkflow(ctx, &storefrontv1.CancelShopperViewWorkflowRequest{
		Context: req.GetContext(),
		ShopperView: &storefrontv1.ShopperView{
			Locator: &storefrontv1.ShopperView_Address{Address: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")}},
			Owner:   &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.storefront.v1.ShopperViewWorkflowService/CancelShopperViewWorkflow")
	}
	pausePartnerOrderWorkflowRequest := &partnerapiv1.PausePartnerOrderWorkflowRequest{}
	pausePartnerOrderWorkflowRequest.Context = req.GetContext()
	pausePartnerOrderWorkflowRequest.PartnerOrder = &partnerapiv1.PartnerOrder{
		Counterparty: &sellersv1.SellerRef{
			DisplayName: cancelShopperViewWorkflowReply.GetShopperView().GetOwner().GetDisplayName(),
			Email:       cancelShopperViewWorkflowReply.GetShopperView().GetOwner().GetEmail(),
		},
		Audit: &commonv1.AuditInfo{
			UpdatedBy: cancelShopperViewWorkflowReply.GetShopperView().GetAudit().GetUpdatedBy(),
		},
		BillingAddress: &commonv1.PostalAddress{
			Lines: cancelShopperViewWorkflowReply.GetShopperView().GetAddress().GetLines(),
		},
	}
	pausePartnerOrderWorkflowReply, err := s.partnerOrderWorkflowClient.PausePartnerOrderWorkflow(ctx, pausePartnerOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerOrderWorkflowService/PausePartnerOrderWorkflow")
	}
	return &storefrontv1.BrowseShopperViewsResponse{
		ShopperViews: []*storefrontv1.ShopperView{&storefrontv1.ShopperView{
			Owner: cancelShopperViewWorkflowReply.GetShopperView().GetOwner(),
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: pausePartnerOrderWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// FindShopperViews lists shopper views a page at a time.
func (s *ShopperViewQueryServer) FindShopperViews(ctx context.Context, req *storefrontv1.FindShopperViewsRequest) (*storefrontv1.FindShopperViewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindShopperViews", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find shopper views", "tenant", req.GetContext().GetTenantId())
	return &storefrontv1.FindShopperViewsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountShopperViews counts shopper views by status.
func (s *ShopperViewQueryServer) CountShopperViews(ctx context.Context, req *storefrontv1.CountShopperViewsRequest) (*storefrontv1.CountShopperViewsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count shopper views", "tenant", req.GetContext().GetTenantId())
	return &storefrontv1.CountShopperViewsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolveShopperViews returns several shopper views at once.
func (s *ShopperViewQueryServer) ResolveShopperViews(ctx context.Context, req *storefrontv1.ResolveShopperViewsRequest) (*storefrontv1.ResolveShopperViewsResponse, error) {
	s.log.InfoContext(ctx, "resolve shopper views", "tenant", req.GetContext().GetTenantId())
	return &storefrontv1.ResolveShopperViewsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportShopperViews streams every matching shopper view.
func (s *ShopperViewQueryServer) ExportShopperViews(req *storefrontv1.ExportShopperViewsRequest, stream grpc.ServerStreamingServer[storefrontv1.ExportShopperViewsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export shopper views", "tenant", req.GetContext().GetTenantId())
	syncCustomerSegmentsReply, err := s.customerSegmentSyncClient.SyncCustomerSegments(ctx, &customersv1.SyncCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/SyncCustomerSegments")
	}
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	connectStorefrontSessionSourceRequest := &storefrontv1.ConnectStorefrontSessionSourceRequest{
		Context: req.GetContext(),
		Reason:  syncCustomerSegmentsReply.GetCustomerSegment().GetBlock().GetReason(),
		Details: &storefrontv1.ConnectStorefrontSessionSourceRequest_Details{
			MessageId:  syncCustomerSegmentsReply.GetOutcome().GetMessage(),
			ReceivedAt: syncCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			Envelope:   packed,
		},
	}
	connectStorefrontSessionSourceReply := new(storefrontv1.ConnectStorefrontSessionSourceResponse)
	if err := s.conn.Invoke(ctx, storefrontStorefrontSessionIntegrationServiceConnectStorefrontSessionSourceMethod, connectStorefrontSessionSourceRequest, connectStorefrontSessionSourceReply); err != nil {
		return grpcx.Wrap(err, "acme.storefront.v1.StorefrontSessionIntegrationService/ConnectStorefrontSessionSource")
	}
	previewPreferenceRequest := &customersv1.PreviewPreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: connectStorefrontSessionSourceReply.GetStorefrontSession().GetAudit().GetUpdatedAt(),
			},
			Action: &customersv1.Preference_Route{Route: &customersv1.PreferenceRouteAction{
				Target: syncCustomerSegmentsReply.GetCustomerSegment().GetRoute().GetTarget(),
			}},
			Owner: &identityv1.UserRef{
				Email: connectStorefrontSessionSourceReply.GetStorefrontSession().GetAssignee().GetEmail(),
			},
		},
	}
	previewPreferenceReply := new(customersv1.PreviewPreferenceResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.customers.v1.PreferenceValidationService/PreviewPreference", previewPreferenceRequest, previewPreferenceReply); err != nil {
		return grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/PreviewPreference")
	}
	if err := stream.Send(&storefrontv1.ExportShopperViewsResponse{
		ShopperView: &storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				Etag: syncCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				Email: connectStorefrontSessionSourceReply.GetStorefrontSession().GetAssignee().GetEmail(),
			},
		},
		Result: &commonv1.OperationResult{
			CorrelationId: syncCustomerSegmentsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeShopperViews summarizes a shopper view.
func (s *ShopperViewQueryServer) SummarizeShopperViews(ctx context.Context, req *storefrontv1.SummarizeShopperViewsRequest) (*storefrontv1.SummarizeShopperViewsResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "summarize_shopper_views"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize shopper views", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "storefront.summarize_shopper_views.sync_synonyms",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"synonym.block.reason": req.GetReason(),
			"context.tenant_id":    req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := shopperViewQueryServerSendPartnerIntegrationTestNoticeSteps[mode]
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendPartnerIntegrationTestNoticeRequest := &partnerapiv1.SendPartnerIntegrationTestNoticeRequest{
		Context: req.GetContext(),
		Details: &partnerapiv1.SendPartnerIntegrationTestNoticeRequest_Details{Payload: packed},
	}
	sendPartnerIntegrationTestNoticeReply, err := run(s, ctx, sendPartnerIntegrationTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.partnerapi.v1.PartnerIntegrationNotificationService/SendPartnerIntegrationTestNotice")
	}
	scheduleCustomerExpiryRequest := &customersv1.ScheduleCustomerExpiryRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: sendPartnerIntegrationTestNoticeReply.GetPartnerIntegration().GetHolder().GetDisplayName(),
			},
		},
	}
	scheduleCustomerExpiryReply, err := customersv1.CustomerLifecycleServiceClient.ScheduleCustomerExpiry(s.customerLifecycleClient, ctx, scheduleCustomerExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerLifecycleService/ScheduleCustomerExpiry")
	}
	return &storefrontv1.SummarizeShopperViewsResponse{
		Result: &commonv1.OperationResult{
			Message:       sendPartnerIntegrationTestNoticeReply.GetOutcome().GetMessage(),
			Status:        sendPartnerIntegrationTestNoticeReply.GetResult().GetStatus(),
			CorrelationId: scheduleCustomerExpiryReply.GetResult().GetCorrelationId(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestShopperViews lists shopper views a page at a time.
func (s *ShopperViewQueryServer) SuggestShopperViews(ctx context.Context, req *storefrontv1.SuggestShopperViewsRequest) (*storefrontv1.SuggestShopperViewsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestShopperViews", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest shopper views", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	scheduleCustomerSegmentExpiryRequest := &customersv1.ScheduleCustomerSegmentExpiryRequest{
		Context: req.GetContext(),
		Details: &customersv1.ScheduleCustomerSegmentExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{UserId: cloned.GetActor().GetUserId()},
		},
	}
	scheduleCustomerSegmentExpiryReply, err := grpcx.Call(ctx, s.customerSegmentLifecycleClient.ScheduleCustomerSegmentExpiry, scheduleCustomerSegmentExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentLifecycleService/ScheduleCustomerSegmentExpiry")
	}
	return &storefrontv1.SuggestShopperViewsResponse{
		ShopperViews: []*storefrontv1.ShopperView{&storefrontv1.ShopperView{
			Owner: scheduleCustomerSegmentExpiryReply.GetCustomerSegment().GetOwner(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PeekShopperView returns a shopper view.
func (s *ShopperViewQueryServer) PeekShopperView(ctx context.Context, req *storefrontv1.PeekShopperViewRequest) (*storefrontv1.PeekShopperViewResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "shopper_view", req.GetShopperViewId(), "peek_shopper_view"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek shopper view", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushCustomerSegmentRequest := &customersv1.PushCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:  cachedContext.GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Details: &customersv1.PushCustomerSegmentRequest_Details{Payload: snapshot},
	}
	pushCustomerSegmentReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*customersv1.PushCustomerSegmentResponse, error) {
		return s.customerSegmentSyncClient.PushCustomerSegment(ctx, pushCustomerSegmentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/PushCustomerSegment")
	}
	return &storefrontv1.PeekShopperViewResponse{
		ShopperView: &storefrontv1.ShopperView{
			Audit: &commonv1.AuditInfo{
				CreatedBy: pushCustomerSegmentReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
			},
			Components: map[string]*storefrontv1.ShopperViewComponent{"primary": &storefrontv1.ShopperViewComponent{
				Description: pushCustomerSegmentReply.GetCustomerSegment().GetDescription(),
			}},
			ShopperViewId: req.GetShopperViewId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// shopperViewQueryServerSendPartnerIntegrationTestNoticeSteps are the ways to run
// acme.partnerapi.v1.PartnerIntegrationNotificationService/SendPartnerIntegrationTestNotice, by name.
var shopperViewQueryServerSendPartnerIntegrationTestNoticeSteps = map[string]func(*ShopperViewQueryServer, context.Context, *partnerapiv1.SendPartnerIntegrationTestNoticeRequest) (*partnerapiv1.SendPartnerIntegrationTestNoticeResponse, error){
	"default": (*ShopperViewQueryServer).sendPartnerIntegrationTestNoticeStep,
	"audited": (*ShopperViewQueryServer).sendPartnerIntegrationTestNoticeAuditedStep,
}

func (s *ShopperViewQueryServer) sendPartnerIntegrationTestNoticeStep(ctx context.Context, request *partnerapiv1.SendPartnerIntegrationTestNoticeRequest) (*partnerapiv1.SendPartnerIntegrationTestNoticeResponse, error) {
	return s.partnerIntegrationNotificationClient.SendPartnerIntegrationTestNotice(ctx, request)
}

func (s *ShopperViewQueryServer) sendPartnerIntegrationTestNoticeAuditedStep(ctx context.Context, request *partnerapiv1.SendPartnerIntegrationTestNoticeRequest) (*partnerapiv1.SendPartnerIntegrationTestNoticeResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.partnerapi.v1.PartnerIntegrationNotificationService/SendPartnerIntegrationTestNotice")
	return s.partnerIntegrationNotificationClient.SendPartnerIntegrationTestNotice(ctx, request)
}
