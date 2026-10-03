package promotions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	licensingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/licensing/v1"
	maintenancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CouponLifecycleServer implements `acme.promotions.v1.CouponLifecycleService`.
// Moves a coupon between the states of its lifecycle.
type CouponLifecycleServer struct {
	promotionsv1.UnimplementedCouponLifecycleServiceServer
	store                           *Store
	log                             *slog.Logger
	meteredPackingSlipApproval      meteredPackingSlipApproval
	shipmentWorkflowClient          shippingv1.ShipmentWorkflowServiceClient
	conn                            *grpc.ClientConn
	orderReportingClient            ordersv1.OrderReportingServiceClient
	softwareLicenseQueryClient      licensingv1.SoftwareLicenseQueryServiceClient
	campaignReportingClient         promotionsv1.CampaignReportingServiceClient
	redemptionImportClient          promotionsv1.RedemptionImportServiceClient
	orderAmendmentApprovalClient    ordersv1.OrderAmendmentApprovalServiceClient
	redemptionSyncClient            promotionsv1.RedemptionSyncServiceClient
	workOrderQueryClient            maintenancev1.WorkOrderQueryServiceClient
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
}

// NewCouponLifecycleServer wires a CouponLifecycleServer.
func NewCouponLifecycleServer(deps platform.Deps) *CouponLifecycleServer {
	return &CouponLifecycleServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.promotions.v1.CouponLifecycleService"),
		meteredPackingSlipApproval:      meteredPackingSlipApproval{PackingSlipApprovalServiceClient: fulfillmentv1.NewPackingSlipApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		shipmentWorkflowClient:          shippingv1.NewShipmentWorkflowServiceClient(deps.Conn),
		conn:                            deps.Conn,
		orderReportingClient:            ordersv1.NewOrderReportingServiceClient(deps.Conn),
		softwareLicenseQueryClient:      licensingv1.NewSoftwareLicenseQueryServiceClient(deps.Conn),
		campaignReportingClient:         promotionsv1.NewCampaignReportingServiceClient(deps.Conn),
		redemptionImportClient:          promotionsv1.NewRedemptionImportServiceClient(deps.Conn),
		orderAmendmentApprovalClient:    ordersv1.NewOrderAmendmentApprovalServiceClient(deps.Conn),
		redemptionSyncClient:            promotionsv1.NewRedemptionSyncServiceClient(deps.Conn),
		workOrderQueryClient:            maintenancev1.NewWorkOrderQueryServiceClient(deps.Conn),
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
	}
}

// ActivateCoupon activates a coupon.
func (s *CouponLifecycleServer) ActivateCoupon(ctx context.Context, req *promotionsv1.ActivateCouponRequest) (*promotionsv1.ActivateCouponResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "activate_coupon"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate coupon", "tenant", req.GetContext().GetTenantId())
	escalatePackingSlipRequest := &fulfillmentv1.EscalatePackingSlipRequest{
		Context: req.GetContext(),
		Details: &fulfillmentv1.EscalatePackingSlipRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
	}
	escalatePackingSlipReply, err := s.meteredPackingSlipApproval.EscalatePackingSlip(ctx, escalatePackingSlipRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipApprovalService/EscalatePackingSlip")
	}
	return &promotionsv1.ActivateCouponResponse{
		Result: &commonv1.OperationResult{Status: escalatePackingSlipReply.GetResult().GetStatus()},
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: escalatePackingSlipReply.GetPackingSlip().GetAudit().GetUpdatedBy(),
			},
			CouponId: req.GetCouponId(),
		},
		Outcome: &promotionsv1.ActivateCouponResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendCoupon suspends a coupon.
func (s *CouponLifecycleServer) SuspendCoupon(ctx context.Context, req *promotionsv1.SuspendCouponRequest) (*promotionsv1.SuspendCouponResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "suspend_coupon"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend coupon", "tenant", req.GetContext().GetTenantId())
	advanceShipmentWorkflowRequest := &shippingv1.AdvanceShipmentWorkflowRequest{
		Context: req.GetContext(),
		Details: &shippingv1.AdvanceShipmentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
		},
	}
	advanceShipmentWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*shippingv1.AdvanceShipmentWorkflowResponse, error) {
		return s.shipmentWorkflowClient.AdvanceShipmentWorkflow(ctx, advanceShipmentWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/AdvanceShipmentWorkflow")
	}
	return &promotionsv1.SuspendCouponResponse{
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: advanceShipmentWorkflowReply.GetShipment().GetCounterparty().GetDisplayName(),
			},
			CouponId: req.GetCouponId(),
		},
		Outcome: &promotionsv1.SuspendCouponResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReinstateCoupon reinstates a coupon.
func (s *CouponLifecycleServer) ReinstateCoupon(ctx context.Context, req *promotionsv1.ReinstateCouponRequest) (*promotionsv1.ReinstateCouponResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "reinstate_coupon"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate coupon", "tenant", req.GetContext().GetTenantId())
	listPendingParcelsRequest := &shippingv1.ListPendingParcelsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListPendingParcelsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetCouponId(),
			Labels:       req.GetAnnotations(),
		},
	}
	listPendingParcelsReply := new(shippingv1.ListPendingParcelsResponse)
	if err := grpcx.Invoke(ctx, s.conn, shippingv1.ParcelApprovalService_ListPendingParcels_FullMethodName, listPendingParcelsRequest, listPendingParcelsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelApprovalService/ListPendingParcels")
	}
	shareOrderReportRequest := &ordersv1.ShareOrderReportRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ShareOrderReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
		},
	}
	send := s.shareOrderReportSender(shareOrderReportRequest)
	shareOrderReportReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/ShareOrderReport")
	}
	browseSoftwareLicensesRequest := &licensingv1.BrowseSoftwareLicensesRequest{
		Context: req.GetContext(),
		Filter: &licensingv1.BrowseSoftwareLicensesRequest_Filter{
			Region:       shareOrderReportReply.GetOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: shareOrderReportReply.GetOrder().GetAudit().GetUpdatedAt(),
			Labels:       listPendingParcelsReply.GetResult().GetDetails(),
			OwnerId:      listPendingParcelsReply.GetResult().GetCorrelationId(),
		},
	}
	var browseSoftwareLicensesReply *licensingv1.BrowseSoftwareLicensesResponse
	if err := pipeline.Run(ctx, "acme.licensing.v1.SoftwareLicenseQueryService/BrowseSoftwareLicenses", func(ctx context.Context) error {
		answer, err := s.softwareLicenseQueryClient.BrowseSoftwareLicenses(ctx, browseSoftwareLicensesRequest)
		browseSoftwareLicensesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.licensing.v1.SoftwareLicenseQueryService/BrowseSoftwareLicenses")
	}
	listShipmentWorkflowsRequest := &shippingv1.ListShipmentWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListShipmentWorkflowsRequest_Filter{
			Region:       shareOrderReportReply.GetOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: shareOrderReportReply.GetOrder().GetAudit().GetUpdatedAt(),
			OwnerId:      shareOrderReportReply.GetOutcome().GetReferenceId(),
		},
	}
	listShipmentWorkflowsReply, err := shippingv1.ShipmentWorkflowServiceClient.ListShipmentWorkflows(s.shipmentWorkflowClient, ctx, listShipmentWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentWorkflowService/ListShipmentWorkflows")
	}
	_ = listShipmentWorkflowsReply
	return &promotionsv1.ReinstateCouponResponse{
		Result: &commonv1.OperationResult{
			Details: listPendingParcelsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &promotionsv1.ReinstateCouponResponse_Outcome{
			Message:     browseSoftwareLicensesReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Coupon: &promotionsv1.Coupon{CouponId: req.GetCouponId()},
	}, nil
}

// TerminateCoupon terminates a coupon.
func (s *CouponLifecycleServer) TerminateCoupon(ctx context.Context, req *promotionsv1.TerminateCouponRequest) (*promotionsv1.TerminateCouponResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "terminate_coupon"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate coupon", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*promotionsv1.TerminateCouponRequest_Details)
	streamCampaignMetricsStream, err := s.campaignReportingClient.StreamCampaignMetrics(ctx, &promotionsv1.StreamCampaignMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:             cloned.GetNotifyContact().GetName(),
			Email:            req.GetDetails().GetNewOwner().GetEmail(),
			Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignReportingService/StreamCampaignMetrics")
	}
	for {
		received, err := streamCampaignMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignReportingService/StreamCampaignMetrics")
		}
		_ = received
	}
	retryRedemptionImportReply, err := s.redemptionImportClient.RetryRedemptionImport(ctx, &promotionsv1.RetryRedemptionImportRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.RetryRedemptionImportRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionImportService/RetryRedemptionImport")
	}
	return &promotionsv1.TerminateCouponResponse{
		Outcome: &promotionsv1.TerminateCouponResponse_Outcome{
			Message:     retryRedemptionImportReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: retryRedemptionImportReply.GetOutcome().GetStatus()},
		Coupon: &promotionsv1.Coupon{CouponId: req.GetCouponId()},
	}, nil
}

// RenewCoupon renews a coupon.
func (s *CouponLifecycleServer) RenewCoupon(ctx context.Context, req *promotionsv1.RenewCouponRequest) (*promotionsv1.RenewCouponResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "renew_coupon"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew coupon", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := couponLifecycleServerGenerateOrderReportSteps[mode]
	generateOrderReportRequest := &ordersv1.GenerateOrderReportRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	generateOrderReportReply, err := run(s, ctx, generateOrderReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/GenerateOrderReport")
	}
	rejectOrderAmendmentRequest := buildRejectOrderAmendmentRequestForRenewCoupon(req)
	rejectOrderAmendmentReply, err := s.orderAmendmentApprovalClient.RejectOrderAmendment(ctx, rejectOrderAmendmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentApprovalService/RejectOrderAmendment")
	}
	return &promotionsv1.RenewCouponResponse{
		Result: &commonv1.OperationResult{
			Status:        generateOrderReportReply.GetResult().GetStatus(),
			CorrelationId: rejectOrderAmendmentReply.GetResult().GetCorrelationId(),
		},
		Outcome: &promotionsv1.RenewCouponResponse_Outcome{
			Status:      rejectOrderAmendmentReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Coupon: &promotionsv1.Coupon{CouponId: req.GetCouponId()},
	}, nil
}

// ExpireCoupon expires a coupon.
func (s *CouponLifecycleServer) ExpireCoupon(ctx context.Context, req *promotionsv1.ExpireCouponRequest) (*promotionsv1.ExpireCouponResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "expire_coupon"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire coupon", "tenant", req.GetContext().GetTenantId())
	resolveRedemptionConflictRequest := buildResolveRedemptionConflictRequestForExpireCoupon(req)
	resolveRedemptionConflictReply, err := s.redemptionSyncClient.ResolveRedemptionConflict(ctx, resolveRedemptionConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionSyncService/ResolveRedemptionConflict")
	}
	resolveWorkOrdersRequest := buildResolveWorkOrdersRequestForExpireCoupon(req)
	resolveWorkOrdersReply, err := s.workOrderQueryClient.ResolveWorkOrders(ctx, resolveWorkOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.maintenance.v1.WorkOrderQueryService/ResolveWorkOrders")
	}
	validateRedemptionRequest := &promotionsv1.ValidateRedemptionRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ValidateRedemptionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetNotifyContact().GetName(),
				Email: req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: resolveRedemptionConflictReply.GetRedemption().GetSubject().GetDisplayName(),
				CouponId:    resolveRedemptionConflictReply.GetRedemption().GetSubject().GetCouponId(),
			},
		},
	}
	validateRedemptionReply := new(promotionsv1.ValidateRedemptionResponse)
	if err := s.conn.Invoke(ctx, promotionsRedemptionValidationServiceValidateRedemptionMethod, validateRedemptionRequest, validateRedemptionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionValidationService/ValidateRedemption")
	}
	return &promotionsv1.ExpireCouponResponse{
		Outcome: &promotionsv1.ExpireCouponResponse_Outcome{
			Status:      resolveRedemptionConflictReply.GetResult().GetStatus(),
			ReferenceId: validateRedemptionReply.GetOutcome().GetReferenceId(),
			NextSteps:   validateRedemptionReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status:  resolveRedemptionConflictReply.GetOutcome().GetStatus(),
			Details: resolveWorkOrdersReply.GetResult().GetDetails(),
		},
		Coupon: &promotionsv1.Coupon{CouponId: req.GetCouponId()},
	}, nil
}

// ScheduleCouponExpiry schedules a coupon.
func (s *CouponLifecycleServer) ScheduleCouponExpiry(ctx context.Context, req *promotionsv1.ScheduleCouponExpiryRequest) (*promotionsv1.ScheduleCouponExpiryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "schedule coupon expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetCoupon().GetContact().GetPreferredChannel())
	subscribeApiCredentialUpdatesRequest := &identityv1.SubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
			},
		},
	}
	subscribeApiCredentialUpdatesReply, err := grpcx.Call(ctx, s.apiCredentialNotificationClient.SubscribeApiCredentialUpdates, subscribeApiCredentialUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SubscribeApiCredentialUpdates")
	}
	couponCache := cache.New[string, *promotionsv1.Coupon](1)
	couponCache.Put(req.GetContext().GetTraceId(), req.GetCoupon())
	cachedCoupon, _ := couponCache.Get(req.GetContext().GetTraceId())
	resumeWarehouseWorkflowRequest := &warehousingv1.ResumeWarehouseWorkflowRequest{
		Context: req.GetContext(),
		Details: &warehousingv1.ResumeWarehouseWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: subscribeApiCredentialUpdatesReply.GetApiCredential().GetContact().GetPreferredChannel(),
				Phone:            cachedCoupon.GetContact().GetPhone(),
			},
		},
		Warehouse: &warehousingv1.Warehouse{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			Audit: &commonv1.AuditInfo{Etag: req.GetCoupon().GetAudit().GetEtag()},
		},
	}
	resumeWarehouseWorkflowReply := new(warehousingv1.ResumeWarehouseWorkflowResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", warehousingWarehouseWorkflowServiceName, "ResumeWarehouseWorkflow"), resumeWarehouseWorkflowRequest, resumeWarehouseWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseWorkflowService/ResumeWarehouseWorkflow")
	}
	lintCampaignsRequest := &promotionsv1.LintCampaignsRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			}},
		},
	}
	lintCampaignsReply := new(promotionsv1.LintCampaignsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.promotions.v1.CampaignValidationService/LintCampaigns", lintCampaignsRequest, lintCampaignsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignValidationService/LintCampaigns")
	}
	retryWarehouseWorkflowRequest := &warehousingv1.RetryWarehouseWorkflowRequest{
		Context: req.GetContext(),
		Warehouse: &warehousingv1.Warehouse{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lintCampaignsReply.GetCampaign().GetAudit().GetUpdatedAt(),
			},
		},
	}
	retryWarehouseWorkflowReply := new(warehousingv1.RetryWarehouseWorkflowResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.warehousing.v1.WarehouseWorkflowService/RetryWarehouseWorkflow", retryWarehouseWorkflowRequest, retryWarehouseWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseWorkflowService/RetryWarehouseWorkflow")
	}
	return &promotionsv1.ScheduleCouponExpiryResponse{
		Result: &commonv1.OperationResult{
			Message:       subscribeApiCredentialUpdatesReply.GetOutcome().GetMessage(),
			CorrelationId: retryWarehouseWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Coupon: &promotionsv1.Coupon{
			Available: subscribeApiCredentialUpdatesReply.GetApiCredential().GetAvailable(),
			Holder: &promotionsv1.PromotionRef{
				DisplayName: resumeWarehouseWorkflowReply.GetWarehouse().GetOwner().GetDisplayName(),
			},
			CouponId: req.GetCouponId(),
			OpenedAt: req.GetCoupon().GetOpenedAt(),
		},
		Outcome: &promotionsv1.ScheduleCouponExpiryResponse_Outcome{Message: lintCampaignsReply.GetResult().GetMessage()},
	}, nil
}

// shareOrderReportSender returns a function that sends one request to
// acme.orders.v1.OrderReportingService/ShareOrderReport.
func (s *CouponLifecycleServer) shareOrderReportSender(request *ordersv1.ShareOrderReportRequest) func(context.Context) (*ordersv1.ShareOrderReportResponse, error) {
	return func(ctx context.Context) (*ordersv1.ShareOrderReportResponse, error) {
		return s.orderReportingClient.ShareOrderReport(ctx, request)
	}
}

// couponLifecycleServerGenerateOrderReportSteps are the ways to run
// acme.orders.v1.OrderReportingService/GenerateOrderReport, by name.
var couponLifecycleServerGenerateOrderReportSteps = map[string]func(*CouponLifecycleServer, context.Context, *ordersv1.GenerateOrderReportRequest) (*ordersv1.GenerateOrderReportResponse, error){
	"default": (*CouponLifecycleServer).generateOrderReportStep,
	"audited": (*CouponLifecycleServer).generateOrderReportAuditedStep,
}

func (s *CouponLifecycleServer) generateOrderReportStep(ctx context.Context, request *ordersv1.GenerateOrderReportRequest) (*ordersv1.GenerateOrderReportResponse, error) {
	return s.orderReportingClient.GenerateOrderReport(ctx, request)
}

func (s *CouponLifecycleServer) generateOrderReportAuditedStep(ctx context.Context, request *ordersv1.GenerateOrderReportRequest) (*ordersv1.GenerateOrderReportResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.orders.v1.OrderReportingService/GenerateOrderReport")
	return s.orderReportingClient.GenerateOrderReport(ctx, request)
}
