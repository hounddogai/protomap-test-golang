package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	configurationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/configuration/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	itsmv1 "github.com/hounddogai/protomap-test-golang/gen/acme/itsm/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// CouponWorkflowServer implements `acme.promotions.v1.CouponWorkflowService`.
// Runs the long-lived workflow that takes a coupon from start to finish.
type CouponWorkflowServer struct {
	promotionsv1.UnimplementedCouponWorkflowServiceServer
	store                                 *Store
	log                                   *slog.Logger
	promotionApprovalPort                 promotionApprovalPort
	pickListWorkflowClient                warehousingv1.PickListWorkflowServiceClient
	venueQueryClient                      venuesv1.VenueQueryServiceClient
	orderAmendmentReportingClient         ordersv1.OrderAmendmentReportingServiceClient
	orderAmendmentReportingClientFallback ordersv1.OrderAmendmentReportingServiceClient
	promotionApprovalClient               promotionsv1.PromotionApprovalServiceClient
	fulfillmentExceptionApprovalClient    fulfillmentv1.FulfillmentExceptionApprovalServiceClient
	campaignSyncClient                    promotionsv1.CampaignSyncServiceClient
	configSetQueryClient                  configurationv1.ConfigSetQueryServiceClient
	conn                                  *grpc.ClientConn
	incidentQueryOnce                     sync.Once
	incidentQueryCached                   itsmv1.IncidentQueryServiceClient
	searchQueryQueryClient                searchv1.SearchQueryQueryServiceClient
	couponLifecycleClient                 promotionsv1.CouponLifecycleServiceClient
	searchIndexExportClient               searchv1.SearchIndexExportServiceClient
	couponReportingClient                 promotionsv1.CouponReportingServiceClient
	clients                               *registry.Registry
	trackingEventApprovalOnce             sync.Once
	trackingEventApprovalCached           shippingv1.TrackingEventApprovalServiceClient
}

// NewCouponWorkflowServer wires a CouponWorkflowServer.
func NewCouponWorkflowServer(deps platform.Deps) *CouponWorkflowServer {
	return &CouponWorkflowServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.promotions.v1.CouponWorkflowService"),
		promotionApprovalPort:                 newPromotionApprovalPort(deps),
		pickListWorkflowClient:                warehousingv1.NewPickListWorkflowServiceClient(deps.Conn),
		venueQueryClient:                      venuesv1.NewVenueQueryServiceClient(deps.Conn),
		orderAmendmentReportingClient:         ordersv1.NewOrderAmendmentReportingServiceClient(deps.Conn),
		orderAmendmentReportingClientFallback: ordersv1.NewOrderAmendmentReportingServiceClient(deps.Conn),
		promotionApprovalClient:               promotionsv1.NewPromotionApprovalServiceClient(deps.Conn),
		fulfillmentExceptionApprovalClient:    fulfillmentv1.NewFulfillmentExceptionApprovalServiceClient(deps.Conn),
		campaignSyncClient:                    promotionsv1.NewCampaignSyncServiceClient(deps.Conn),
		configSetQueryClient:                  configurationv1.NewConfigSetQueryServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		searchQueryQueryClient:                searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		couponLifecycleClient:                 promotionsv1.NewCouponLifecycleServiceClient(deps.Conn),
		searchIndexExportClient:               searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		couponReportingClient:                 promotionsv1.NewCouponReportingServiceClient(deps.Conn),
		clients:                               deps.Clients,
	}
}

// StartCouponWorkflow starts a coupon.
func (s *CouponWorkflowServer) StartCouponWorkflow(ctx context.Context, req *promotionsv1.StartCouponWorkflowRequest) (*promotionsv1.StartCouponWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "start coupon workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	delegatePromotionApprovalRequest := &promotionsv1.DelegatePromotionApprovalRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.DelegatePromotionApprovalRequest_Details{
			Approver: &identityv1.UserRef{DisplayName: req.GetCoupon().GetHolder().GetDisplayName()},
		},
		Promotion: &promotionsv1.Promotion{
			Audit:      &commonv1.AuditInfo{CreatedAt: req.GetCoupon().GetAudit().GetCreatedAt()},
			Parameters: req.GetDetails().GetParameters(),
		},
	}
	delegatePromotionApprovalReply, err := s.promotionApprovalPort.DelegatePromotionApproval(ctx, delegatePromotionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionApprovalService/DelegatePromotionApproval")
	}
	encoded, err := protojson.Marshal(req.GetCoupon())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.Coupon
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	signalPickListWorkflowReply, err := s.pickListWorkflowClient.SignalPickListWorkflow(ctx, &warehousingv1.SignalPickListWorkflowRequest{
		Context: req.GetContext(),
		Details: &warehousingv1.SignalPickListWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		PickList: &warehousingv1.PickList{
			Audit: &commonv1.AuditInfo{UpdatedAt: (&decoded).GetAudit().GetUpdatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListWorkflowService/SignalPickListWorkflow")
	}
	summarizeVenuesRequest := buildSummarizeVenuesRequestForStartCouponWorkflow(req, delegatePromotionApprovalReply)
	summarizeVenuesReply, err := s.venueQueryClient.SummarizeVenues(ctx, summarizeVenuesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueQueryService/SummarizeVenues")
	}
	routes := map[string]func(context.Context, *ordersv1.ShareOrderAmendmentReportRequest, ...grpc.CallOption) (*ordersv1.ShareOrderAmendmentReportResponse, error){
		"primary":  s.orderAmendmentReportingClient.ShareOrderAmendmentReport,
		"fallback": s.orderAmendmentReportingClientFallback.ShareOrderAmendmentReport,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	shareOrderAmendmentReportRequest := &ordersv1.ShareOrderAmendmentReportRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ShareOrderAmendmentReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: signalPickListWorkflowReply.GetPickList().GetCounterparty().GetEmail(),
			},
		},
	}
	shareOrderAmendmentReportReply, err := route(ctx, shareOrderAmendmentReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentReportingService/ShareOrderAmendmentReport")
	}
	return &promotionsv1.StartCouponWorkflowResponse{
		Outcome: &promotionsv1.StartCouponWorkflowResponse_Outcome{
			Message: delegatePromotionApprovalReply.GetOutcome().GetMessage(),
			Status:  summarizeVenuesReply.GetResult().GetStatus(),
		},
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{
				Email: signalPickListWorkflowReply.GetPickList().GetCounterparty().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: signalPickListWorkflowReply.GetPickList().GetAudit().GetCreatedBy(),
				UpdatedAt: shareOrderAmendmentReportReply.GetOrderAmendment().GetAudit().GetUpdatedAt(),
				CreatedAt: shareOrderAmendmentReportReply.GetOrderAmendment().GetAudit().GetCreatedAt(),
			},
			CouponId: req.GetCouponId(),
			Labels:   req.GetCoupon().GetLabels(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceCouponWorkflow advances a coupon.
func (s *CouponWorkflowServer) AdvanceCouponWorkflow(ctx context.Context, req *promotionsv1.AdvanceCouponWorkflowRequest) (*promotionsv1.AdvanceCouponWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceCouponWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "advance coupon workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	rejectPromotionRequest := &promotionsv1.RejectPromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCoupon().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetCoupon().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{DisplayName: req.GetCoupon().GetHolder().GetDisplayName()},
		},
		Details: &promotionsv1.RejectPromotionRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetCoupon().GetContact().GetEmail()},
		},
	}
	rejectPromotionReply, err := promotionsv1.PromotionApprovalServiceClient.RejectPromotion(s.promotionApprovalClient, ctx, rejectPromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionApprovalService/RejectPromotion")
	}
	submitFulfillmentExceptionRequest := buildSubmitFulfillmentExceptionRequestForAdvanceCouponWorkflow(req, rejectPromotionReply)
	submitFulfillmentExceptionReply, err := s.fulfillmentExceptionApprovalClient.SubmitFulfillmentException(ctx, submitFulfillmentExceptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionApprovalService/SubmitFulfillmentException")
	}
	encoded, err := protojson.Marshal(req.GetCoupon())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.Coupon
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	snapshot, err := protojson.Marshal(req.GetCoupon())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveCampaignConflictRequest := &promotionsv1.ResolveCampaignConflictRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetCoupon().GetContact().GetEmail()},
			}},
			Audit: &commonv1.AuditInfo{UpdatedBy: (&decoded).GetAudit().GetUpdatedBy()},
		},
		Details: &promotionsv1.ResolveCampaignConflictRequest_Details{Payload: snapshot},
	}
	resolveCampaignConflictReply, err := grpcx.Call(ctx, s.campaignSyncClient.ResolveCampaignConflict, resolveCampaignConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/ResolveCampaignConflict")
	}
	return &promotionsv1.AdvanceCouponWorkflowResponse{
		Outcome: &promotionsv1.AdvanceCouponWorkflowResponse_Outcome{
			NextSteps: rejectPromotionReply.GetOutcome().GetNextSteps(),
			Status:    submitFulfillmentExceptionReply.GetOutcome().GetStatus(),
		},
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{Version: rejectPromotionReply.GetPromotion().GetVersion()},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: submitFulfillmentExceptionReply.GetFulfillmentException().GetAudit().GetUpdatedBy(),
			},
			CouponId: req.GetCouponId(),
			Status:   req.GetCoupon().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: resolveCampaignConflictReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CancelCouponWorkflow cancels a coupon.
func (s *CouponWorkflowServer) CancelCouponWorkflow(ctx context.Context, req *promotionsv1.CancelCouponWorkflowRequest) (*promotionsv1.CancelCouponWorkflowResponse, error) {
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "cancel coupon workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetCoupon().GetLabels()))
	for key, value := range req.GetCoupon().GetLabels() {
		labels[key] = value
	}
	var externalAccountId string
	switch choice := req.GetCoupon().GetFunding().(type) {
	case *promotionsv1.Coupon_Card:
		externalAccountId = choice.Card.GetHolderName()
	case *promotionsv1.Coupon_Bank:
		externalAccountId = choice.Bank.GetIban()
	case *promotionsv1.Coupon_ExternalAccountId:
		externalAccountId = choice.ExternalAccountId
	}
	browseConfigSetsRequest := &configurationv1.BrowseConfigSetsRequest{
		Context: req.GetContext(),
		Filter: &configurationv1.BrowseConfigSetsRequest_Filter{
			Labels:       labels,
			CreatedAfter: req.GetCoupon().GetAudit().GetUpdatedAt(),
			OwnerId:      externalAccountId,
		},
	}
	var browseConfigSetsReply *configurationv1.BrowseConfigSetsResponse
	if err := pipeline.Run(ctx, "acme.configuration.v1.ConfigSetQueryService/BrowseConfigSets", func(ctx context.Context) error {
		answer, err := s.configSetQueryClient.BrowseConfigSets(ctx, browseConfigSetsRequest)
		browseConfigSetsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.configuration.v1.ConfigSetQueryService/BrowseConfigSets")
	}
	labels2 := make(map[string]string, len(req.GetCoupon().GetLabels()))
	for key2, value2 := range req.GetCoupon().GetLabels() {
		labels2[key2] = value2
	}
	suggestIncidentsRequest := &itsmv1.SuggestIncidentsRequest{
		Context: req.GetContext(),
		Filter: &itsmv1.SuggestIncidentsRequest_Filter{
			Labels:       labels2,
			CreatedAfter: req.GetCoupon().GetAudit().GetCreatedAt(),
			OwnerId:      browseConfigSetsReply.GetResult().GetCorrelationId(),
		},
	}
	suggestIncidentsReply, err := s.incidentQueryLazy().SuggestIncidents(ctx, suggestIncidentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.itsm.v1.IncidentQueryService/SuggestIncidents")
	}
	_ = suggestIncidentsReply
	return &promotionsv1.CancelCouponWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details:       browseConfigSetsReply.GetResult().GetDetails(),
			CorrelationId: browseConfigSetsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Coupon: &promotionsv1.Coupon{
			CouponId:  req.GetCouponId(),
			Available: req.GetCoupon().GetAvailable(),
		},
	}, nil
}

// RetryCouponWorkflow retrys a coupon.
func (s *CouponWorkflowServer) RetryCouponWorkflow(ctx context.Context, req *promotionsv1.RetryCouponWorkflowRequest) (*promotionsv1.RetryCouponWorkflowResponse, error) {
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "retry coupon workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetCoupon().GetLabels()))
	for key, value := range req.GetCoupon().GetLabels() {
		labels[key] = value
	}
	exportSearchQueriesStream, err := s.searchQueryQueryClient.ExportSearchQueries(ctx, &searchv1.ExportSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchQueriesRequest_Filter{
			Labels:       labels,
			CreatedAfter: req.GetCoupon().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetCoupon().GetHolder().GetPromotionId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
	}
	for {
		received, err := exportSearchQueriesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
		}
		_ = received
	}
	terminateCouponRequest := &promotionsv1.TerminateCouponRequest{
		Context:  req.GetContext(),
		CouponId: req.GetCouponId(),
		Details: &promotionsv1.TerminateCouponRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotify().GetName(),
				PreferredChannel: req.GetCoupon().GetContact().GetPreferredChannel(),
				Email:            req.GetCoupon().GetContact().GetEmail(),
			},
			NewOwner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
	}
	send := s.terminateCouponSender(terminateCouponRequest)
	terminateCouponReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponLifecycleService/TerminateCoupon")
	}
	couponCache := cache.New[string, *promotionsv1.Coupon](1)
	couponCache.Put(req.GetContext().GetTraceId(), req.GetCoupon())
	cachedCoupon, _ := couponCache.Get(req.GetContext().GetTraceId())
	snapshot, err := protojson.Marshal(req.GetCoupon())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileCampaignsRequest := newReconcileCampaignsRequest(withReconcileCampaignsContext(req.GetContext()), withReconcileCampaignsCampaign(&promotionsv1.Campaign{
		Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
			Contact: &commonv1.ContactInfo{
				Name:  terminateCouponReply.GetCoupon().GetContact().GetName(),
				Email: req.GetCoupon().GetContact().GetEmail(),
				Phone: cachedCoupon.GetContact().GetPhone(),
			},
		}},
	}), withReconcileCampaignsDetails(&promotionsv1.ReconcileCampaignsRequest_Details{Payload: snapshot}))
	reconcileCampaignsReply, err := s.campaignSyncClient.ReconcileCampaigns(ctx, reconcileCampaignsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/ReconcileCampaigns")
	}
	commentFulfillmentExceptionApprovalRequest := &fulfillmentv1.CommentFulfillmentExceptionApprovalRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: req.GetCoupon().GetContact().GetPreferredChannel(),
				Email:            terminateCouponReply.GetCoupon().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetCoupon().GetAudit().GetCreatedBy()},
		},
	}
	commentFulfillmentExceptionApprovalReply, err := grpcx.Call(ctx, s.fulfillmentExceptionApprovalClient.CommentFulfillmentExceptionApproval, commentFulfillmentExceptionApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionApprovalService/CommentFulfillmentExceptionApproval")
	}
	_ = reconcileCampaignsReply
	return &promotionsv1.RetryCouponWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: terminateCouponReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &promotionsv1.RetryCouponWorkflowResponse_Outcome{
			EffectiveAt: terminateCouponReply.GetOutcome().GetEffectiveAt(),
			ReferenceId: commentFulfillmentExceptionApprovalReply.GetOutcome().GetReferenceId(),
		},
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Contact:  req.GetCoupon().GetContact(),
		},
	}, nil
}

// PauseCouponWorkflow pauses a coupon.
func (s *CouponWorkflowServer) PauseCouponWorkflow(ctx context.Context, req *promotionsv1.PauseCouponWorkflowRequest) (*promotionsv1.PauseCouponWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "pause coupon workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetCoupon().GetLabels()))
	for key, value := range req.GetCoupon().GetLabels() {
		labels[key] = value
	}
	searchNotificationsRequest := &notificationsv1.SearchNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.SearchNotificationsRequest_Filter{
			Labels:       labels,
			CreatedAfter: req.GetCoupon().GetAudit().GetUpdatedAt(),
		},
	}
	searchNotificationsReply := new(notificationsv1.SearchNotificationsResponse)
	if err := grpcx.Invoke(ctx, s.conn, notificationsv1.NotificationService_SearchNotifications_FullMethodName, searchNotificationsRequest, searchNotificationsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/SearchNotifications")
	}
	labels2 := make(map[string]string, len(req.GetCoupon().GetLabels()))
	for key2, value2 := range req.GetCoupon().GetLabels() {
		labels2[key2] = value2
	}
	streamSearchIndexExportStream, err := s.searchIndexExportClient.StreamSearchIndexExport(ctx, &searchv1.StreamSearchIndexExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSearchIndexExportRequest_Filter{
			Labels:       labels2,
			CreatedAfter: req.GetCoupon().GetOpenedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
	}
	for {
		received, err := streamSearchIndexExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
		}
		_ = received
	}
	return &promotionsv1.PauseCouponWorkflowResponse{
		Outcome: &promotionsv1.PauseCouponWorkflowResponse_Outcome{Message: searchNotificationsReply.GetResult().GetMessage()},
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Balance:  req.GetCoupon().GetBalance(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeCouponWorkflow resumes a coupon.
func (s *CouponWorkflowServer) ResumeCouponWorkflow(ctx context.Context, req *promotionsv1.ResumeCouponWorkflowRequest) (*promotionsv1.ResumeCouponWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "resume coupon workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetCoupon().GetLabels()))
	for key, value := range req.GetCoupon().GetLabels() {
		labels[key] = value
	}
	cloned := proto.Clone(req.GetCoupon()).(*promotionsv1.Coupon)
	listCouponReportsReply, err := s.couponReportingClient.ListCouponReports(ctx, &promotionsv1.ListCouponReportsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.ListCouponReportsRequest_Filter{
			Labels:       labels,
			CreatedAfter: cloned.GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetCoupon().GetHolder().GetPromotionId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponReportingService/ListCouponReports")
	}
	couponLifecycle, err := registry.Lookup[promotionsv1.CouponLifecycleServiceClient](s.clients, "acme.promotions.v1.CouponLifecycleService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "coupon_id", req.GetCoupon().GetCouponId())
	activateCouponRequest := &promotionsv1.ActivateCouponRequest{
		Context:  req.GetContext(),
		CouponId: tenancy.Value(ctx, "coupon_id"),
		Details: &promotionsv1.ActivateCouponRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Name: req.GetCoupon().GetContact().GetName()},
			NewOwner:      &identityv1.UserRef{Email: req.GetCoupon().GetContact().GetEmail()},
		},
	}
	activateCouponReply, err := couponLifecycle.ActivateCoupon(ctx, activateCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponLifecycleService/ActivateCoupon")
	}
	return &promotionsv1.ResumeCouponWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: listCouponReportsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Coupon: &promotionsv1.Coupon{
			Audit:        &commonv1.AuditInfo{Etag: activateCouponReply.GetCoupon().GetAudit().GetEtag()},
			Contact:      activateCouponReply.GetCoupon().GetContact(),
			CouponId:     req.GetCouponId(),
			CurrencyCode: req.GetCoupon().GetCurrencyCode(),
		},
	}, nil
}

// InspectCouponWorkflow returns a coupon.
func (s *CouponWorkflowServer) InspectCouponWorkflow(ctx context.Context, req *promotionsv1.InspectCouponWorkflowRequest) (*promotionsv1.InspectCouponWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "inspect_coupon_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect coupon workflow", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	approveTrackingEventRequest := &shippingv1.ApproveTrackingEventRequest{
		Context: req.GetContext(),
		Details: &shippingv1.ApproveTrackingEventRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		TrackingEvent: &shippingv1.TrackingEvent{Detail: packed},
	}
	approveTrackingEventReply, err := s.trackingEventApprovalLazy().ApproveTrackingEvent(ctx, approveTrackingEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventApprovalService/ApproveTrackingEvent")
	}
	return &promotionsv1.InspectCouponWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: approveTrackingEventReply.GetResult().GetCorrelationId(),
			ReferenceIds:  approveTrackingEventReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Coupon: &promotionsv1.Coupon{CouponId: req.GetCouponId()},
	}, nil
}

// ListCouponWorkflows lists coupons a page at a time.
func (s *CouponWorkflowServer) ListCouponWorkflows(ctx context.Context, req *promotionsv1.ListCouponWorkflowsRequest) (*promotionsv1.ListCouponWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListCouponWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list coupon workflows", "tenant", req.GetContext().GetTenantId())
	escalateCampaignRequest := &promotionsv1.EscalateCampaignRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	escalateCampaignReply := new(promotionsv1.EscalateCampaignResponse)
	if err := grpc.Invoke(ctx, "/acme.promotions.v1.CampaignApprovalService/EscalateCampaign", escalateCampaignRequest, escalateCampaignReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignApprovalService/EscalateCampaign")
	}
	return &promotionsv1.ListCouponWorkflowsResponse{
		Result: &commonv1.OperationResult{Status: escalateCampaignReply.GetOutcome().GetStatus()},
		Coupons: []*promotionsv1.Coupon{&promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: escalateCampaignReply.GetCampaign().GetAudit().GetUpdatedAt(),
			},
		}},
	}, nil
}

// SignalCouponWorkflow signals a coupon.
func (s *CouponWorkflowServer) SignalCouponWorkflow(ctx context.Context, req *promotionsv1.SignalCouponWorkflowRequest) (*promotionsv1.SignalCouponWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCoupon(ctx, req.GetContext().GetTenantId(), req.GetCoupon()); err != nil {
		return nil, grpcx.Wrap(err, "save coupon")
	}
	s.log.InfoContext(ctx, "signal coupon workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCoupon().GetAudit().GetEtag())
	promotionLifecycle, err := registry.Lookup[promotionsv1.PromotionLifecycleServiceClient](s.clients, "acme.promotions.v1.PromotionLifecycleService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	activatePromotionRequest := &promotionsv1.ActivatePromotionRequest{
		Context:     req.GetContext(),
		PromotionId: req.GetCoupon().GetHolder().GetPromotionId(),
		Details: &promotionsv1.ActivatePromotionRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: req.GetCoupon().GetContact().GetEmail()},
		},
	}
	activatePromotionReply, err := promotionLifecycle.ActivatePromotion(ctx, activatePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionLifecycleService/ActivatePromotion")
	}
	return &promotionsv1.SignalCouponWorkflowResponse{
		Outcome: &promotionsv1.SignalCouponWorkflowResponse_Outcome{
			NextSteps: activatePromotionReply.GetOutcome().GetNextSteps(),
		},
		Coupon: &promotionsv1.Coupon{
			CouponId: req.GetCouponId(),
			Status:   req.GetCoupon().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// incidentQueryLazy dials acme.itsm.v1.IncidentQueryService the first time a handler needs it.
func (s *CouponWorkflowServer) incidentQueryLazy() itsmv1.IncidentQueryServiceClient {
	s.incidentQueryOnce.Do(func() { s.incidentQueryCached = itsmv1.NewIncidentQueryServiceClient(s.conn) })
	return s.incidentQueryCached
}

// terminateCouponSender returns a function that sends one request to
// acme.promotions.v1.CouponLifecycleService/TerminateCoupon.
func (s *CouponWorkflowServer) terminateCouponSender(request *promotionsv1.TerminateCouponRequest) func(context.Context) (*promotionsv1.TerminateCouponResponse, error) {
	return func(ctx context.Context) (*promotionsv1.TerminateCouponResponse, error) {
		return s.couponLifecycleClient.TerminateCoupon(ctx, request)
	}
}

// trackingEventApprovalLazy dials acme.shipping.v1.TrackingEventApprovalService the first time a handler needs it.
func (s *CouponWorkflowServer) trackingEventApprovalLazy() shippingv1.TrackingEventApprovalServiceClient {
	s.trackingEventApprovalOnce.Do(func() { s.trackingEventApprovalCached = shippingv1.NewTrackingEventApprovalServiceClient(s.conn) })
	return s.trackingEventApprovalCached
}
