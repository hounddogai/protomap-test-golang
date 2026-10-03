package promotions

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	maintenancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// CampaignReportingServer implements `acme.promotions.v1.CampaignReportingService`.
// Builds and schedules reports about campaigns.
type CampaignReportingServer struct {
	promotionsv1.UnimplementedCampaignReportingServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	promotionReportingClient      promotionsv1.PromotionReportingServiceClient
	campaignSyncClient            promotionsv1.CampaignSyncServiceClient
	refundWorkflowOnce            sync.Once
	refundWorkflowCached          paymentsv1.RefundWorkflowServiceClient
	redemptionSyncClient          promotionsv1.RedemptionSyncServiceClient
	unsubscribeTenantUpdatesFn    func(context.Context, *identityv1.UnsubscribeTenantUpdatesRequest, ...grpc.CallOption) (*identityv1.UnsubscribeTenantUpdatesResponse, error)
	couponReportingClient         promotionsv1.CouponReportingServiceClient
	checkoutSessionApprovalClient checkoutv1.CheckoutSessionApprovalServiceClient
	productReviewWorkflowClient   reviewsv1.ProductReviewWorkflowServiceClient
	workOrderQueryClient          maintenancev1.WorkOrderQueryServiceClient
}

// NewCampaignReportingServer wires a CampaignReportingServer.
func NewCampaignReportingServer(deps platform.Deps) *CampaignReportingServer {
	tenantNotification := identityv1.NewTenantNotificationServiceClient(deps.Conn)
	return &CampaignReportingServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.promotions.v1.CampaignReportingService"),
		conn:                          deps.Conn,
		promotionReportingClient:      promotionsv1.NewPromotionReportingServiceClient(deps.Conn),
		campaignSyncClient:            promotionsv1.NewCampaignSyncServiceClient(deps.Conn),
		redemptionSyncClient:          promotionsv1.NewRedemptionSyncServiceClient(deps.Conn),
		unsubscribeTenantUpdatesFn:    tenantNotification.UnsubscribeTenantUpdates,
		couponReportingClient:         promotionsv1.NewCouponReportingServiceClient(deps.Conn),
		checkoutSessionApprovalClient: checkoutv1.NewCheckoutSessionApprovalServiceClient(deps.Conn),
		productReviewWorkflowClient:   reviewsv1.NewProductReviewWorkflowServiceClient(deps.Conn),
		workOrderQueryClient:          maintenancev1.NewWorkOrderQueryServiceClient(deps.Conn),
	}
}

// GenerateCampaignReport generates a campaign.
func (s *CampaignReportingServer) GenerateCampaignReport(ctx context.Context, req *promotionsv1.GenerateCampaignReportRequest) (*promotionsv1.GenerateCampaignReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "generate_campaign_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate campaign report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	cancelRefundWorkflowRequest := &paymentsv1.CancelRefundWorkflowRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	cancelRefundWorkflowReply := new(paymentsv1.CancelRefundWorkflowResponse)
	if err := s.conn.Invoke(ctx, paymentsRefundWorkflowServiceCancelRefundWorkflowMethod, cancelRefundWorkflowRequest, cancelRefundWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundWorkflowService/CancelRefundWorkflow")
	}
	annotations2 := make(map[string]string, len(req.GetAnnotations()))
	for key2, value2 := range req.GetAnnotations() {
		annotations2[key2] = value2
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sharePromotionReportRequest := newSharePromotionReportRequest(withSharePromotionReportContext(req.GetContext()), withSharePromotionReportDetails(&promotionsv1.SharePromotionReportRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Email: cancelRefundWorkflowReply.GetRefund().GetCounterparty().GetEmail(),
		},
		Variables: req.GetDetails().GetFilters(),
		Payload:   packed,
	}), withSharePromotionReportAnnotations(annotations2), withSharePromotionReportReason(req.GetReason()))
	sharePromotionReportReply, err := s.promotionReportingClient.SharePromotionReport(ctx, sharePromotionReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/SharePromotionReport")
	}
	return &promotionsv1.GenerateCampaignReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: cancelRefundWorkflowReply.GetResult().GetCorrelationId(),
			Status:        sharePromotionReportReply.GetOutcome().GetStatus(),
			Message:       sharePromotionReportReply.GetOutcome().GetMessage(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchCampaignReport returns a campaign.
func (s *CampaignReportingServer) FetchCampaignReport(ctx context.Context, req *promotionsv1.FetchCampaignReportRequest) (*promotionsv1.FetchCampaignReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchCampaignReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "fetch_campaign_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch campaign report", "tenant", req.GetContext().GetTenantId())
	countCouponsRequest := &promotionsv1.CountCouponsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.CountCouponsRequest_Filter{
			OwnerId:      req.GetCampaignId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	}
	countCouponsReply, err := countCouponsCall(ctx, countCouponsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponQueryService/CountCoupons")
	}
	getCampaignSyncStateReply, err := s.campaignSyncClient.GetCampaignSyncState(ctx, &promotionsv1.GetCampaignSyncStateRequest{
		Context:    req.GetContext(),
		CampaignId: req.GetCampaignId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/GetCampaignSyncState")
	}
	return &promotionsv1.FetchCampaignReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: countCouponsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  countCouponsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Campaign: &promotionsv1.Campaign{
			Metadata: getCampaignSyncStateReply.GetCampaign().GetMetadata(),
			Organizer: &identityv1.UserRef{
				UserId: getCampaignSyncStateReply.GetCampaign().GetOrganizer().GetUserId(),
			},
			CampaignId: req.GetCampaignId(),
		},
	}, nil
}

// ScheduleCampaignReport schedules a campaign.
func (s *CampaignReportingServer) ScheduleCampaignReport(ctx context.Context, req *promotionsv1.ScheduleCampaignReportRequest) (*promotionsv1.ScheduleCampaignReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "schedule_campaign_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule campaign report", "tenant", req.GetContext().GetTenantId())
	compareCampaignOptionsRequest := &promotionsv1.CompareCampaignOptionsRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			}},
			CampaignId: req.GetCampaignId(),
		},
		Reason: req.GetReason(),
	}
	compareCampaignOptionsReply := new(promotionsv1.CompareCampaignOptionsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", promotionsCampaignCalculationServiceName, "CompareCampaignOptions"), compareCampaignOptionsRequest, compareCampaignOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignCalculationService/CompareCampaignOptions")
	}
	retryRefundWorkflowRequest := &paymentsv1.RetryRefundWorkflowRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				Locality:   compareCampaignOptionsReply.GetCampaign().GetLocation().GetLocality(),
				Lines:      compareCampaignOptionsReply.GetCampaign().GetLocation().GetLines(),
				PostalCode: compareCampaignOptionsReply.GetCampaign().GetLocation().GetPostalCode(),
			},
		},
	}
	retryRefundWorkflowReply, err := s.refundWorkflowLazy().RetryRefundWorkflow(ctx, retryRefundWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundWorkflowService/RetryRefundWorkflow")
	}
	return &promotionsv1.ScheduleCampaignReportResponse{
		Result: &commonv1.OperationResult{
			Message:       compareCampaignOptionsReply.GetResult().GetMessage(),
			Status:        compareCampaignOptionsReply.GetResult().GetStatus(),
			Details:       retryRefundWorkflowReply.GetResult().GetDetails(),
			CorrelationId: retryRefundWorkflowReply.GetResult().GetCorrelationId(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamCampaignMetrics streams the changes of campaigns as they happen.
func (s *CampaignReportingServer) StreamCampaignMetrics(req *promotionsv1.StreamCampaignMetricsRequest, stream grpc.ServerStreamingServer[promotionsv1.StreamCampaignMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream campaign metrics", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "encode snapshot")
	}
	pullRedemptionsReply, err := s.redemptionSyncClient.PullRedemptions(ctx, &promotionsv1.PullRedemptionsRequest{
		Context: req.GetContext(),
		Redemption: &promotionsv1.Redemption{
			Measurements: []*promotionsv1.RedemptionMeasurement{&promotionsv1.RedemptionMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Source:       req.GetFilter().GetOwnerId(),
			Detail:       packed,
		},
		Details: &promotionsv1.PullRedemptionsRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.promotions.v1.RedemptionSyncService/PullRedemptions")
	}
	unsubscribeTenantUpdatesRequest := &identityv1.UnsubscribeTenantUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeTenantUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: req.GetSubscriber().GetEmail(),
				Name:  req.GetSubscriber().GetName(),
			},
		},
	}
	unsubscribeTenantUpdatesReply, err := s.unsubscribeTenantUpdatesFn(ctx, unsubscribeTenantUpdatesRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/UnsubscribeTenantUpdates")
	}
	packed2, err := anypb.New(req.GetFilter())
	if err != nil {
		return grpcx.Wrap(err, "pack payload")
	}
	shareCouponReportReply, err := s.couponReportingClient.ShareCouponReport(ctx, &promotionsv1.ShareCouponReportRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ShareCouponReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            req.GetSubscriber().GetEmail(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
			},
			Payload: packed2,
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.promotions.v1.CouponReportingService/ShareCouponReport")
	}
	pausePlacementWorkflowRequest := &merchandisingv1.PausePlacementWorkflowRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Location: &commonv1.PostalAddress{
				Recipient: unsubscribeTenantUpdatesReply.GetTenant().GetLocation().GetRecipient(),
			},
		},
	}
	pausePlacementWorkflowReply, err := grpcx.NewClient(s.conn, merchandisingv1.NewPlacementWorkflowServiceClient).PausePlacementWorkflow(ctx, pausePlacementWorkflowRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/PausePlacementWorkflow")
	}
	if err := stream.Send(&promotionsv1.StreamCampaignMetricsResponse{
		Result: &commonv1.OperationResult{
			Status:  pullRedemptionsReply.GetResult().GetStatus(),
			Message: shareCouponReportReply.GetOutcome().GetMessage(),
		},
		Sequence: pullRedemptionsReply.GetRedemption().GetSequence(),
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{
				DisplayName: unsubscribeTenantUpdatesReply.GetTenant().GetManager().GetDisplayName(),
				Email:       req.GetSubscriber().GetEmail(),
			},
			Location: &commonv1.PostalAddress{
				CountryCode: unsubscribeTenantUpdatesReply.GetTenant().GetLocation().GetCountryCode(),
				Region:      req.GetFilter().GetRegion(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: pausePlacementWorkflowReply.GetPlacement().GetAudit().GetEtag(),
			},
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Email: pausePlacementWorkflowReply.GetPlacement().GetOrganizer().GetEmail(),
				},
			}},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ShareCampaignReport shares a campaign.
func (s *CampaignReportingServer) ShareCampaignReport(ctx context.Context, req *promotionsv1.ShareCampaignReportRequest) (*promotionsv1.ShareCampaignReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "share_campaign_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share campaign report", "tenant", req.GetContext().GetTenantId())
	recallCheckoutSessionRequest := buildRecallCheckoutSessionRequestForShareCampaignReport(req)
	recallCheckoutSessionReply, err := s.checkoutSessionApprovalClient.RecallCheckoutSession(ctx, recallCheckoutSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.CheckoutSessionApprovalService/RecallCheckoutSession")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	countPromotionTotalsRequest := &promotionsv1.CountPromotionTotalsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.CountPromotionTotalsRequest_Filter{
			Region:       recallCheckoutSessionReply.GetCheckoutSession().GetBillingAddress().GetRegion(),
			CreatedAfter: recallCheckoutSessionReply.GetCheckoutSession().GetAudit().GetUpdatedAt(),
			OwnerId:      recallCheckoutSessionReply.GetCheckoutSession().GetCounterparty().GetShippingQuoteId(),
			Labels:       req.GetDetails().GetVariables(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	countPromotionTotalsReply := new(promotionsv1.CountPromotionTotalsResponse)
	if err := grpcx.Invoke(ctx, s.conn, promotionsv1.PromotionReportingService_CountPromotionTotals_FullMethodName, countPromotionTotalsRequest, countPromotionTotalsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/CountPromotionTotals")
	}
	return &promotionsv1.ShareCampaignReportResponse{
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recallCheckoutSessionReply.GetCheckoutSession().GetAudit().GetUpdatedBy(),
			},
			Location: &commonv1.PostalAddress{
				CountryCode: recallCheckoutSessionReply.GetCheckoutSession().GetBillingAddress().GetCountryCode(),
			},
			CampaignId: req.GetCampaignId(),
			Organizer:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: countPromotionTotalsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CountCampaignTotals counts campaigns by status.
func (s *CampaignReportingServer) CountCampaignTotals(ctx context.Context, req *promotionsv1.CountCampaignTotalsRequest) (*promotionsv1.CountCampaignTotalsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "count campaign totals", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	startProductReviewWorkflowRequest := &reviewsv1.StartProductReviewWorkflowRequest{
		Context: req.GetContext(),
		ProductReview: &reviewsv1.ProductReview{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: tenancy.Value(ctx, "email")},
			Audit:          &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
			Lines:          []*reviewsv1.ProductReviewLine{&reviewsv1.ProductReviewLine{Attributes: labels}},
		},
	}
	type startProductReviewWorkflowResult struct {
		reply *reviewsv1.StartProductReviewWorkflowResponse
		err   error
	}
	startProductReviewWorkflowResults := make(chan startProductReviewWorkflowResult, 1)
	go func() {
		reply, err := s.productReviewWorkflowClient.StartProductReviewWorkflow(ctx, startProductReviewWorkflowRequest)
		startProductReviewWorkflowResults <- startProductReviewWorkflowResult{reply, err}
	}()
	var startProductReviewWorkflowReply *reviewsv1.StartProductReviewWorkflowResponse
	select {
	case result := <-startProductReviewWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.reviews.v1.ProductReviewWorkflowService/StartProductReviewWorkflow")
		}
		startProductReviewWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	summarizeWorkOrdersRequest := &maintenancev1.SummarizeWorkOrdersRequest{
		Context:     req.GetContext(),
		Annotations: req.GetFilter().GetLabels(),
		Details: &maintenancev1.SummarizeWorkOrdersRequest_Details{
			Filters: startProductReviewWorkflowReply.GetProductReview().GetMetadata(),
		},
	}
	summarizeWorkOrdersReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*maintenancev1.SummarizeWorkOrdersResponse, error) {
		return s.workOrderQueryClient.SummarizeWorkOrders(ctx, summarizeWorkOrdersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.maintenance.v1.WorkOrderQueryService/SummarizeWorkOrders")
	}
	return &promotionsv1.CountCampaignTotalsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  startProductReviewWorkflowReply.GetResult().GetReferenceIds(),
			Status:        startProductReviewWorkflowReply.GetResult().GetStatus(),
			Details:       summarizeWorkOrdersReply.GetResult().GetDetails(),
			CorrelationId: summarizeWorkOrdersReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// refundWorkflowLazy dials acme.payments.v1.RefundWorkflowService the first time a handler needs it.
func (s *CampaignReportingServer) refundWorkflowLazy() paymentsv1.RefundWorkflowServiceClient {
	s.refundWorkflowOnce.Do(func() { s.refundWorkflowCached = paymentsv1.NewRefundWorkflowServiceClient(s.conn) })
	return s.refundWorkflowCached
}
