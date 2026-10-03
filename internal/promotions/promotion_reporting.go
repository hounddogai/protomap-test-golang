package promotions

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	driversv1 "github.com/hounddogai/protomap-test-golang/gen/acme/drivers/v1"
	fuelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fuel/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	lineagev1 "github.com/hounddogai/protomap-test-golang/gen/acme/lineage/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PromotionReportingServer implements `acme.promotions.v1.PromotionReportingService`.
// Builds and schedules reports about promotions.
type PromotionReportingServer struct {
	*promotionReportingHandlers
}

// promotionReportingHandlers holds the handlers of PromotionReportingServer, with what they use.
type promotionReportingHandlers struct {
	promotionsv1.UnimplementedPromotionReportingServiceServer
	store                                  *Store
	log                                    *slog.Logger
	generateOrderReportFn                  func(context.Context, *ordersv1.GenerateOrderReportRequest, ...grpc.CallOption) (*ordersv1.GenerateOrderReportResponse, error)
	lineageNodeClient                      lineagev1.LineageNodeServiceClient
	campaignCalculationClient              promotionsv1.CampaignCalculationServiceClient
	fuelCardClient                         fuelv1.FuelCardServiceClient
	stockReservationApprovalClient         inventoryv1.StockReservationApprovalServiceClient
	couponCalculationClient                promotionsv1.CouponCalculationServiceClient
	conn                                   *grpc.ClientConn
	couponValidationPort                   couponValidationPort
	couponValidationClient                 promotionsv1.CouponValidationServiceClient
	stockReservationApprovalClientFallback inventoryv1.StockReservationApprovalServiceClient
}

// NewPromotionReportingServer wires a PromotionReportingServer.
func NewPromotionReportingServer(deps platform.Deps) *PromotionReportingServer {
	orderReporting := ordersv1.NewOrderReportingServiceClient(deps.Conn)
	return &PromotionReportingServer{promotionReportingHandlers: &promotionReportingHandlers{
		store:                                  NewStore(deps.DB),
		log:                                    deps.Logger.With("service", "acme.promotions.v1.PromotionReportingService"),
		generateOrderReportFn:                  orderReporting.GenerateOrderReport,
		lineageNodeClient:                      lineagev1.NewLineageNodeServiceClient(deps.Conn),
		campaignCalculationClient:              promotionsv1.NewCampaignCalculationServiceClient(deps.Conn),
		fuelCardClient:                         fuelv1.NewFuelCardServiceClient(deps.Conn),
		stockReservationApprovalClient:         inventoryv1.NewStockReservationApprovalServiceClient(deps.Conn),
		couponCalculationClient:                promotionsv1.NewCouponCalculationServiceClient(deps.Conn),
		conn:                                   deps.Conn,
		couponValidationPort:                   newCouponValidationPort(deps),
		couponValidationClient:                 promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		stockReservationApprovalClientFallback: inventoryv1.NewStockReservationApprovalServiceClient(deps.Conn),
	}}
}

// GeneratePromotionReport generates a promotion.
func (s *promotionReportingHandlers) GeneratePromotionReport(ctx context.Context, req *promotionsv1.GeneratePromotionReportRequest) (*promotionsv1.GeneratePromotionReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "generate_promotion_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate promotion report", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Cron string }
	if record := req.GetDetails(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Cron"); err != nil {
			s.log.WarnContext(ctx, "copy cron", "error", err)
		}
	}
	generateOrderReportRequest := &ordersv1.GenerateOrderReportRequest{
		Context: req.GetContext(),
		Details: &ordersv1.GenerateOrderReportRequest_Details{
			Cron:   holder.Cron,
			Format: req.GetDetails().GetFormat(),
		},
	}
	generateOrderReportReply, err := s.generateOrderReportFn(ctx, generateOrderReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/GenerateOrderReport")
	}
	createLineageNodeReply, err := s.lineageNodeClient.CreateLineageNode(ctx, &lineagev1.CreateLineageNodeRequest{
		Context: req.GetContext(),
		LineageNode: &lineagev1.LineageNode{
			Children: []*lineagev1.LineageNode{&lineagev1.LineageNode{
				Children: []*lineagev1.LineageNode{&lineagev1.LineageNode{
					Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
				}},
			}},
			Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lineage.v1.LineageNodeService/CreateLineageNode")
	}
	compareCampaignOptionsRequest := &promotionsv1.CompareCampaignOptionsRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				CreatedBy: createLineageNodeReply.GetLineageNode().GetAudit().GetCreatedBy(),
			},
		},
		Details: &promotionsv1.CompareCampaignOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: createLineageNodeReply.GetLineageNode().GetLocation().GetLines(),
			},
		},
	}
	compareCampaignOptionsReply, err := promotionsv1.CampaignCalculationServiceClient.CompareCampaignOptions(s.campaignCalculationClient, ctx, compareCampaignOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignCalculationService/CompareCampaignOptions")
	}
	createFuelCardRequest := &fuelv1.CreateFuelCardRequest{
		Context: req.GetContext(),
		FuelCard: &fuelv1.FuelCard{
			Audit: &commonv1.AuditInfo{
				Etag: compareCampaignOptionsReply.GetCampaign().GetAudit().GetEtag(),
			},
			Holder: &driversv1.DriverRef{
				DisplayName: createLineageNodeReply.GetLineageNode().GetManager().GetDisplayName(),
			},
		},
	}
	createFuelCardReply, err := grpcx.Call(ctx, s.fuelCardClient.CreateFuelCard, createFuelCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fuel.v1.FuelCardService/CreateFuelCard")
	}
	_ = createFuelCardReply
	return &promotionsv1.GeneratePromotionReportResponse{
		ReportId: generateOrderReportReply.GetReportId(),
		Result: &commonv1.OperationResult{
			CorrelationId: generateOrderReportReply.GetResult().GetCorrelationId(),
			ReferenceIds:  createLineageNodeReply.GetResult().GetReferenceIds(),
			Message:       createLineageNodeReply.GetResult().GetMessage(),
			Status:        compareCampaignOptionsReply.GetResult().GetStatus(),
			Details:       compareCampaignOptionsReply.GetResult().GetDetails(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchPromotionReport returns a promotion.
func (s *promotionReportingHandlers) FetchPromotionReport(ctx context.Context, req *promotionsv1.FetchPromotionReportRequest) (*promotionsv1.FetchPromotionReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchPromotionReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "fetch_promotion_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch promotion report", "tenant", req.GetContext().GetTenantId())
	delegateStockReservationApprovalRequest := buildDelegateStockReservationApprovalRequestForFetchPromotionReport(req)
	delegateStockReservationApprovalReply, err := s.stockReservationApprovalClient.DelegateStockReservationApproval(ctx, delegateStockReservationApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationApprovalService/DelegateStockReservationApproval")
	}
	return &promotionsv1.FetchPromotionReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: delegateStockReservationApprovalReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Promotion: &promotionsv1.Promotion{PromotionId: req.GetPromotionId()},
	}, nil
}

// SchedulePromotionReport schedules a promotion.
func (s *promotionReportingHandlers) SchedulePromotionReport(ctx context.Context, req *promotionsv1.SchedulePromotionReportRequest) (*promotionsv1.SchedulePromotionReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "schedule_promotion_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule promotion report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	calculateCouponRequest := &promotionsv1.CalculateCouponRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
	}
	var calculateCouponReply *promotionsv1.CalculateCouponResponse
	if err := pipeline.Run(ctx, "acme.promotions.v1.CouponCalculationService/CalculateCoupon", func(ctx context.Context) error {
		answer, err := s.couponCalculationClient.CalculateCoupon(ctx, calculateCouponRequest)
		calculateCouponReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/CalculateCoupon")
	}
	return &promotionsv1.SchedulePromotionReportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: calculateCouponReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamPromotionMetrics streams the changes of promotions as they happen.
func (s *promotionReportingHandlers) StreamPromotionMetrics(req *promotionsv1.StreamPromotionMetricsRequest, stream grpc.ServerStreamingServer[promotionsv1.StreamPromotionMetricsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamPromotionMetrics", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream promotion metrics", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetSubscriber().GetEmail())
	cloned := proto.Clone(req.GetFilter()).(*promotionsv1.StreamPromotionMetricsRequest_Filter)
	checkCampaignEligibilityRequest := &promotionsv1.CheckCampaignEligibilityRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{PreferredChannel: req.GetSubscriber().GetPreferredChannel()},
			}},
			Location: &commonv1.PostalAddress{Region: cloned.GetRegion()},
		},
	}
	checkCampaignEligibilityReply, err := grpcx.NewClient(s.conn, promotionsv1.NewCampaignValidationServiceClient).CheckCampaignEligibility(ctx, checkCampaignEligibilityRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.promotions.v1.CampaignValidationService/CheckCampaignEligibility")
	}
	scoreCouponRequest := &promotionsv1.ScoreCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkCampaignEligibilityReply.GetCampaign().GetAudit().GetCreatedBy(),
				UpdatedAt: checkCampaignEligibilityReply.GetCampaign().GetAudit().GetUpdatedAt(),
				UpdatedBy: checkCampaignEligibilityReply.GetCampaign().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &promotionsv1.ScoreCouponRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: checkCampaignEligibilityReply.GetCampaign().GetLocation().GetLines(),
			},
		},
	}
	scoreCouponReply, err := s.couponValidationPort.ScoreCoupon(ctx, scoreCouponRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ScoreCoupon")
	}
	if err := stream.Send(&promotionsv1.StreamPromotionMetricsResponse{
		Result: &commonv1.OperationResult{
			Status:        checkCampaignEligibilityReply.GetResult().GetStatus(),
			CorrelationId: scoreCouponReply.GetResult().GetCorrelationId(),
		},
		Promotion: &promotionsv1.Promotion{
			PromotionId: scoreCouponReply.GetCoupon().GetHolder().GetPromotionId(),
			Owner:       &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SharePromotionReport shares a promotion.
func (s *promotionReportingHandlers) SharePromotionReport(ctx context.Context, req *promotionsv1.SharePromotionReportRequest) (*promotionsv1.SharePromotionReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "share_promotion_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share promotion report", "tenant", req.GetContext().GetTenantId())
	var envelope promotionsv1.PromotionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintCouponsRequest := &promotionsv1.LintCouponsRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetRecipient().GetPhone(),
				Name:  req.GetDetails().GetRecipient().GetName(),
			},
		},
		CouponId: envelope.GetDisplayName(),
	}
	send := s.lintCouponsSender(lintCouponsRequest)
	lintCouponsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/LintCoupons")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	shareOrderReportRequest := &ordersv1.ShareOrderReportRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ShareOrderReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: req.GetDetails().GetRecipient().GetEmail(),
				Phone: req.GetDetails().GetRecipient().GetPhone(),
				Name:  req.GetDetails().GetRecipient().GetName(),
			},
			Payload: packed,
		},
		OrderId: envelope2.GetDisplayName(),
	}
	shareOrderReportReply := new(ordersv1.ShareOrderReportResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.orders.v1.OrderReportingService/ShareOrderReport", shareOrderReportRequest, shareOrderReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/ShareOrderReport")
	}
	routes := map[string]func(context.Context, *inventoryv1.DelegateStockReservationApprovalRequest, ...grpc.CallOption) (*inventoryv1.DelegateStockReservationApprovalResponse, error){
		"primary":  s.stockReservationApprovalClient.DelegateStockReservationApproval,
		"fallback": s.stockReservationApprovalClientFallback.DelegateStockReservationApproval,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	delegateStockReservationApprovalRequest := &inventoryv1.DelegateStockReservationApprovalRequest{
		Context: req.GetContext(),
		StockReservation: &inventoryv1.StockReservation{
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact: &commonv1.ContactInfo{
					Phone: lintCouponsReply.GetCoupon().GetContact().GetPhone(),
					Email: lintCouponsReply.GetCoupon().GetContact().GetEmail(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: shareOrderReportReply.GetOrder().GetAudit().GetUpdatedAt(),
			},
		},
	}
	delegateStockReservationApprovalReply, err := route(ctx, delegateStockReservationApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationApprovalService/DelegateStockReservationApproval")
	}
	simulateCouponRequest := &promotionsv1.SimulateCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: lintCouponsReply.GetCoupon().GetContact().GetPreferredChannel(),
			},
			Holder: &promotionsv1.PromotionRef{
				DisplayName: delegateStockReservationApprovalReply.GetStockReservation().GetOrganizer().GetDisplayName(),
			},
			Funding: &promotionsv1.Coupon_Bank{Bank: &promotionsv1.CouponBankFunding{Bic: shareOrderReportReply.GetOrder().GetBank().GetBic()}},
		},
	}
	simulateCouponReply, err := grpcx.Call(ctx, s.couponCalculationClient.SimulateCoupon, simulateCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/SimulateCoupon")
	}
	return &promotionsv1.SharePromotionReportResponse{
		Promotion: &promotionsv1.Promotion{
			PromotionId: lintCouponsReply.GetCoupon().GetHolder().GetPromotionId(),
			Version:     lintCouponsReply.GetCoupon().GetHolder().GetVersion(),
			Owner: &identityv1.UserRef{
				UserId: delegateStockReservationApprovalReply.GetStockReservation().GetOrganizer().GetUserId(),
				Email:  delegateStockReservationApprovalReply.GetStockReservation().GetOrganizer().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      simulateCouponReply.GetCoupon().GetAudit().GetEtag(),
				UpdatedBy: simulateCouponReply.GetCoupon().GetAudit().GetUpdatedBy(),
			},
		},
		Result: &commonv1.OperationResult{
			Message: shareOrderReportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// CountPromotionTotals counts promotions by status.
func (s *promotionReportingHandlers) CountPromotionTotals(ctx context.Context, req *promotionsv1.CountPromotionTotalsRequest) (*promotionsv1.CountPromotionTotalsResponse, error) {
	s.log.InfoContext(ctx, "count promotion totals", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	previewCouponRequest := &promotionsv1.PreviewCouponRequest{
		Context: req.GetContext(),
		Coupon:  &promotionsv1.Coupon{Labels: labels},
		Details: &promotionsv1.PreviewCouponRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
	}
	previewCouponReply, err := previewCouponCall(ctx, previewCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/PreviewCoupon")
	}
	return &promotionsv1.CountPromotionTotalsResponse{
		Result: &commonv1.OperationResult{Status: previewCouponReply.GetOutcome().GetStatus()},
	}, nil
}

// lintCouponsSender returns a function that sends one request to
// acme.promotions.v1.CouponValidationService/LintCoupons.
func (s *promotionReportingHandlers) lintCouponsSender(request *promotionsv1.LintCouponsRequest) func(context.Context) (*promotionsv1.LintCouponsResponse, error) {
	return func(ctx context.Context) (*promotionsv1.LintCouponsResponse, error) {
		return s.couponValidationClient.LintCoupons(ctx, request)
	}
}
