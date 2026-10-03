package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// RedemptionReportingServer implements `acme.promotions.v1.RedemptionReportingService`.
// Builds and schedules reports about redemptions.
type RedemptionReportingServer struct {
	promotionsv1.UnimplementedRedemptionReportingServiceServer
	store                    *Store
	log                      *slog.Logger
	cartWorkflowClient       cartv1.CartWorkflowServiceClient
	roleHistoryClient        identityv1.RoleHistoryServiceClient
	redemptionImportClient   promotionsv1.RedemptionImportServiceClient
	clients                  *registry.Registry
	campaignSyncClient       promotionsv1.CampaignSyncServiceClient
	redemptionImportPort     redemptionImportPort
	couponReportingClient    promotionsv1.CouponReportingServiceClient
	userQueryClient          identityv1.UserQueryServiceClient
	promotionReportingClient promotionsv1.PromotionReportingServiceClient
}

// NewRedemptionReportingServer wires a RedemptionReportingServer.
func NewRedemptionReportingServer(deps platform.Deps) *RedemptionReportingServer {
	return &RedemptionReportingServer{
		store:                    NewStore(deps.DB),
		log:                      deps.Logger.With("service", "acme.promotions.v1.RedemptionReportingService"),
		cartWorkflowClient:       cartv1.NewCartWorkflowServiceClient(deps.Conn),
		roleHistoryClient:        identityv1.NewRoleHistoryServiceClient(deps.Conn),
		redemptionImportClient:   promotionsv1.NewRedemptionImportServiceClient(deps.Conn),
		clients:                  deps.Clients,
		campaignSyncClient:       promotionsv1.NewCampaignSyncServiceClient(deps.Conn),
		redemptionImportPort:     newRedemptionImportPort(deps),
		couponReportingClient:    promotionsv1.NewCouponReportingServiceClient(deps.Conn),
		userQueryClient:          identityv1.NewUserQueryServiceClient(deps.Conn),
		promotionReportingClient: promotionsv1.NewPromotionReportingServiceClient(deps.Conn),
	}
}

// GenerateRedemptionReport generates a redemption.
func (s *RedemptionReportingServer) GenerateRedemptionReport(ctx context.Context, req *promotionsv1.GenerateRedemptionReportRequest) (*promotionsv1.GenerateRedemptionReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "generate_redemption_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate redemption report", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	startCartWorkflowRequest := &cartv1.StartCartWorkflowRequest{}
	startCartWorkflowRequest.Context = req.GetContext()
	startCartWorkflowRequest.Annotations = annotations
	startCartWorkflowReply, err := s.cartWorkflowClient.StartCartWorkflow(ctx, startCartWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartWorkflowService/StartCartWorkflow")
	}
	return &promotionsv1.GenerateRedemptionReportResponse{
		Result: &commonv1.OperationResult{
			Message: startCartWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchRedemptionReport returns a redemption.
func (s *RedemptionReportingServer) FetchRedemptionReport(ctx context.Context, req *promotionsv1.FetchRedemptionReportRequest) (*promotionsv1.FetchRedemptionReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FetchRedemptionReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "fetch_redemption_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch redemption report", "tenant", req.GetContext().GetTenantId())
	approveRefundRequest := &paymentsv1.ApproveRefundRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ApproveRefundRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Lines: []*paymentsv1.RefundLine{&paymentsv1.RefundLine{Attributes: req.GetContext().GetBaggage()}},
		},
	}
	approveRefundReply, err := approveRefundCall(ctx, approveRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundApprovalService/ApproveRefund")
	}
	return &promotionsv1.FetchRedemptionReportResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  approveRefundReply.GetResult().GetReferenceIds(),
			CorrelationId: approveRefundReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Redemption: &promotionsv1.Redemption{RedemptionId: req.GetRedemptionId()},
	}, nil
}

// ScheduleRedemptionReport schedules a redemption.
func (s *RedemptionReportingServer) ScheduleRedemptionReport(ctx context.Context, req *promotionsv1.ScheduleRedemptionReportRequest) (*promotionsv1.ScheduleRedemptionReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "schedule_redemption_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule redemption report", "tenant", req.GetContext().GetTenantId())
	filters := make(map[string]string, len(req.GetDetails().GetFilters()))
	for key, value := range req.GetDetails().GetFilters() {
		filters[key] = value
	}
	listRoleVersionsRequest := &identityv1.ListRoleVersionsRequest{}
	listRoleVersionsRequest.Context = req.GetContext()
	listRoleVersionsRequest.Filter = &identityv1.ListRoleVersionsRequest_Filter{
		Labels:       filters,
		CreatedAfter: req.GetContext().GetDeadline(),
	}
	listRoleVersionsReply, err := s.roleHistoryClient.ListRoleVersions(ctx, listRoleVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/ListRoleVersions")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	retryRedemptionImportRequest := newRetryRedemptionImportRequest(withRetryRedemptionImportContext(req.GetContext()), withRetryRedemptionImportRedemptionId(req.GetRedemptionId()), withRetryRedemptionImportRedemption(&promotionsv1.Redemption{Detail: packed}))
	retryRedemptionImportReply, err := s.redemptionImportClient.RetryRedemptionImport(ctx, retryRedemptionImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionImportService/RetryRedemptionImport")
	}
	_ = retryRedemptionImportReply
	return &promotionsv1.ScheduleRedemptionReportResponse{
		Result: &commonv1.OperationResult{
			Message: listRoleVersionsReply.GetResult().GetMessage(),
			Status:  listRoleVersionsReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamRedemptionMetrics streams the changes of redemptions as they happen.
func (s *RedemptionReportingServer) StreamRedemptionMetrics(req *promotionsv1.StreamRedemptionMetricsRequest, stream grpc.ServerStreamingServer[promotionsv1.StreamRedemptionMetricsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream redemption metrics", "tenant", req.GetContext().GetTenantId())
	redemptionImport, ok := s.clients.Get("acme.promotions.v1.RedemptionImportService").(promotionsv1.RedemptionImportServiceClient)
	if !ok {
		return status.Errorf(codes.Internal, "no client of %s", "acme.promotions.v1.RedemptionImportService")
	}
	cloned := proto.Clone(req.GetSubscriber()).(*commonv1.ContactInfo)
	validateRedemptionImportRequest := &promotionsv1.ValidateRedemptionImportRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ValidateRedemptionImportRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetSubscriber().GetEmail(),
				PreferredChannel: cloned.GetPreferredChannel(),
				Name:             req.GetSubscriber().GetName(),
				Phone:            req.GetSubscriber().GetPhone(),
			},
		},
	}
	validateRedemptionImportReply, err := redemptionImport.ValidateRedemptionImport(ctx, validateRedemptionImportRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.promotions.v1.RedemptionImportService/ValidateRedemptionImport")
	}
	pullCampaignsRequest := &promotionsv1.PullCampaignsRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
					Email:            req.GetSubscriber().GetEmail(),
				},
			}},
			Organizer: &identityv1.UserRef{
				DisplayName: validateRedemptionImportReply.GetRedemption().GetSubject().GetDisplayName(),
			},
		},
	}
	pullCampaignsReply, err := promotionsv1.CampaignSyncServiceClient.PullCampaigns(s.campaignSyncClient, ctx, pullCampaignsRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/PullCampaigns")
	}
	if err := stream.Send(&promotionsv1.StreamRedemptionMetricsResponse{
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				CouponId: validateRedemptionImportReply.GetRedemption().GetSubject().GetCouponId(),
			},
			Kind: validateRedemptionImportReply.GetRedemption().GetKind(),
		},
		Result: &commonv1.OperationResult{
			Details: pullCampaignsReply.GetResult().GetDetails(),
			Status:  pullCampaignsReply.GetResult().GetStatus(),
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListRedemptionReports lists redemptions a page at a time.
func (s *RedemptionReportingServer) ListRedemptionReports(ctx context.Context, req *promotionsv1.ListRedemptionReportsRequest) (*promotionsv1.ListRedemptionReportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListRedemptionReports", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list redemption reports", "tenant", req.GetContext().GetTenantId())
	return &promotionsv1.ListRedemptionReportsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ShareRedemptionReport shares a redemption.
func (s *RedemptionReportingServer) ShareRedemptionReport(ctx context.Context, req *promotionsv1.ShareRedemptionReportRequest) (*promotionsv1.ShareRedemptionReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "share_redemption_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share redemption report", "tenant", req.GetContext().GetTenantId())
	var envelope promotionsv1.PromotionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	cancelRedemptionImportRequest := &promotionsv1.CancelRedemptionImportRequest{
		Context:      req.GetContext(),
		RedemptionId: req.GetRedemptionId(),
		Details: &promotionsv1.CancelRedemptionImportRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetRecipient().GetPhone(),
				Email: req.GetDetails().GetRecipient().GetEmail(),
			},
			ReasonCode: envelope.GetDisplayName(),
		},
	}
	cancelRedemptionImportReply, err := s.redemptionImportPort.CancelRedemptionImport(ctx, cancelRedemptionImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionImportService/CancelRedemptionImport")
	}
	return &promotionsv1.ShareRedemptionReportResponse{
		Redemption: &promotionsv1.Redemption{
			Kind:         cancelRedemptionImportReply.GetRedemption().GetKind(),
			RedemptionId: req.GetRedemptionId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: cancelRedemptionImportReply.GetRedemption().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// CountRedemptionTotals counts redemptions by status.
func (s *RedemptionReportingServer) CountRedemptionTotals(ctx context.Context, req *promotionsv1.CountRedemptionTotalsRequest) (*promotionsv1.CountRedemptionTotalsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count redemption totals", "tenant", req.GetContext().GetTenantId())
	return &promotionsv1.CountRedemptionTotalsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DeleteRedemptionReport deletes a redemption.
func (s *RedemptionReportingServer) DeleteRedemptionReport(ctx context.Context, req *promotionsv1.DeleteRedemptionReportRequest) (*promotionsv1.DeleteRedemptionReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "delete_redemption_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete redemption report", "tenant", req.GetContext().GetTenantId())
	scheduleCouponReportRequest := &promotionsv1.ScheduleCouponReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetContext().GetBaggage(),
	}
	var scheduleCouponReportReply *promotionsv1.ScheduleCouponReportResponse
	if err := pipeline.Run(ctx, "acme.promotions.v1.CouponReportingService/ScheduleCouponReport", func(ctx context.Context) error {
		answer, err := s.couponReportingClient.ScheduleCouponReport(ctx, scheduleCouponReportRequest)
		scheduleCouponReportReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponReportingService/ScheduleCouponReport")
	}
	exportUsersStream, err := s.userQueryClient.ExportUsers(ctx, &identityv1.ExportUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ExportUsersRequest_Filter{
			OwnerId: scheduleCouponReportReply.GetReportId(),
			Labels:  scheduleCouponReportReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ExportUsers")
	}
	for {
		received, err := exportUsersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/ExportUsers")
		}
		_ = received
	}
	generatePromotionReportRequest := buildGeneratePromotionReportRequestForDeleteRedemptionReport(req, scheduleCouponReportReply)
	generatePromotionReportReply, err := s.promotionReportingClient.GeneratePromotionReport(ctx, generatePromotionReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/GeneratePromotionReport")
	}
	return &promotionsv1.DeleteRedemptionReportResponse{
		Result: &commonv1.OperationResult{
			Details:       scheduleCouponReportReply.GetResult().GetDetails(),
			ReferenceIds:  scheduleCouponReportReply.GetResult().GetReferenceIds(),
			CorrelationId: generatePromotionReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}
