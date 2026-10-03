package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CouponReportingServer implements `acme.promotions.v1.CouponReportingService`.
// Builds and schedules reports about coupons.
type CouponReportingServer struct {
	*couponReportingHandlers
}

// couponReportingHandlers holds the handlers of CouponReportingServer, with what they use.
type couponReportingHandlers struct {
	promotionsv1.UnimplementedCouponReportingServiceServer
	store                         *Store
	log                           *slog.Logger
	ledgerAccountValidationClient ledgerv1.LedgerAccountValidationServiceClient
	sellerScorecardApprovalClient sellersv1.SellerScorecardApprovalServiceClient
	synonymNotificationClient     searchv1.SynonymNotificationServiceClient
	promotionReportingClient      promotionsv1.PromotionReportingServiceClient
	sessionQueryClient            identityv1.SessionQueryServiceClient
	couponQueryClient             promotionsv1.CouponQueryServiceClient
	productReviewApprovalPort     productReviewApprovalPort
	campaignValidationClient      promotionsv1.CampaignValidationServiceClient
	conn                          *grpc.ClientConn
	clients                       *registry.Registry
	resolveCampaignConflictFn     func(context.Context, *promotionsv1.ResolveCampaignConflictRequest, ...grpc.CallOption) (*promotionsv1.ResolveCampaignConflictResponse, error)
	redemptionValidationClient    promotionsv1.RedemptionValidationServiceClient
}

// NewCouponReportingServer wires a CouponReportingServer.
func NewCouponReportingServer(deps platform.Deps) *CouponReportingServer {
	campaignSync := promotionsv1.NewCampaignSyncServiceClient(deps.Conn)
	return &CouponReportingServer{couponReportingHandlers: &couponReportingHandlers{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.promotions.v1.CouponReportingService"),
		ledgerAccountValidationClient: ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		sellerScorecardApprovalClient: sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		synonymNotificationClient:     searchv1.NewSynonymNotificationServiceClient(deps.Conn),
		promotionReportingClient:      promotionsv1.NewPromotionReportingServiceClient(deps.Conn),
		sessionQueryClient:            identityv1.NewSessionQueryServiceClient(deps.Conn),
		couponQueryClient:             promotionsv1.NewCouponQueryServiceClient(deps.Conn),
		productReviewApprovalPort:     newProductReviewApprovalPort(deps),
		campaignValidationClient:      promotionsv1.NewCampaignValidationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		clients:                       deps.Clients,
		resolveCampaignConflictFn:     campaignSync.ResolveCampaignConflict,
		redemptionValidationClient:    promotionsv1.NewRedemptionValidationServiceClient(deps.Conn),
	}}
}

// GenerateCouponReport generates a coupon.
func (s *couponReportingHandlers) GenerateCouponReport(ctx context.Context, req *promotionsv1.GenerateCouponReportRequest) (*promotionsv1.GenerateCouponReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "generate_coupon_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "generate coupon report", "tenant", req.GetContext().GetTenantId())
	scoreLedgerAccountReply, err := s.ledgerAccountValidationClient.ScoreLedgerAccount(ctx, &ledgerv1.ScoreLedgerAccountRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount")
	}
	recallSellerScorecardRequest := buildRecallSellerScorecardRequestForGenerateCouponReport(req, scoreLedgerAccountReply)
	recallSellerScorecardReply, err := s.sellerScorecardApprovalClient.RecallSellerScorecard(ctx, recallSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/RecallSellerScorecard")
	}
	step := &couponReportingHandlersGenerateCouponReportUnsubscribeSynonymUpdatesStep{request: &searchv1.UnsubscribeSynonymUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: recallSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
		},
	}}
	if err := s.runGenerateCouponReportUnsubscribeSynonymUpdates(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/UnsubscribeSynonymUpdates")
	}
	unsubscribeSynonymUpdatesReply := step.reply
	_ = unsubscribeSynonymUpdatesReply
	return &promotionsv1.GenerateCouponReportResponse{
		Result: &commonv1.OperationResult{
			Status:  scoreLedgerAccountReply.GetResult().GetStatus(),
			Message: recallSellerScorecardReply.GetResult().GetMessage(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// FetchCouponReport returns a coupon.
func (s *couponReportingHandlers) FetchCouponReport(ctx context.Context, req *promotionsv1.FetchCouponReportRequest) (*promotionsv1.FetchCouponReportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "fetch_coupon_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "fetch coupon report", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	countPromotionTotalsRequest := &promotionsv1.CountPromotionTotalsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.CountPromotionTotalsRequest_Filter{
			OwnerId:      req.GetCouponId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	}
	countPromotionTotalsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*promotionsv1.CountPromotionTotalsResponse, error) {
		return s.promotionReportingClient.CountPromotionTotals(ctx, countPromotionTotalsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/CountPromotionTotals")
	}
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	resolveCampaignConflictRequest := &promotionsv1.ResolveCampaignConflictRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			}},
			Organizer: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
			Metadata:  countPromotionTotalsReply.GetResult().GetDetails(),
		},
	}
	resolveCampaignConflictReply, err := resolveCampaignConflictCall(ctx, resolveCampaignConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/ResolveCampaignConflict")
	}
	return &promotionsv1.FetchCouponReportResponse{
		Result: &commonv1.OperationResult{
			Message: countPromotionTotalsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				Etag: resolveCampaignConflictReply.GetCampaign().GetAudit().GetEtag(),
			},
			CouponId: req.GetCouponId(),
		},
	}, nil
}

// ScheduleCouponReport schedules a coupon.
func (s *couponReportingHandlers) ScheduleCouponReport(ctx context.Context, req *promotionsv1.ScheduleCouponReportRequest) (*promotionsv1.ScheduleCouponReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleCouponReport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "schedule_coupon_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule coupon report", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := couponReportingHandlersSuggestSessionsSteps[mode]
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	suggestSessionsRequest := &identityv1.SuggestSessionsRequest{
		Context: req.GetContext(),
		Filter:  &identityv1.SuggestSessionsRequest_Filter{Labels: annotations},
	}
	suggestSessionsReply, err := run(s, ctx, suggestSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SuggestSessions")
	}
	resolveCouponsRequest := &promotionsv1.ResolveCouponsRequest{Context: req.GetContext()}
	resolveCouponsReply, err := grpcx.Call(ctx, s.couponQueryClient.ResolveCoupons, resolveCouponsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponQueryService/ResolveCoupons")
	}
	approveProductReviewRequest := &reviewsv1.ApproveProductReviewRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		ProductReview: &reviewsv1.ProductReview{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Reason: req.GetReason(),
	}
	approveProductReviewReply, err := s.productReviewApprovalPort.ApproveProductReview(ctx, approveProductReviewRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ProductReviewApprovalService/ApproveProductReview")
	}
	_ = approveProductReviewReply
	return &promotionsv1.ScheduleCouponReportResponse{
		Result: &commonv1.OperationResult{
			Message:      suggestSessionsReply.GetResult().GetMessage(),
			Status:       suggestSessionsReply.GetResult().GetStatus(),
			Details:      resolveCouponsReply.GetResult().GetDetails(),
			ReferenceIds: resolveCouponsReply.GetResult().GetReferenceIds(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// StreamCouponMetrics streams the changes of coupons as they happen.
func (s *couponReportingHandlers) StreamCouponMetrics(req *promotionsv1.StreamCouponMetricsRequest, stream grpc.ServerStreamingServer[promotionsv1.StreamCouponMetricsResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream coupon metrics", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := couponReportingHandlersNormalizeCampaignSteps[mode]
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	normalizeCampaignRequest := &promotionsv1.NormalizeCampaignRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Email: req.GetSubscriber().GetEmail(),
					Phone: req.GetSubscriber().GetPhone(),
				},
			}},
			Location: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
	}
	normalizeCampaignReply, err := run(s, ctx, normalizeCampaignRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.promotions.v1.CampaignValidationService/NormalizeCampaign")
	}
	if err := stream.Send(&promotionsv1.StreamCouponMetricsResponse{
		Result: &commonv1.OperationResult{
			Message: normalizeCampaignReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{
				Phone:            req.GetSubscriber().GetPhone(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// ListCouponReports lists coupons a page at a time.
func (s *couponReportingHandlers) ListCouponReports(ctx context.Context, req *promotionsv1.ListCouponReportsRequest) (*promotionsv1.ListCouponReportsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list coupon reports", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	recordUserVersionRequest := &identityv1.RecordUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress:      &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			RelationshipManager: &identityv1.UserRef{Email: cloned.GetActor().GetEmail()},
		},
		UserId: req.GetContext().GetActor().GetUserId(),
	}
	recordUserVersionReply := new(identityv1.RecordUserVersionResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.UserHistoryService_RecordUserVersion_FullMethodName, recordUserVersionRequest, recordUserVersionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RecordUserVersion")
	}
	return &promotionsv1.ListCouponReportsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: recordUserVersionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ShareCouponReport shares a coupon.
func (s *couponReportingHandlers) ShareCouponReport(ctx context.Context, req *promotionsv1.ShareCouponReportRequest) (*promotionsv1.ShareCouponReportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "share_coupon_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share coupon report", "tenant", req.GetContext().GetTenantId())
	userHistory, err := registry.Lookup[identityv1.UserHistoryServiceClient](s.clients, "acme.identity.v1.UserHistoryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetRecipient().GetEmail())
	var envelope promotionsv1.PromotionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	recordUserVersionRequest := &identityv1.RecordUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
		},
		UserId: envelope.GetDisplayName(),
	}
	recordUserVersionReply, err := userHistory.RecordUserVersion(ctx, recordUserVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RecordUserVersion")
	}
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	summarizeCouponsRequest := &promotionsv1.SummarizeCouponsRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		CouponId:    req.GetCouponId(),
		Reason:      req.GetReason(),
		Details: &promotionsv1.SummarizeCouponsRequest_Details{
			Period: recordUserVersionReply.GetUser().GetPreferences().GetQuietHours(),
			Format: envelope2.GetDisplayName(),
		},
	}
	summarizeCouponsReply, err := grpcx.Call(ctx, s.couponQueryClient.SummarizeCoupons, summarizeCouponsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponQueryService/SummarizeCoupons")
	}
	return &promotionsv1.ShareCouponReportResponse{
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: recordUserVersionReply.GetUser().GetContact().GetPreferredChannel(),
				Name:             req.GetDetails().GetRecipient().GetName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: recordUserVersionReply.GetUser().GetAudit().GetCreatedAt(),
			},
			CouponId: req.GetCouponId(),
		},
		Outcome: &promotionsv1.ShareCouponReportResponse_Outcome{Message: summarizeCouponsReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CountCouponTotals counts coupons by status.
func (s *couponReportingHandlers) CountCouponTotals(ctx context.Context, req *promotionsv1.CountCouponTotalsRequest) (*promotionsv1.CountCouponTotalsResponse, error) {
	s.log.InfoContext(ctx, "count coupon totals", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveCampaignConflictRequest := &promotionsv1.ResolveCampaignConflictRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			}},
			Organizer: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit:     &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &promotionsv1.ResolveCampaignConflictRequest_Details{Payload: snapshot},
	}
	resolveCampaignConflictReply, err := s.resolveCampaignConflictFn(ctx, resolveCampaignConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/ResolveCampaignConflict")
	}
	return &promotionsv1.CountCouponTotalsResponse{
		Result: &commonv1.OperationResult{
			Status:  resolveCampaignConflictReply.GetOutcome().GetStatus(),
			Message: resolveCampaignConflictReply.GetResult().GetMessage(),
		},
	}, nil
}

// DeleteCouponReport deletes a coupon.
func (s *couponReportingHandlers) DeleteCouponReport(ctx context.Context, req *promotionsv1.DeleteCouponReportRequest) (*promotionsv1.DeleteCouponReportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "coupon", req.GetCouponId(), "delete_coupon_report"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete coupon report", "tenant", req.GetContext().GetTenantId())
	explainRedemptionDecisionRequest := buildExplainRedemptionDecisionRequestForDeleteCouponReport(req)
	explainRedemptionDecisionReply, err := s.redemptionValidationClient.ExplainRedemptionDecision(ctx, explainRedemptionDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionValidationService/ExplainRedemptionDecision")
	}
	streamPromotionMetricsStream, err := s.promotionReportingClient.StreamPromotionMetrics(ctx, &promotionsv1.StreamPromotionMetricsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Filter: &promotionsv1.StreamPromotionMetricsRequest_Filter{
			CreatedAfter: explainRedemptionDecisionReply.GetOutcome().GetEffectiveAt(),
			OwnerId:      explainRedemptionDecisionReply.GetRedemption().GetCorrelationId(),
		},
		Since: explainRedemptionDecisionReply.GetRedemption().GetOccurredAt(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/StreamPromotionMetrics")
	}
	for {
		received, err := streamPromotionMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/StreamPromotionMetrics")
		}
		_ = received
	}
	return &promotionsv1.DeleteCouponReportResponse{
		Result: &commonv1.OperationResult{
			Status:  explainRedemptionDecisionReply.GetOutcome().GetStatus(),
			Details: explainRedemptionDecisionReply.GetResult().GetDetails(),
		},
	}, nil
}

// couponReportingHandlersGenerateCouponReportUnsubscribeSynonymUpdatesStep carries the request of one step of
// GenerateCouponReport and, once it ran, its reply.
type couponReportingHandlersGenerateCouponReportUnsubscribeSynonymUpdatesStep struct {
	request *searchv1.UnsubscribeSynonymUpdatesRequest
	reply   *searchv1.UnsubscribeSynonymUpdatesResponse
}

func (s *couponReportingHandlers) runGenerateCouponReportUnsubscribeSynonymUpdates(ctx context.Context, step *couponReportingHandlersGenerateCouponReportUnsubscribeSynonymUpdatesStep) error {
	reply, err := s.synonymNotificationClient.UnsubscribeSynonymUpdates(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// couponReportingHandlersSuggestSessionsSteps are the ways to run acme.identity.v1.SessionQueryService/SuggestSessions,
// by name.
var couponReportingHandlersSuggestSessionsSteps = map[string]func(*couponReportingHandlers, context.Context, *identityv1.SuggestSessionsRequest) (*identityv1.SuggestSessionsResponse, error){
	"default": (*couponReportingHandlers).suggestSessionsStep,
	"audited": (*couponReportingHandlers).suggestSessionsAuditedStep,
}

func (s *couponReportingHandlers) suggestSessionsStep(ctx context.Context, request *identityv1.SuggestSessionsRequest) (*identityv1.SuggestSessionsResponse, error) {
	return s.sessionQueryClient.SuggestSessions(ctx, request)
}

func (s *couponReportingHandlers) suggestSessionsAuditedStep(ctx context.Context, request *identityv1.SuggestSessionsRequest) (*identityv1.SuggestSessionsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.SessionQueryService/SuggestSessions")
	return s.sessionQueryClient.SuggestSessions(ctx, request)
}

// couponReportingHandlersNormalizeCampaignSteps are the ways to run
// acme.promotions.v1.CampaignValidationService/NormalizeCampaign, by name.
var couponReportingHandlersNormalizeCampaignSteps = map[string]func(*couponReportingHandlers, context.Context, *promotionsv1.NormalizeCampaignRequest) (*promotionsv1.NormalizeCampaignResponse, error){
	"default": (*couponReportingHandlers).normalizeCampaignStep,
	"audited": (*couponReportingHandlers).normalizeCampaignAuditedStep,
}

func (s *couponReportingHandlers) normalizeCampaignStep(ctx context.Context, request *promotionsv1.NormalizeCampaignRequest) (*promotionsv1.NormalizeCampaignResponse, error) {
	return s.campaignValidationClient.NormalizeCampaign(ctx, request)
}

func (s *couponReportingHandlers) normalizeCampaignAuditedStep(ctx context.Context, request *promotionsv1.NormalizeCampaignRequest) (*promotionsv1.NormalizeCampaignResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.promotions.v1.CampaignValidationService/NormalizeCampaign")
	return s.campaignValidationClient.NormalizeCampaign(ctx, request)
}
