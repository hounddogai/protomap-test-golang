package promotions

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PromotionQueryServer implements `acme.promotions.v1.PromotionQueryService`.
// Answers read-only questions about promotions, for screens and reports.
type PromotionQueryServer struct {
	promotionsv1.UnimplementedPromotionQueryServiceServer
	store                                *Store
	log                                  *slog.Logger
	campaignValidationClient             promotionsv1.CampaignValidationServiceClient
	redemptionValidationClient           promotionsv1.RedemptionValidationServiceClient
	conn                                 *grpc.ClientConn
	couponValidationClient               promotionsv1.CouponValidationServiceClient
	searchIndexClient                    searchv1.SearchIndexServiceClient
	campaignCalculationClient            promotionsv1.CampaignCalculationServiceClient
	fraudSignalCalculationPort           fraudSignalCalculationPort
	sellerPayoutScheduleValidationClient sellersv1.SellerPayoutScheduleValidationServiceClient
}

// NewPromotionQueryServer wires a PromotionQueryServer.
func NewPromotionQueryServer(deps platform.Deps) *PromotionQueryServer {
	return &PromotionQueryServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.promotions.v1.PromotionQueryService"),
		campaignValidationClient:             promotionsv1.NewCampaignValidationServiceClient(deps.Conn),
		redemptionValidationClient:           promotionsv1.NewRedemptionValidationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		couponValidationClient:               promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		searchIndexClient:                    searchv1.NewSearchIndexServiceClient(deps.Conn),
		campaignCalculationClient:            promotionsv1.NewCampaignCalculationServiceClient(deps.Conn),
		fraudSignalCalculationPort:           newFraudSignalCalculationPort(deps),
		sellerPayoutScheduleValidationClient: sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn),
	}
}

// LookupPromotion returns a promotion.
func (s *PromotionQueryServer) LookupPromotion(ctx context.Context, req *promotionsv1.LookupPromotionRequest) (*promotionsv1.LookupPromotionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "lookup_promotion"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup promotion", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	previewCampaignRequest := &promotionsv1.PreviewCampaignRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
		Details: &promotionsv1.PreviewCampaignRequest_Details{Contact: &commonv1.ContactInfo{Email: email}},
	}
	previewCampaignReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*promotionsv1.PreviewCampaignResponse, error) {
		return s.campaignValidationClient.PreviewCampaign(ctx, previewCampaignRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignValidationService/PreviewCampaign")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainRedemptionDecisionRequest := newExplainRedemptionDecisionRequest(withExplainRedemptionDecisionContext(req.GetContext()), withExplainRedemptionDecisionRedemption(&promotionsv1.Redemption{
		Subject: &promotionsv1.CouponRef{
			DisplayName: previewCampaignReply.GetCampaign().GetOrganizer().GetDisplayName(),
		},
		Detail: packed,
	}))
	explainRedemptionDecisionReply, err := s.redemptionValidationClient.ExplainRedemptionDecision(ctx, explainRedemptionDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionValidationService/ExplainRedemptionDecision")
	}
	return &promotionsv1.LookupPromotionResponse{
		Result: &commonv1.OperationResult{
			Message: previewCampaignReply.GetOutcome().GetMessage(),
			Status:  explainRedemptionDecisionReply.GetResult().GetStatus(),
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedAt: previewCampaignReply.GetCampaign().GetAudit().GetCreatedAt(),
			},
			PromotionId: req.GetPromotionId(),
		},
	}, nil
}

// BrowsePromotions lists promotions a page at a time.
func (s *PromotionQueryServer) BrowsePromotions(ctx context.Context, req *promotionsv1.BrowsePromotionsRequest) (*promotionsv1.BrowsePromotionsResponse, error) {
	s.log.InfoContext(ctx, "browse promotions", "tenant", req.GetContext().GetTenantId())
	cloneUserRequest := &identityv1.CloneUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	cloneUserReply, err := grpcx.NewClient(s.conn, identityv1.NewUserServiceClient).CloneUser(ctx, cloneUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/CloneUser")
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	normalizeRedemptionRequest := &promotionsv1.NormalizeRedemptionRequest{}
	normalizeRedemptionRequest.Context = req.GetContext()
	normalizeRedemptionRequest.Redemption = &promotionsv1.Redemption{
		Subject: &promotionsv1.CouponRef{
			DisplayName: cloneUserReply.GetUser().GetRelationshipManager().GetDisplayName(),
		},
		Attributes: cloneUserReply.GetUser().GetAttributes(),
		Detail:     packed,
	}
	normalizeRedemptionRequest.Details = &promotionsv1.NormalizeRedemptionRequest_Details{
		Contact: &commonv1.ContactInfo{
			Email: cloneUserReply.GetUser().GetRelationshipManager().GetEmail(),
			Name:  cloneUserReply.GetUser().GetContact().GetName(),
		},
	}
	normalizeRedemptionReply, err := s.redemptionValidationClient.NormalizeRedemption(ctx, normalizeRedemptionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionValidationService/NormalizeRedemption")
	}
	return &promotionsv1.BrowsePromotionsResponse{
		Result: &commonv1.OperationResult{
			Message: cloneUserReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Promotions: []*promotionsv1.Promotion{&promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				DisplayName: normalizeRedemptionReply.GetRedemption().GetSubject().GetDisplayName(),
			},
		}},
	}, nil
}

// CountPromotions counts promotions by status.
func (s *PromotionQueryServer) CountPromotions(ctx context.Context, req *promotionsv1.CountPromotionsRequest) (*promotionsv1.CountPromotionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count promotions", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	searchPaymentsRequest := &paymentsv1.SearchPaymentsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.SearchPaymentsRequest_Filter{
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			Labels:       labels,
			Region:       req.GetFilter().GetRegion(),
		},
		Page: &commonv1.PageRequest{PageToken: req.GetPage().GetPageToken()},
	}
	searchPaymentsReply, err := grpcx.NewClient(s.conn, paymentsv1.NewPaymentServiceClient).SearchPayments(ctx, searchPaymentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentService/SearchPayments")
	}
	return &promotionsv1.CountPromotionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: searchPaymentsReply.GetResult().GetReferenceIds(),
			Message:      searchPaymentsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
	}, nil
}

// ResolvePromotions returns several promotions at once.
func (s *PromotionQueryServer) ResolvePromotions(ctx context.Context, req *promotionsv1.ResolvePromotionsRequest) (*promotionsv1.ResolvePromotionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve promotions", "tenant", req.GetContext().GetTenantId())
	checkCouponEligibilityRequest := &promotionsv1.CheckCouponEligibilityRequest{}
	checkCouponEligibilityRequest.Context = req.GetContext()
	checkCouponEligibilityRequest.Details = &promotionsv1.CheckCouponEligibilityRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	}
	checkCouponEligibilityReply, err := s.couponValidationClient.CheckCouponEligibility(ctx, checkCouponEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/CheckCouponEligibility")
	}
	return &promotionsv1.ResolvePromotionsResponse{
		Promotions: []*promotionsv1.Promotion{&promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedAt: checkCouponEligibilityReply.GetCoupon().GetAudit().GetCreatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportPromotions streams every matching promotion.
func (s *PromotionQueryServer) ExportPromotions(req *promotionsv1.ExportPromotionsRequest, stream grpc.ServerStreamingServer[promotionsv1.ExportPromotionsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export promotions", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*promotionsv1.ExportPromotionsRequest_Filter)
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	updateSearchIndexRequest := &searchv1.UpdateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{Region: cloned.GetRegion()}},
			Owner: &identityv1.UserRef{
				UserId: tenancy.Value(ctx, "user_id"),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	updateSearchIndexReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*searchv1.UpdateSearchIndexResponse, error) {
		return s.searchIndexClient.UpdateSearchIndex(ctx, updateSearchIndexRequest)
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.search.v1.SearchIndexService/UpdateSearchIndex")
	}
	quoteCampaignRequest := &promotionsv1.QuoteCampaignRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{
				DisplayName: updateSearchIndexReply.GetSearchIndex().GetOwner().GetDisplayName(),
				Email:       updateSearchIndexReply.GetSearchIndex().GetOwner().GetEmail(),
			},
		},
	}
	quoteCampaignReply := new(promotionsv1.QuoteCampaignResponse)
	if err := grpcx.Invoke(ctx, s.conn, promotionsv1.CampaignCalculationService_QuoteCampaign_FullMethodName, quoteCampaignRequest, quoteCampaignReply); err != nil {
		return grpcx.Wrap(err, "acme.promotions.v1.CampaignCalculationService/QuoteCampaign")
	}
	if err := stream.Send(&promotionsv1.ExportPromotionsResponse{
		Result: &commonv1.OperationResult{
			Details: updateSearchIndexReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: quoteCampaignReply.GetCampaign().GetAudit().GetUpdatedBy(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// SuggestPromotions lists promotions a page at a time.
func (s *PromotionQueryServer) SuggestPromotions(ctx context.Context, req *promotionsv1.SuggestPromotionsRequest) (*promotionsv1.SuggestPromotionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest promotions", "tenant", req.GetContext().GetTenantId())
	explainCampaignCalculationReply, err := s.campaignCalculationClient.ExplainCampaignCalculation(ctx, &promotionsv1.ExplainCampaignCalculationRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			}},
			Organizer: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit:     &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignCalculationService/ExplainCampaignCalculation")
	}
	return &promotionsv1.SuggestPromotionsResponse{
		Promotions: []*promotionsv1.Promotion{&promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				Email: explainCampaignCalculationReply.GetCampaign().GetOrganizer().GetEmail(),
			},
		}},
		Result: &commonv1.OperationResult{
			Message: explainCampaignCalculationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// PeekPromotion returns a promotion.
func (s *PromotionQueryServer) PeekPromotion(ctx context.Context, req *promotionsv1.PeekPromotionRequest) (*promotionsv1.PeekPromotionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "peek_promotion"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek promotion", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "promotion_id", req.GetPromotionId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	explainFraudSignalCalculationRequest := &fraudv1.ExplainFraudSignalCalculationRequest{
		Context: req.GetContext(),
		Details: &fraudv1.ExplainFraudSignalCalculationRequest_Details{
			Locale:  req.GetContext().GetLocale(),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: tenancy.Value(ctx, "promotion_id"),
			Detail:        packed,
		},
	}
	explainFraudSignalCalculationReply, err := s.fraudSignalCalculationPort.ExplainFraudSignalCalculation(ctx, explainFraudSignalCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/ExplainFraudSignalCalculation")
	}
	validateSellerPayoutScheduleRequest := buildValidateSellerPayoutScheduleRequestForPeekPromotion(req, explainFraudSignalCalculationReply)
	validateSellerPayoutScheduleReply, err := s.sellerPayoutScheduleValidationClient.ValidateSellerPayoutSchedule(ctx, validateSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/ValidateSellerPayoutSchedule")
	}
	_ = validateSellerPayoutScheduleReply
	return &promotionsv1.PeekPromotionResponse{
		Result: &commonv1.OperationResult{
			Details:       explainFraudSignalCalculationReply.GetResult().GetDetails(),
			CorrelationId: explainFraudSignalCalculationReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Promotion: &promotionsv1.Promotion{PromotionId: req.GetPromotionId()},
	}, nil
}
