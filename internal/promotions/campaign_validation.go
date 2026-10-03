package promotions

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	accountingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/accounting/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CampaignValidationServer implements `acme.promotions.v1.CampaignValidationService`.
// Checks campaigns against the rules before anything acts on them.
type CampaignValidationServer struct {
	*campaignValidationHandlers
}

// campaignValidationHandlers holds the handlers of CampaignValidationServer, with what they use.
type campaignValidationHandlers struct {
	promotionsv1.UnimplementedCampaignValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	couponValidationClient           promotionsv1.CouponValidationServiceClient
	meteredCouponValidation          meteredCouponValidation
	tierValidationClient             loyaltyv1.TierValidationServiceClient
	sellerListingCalculationClient   sellersv1.SellerListingCalculationServiceClient
	conn                             *grpc.ClientConn
	promotionValidationClient        promotionsv1.PromotionValidationServiceClient
	orderQueryClient                 ordersv1.OrderQueryServiceClient
	restockDecisionCalculationClient returnsv1.RestockDecisionCalculationServiceClient
	searchQueryHistoryClient         searchv1.SearchQueryHistoryServiceClient
	refundValidationClient           paymentsv1.RefundValidationServiceClient
	promotionCalculationClient       promotionsv1.PromotionCalculationServiceClient
	clients                          *registry.Registry
	chartOfAccountsClient            accountingv1.ChartOfAccountsServiceClient
}

// NewCampaignValidationServer wires a CampaignValidationServer.
func NewCampaignValidationServer(deps platform.Deps) *CampaignValidationServer {
	return &CampaignValidationServer{campaignValidationHandlers: &campaignValidationHandlers{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.promotions.v1.CampaignValidationService"),
		couponValidationClient:           promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		meteredCouponValidation:          meteredCouponValidation{CouponValidationServiceClient: promotionsv1.NewCouponValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		tierValidationClient:             loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		sellerListingCalculationClient:   sellersv1.NewSellerListingCalculationServiceClient(deps.Conn),
		conn:                             deps.Conn,
		promotionValidationClient:        promotionsv1.NewPromotionValidationServiceClient(deps.Conn),
		orderQueryClient:                 ordersv1.NewOrderQueryServiceClient(deps.Conn),
		restockDecisionCalculationClient: returnsv1.NewRestockDecisionCalculationServiceClient(deps.Conn),
		searchQueryHistoryClient:         searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		refundValidationClient:           paymentsv1.NewRefundValidationServiceClient(deps.Conn),
		promotionCalculationClient:       promotionsv1.NewPromotionCalculationServiceClient(deps.Conn),
		clients:                          deps.Clients,
		chartOfAccountsClient:            accountingv1.NewChartOfAccountsServiceClient(deps.Conn),
	}}
}

// ValidateCampaign validates a campaign.
func (s *campaignValidationHandlers) ValidateCampaign(ctx context.Context, req *promotionsv1.ValidateCampaignRequest) (*promotionsv1.ValidateCampaignResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "validate campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	for _, participant := range req.GetCampaign().GetParticipants() {
		if _, err := s.couponValidationClient.LintCoupons(ctx, &promotionsv1.LintCouponsRequest{
			Context: req.GetContext(),
			Details: &promotionsv1.LintCouponsRequest_Details{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: participant.GetContact().GetPreferredChannel(),
					Phone:            participant.GetContact().GetPhone(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/LintCoupons")
		}
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetContact().GetPreferredChannel())
	explainCouponDecisionRequest := &promotionsv1.ExplainCouponDecisionRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
		},
	}
	explainCouponDecisionReply, err := s.meteredCouponValidation.ExplainCouponDecision(ctx, explainCouponDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ExplainCouponDecision")
	}
	jobs := make(chan *promotionsv1.CampaignParticipant)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for participant2 := range jobs {
				if _, err := s.tierValidationClient.CheckTierEligibility(workCtx, &loyaltyv1.CheckTierEligibilityRequest{
					Context: req.GetContext(),
					Tier: &loyaltyv1.Tier{
						Owner: &identityv1.UserRef{Email: participant2.GetContact().GetEmail()},
					},
					Details: &loyaltyv1.CheckTierEligibilityRequest_Details{Contact: participant2.GetContact()},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetCampaign().GetParticipants() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/CheckTierEligibility")
	}
	estimateSellerListingRequest := &sellersv1.EstimateSellerListingRequest{}
	estimateSellerListingRequest.Context = req.GetContext()
	estimateSellerListingRequest.Details = &sellersv1.EstimateSellerListingRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: req.GetCampaign().GetLocation().GetCountryCode(),
		},
	}
	estimateSellerListingReply, err := s.sellerListingCalculationClient.EstimateSellerListing(ctx, estimateSellerListingRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingCalculationService/EstimateSellerListing")
	}
	return &promotionsv1.ValidateCampaignResponse{
		Outcome: &promotionsv1.ValidateCampaignResponse_Outcome{
			Message: explainCouponDecisionReply.GetOutcome().GetMessage(),
			Status:  estimateSellerListingReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: explainCouponDecisionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Title:      req.GetCampaign().GetTitle(),
		},
	}, nil
}

// PreviewCampaign previews a campaign.
func (s *campaignValidationHandlers) PreviewCampaign(ctx context.Context, req *promotionsv1.PreviewCampaignRequest) (*promotionsv1.PreviewCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewCampaign", "error", err)
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "preview campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCampaign()).(*promotionsv1.Campaign)
	normalizePromotionRequest := &promotionsv1.NormalizePromotionRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.NormalizePromotionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned.GetOrganizer().GetEmail()},
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{Etag: req.GetCampaign().GetAudit().GetEtag()},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCampaign().GetOrganizer().GetDisplayName(),
				UserId:      req.GetCampaign().GetOrganizer().GetUserId(),
			},
		},
	}
	normalizePromotionReply := new(promotionsv1.NormalizePromotionResponse)
	if err := s.conn.Invoke(ctx, promotionsPromotionValidationServiceNormalizePromotionMethod, normalizePromotionRequest, normalizePromotionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/NormalizePromotion")
	}
	previewPromotionRequest := buildPreviewPromotionRequestForPreviewCampaign(req, normalizePromotionReply)
	previewPromotionReply, err := s.promotionValidationClient.PreviewPromotion(ctx, previewPromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/PreviewPromotion")
	}
	return &promotionsv1.PreviewCampaignResponse{
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				Etag: normalizePromotionReply.GetPromotion().GetAudit().GetEtag(),
			},
			CampaignId: req.GetCampaignId(),
			Metadata:   req.GetCampaign().GetMetadata(),
		},
		Outcome: &promotionsv1.PreviewCampaignResponse_Outcome{
			NextSteps: previewPromotionReply.GetOutcome().GetNextSteps(),
			Message:   previewPromotionReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckCampaignEligibility checks a campaign.
func (s *campaignValidationHandlers) CheckCampaignEligibility(ctx context.Context, req *promotionsv1.CheckCampaignEligibilityRequest) (*promotionsv1.CheckCampaignEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "check campaign eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*ordersv1.CountOrdersResponse, len(req.GetCampaign().GetParticipants()))
	for i, participant := range req.GetCampaign().GetParticipants() {
		group.Go(func() error {
			reply, err := s.orderQueryClient.CountOrders(groupCtx, &ordersv1.CountOrdersRequest{
				Context: req.GetContext(),
				Filter: &ordersv1.CountOrdersRequest_Filter{
					Region:       req.GetCampaign().GetLocation().GetRegion(),
					OwnerId:      participant.GetParticipantId(),
					CreatedAfter: req.GetCampaign().GetAudit().GetCreatedAt(),
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderQueryService/CountOrders")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	group2, groupCtx2 := errgroup.WithContext(ctx)
	group2.SetLimit(8)
	replies2 := make([]*returnsv1.SimulateRestockDecisionResponse, len(req.GetCampaign().GetParticipants()))
	for i2, participant2 := range req.GetCampaign().GetParticipants() {
		group2.Go(func() error {
			reply, err := s.restockDecisionCalculationClient.SimulateRestockDecision(groupCtx2, &returnsv1.SimulateRestockDecisionRequest{
				Context: req.GetContext(),
				RestockDecision: &returnsv1.RestockDecision{
					Owner: &identityv1.UserRef{
						Email:       participant2.GetContact().GetEmail(),
						DisplayName: req.GetCampaign().GetOrganizer().GetDisplayName(),
					},
				},
				Details: &returnsv1.SimulateRestockDecisionRequest_Details{
					Destination: &commonv1.PostalAddress{PostalCode: req.GetCampaign().GetLocation().GetPostalCode()},
				},
			})
			if err != nil {
				return err
			}
			replies2[i2] = reply
			return nil
		})
	}
	if err := group2.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/SimulateRestockDecision")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies2))
	return &promotionsv1.CheckCampaignEligibilityResponse{
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Metadata:   req.GetCampaign().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeCampaign normalizes a campaign.
func (s *campaignValidationHandlers) NormalizeCampaign(ctx context.Context, req *promotionsv1.NormalizeCampaignRequest) (*promotionsv1.NormalizeCampaignResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "normalize campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*promotionsv1.NormalizeCampaignRequest_Details)
	campaignCache := cache.New[string, *promotionsv1.Campaign](1)
	campaignCache.Put(req.GetContext().GetTraceId(), req.GetCampaign())
	cachedCampaign, _ := campaignCache.Get(req.GetContext().GetTraceId())
	normalizePromotionRequest := &promotionsv1.NormalizePromotionRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.NormalizePromotionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: req.GetDetails().GetContact().GetEmail(),
				Phone: cloned.GetContact().GetPhone(),
			},
		},
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				UserId:      req.GetCampaign().GetOrganizer().GetUserId(),
				DisplayName: cachedCampaign.GetOrganizer().GetDisplayName(),
			},
		},
	}
	normalizePromotionReply := new(promotionsv1.NormalizePromotionResponse)
	if err := s.conn.Invoke(ctx, promotionsPromotionValidationServiceNormalizePromotionMethod, normalizePromotionRequest, normalizePromotionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/NormalizePromotion")
	}
	ctx = tenancy.With(ctx, "country_code", req.GetCampaign().GetLocation().GetCountryCode())
	scorePromotionRequest := &promotionsv1.ScorePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{UserId: req.GetCampaign().GetOrganizer().GetUserId()},
			Action: &promotionsv1.Promotion_Route{Route: &promotionsv1.PromotionRouteAction{
				Weight: normalizePromotionReply.GetPromotion().GetRoute().GetWeight(),
			}},
		},
		Details: &promotionsv1.ScorePromotionRequest_Details{
			Destination: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
		},
	}
	type scorePromotionResult struct {
		reply *promotionsv1.ScorePromotionResponse
		err   error
	}
	scorePromotionResults := make(chan scorePromotionResult, 1)
	go func() {
		reply, err := s.promotionValidationClient.ScorePromotion(ctx, scorePromotionRequest)
		scorePromotionResults <- scorePromotionResult{reply, err}
	}()
	var scorePromotionReply *promotionsv1.ScorePromotionResponse
	select {
	case result := <-scorePromotionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.promotions.v1.PromotionValidationService/ScorePromotion")
		}
		scorePromotionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	estimateRewardRequest := &loyaltyv1.EstimateRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				DisplayName: normalizePromotionReply.GetPromotion().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: scorePromotionReply.GetPromotion().GetAudit().GetUpdatedAt(),
			},
		},
	}
	estimateRewardReply := new(loyaltyv1.EstimateRewardResponse)
	if err := grpc.Invoke(ctx, "/acme.loyalty.v1.RewardCalculationService/EstimateReward", estimateRewardRequest, estimateRewardReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/EstimateReward")
	}
	return &promotionsv1.NormalizeCampaignResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: normalizePromotionReply.GetResult().GetCorrelationId(),
			Status:        estimateRewardReply.GetResult().GetStatus(),
		},
		Outcome: &promotionsv1.NormalizeCampaignResponse_Outcome{
			EffectiveAt: normalizePromotionReply.GetOutcome().GetEffectiveAt(),
		},
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{
				UserId: scorePromotionReply.GetPromotion().GetOwner().GetUserId(),
			},
			CampaignId: req.GetCampaignId(),
			Metadata:   req.GetCampaign().GetMetadata(),
		},
	}, nil
}

// ExplainCampaignDecision explains a campaign.
func (s *campaignValidationHandlers) ExplainCampaignDecision(ctx context.Context, req *promotionsv1.ExplainCampaignDecisionRequest) (*promotionsv1.ExplainCampaignDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainCampaignDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "explain campaign decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := campaignValidationHandlersListSearchQueryVersionsSteps[mode]
	source := req.GetCampaign().GetLocation().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listSearchQueryVersionsRequest := &searchv1.ListSearchQueryVersionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchQueryVersionsRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetCampaign().GetAudit().GetUpdatedAt(),
			Labels:       annotations,
		},
	}
	listSearchQueryVersionsReply, err := run(s, ctx, listSearchQueryVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/ListSearchQueryVersions")
	}
	mode2 := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode2 = "audited"
	}
	run2 := campaignValidationHandlersPreviewRefundSteps[mode2]
	cloned := proto.Clone(req.GetDetails()).(*promotionsv1.ExplainCampaignDecisionRequest_Details)
	previewRefundRequest := &paymentsv1.PreviewRefundRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.PreviewRefundRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: cloned.GetContact().GetPhone()},
		},
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetCampaign().GetAudit().GetEtag(),
				CreatedAt: req.GetCampaign().GetAudit().GetCreatedAt(),
			},
		},
	}
	previewRefundReply, err := run2(s, ctx, previewRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/PreviewRefund")
	}
	scorePromotionRequest := &promotionsv1.ScorePromotionRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ScorePromotionRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: previewRefundReply.GetRefund().GetBillingAddress().GetCountryCode(),
			},
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedBy: previewRefundReply.GetRefund().GetAudit().GetCreatedBy(),
			},
		},
	}
	scorePromotionReply, err := scorePromotionCall(ctx, scorePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/ScorePromotion")
	}
	return &promotionsv1.ExplainCampaignDecisionResponse{
		Result: &commonv1.OperationResult{
			Details:       listSearchQueryVersionsReply.GetResult().GetDetails(),
			CorrelationId: scorePromotionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Campaign: &promotionsv1.Campaign{
			Location: &commonv1.PostalAddress{
				Locality: previewRefundReply.GetRefund().GetBillingAddress().GetLocality(),
			},
			Metadata:   previewRefundReply.GetRefund().GetMetadata(),
			CampaignId: req.GetCampaignId(),
			Window:     req.GetCampaign().GetWindow(),
		},
	}, nil
}

// ScoreCampaign scores a campaign.
func (s *campaignValidationHandlers) ScoreCampaign(ctx context.Context, req *promotionsv1.ScoreCampaignRequest) (*promotionsv1.ScoreCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "score campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	for _, participant := range req.GetCampaign().GetParticipants() {
		if _, err := s.couponValidationClient.PreviewCoupon(ctx, &promotionsv1.PreviewCouponRequest{
			Context: req.GetContext(),
			Details: &promotionsv1.PreviewCouponRequest_Details{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: participant.GetContact().GetPreferredChannel(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/PreviewCoupon")
		}
	}
	for participant2 := range iterx.Each(req.GetCampaign().GetParticipants(), func(candidate *promotionsv1.CampaignParticipant) bool { return candidate != nil }) {
		if _, err := s.promotionCalculationClient.EstimatePromotion(ctx, &promotionsv1.EstimatePromotionRequest{
			Context: req.GetContext(),
			Promotion: &promotionsv1.Promotion{
				Action: &promotionsv1.Promotion_Block{Block: &promotionsv1.PromotionBlockAction{NotifyEmail: participant2.GetContact().GetEmail()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/EstimatePromotion")
		}
	}
	normalizeCouponRequest := &promotionsv1.NormalizeCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetCampaign().GetAudit().GetUpdatedBy()},
		},
	}
	normalizeCouponReply := new(promotionsv1.NormalizeCouponResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", promotionsCouponValidationServiceName, "NormalizeCoupon"), normalizeCouponRequest, normalizeCouponReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/NormalizeCoupon")
	}
	paymentValidation, err := registry.Lookup[paymentsv1.PaymentValidationServiceClient](s.clients, "acme.payments.v1.PaymentValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	checkPaymentEligibilityRequest := &paymentsv1.CheckPaymentEligibilityRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetCampaign().GetLocation().GetCountryCode(),
			},
		},
	}
	checkPaymentEligibilityReply, err := paymentValidation.CheckPaymentEligibility(ctx, checkPaymentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/CheckPaymentEligibility")
	}
	return &promotionsv1.ScoreCampaignResponse{
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{
					Phone: normalizeCouponReply.GetCoupon().GetContact().GetPhone(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkPaymentEligibilityReply.GetPayment().GetAudit().GetCreatedBy(),
			},
			Location: &commonv1.PostalAddress{
				Lines: checkPaymentEligibilityReply.GetPayment().GetBillingAddress().GetLines(),
			},
			CampaignId: req.GetCampaignId(),
			Metadata:   req.GetCampaign().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintCampaigns lints a campaign.
func (s *campaignValidationHandlers) LintCampaigns(ctx context.Context, req *promotionsv1.LintCampaignsRequest) (*promotionsv1.LintCampaignsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "lint campaigns", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetContact().GetEmail())
	updateChartOfAccountsRequest := &accountingv1.UpdateChartOfAccountsRequest{
		Context: req.GetContext(),
		ChartOfAccounts: &accountingv1.ChartOfAccounts{
			Children: []*accountingv1.ChartOfAccounts{&accountingv1.ChartOfAccounts{
				Children: []*accountingv1.ChartOfAccounts{&accountingv1.ChartOfAccounts{
					Manager: &identityv1.UserRef{
						DisplayName: req.GetCampaign().GetOrganizer().GetDisplayName(),
						Email:       tenancy.Value(ctx, "email"),
					},
				}},
			}},
		},
	}
	updateChartOfAccountsReply, err := grpcx.Call(ctx, s.chartOfAccountsClient.UpdateChartOfAccounts, updateChartOfAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.accounting.v1.ChartOfAccountsService/UpdateChartOfAccounts")
	}
	batchGetRiskAssessmentsRequest := &fraudv1.BatchGetRiskAssessmentsRequest{Context: req.GetContext()}
	batchGetRiskAssessmentsReply, err := batchGetRiskAssessmentsCall(ctx, batchGetRiskAssessmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentService/BatchGetRiskAssessments")
	}
	return &promotionsv1.LintCampaignsResponse{
		Campaign: &promotionsv1.Campaign{
			Location: &commonv1.PostalAddress{
				CountryCode: updateChartOfAccountsReply.GetChartOfAccounts().GetLocation().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: updateChartOfAccountsReply.GetChartOfAccounts().GetAudit().GetCreatedAt(),
			},
			CampaignId: req.GetCampaignId(),
			Metadata:   req.GetCampaign().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			Message: batchGetRiskAssessmentsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetCampaignRules returns a campaign.
func (s *campaignValidationHandlers) GetCampaignRules(ctx context.Context, req *promotionsv1.GetCampaignRulesRequest) (*promotionsv1.GetCampaignRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "get_campaign_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get campaign rules", "tenant", req.GetContext().GetTenantId())
	return &promotionsv1.GetCampaignRulesResponse{
		Campaign: &promotionsv1.Campaign{CampaignId: req.GetCampaignId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// campaignValidationHandlersListSearchQueryVersionsSteps are the ways to run
// acme.search.v1.SearchQueryHistoryService/ListSearchQueryVersions, by name.
var campaignValidationHandlersListSearchQueryVersionsSteps = map[string]func(*campaignValidationHandlers, context.Context, *searchv1.ListSearchQueryVersionsRequest) (*searchv1.ListSearchQueryVersionsResponse, error){
	"default": (*campaignValidationHandlers).listSearchQueryVersionsStep,
	"audited": (*campaignValidationHandlers).listSearchQueryVersionsAuditedStep,
}

func (s *campaignValidationHandlers) listSearchQueryVersionsStep(ctx context.Context, request *searchv1.ListSearchQueryVersionsRequest) (*searchv1.ListSearchQueryVersionsResponse, error) {
	return s.searchQueryHistoryClient.ListSearchQueryVersions(ctx, request)
}

func (s *campaignValidationHandlers) listSearchQueryVersionsAuditedStep(ctx context.Context, request *searchv1.ListSearchQueryVersionsRequest) (*searchv1.ListSearchQueryVersionsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SearchQueryHistoryService/ListSearchQueryVersions")
	return s.searchQueryHistoryClient.ListSearchQueryVersions(ctx, request)
}

// campaignValidationHandlersPreviewRefundSteps are the ways to run
// acme.payments.v1.RefundValidationService/PreviewRefund, by name.
var campaignValidationHandlersPreviewRefundSteps = map[string]func(*campaignValidationHandlers, context.Context, *paymentsv1.PreviewRefundRequest) (*paymentsv1.PreviewRefundResponse, error){
	"default": (*campaignValidationHandlers).previewRefundStep,
	"audited": (*campaignValidationHandlers).previewRefundAuditedStep,
}

func (s *campaignValidationHandlers) previewRefundStep(ctx context.Context, request *paymentsv1.PreviewRefundRequest) (*paymentsv1.PreviewRefundResponse, error) {
	return s.refundValidationClient.PreviewRefund(ctx, request)
}

func (s *campaignValidationHandlers) previewRefundAuditedStep(ctx context.Context, request *paymentsv1.PreviewRefundRequest) (*paymentsv1.PreviewRefundResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.payments.v1.RefundValidationService/PreviewRefund")
	return s.refundValidationClient.PreviewRefund(ctx, request)
}
