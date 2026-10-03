package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	schedulingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/scheduling/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// CampaignCalculationServer implements `acme.promotions.v1.CampaignCalculationService`.
// Computes the figures of a campaign, such as totals, taxes, and estimates.
type CampaignCalculationServer struct {
	promotionsv1.UnimplementedCampaignCalculationServiceServer
	store                                *Store
	log                                  *slog.Logger
	sellerCalculationClient              sellersv1.SellerCalculationServiceClient
	userQueryClient                      identityv1.UserQueryServiceClient
	userValidationClient                 identityv1.UserValidationServiceClient
	promotionCalculationClient           promotionsv1.PromotionCalculationServiceClient
	loyaltyMemberQueryClient             loyaltyv1.LoyaltyMemberQueryServiceClient
	conn                                 *grpc.ClientConn
	payoutCalculationOnce                sync.Once
	payoutCalculationCached              paymentsv1.PayoutCalculationServiceClient
	couponCalculationClient              promotionsv1.CouponCalculationServiceClient
	shiftQueryClient                     schedulingv1.ShiftQueryServiceClient
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	searchIndexValidationClient          searchv1.SearchIndexValidationServiceClient
	sessionQueryClient                   identityv1.SessionQueryServiceClient
}

// NewCampaignCalculationServer wires a CampaignCalculationServer.
func NewCampaignCalculationServer(deps platform.Deps) *CampaignCalculationServer {
	return &CampaignCalculationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.promotions.v1.CampaignCalculationService"),
		sellerCalculationClient:              sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		userQueryClient:                      identityv1.NewUserQueryServiceClient(deps.Conn),
		userValidationClient:                 identityv1.NewUserValidationServiceClient(deps.Conn),
		promotionCalculationClient:           promotionsv1.NewPromotionCalculationServiceClient(deps.Conn),
		loyaltyMemberQueryClient:             loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		couponCalculationClient:              promotionsv1.NewCouponCalculationServiceClient(deps.Conn),
		shiftQueryClient:                     schedulingv1.NewShiftQueryServiceClient(deps.Conn),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		searchIndexValidationClient:          searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		sessionQueryClient:                   identityv1.NewSessionQueryServiceClient(deps.Conn),
	}
}

// CalculateCampaign calculates a campaign.
func (s *CampaignCalculationServer) CalculateCampaign(ctx context.Context, req *promotionsv1.CalculateCampaignRequest) (*promotionsv1.CalculateCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateCampaign", "error", err)
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "calculate campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	for participant := range iterx.Each(req.GetCampaign().GetParticipants(), func(candidate *promotionsv1.CampaignParticipant) bool { return candidate != nil }) {
		if _, err := s.sellerCalculationClient.ExplainSellerCalculation(ctx, &sellersv1.ExplainSellerCalculationRequest{
			Context: req.GetContext(),
			Seller: &sellersv1.Seller{
				Contact: &commonv1.ContactInfo{Name: participant.GetContact().GetName()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation")
		}
	}
	for _, participant2 := range req.GetCampaign().GetParticipants() {
		if _, err := s.userQueryClient.FindUsers(ctx, &identityv1.FindUsersRequest{
			Context: req.GetContext(),
			Filter: &identityv1.FindUsersRequest_Filter{
				Region:  req.GetCampaign().GetLocation().GetRegion(),
				OwnerId: participant2.GetParticipantId(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/FindUsers")
		}
	}
	return &promotionsv1.CalculateCampaignResponse{
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Location:   req.GetCampaign().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateCampaign estimates a campaign.
func (s *CampaignCalculationServer) EstimateCampaign(ctx context.Context, req *promotionsv1.EstimateCampaignRequest) (*promotionsv1.EstimateCampaignResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "estimate campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	calculateCouponRequest := &promotionsv1.CalculateCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: req.GetCampaign().GetOrganizer().GetDisplayName(),
			},
		},
	}
	calculateCouponReply, err := calculateCouponCall(ctx, calculateCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/CalculateCoupon")
	}
	return &promotionsv1.EstimateCampaignResponse{
		Campaign: &promotionsv1.Campaign{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateCouponReply.GetCoupon().GetAudit().GetUpdatedBy(),
			},
			CampaignId: req.GetCampaignId(),
			Window:     req.GetCampaign().GetWindow(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateCouponReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// QuoteCampaign quotes a campaign.
func (s *CampaignCalculationServer) QuoteCampaign(ctx context.Context, req *promotionsv1.QuoteCampaignRequest) (*promotionsv1.QuoteCampaignResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "quote campaign", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCampaign()).(*promotionsv1.Campaign)
	checkUserEligibilityReply, err := s.userValidationClient.CheckUserEligibility(ctx, &identityv1.CheckUserEligibilityRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{UpdatedBy: cloned.GetAudit().GetUpdatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*promotionsv1.SimulatePromotionResponse, len(req.GetCampaign().GetParticipants()))
	for i, participant := range req.GetCampaign().GetParticipants() {
		group.Go(func() error {
			reply, err := s.promotionCalculationClient.SimulatePromotion(groupCtx, &promotionsv1.SimulatePromotionRequest{
				Context: req.GetContext(),
				Promotion: &promotionsv1.Promotion{
					Owner: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
				},
				Details: &promotionsv1.SimulatePromotionRequest_Details{
					Destination: &commonv1.PostalAddress{Recipient: req.GetCampaign().GetLocation().GetRecipient()},
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
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/SimulatePromotion")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	findLoyaltyMembersRequest := &loyaltyv1.FindLoyaltyMembersRequest{
		Context: req.GetContext(),
		Filter:  &loyaltyv1.FindLoyaltyMembersRequest_Filter{Region: req.GetCampaign().GetLocation().GetRegion()},
	}
	findLoyaltyMembersReply, err := grpcx.Call(ctx, s.loyaltyMemberQueryClient.FindLoyaltyMembers, findLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/FindLoyaltyMembers")
	}
	return &promotionsv1.QuoteCampaignResponse{
		Campaign: &promotionsv1.Campaign{
			Organizer: &identityv1.UserRef{
				DisplayName: checkUserEligibilityReply.GetUser().GetRelationshipManager().GetDisplayName(),
				Email:       checkUserEligibilityReply.GetUser().GetRelationshipManager().GetEmail(),
			},
			CampaignId: req.GetCampaignId(),
			Location:   req.GetCampaign().GetLocation(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: findLoyaltyMembersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CompareCampaignOptions compares a campaign.
func (s *CampaignCalculationServer) CompareCampaignOptions(ctx context.Context, req *promotionsv1.CompareCampaignOptionsRequest) (*promotionsv1.CompareCampaignOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "compare campaign options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCampaign())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.Campaign
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	campaignCache := cache.New[string, *promotionsv1.Campaign](1)
	campaignCache.Put(req.GetContext().GetTraceId(), req.GetCampaign())
	cachedCampaign, _ := campaignCache.Get(req.GetContext().GetTraceId())
	estimateSellerRequest := newEstimateSellerRequest(withEstimateSellerContext(req.GetContext()), withEstimateSellerDetails(&sellersv1.EstimateSellerRequest_Details{
		Destination: &commonv1.PostalAddress{
			Lines:     req.GetCampaign().GetLocation().GetLines(),
			Recipient: (&decoded).GetLocation().GetRecipient(),
		},
	}), withEstimateSellerSeller(&sellersv1.Seller{
		Audit:               &commonv1.AuditInfo{UpdatedAt: req.GetCampaign().GetAudit().GetUpdatedAt()},
		RelationshipManager: &identityv1.UserRef{Email: cachedCampaign.GetOrganizer().GetEmail()},
	}))
	estimateSellerReply, err := s.sellerCalculationClient.EstimateSeller(ctx, estimateSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/EstimateSeller")
	}
	encoded2, err := protojson.Marshal(req.GetCampaign())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 promotionsv1.Campaign
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	simulatePayoutRequest := &paymentsv1.SimulatePayoutRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.SimulatePayoutRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: estimateSellerReply.GetSeller().GetPrimaryAddress().GetLines(),
			},
		},
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				CreatedBy: estimateSellerReply.GetSeller().GetAudit().GetCreatedBy(),
				UpdatedAt: (&decoded2).GetAudit().GetUpdatedAt(),
			},
		},
	}
	simulatePayoutReply, err := s.payoutCalculationLazy().SimulatePayout(ctx, simulatePayoutRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutCalculationService/SimulatePayout")
	}
	for _, participant := range req.GetCampaign().GetParticipants() {
		if _, err := s.couponCalculationClient.CompareCouponOptions(ctx, &promotionsv1.CompareCouponOptionsRequest{
			Context: req.GetContext(),
			Coupon:  &promotionsv1.Coupon{Contact: participant.GetContact()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/CompareCouponOptions")
		}
	}
	return &promotionsv1.CompareCampaignOptionsResponse{
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{Contact: estimateSellerReply.GetSeller().GetContact()}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: estimateSellerReply.GetSeller().GetAudit().GetCreatedBy(),
			},
			CampaignId: req.GetCampaignId(),
			Title:      req.GetCampaign().GetTitle(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: simulatePayoutReply.GetResult().GetCorrelationId(),
			Status:        simulatePayoutReply.GetResult().GetStatus(),
		},
	}, nil
}

// RecalculateCampaigns recalculates a campaign.
func (s *CampaignCalculationServer) RecalculateCampaigns(ctx context.Context, req *promotionsv1.RecalculateCampaignsRequest) (*promotionsv1.RecalculateCampaignsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "recalculate campaigns", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCampaign()).(*promotionsv1.Campaign)
	recalculateCouponsRequest := &promotionsv1.RecalculateCouponsRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{Etag: req.GetCampaign().GetAudit().GetEtag()},
		},
		Details: &promotionsv1.RecalculateCouponsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality:  req.GetCampaign().GetLocation().GetLocality(),
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
				Region:    cloned.GetLocation().GetRegion(),
			},
		},
	}
	send := s.recalculateCouponsSender(recalculateCouponsRequest)
	recalculateCouponsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/RecalculateCoupons")
	}
	suggestShiftsReply, err := s.shiftQueryClient.SuggestShifts(ctx, &schedulingv1.SuggestShiftsRequest{
		Context: req.GetContext(),
		Filter: &schedulingv1.SuggestShiftsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			Labels:       recalculateCouponsReply.GetCoupon().GetLabels(),
			CreatedAfter: req.GetCampaign().GetAudit().GetCreatedAt(),
			OwnerId:      recalculateCouponsReply.GetCoupon().GetHolder().GetPromotionId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.scheduling.v1.ShiftQueryService/SuggestShifts")
	}
	packed, err := anypb.New(req.GetCampaign())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateGiftCardTransactionRequest := &giftcardsv1.CalculateGiftCardTransactionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.CalculateGiftCardTransactionRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetCampaign().GetLocation().GetLines()},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	calculateGiftCardTransactionReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*giftcardsv1.CalculateGiftCardTransactionResponse, error) {
		return s.giftCardTransactionCalculationClient.CalculateGiftCardTransaction(ctx, calculateGiftCardTransactionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/CalculateGiftCardTransaction")
	}
	return &promotionsv1.RecalculateCampaignsResponse{
		Result: &commonv1.OperationResult{
			Message:      recalculateCouponsReply.GetResult().GetMessage(),
			Details:      suggestShiftsReply.GetResult().GetDetails(),
			ReferenceIds: suggestShiftsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Breakdown: &promotionsv1.RecalculateCampaignsResponse_Breakdown{
			CurrencyCode: calculateGiftCardTransactionReply.GetBreakdown().GetCurrencyCode(),
		},
		Campaign: &promotionsv1.Campaign{
			CampaignId: req.GetCampaignId(),
			Window:     req.GetCampaign().GetWindow(),
		},
	}, nil
}

// ExplainCampaignCalculation explains a campaign.
func (s *CampaignCalculationServer) ExplainCampaignCalculation(ctx context.Context, req *promotionsv1.ExplainCampaignCalculationRequest) (*promotionsv1.ExplainCampaignCalculationResponse, error) {
	if err := s.store.SaveCampaign(ctx, req.GetContext().GetTenantId(), req.GetCampaign()); err != nil {
		return nil, grpcx.Wrap(err, "save campaign")
	}
	s.log.InfoContext(ctx, "explain campaign calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCampaign().GetAudit().GetEtag())
	checkSearchIndexEligibilityRequest := &searchv1.CheckSearchIndexEligibilityRequest{
		Context: req.GetContext(),
		Details: &searchv1.CheckSearchIndexEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	checkSearchIndexEligibilityReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*searchv1.CheckSearchIndexEligibilityResponse, error) {
		return s.searchIndexValidationClient.CheckSearchIndexEligibility(ctx, checkSearchIndexEligibilityRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/CheckSearchIndexEligibility")
	}
	quoteCouponRequest := &promotionsv1.QuoteCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{
				Email: checkSearchIndexEligibilityReply.GetSearchIndex().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetCampaign().GetAudit().GetCreatedBy()},
		},
		Details: &promotionsv1.QuoteCouponRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: checkSearchIndexEligibilityReply.GetSearchIndex().GetAddress().GetLocality(),
				Lines:    checkSearchIndexEligibilityReply.GetSearchIndex().GetAddress().GetLines(),
			},
		},
	}
	quoteCouponReply, err := quoteCouponCall(ctx, quoteCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/QuoteCoupon")
	}
	validateUserRequest := &identityv1.ValidateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{
				PostalCode: checkSearchIndexEligibilityReply.GetSearchIndex().GetAddress().GetPostalCode(),
			},
			Audit:               &commonv1.AuditInfo{CreatedAt: req.GetCampaign().GetAudit().GetCreatedAt()},
			RelationshipManager: &identityv1.UserRef{Email: quoteCouponReply.GetCoupon().GetContact().GetEmail()},
		},
		Details: &identityv1.ValidateUserRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetCampaign().GetOrganizer().GetEmail()},
		},
	}
	validateUserReply, err := grpcx.Call(ctx, s.userValidationClient.ValidateUser, validateUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ValidateUser")
	}
	_ = validateUserReply
	return &promotionsv1.ExplainCampaignCalculationResponse{
		Campaign: &promotionsv1.Campaign{
			Location: &commonv1.PostalAddress{
				Recipient: checkSearchIndexEligibilityReply.GetSearchIndex().GetAddress().GetRecipient(),
			},
			Organizer: &identityv1.UserRef{Email: quoteCouponReply.GetCoupon().GetContact().GetEmail()},
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Name: quoteCouponReply.GetCoupon().GetContact().GetName()},
			}},
			CampaignId: req.GetCampaignId(),
			Window:     req.GetCampaign().GetWindow(),
		},
		Result: &commonv1.OperationResult{
			Status: checkSearchIndexEligibilityReply.GetOutcome().GetStatus(),
		},
	}, nil
}

// GetCampaignRates returns a campaign.
func (s *CampaignCalculationServer) GetCampaignRates(ctx context.Context, req *promotionsv1.GetCampaignRatesRequest) (*promotionsv1.GetCampaignRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "campaign", req.GetCampaignId(), "get_campaign_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get campaign rates", "tenant", req.GetContext().GetTenantId())
	exportSessionsStream, err := s.sessionQueryClient.ExportSessions(ctx, &identityv1.ExportSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ExportSessionsRequest_Filter{
			OwnerId:      req.GetCampaignId(),
			Labels:       req.GetContext().GetBaggage(),
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
	}
	for {
		received, err := exportSessionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/ExportSessions")
		}
		_ = received
	}
	return &promotionsv1.GetCampaignRatesResponse{
		Campaign: &promotionsv1.Campaign{CampaignId: req.GetCampaignId()},
		Result:   &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// payoutCalculationLazy dials acme.payments.v1.PayoutCalculationService the first time a handler needs it.
func (s *CampaignCalculationServer) payoutCalculationLazy() paymentsv1.PayoutCalculationServiceClient {
	s.payoutCalculationOnce.Do(func() { s.payoutCalculationCached = paymentsv1.NewPayoutCalculationServiceClient(s.conn) })
	return s.payoutCalculationCached
}

// recalculateCouponsSender returns a function that sends one request to
// acme.promotions.v1.CouponCalculationService/RecalculateCoupons.
func (s *CampaignCalculationServer) recalculateCouponsSender(request *promotionsv1.RecalculateCouponsRequest) func(context.Context) (*promotionsv1.RecalculateCouponsResponse, error) {
	return func(ctx context.Context) (*promotionsv1.RecalculateCouponsResponse, error) {
		return s.couponCalculationClient.RecalculateCoupons(ctx, request)
	}
}
