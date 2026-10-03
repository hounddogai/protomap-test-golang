package loyalty

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// TierValidationServer implements `acme.loyalty.v1.TierValidationService`.
// Checks tiers against the rules before anything acts on them.
type TierValidationServer struct {
	loyaltyv1.UnimplementedTierValidationServiceServer
	store                                 *Store
	log                                   *slog.Logger
	pointsLedgerValidationClient          loyaltyv1.PointsLedgerValidationServiceClient
	conn                                  *grpc.ClientConn
	sellerPayoutScheduleValidationClient  sellersv1.SellerPayoutScheduleValidationServiceClient
	roleAdminClient                       identityv1.RoleAdminServiceClient
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	rewardCalculationClient               loyaltyv1.RewardCalculationServiceClient
	giftCardBatchValidationClient         giftcardsv1.GiftCardBatchValidationServiceClient
	pointsLedgerCalculationClient         loyaltyv1.PointsLedgerCalculationServiceClient
	loyaltyMemberCalculationClient        loyaltyv1.LoyaltyMemberCalculationServiceClient
	sessionNotificationClient             identityv1.SessionNotificationServiceClient
}

// NewTierValidationServer wires a TierValidationServer.
func NewTierValidationServer(deps platform.Deps) *TierValidationServer {
	return &TierValidationServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.loyalty.v1.TierValidationService"),
		pointsLedgerValidationClient:          loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
		conn:                                  deps.Conn,
		sellerPayoutScheduleValidationClient:  sellersv1.NewSellerPayoutScheduleValidationServiceClient(deps.Conn),
		roleAdminClient:                       identityv1.NewRoleAdminServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		rewardCalculationClient:               loyaltyv1.NewRewardCalculationServiceClient(deps.Conn),
		giftCardBatchValidationClient:         giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
		pointsLedgerCalculationClient:         loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		loyaltyMemberCalculationClient:        loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		sessionNotificationClient:             identityv1.NewSessionNotificationServiceClient(deps.Conn),
	}
}

// ValidateTier validates a tier.
func (s *TierValidationServer) ValidateTier(ctx context.Context, req *loyaltyv1.ValidateTierRequest) (*loyaltyv1.ValidateTierResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidateTier", "error", err)
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "validate tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	scorePointsLedgerRequest := &loyaltyv1.ScorePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
	}
	scorePointsLedgerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*loyaltyv1.ScorePointsLedgerResponse, error) {
		return s.pointsLedgerValidationClient.ScorePointsLedger(ctx, scorePointsLedgerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ScorePointsLedger")
	}
	explainPointsLedgerCalculationRequest := &loyaltyv1.ExplainPointsLedgerCalculationRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Funding: &loyaltyv1.PointsLedger_Bank{Bank: &loyaltyv1.PointsLedgerBankFunding{
				AccountHolder: scorePointsLedgerReply.GetPointsLedger().GetBank().GetAccountHolder(),
			}},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetTier().GetAudit().GetCreatedAt()},
		},
	}
	explainPointsLedgerCalculationReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewPointsLedgerCalculationServiceClient).ExplainPointsLedgerCalculation(ctx, explainPointsLedgerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/ExplainPointsLedgerCalculation")
	}
	return &loyaltyv1.ValidateTierResponse{
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				CreatedBy: scorePointsLedgerReply.GetPointsLedger().GetAudit().GetCreatedBy(),
			},
			TierId:    req.GetTierId(),
			Effective: req.GetTier().GetEffective(),
		},
		Outcome: &loyaltyv1.ValidateTierResponse_Outcome{
			Message: explainPointsLedgerCalculationReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewTier previews a tier.
func (s *TierValidationServer) PreviewTier(ctx context.Context, req *loyaltyv1.PreviewTierRequest) (*loyaltyv1.PreviewTierResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PreviewTier", "error", err)
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "preview tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetContact().GetEmail())
	checkSellerPayoutScheduleEligibilityRequest := &sellersv1.CheckSellerPayoutScheduleEligibilityRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Phone: req.GetDetails().GetContact().GetPhone(),
					Email: req.GetTier().GetOwner().GetEmail(),
				},
			}},
		},
		Details: &sellersv1.CheckSellerPayoutScheduleEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetDetails().GetContact().GetName(),
				Email: tenancy.Value(ctx, "email"),
			},
		},
	}
	checkSellerPayoutScheduleEligibilityReply, err := sellersv1.SellerPayoutScheduleValidationServiceClient.CheckSellerPayoutScheduleEligibility(s.sellerPayoutScheduleValidationClient, ctx, checkSellerPayoutScheduleEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/CheckSellerPayoutScheduleEligibility")
	}
	snapshot, err := protojson.Marshal(req.GetTier())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairRoleReply, err := s.roleAdminClient.RepairRole(ctx, &identityv1.RepairRoleRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetTier().GetAudit().GetCreatedBy(),
				CreatedAt: checkSellerPayoutScheduleEligibilityReply.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: checkSellerPayoutScheduleEligibilityReply.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
			Action: &identityv1.Role_Discount{Discount: &identityv1.RoleDiscountAction{Amount: req.GetTier().GetDiscount().GetAmount()}},
		},
		Details: &identityv1.RepairRoleRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleAdminService/RepairRole")
	}
	detailsCache := cache.New[string, *loyaltyv1.PreviewTierRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	cloned := proto.Clone(req.GetTier()).(*loyaltyv1.Tier)
	quoteSellerPayoutScheduleRequest := &sellersv1.QuoteSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Recurrence: &sellersv1.SellerPayoutSchedule_Daily{Daily: &sellersv1.SellerPayoutScheduleDailyRule{
				Interval: checkSellerPayoutScheduleEligibilityReply.GetSellerPayoutSchedule().GetWeekly().GetInterval(),
			}},
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Name: cachedDetails.GetContact().GetName()},
			}},
			Audit: &commonv1.AuditInfo{UpdatedBy: cloned.GetAudit().GetUpdatedBy()},
		},
		Details: &sellersv1.QuoteSellerPayoutScheduleRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: checkSellerPayoutScheduleEligibilityReply.GetSellerPayoutSchedule().GetLocation().GetRegion(),
			},
		},
	}
	quoteSellerPayoutScheduleReply, err := sellersv1.SellerPayoutScheduleCalculationServiceClient.QuoteSellerPayoutSchedule(s.sellerPayoutScheduleCalculationClient, ctx, quoteSellerPayoutScheduleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/QuoteSellerPayoutSchedule")
	}
	calculateRewardRequest := &loyaltyv1.CalculateRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Action: &loyaltyv1.Reward_Route{Route: &loyaltyv1.RewardRouteAction{Target: req.GetTier().GetRoute().GetTarget()}},
		},
	}
	send := s.calculateRewardSender(calculateRewardRequest)
	calculateRewardReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/CalculateReward")
	}
	return &loyaltyv1.PreviewTierResponse{
		Result: &commonv1.OperationResult{
			Status:        checkSellerPayoutScheduleEligibilityReply.GetOutcome().GetStatus(),
			CorrelationId: quoteSellerPayoutScheduleReply.GetResult().GetCorrelationId(),
		},
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				UserId: checkSellerPayoutScheduleEligibilityReply.GetSellerPayoutSchedule().GetOrganizer().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: repairRoleReply.GetRole().GetAudit().GetCreatedAt(),
				UpdatedBy: quoteSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetUpdatedBy(),
			},
			Scopes:      calculateRewardReply.GetReward().GetScopes(),
			TierId:      req.GetTierId(),
			Description: req.GetTier().GetDescription(),
		},
	}, nil
}

// CheckTierEligibility checks a tier.
func (s *TierValidationServer) CheckTierEligibility(ctx context.Context, req *loyaltyv1.CheckTierEligibilityRequest) (*loyaltyv1.CheckTierEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckTierEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "check tier eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	previewGiftCardBatchRequest := buildPreviewGiftCardBatchRequestForCheckTierEligibility(req)
	previewGiftCardBatchReply, err := s.giftCardBatchValidationClient.PreviewGiftCardBatch(ctx, previewGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/PreviewGiftCardBatch")
	}
	return &loyaltyv1.CheckTierEligibilityResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: previewGiftCardBatchReply.GetResult().GetReferenceIds(),
			Status:       previewGiftCardBatchReply.GetOutcome().GetStatus(),
		},
		Tier: &loyaltyv1.Tier{
			TierId: req.GetTierId(),
			Owner:  req.GetTier().GetOwner(),
		},
	}, nil
}

// NormalizeTier normalizes a tier.
func (s *TierValidationServer) NormalizeTier(ctx context.Context, req *loyaltyv1.NormalizeTierRequest) (*loyaltyv1.NormalizeTierResponse, error) {
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "normalize tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	var holder struct{ UserId string }
	if record := req.GetTier().GetOwner(); record != nil {
		if err := mapper.CopyFields(&holder, record, "UserId"); err != nil {
			s.log.WarnContext(ctx, "copy user_id", "error", err)
		}
	}
	estimateRewardRequest := &loyaltyv1.EstimateRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				Email:  req.GetTier().GetOwner().GetEmail(),
				UserId: holder.UserId,
			},
			Audit:  &commonv1.AuditInfo{UpdatedBy: req.GetTier().GetAudit().GetUpdatedBy()},
			Action: &loyaltyv1.Reward_Discount{Discount: &loyaltyv1.RewardDiscountAction{Amount: req.GetTier().GetDiscount().GetAmount()}},
		},
	}
	estimateRewardReply := new(loyaltyv1.EstimateRewardResponse)
	if err := s.conn.Invoke(ctx, loyaltyRewardCalculationServiceEstimateRewardMethod, estimateRewardRequest, estimateRewardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/EstimateReward")
	}
	explainPointsLedgerCalculationRequest := &loyaltyv1.ExplainPointsLedgerCalculationRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	var explainPointsLedgerCalculationReply *loyaltyv1.ExplainPointsLedgerCalculationResponse
	if err := pipeline.Run(ctx, "acme.loyalty.v1.PointsLedgerCalculationService/ExplainPointsLedgerCalculation", func(ctx context.Context) error {
		answer, err := s.pointsLedgerCalculationClient.ExplainPointsLedgerCalculation(ctx, explainPointsLedgerCalculationRequest)
		explainPointsLedgerCalculationReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/ExplainPointsLedgerCalculation")
	}
	explainLoyaltyMemberCalculationRequest := &loyaltyv1.ExplainLoyaltyMemberCalculationRequest{}
	explainLoyaltyMemberCalculationRequest.Context = req.GetContext()
	explainLoyaltyMemberCalculationRequest.LoyaltyMember = &loyaltyv1.LoyaltyMember{
		Audit: &commonv1.AuditInfo{CreatedAt: req.GetTier().GetAudit().GetCreatedAt()},
		Contact: &commonv1.ContactInfo{
			PreferredChannel: explainPointsLedgerCalculationReply.GetPointsLedger().GetContact().GetPreferredChannel(),
		},
	}
	explainLoyaltyMemberCalculationReply, err := s.loyaltyMemberCalculationClient.ExplainLoyaltyMemberCalculation(ctx, explainLoyaltyMemberCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/ExplainLoyaltyMemberCalculation")
	}
	return &loyaltyv1.NormalizeTierResponse{
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				CreatedBy: estimateRewardReply.GetReward().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				Email: explainLoyaltyMemberCalculationReply.GetLoyaltyMember().GetContact().GetEmail(),
			},
			TierId:    req.GetTierId(),
			Effective: req.GetTier().GetEffective(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: estimateRewardReply.GetResult().GetCorrelationId(),
			Status:        explainPointsLedgerCalculationReply.GetResult().GetStatus(),
			ReferenceIds:  explainLoyaltyMemberCalculationReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// ExplainTierDecision explains a tier.
func (s *TierValidationServer) ExplainTierDecision(ctx context.Context, req *loyaltyv1.ExplainTierDecisionRequest) (*loyaltyv1.ExplainTierDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "explain tier decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	notifySessionWatchersRequest := &identityv1.NotifySessionWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifySessionWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
	}
	notifySessionWatchersReply, err := grpcx.Call(ctx, s.sessionNotificationClient.NotifySessionWatchers, notifySessionWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/NotifySessionWatchers")
	}
	return &loyaltyv1.ExplainTierDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: notifySessionWatchersReply.GetResult().GetMessage(),
			Details: notifySessionWatchersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Tier: &loyaltyv1.Tier{
			TierId:     req.GetTierId(),
			Parameters: req.GetTier().GetParameters(),
		},
	}, nil
}

// GetTierRules returns a tier.
func (s *TierValidationServer) GetTierRules(ctx context.Context, req *loyaltyv1.GetTierRulesRequest) (*loyaltyv1.GetTierRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "get_tier_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get tier rules", "tenant", req.GetContext().GetTenantId())
	return &loyaltyv1.GetTierRulesResponse{
		Tier:   &loyaltyv1.Tier{TierId: req.GetTierId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// calculateRewardSender returns a function that sends one request to
// acme.loyalty.v1.RewardCalculationService/CalculateReward.
func (s *TierValidationServer) calculateRewardSender(request *loyaltyv1.CalculateRewardRequest) func(context.Context) (*loyaltyv1.CalculateRewardResponse, error) {
	return func(ctx context.Context) (*loyaltyv1.CalculateRewardResponse, error) {
		return s.rewardCalculationClient.CalculateReward(ctx, request)
	}
}
