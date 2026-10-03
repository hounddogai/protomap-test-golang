package loyalty

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	treasuryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/treasury/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PointsLedgerValidationServer implements `acme.loyalty.v1.PointsLedgerValidationService`.
// Checks points ledgers against the rules before anything acts on them.
type PointsLedgerValidationServer struct {
	loyaltyv1.UnimplementedPointsLedgerValidationServiceServer
	store                         *Store
	log                           *slog.Logger
	pointsLedgerCalculationClient loyaltyv1.PointsLedgerCalculationServiceClient
	rewardCalculationClient       loyaltyv1.RewardCalculationServiceClient
	ledgerAccountQueryClient      ledgerv1.LedgerAccountQueryServiceClient
	giftCardClient                giftcardsv1.GiftCardServiceClient
	sellerClient                  sellersv1.SellerServiceClient
	meteredCashPosition           meteredCashPosition
	clients                       *registry.Registry
}

// NewPointsLedgerValidationServer wires a PointsLedgerValidationServer.
func NewPointsLedgerValidationServer(deps platform.Deps) *PointsLedgerValidationServer {
	return &PointsLedgerValidationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.loyalty.v1.PointsLedgerValidationService"),
		pointsLedgerCalculationClient: loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		rewardCalculationClient:       loyaltyv1.NewRewardCalculationServiceClient(deps.Conn),
		ledgerAccountQueryClient:      ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		giftCardClient:                giftcardsv1.NewGiftCardServiceClient(deps.Conn),
		sellerClient:                  sellersv1.NewSellerServiceClient(deps.Conn),
		meteredCashPosition:           meteredCashPosition{CashPositionServiceClient: treasuryv1.NewCashPositionServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                       deps.Clients,
	}
}

// ValidatePointsLedger validates a points ledger.
func (s *PointsLedgerValidationServer) ValidatePointsLedger(ctx context.Context, req *loyaltyv1.ValidatePointsLedgerRequest) (*loyaltyv1.ValidatePointsLedgerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidatePointsLedger", "error", err)
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "validate points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	explainPointsLedgerCalculationRequest := newExplainPointsLedgerCalculationRequest(withExplainPointsLedgerCalculationContext(req.GetContext()), withExplainPointsLedgerCalculationPointsLedger(&loyaltyv1.PointsLedger{
		Audit: &commonv1.AuditInfo{CreatedBy: req.GetPointsLedger().GetAudit().GetCreatedBy()},
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetPointsLedger().GetContact().GetPreferredChannel(),
		},
		Holder: &loyaltyv1.RewardRef{
			DisplayName: req.GetPointsLedger().GetHolder().GetDisplayName(),
		},
	}), withExplainPointsLedgerCalculationDetails(&loyaltyv1.ExplainPointsLedgerCalculationRequest_Details{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
		},
	}))
	explainPointsLedgerCalculationReply, err := s.pointsLedgerCalculationClient.ExplainPointsLedgerCalculation(ctx, explainPointsLedgerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/ExplainPointsLedgerCalculation")
	}
	getRewardRatesRequest := &loyaltyv1.GetRewardRatesRequest{
		Context:  req.GetContext(),
		RewardId: req.GetPointsLedger().GetHolder().GetRewardId(),
	}
	var getRewardRatesReply *loyaltyv1.GetRewardRatesResponse
	if err := pipeline.Run(ctx, "acme.loyalty.v1.RewardCalculationService/GetRewardRates", func(ctx context.Context) error {
		answer, err := s.rewardCalculationClient.GetRewardRates(ctx, getRewardRatesRequest)
		getRewardRatesReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/GetRewardRates")
	}
	exportLedgerAccountsStream, err := s.ledgerAccountQueryClient.ExportLedgerAccounts(ctx, &ledgerv1.ExportLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.ExportLedgerAccountsRequest_Filter{
			Labels:       explainPointsLedgerCalculationReply.GetPointsLedger().GetLabels(),
			CreatedAfter: explainPointsLedgerCalculationReply.GetPointsLedger().GetAudit().GetCreatedAt(),
			OwnerId:      explainPointsLedgerCalculationReply.GetPointsLedger().GetHolder().GetRewardId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ExportLedgerAccounts")
	}
	for {
		received, err := exportLedgerAccountsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/ExportLedgerAccounts")
		}
		_ = received
	}
	estimateRewardRequest := &loyaltyv1.EstimateRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				DisplayName: getRewardRatesReply.GetReward().GetOwner().GetDisplayName(),
				Email:       req.GetPointsLedger().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: getRewardRatesReply.GetReward().GetAudit().GetUpdatedAt(),
			},
		},
	}
	estimateRewardReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*loyaltyv1.EstimateRewardResponse, error) {
		return s.rewardCalculationClient.EstimateReward(ctx, estimateRewardRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/EstimateReward")
	}
	return &loyaltyv1.ValidatePointsLedgerResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			PointsLedgerId: explainPointsLedgerCalculationReply.GetPointsLedger().GetPointsLedgerId(),
			Holder: &loyaltyv1.RewardRef{
				DisplayName: getRewardRatesReply.GetReward().GetOwner().GetDisplayName(),
			},
			OpenedAt: req.GetPointsLedger().GetOpenedAt(),
			Labels:   req.GetPointsLedger().GetLabels(),
		},
		Result: &commonv1.OperationResult{
			Message: estimateRewardReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// PreviewPointsLedger previews a points ledger.
func (s *PointsLedgerValidationServer) PreviewPointsLedger(ctx context.Context, req *loyaltyv1.PreviewPointsLedgerRequest) (*loyaltyv1.PreviewPointsLedgerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "preview points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetPointsLedger().GetContact().GetPreferredChannel())
	watchGiftCardsStream, err := s.giftCardClient.WatchGiftCards(ctx, &giftcardsv1.WatchGiftCardsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email:            req.GetDetails().GetContact().GetEmail(),
			Name:             req.GetPointsLedger().GetContact().GetName(),
			Phone:            req.GetDetails().GetContact().GetPhone(),
			PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/WatchGiftCards")
	}
	for {
		received, err := watchGiftCardsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/WatchGiftCards")
		}
		_ = received
	}
	for _, limit := range req.GetPointsLedger().GetLimits() {
		if _, err := s.rewardCalculationClient.RecalculateRewards(ctx, &loyaltyv1.RecalculateRewardsRequest{
			Context: req.GetContext(),
			Reward: &loyaltyv1.Reward{
				Action: &loyaltyv1.Reward_Discount{Discount: &loyaltyv1.RewardDiscountAction{Amount: limit.GetAmount()}},
				Name:   limit.GetName(),
				Owner:  &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/RecalculateRewards")
		}
	}
	return &loyaltyv1.PreviewPointsLedgerResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			PointsLedgerId: req.GetPointsLedgerId(),
			OpenedAt:       req.GetPointsLedger().GetOpenedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckPointsLedgerEligibility checks a points ledger.
func (s *PointsLedgerValidationServer) CheckPointsLedgerEligibility(ctx context.Context, req *loyaltyv1.CheckPointsLedgerEligibilityRequest) (*loyaltyv1.CheckPointsLedgerEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckPointsLedgerEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "check points ledger eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	createSellerReply, err := s.sellerClient.CreateSeller(ctx, &sellersv1.CreateSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPointsLedger().GetAudit().GetCreatedAt(),
				UpdatedBy: req.GetPointsLedger().GetAudit().GetUpdatedBy(),
			},
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetPointsLedger().GetHolder().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/CreateSeller")
	}
	cloneCashPositionRequest := &treasuryv1.CloneCashPositionRequest{
		Context: req.GetContext(),
		CashPosition: &treasuryv1.CashPosition{
			Holder: &treasuryv1.FxHedgeRef{
				Number: createSellerReply.GetSeller().GetPassport().GetNumber(),
			},
			Contact: &commonv1.ContactInfo{
				Email: createSellerReply.GetSeller().GetRelationshipManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPointsLedger().GetAudit().GetEtag(),
				UpdatedAt: req.GetPointsLedger().GetAudit().GetUpdatedAt(),
			},
		},
	}
	cloneCashPositionReply, err := s.meteredCashPosition.CloneCashPosition(ctx, cloneCashPositionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.treasury.v1.CashPositionService/CloneCashPosition")
	}
	return &loyaltyv1.CheckPointsLedgerEligibilityResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: createSellerReply.GetSeller().GetAudit().GetUpdatedAt(),
			},
			Contact: &commonv1.ContactInfo{
				PreferredChannel: cloneCashPositionReply.GetCashPosition().GetContact().GetPreferredChannel(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Available:      req.GetPointsLedger().GetAvailable(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: cloneCashPositionReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ExplainPointsLedgerDecision explains a points ledger.
func (s *PointsLedgerValidationServer) ExplainPointsLedgerDecision(ctx context.Context, req *loyaltyv1.ExplainPointsLedgerDecisionRequest) (*loyaltyv1.ExplainPointsLedgerDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainPointsLedgerDecision", "error", err)
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "explain points ledger decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	searchIndexHistory, ok := s.clients.Get("acme.search.v1.SearchIndexHistoryService").(searchv1.SearchIndexHistoryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SearchIndexHistoryService")
	}
	listSearchIndexVersionsRequest := &searchv1.ListSearchIndexVersionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexVersionsRequest_Filter{
			Labels:       req.GetPointsLedger().GetLabels(),
			CreatedAfter: req.GetPointsLedger().GetAudit().GetUpdatedAt(),
		},
	}
	listSearchIndexVersionsReply, err := searchIndexHistory.ListSearchIndexVersions(ctx, listSearchIndexVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/ListSearchIndexVersions")
	}
	recalculateRewardsRequest := &loyaltyv1.RecalculateRewardsRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPointsLedger().GetAudit().GetUpdatedBy()},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetPointsLedger().GetHolder().GetDisplayName(),
			},
		},
	}
	type recalculateRewardsResult struct {
		reply *loyaltyv1.RecalculateRewardsResponse
		err   error
	}
	recalculateRewardsResults := make(chan recalculateRewardsResult, 1)
	go func() {
		reply, err := s.rewardCalculationClient.RecalculateRewards(ctx, recalculateRewardsRequest)
		recalculateRewardsResults <- recalculateRewardsResult{reply, err}
	}()
	var recalculateRewardsReply *loyaltyv1.RecalculateRewardsResponse
	select {
	case result := <-recalculateRewardsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.loyalty.v1.RewardCalculationService/RecalculateRewards")
		}
		recalculateRewardsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &loyaltyv1.ExplainPointsLedgerDecisionResponse{
		Outcome: &loyaltyv1.ExplainPointsLedgerDecisionResponse_Outcome{
			Message: listSearchIndexVersionsReply.GetResult().GetMessage(),
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculateRewardsReply.GetReward().GetAudit().GetUpdatedBy(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			CurrencyCode:   req.GetPointsLedger().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScorePointsLedger scores a points ledger.
func (s *PointsLedgerValidationServer) ScorePointsLedger(ctx context.Context, req *loyaltyv1.ScorePointsLedgerRequest) (*loyaltyv1.ScorePointsLedgerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScorePointsLedger", "error", err)
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "score points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "updated_by", req.GetPointsLedger().GetAudit().GetUpdatedBy())
	quoteRewardReply, err := s.rewardCalculationClient.QuoteReward(ctx, &loyaltyv1.QuoteRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPointsLedger().GetAudit().GetEtag(),
				UpdatedBy: tenancy.Value(ctx, "updated_by"),
			},
		},
		Details: &loyaltyv1.QuoteRewardRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/QuoteReward")
	}
	return &loyaltyv1.ScorePointsLedgerResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quoteRewardReply.GetReward().GetAudit().GetUpdatedAt(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Holder:         req.GetPointsLedger().GetHolder(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPointsLedgerRules returns a points ledger.
func (s *PointsLedgerValidationServer) GetPointsLedgerRules(ctx context.Context, req *loyaltyv1.GetPointsLedgerRulesRequest) (*loyaltyv1.GetPointsLedgerRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPointsLedgerRules", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "points_ledger", req.GetPointsLedgerId(), "get_points_ledger_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get points ledger rules", "tenant", req.GetContext().GetTenantId())
	return &loyaltyv1.GetPointsLedgerRulesResponse{
		PointsLedger: &loyaltyv1.PointsLedger{PointsLedgerId: req.GetPointsLedgerId()},
		Result:       &commonv1.OperationResult{Status: "ok"},
	}, nil
}
