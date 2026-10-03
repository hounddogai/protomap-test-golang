package loyalty

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// TierCalculationServer implements `acme.loyalty.v1.TierCalculationService`.
// Computes the figures of a tier, such as totals, taxes, and estimates.
type TierCalculationServer struct {
	loyaltyv1.UnimplementedTierCalculationServiceServer
	store                             *Store
	log                               *slog.Logger
	reviewVoteCalculationClient       reviewsv1.ReviewVoteCalculationServiceClient
	loyaltyMemberCalculationClient    loyaltyv1.LoyaltyMemberCalculationServiceClient
	pointsLedgerCalculationClient     loyaltyv1.PointsLedgerCalculationServiceClient
	meteredApiCredentialNotification  meteredApiCredentialNotification
	bus                               *eventbus.Bus
	conn                              *grpc.ClientConn
	reviewModerationCalculationOnce   sync.Once
	reviewModerationCalculationCached reviewsv1.ReviewModerationCalculationServiceClient
	loyaltyMemberCalculationPort      loyaltyMemberCalculationPort
	rewardCalculationClient           loyaltyv1.RewardCalculationServiceClient
	sellerScorecardCalculationClient  sellersv1.SellerScorecardCalculationServiceClient
	pointsLedgerValidationClient      loyaltyv1.PointsLedgerValidationServiceClient
	sellerScorecardValidationClient   sellersv1.SellerScorecardValidationServiceClient
}

// NewTierCalculationServer wires a TierCalculationServer.
func NewTierCalculationServer(deps platform.Deps) *TierCalculationServer {
	return &TierCalculationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.loyalty.v1.TierCalculationService"),
		reviewVoteCalculationClient:      reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn),
		loyaltyMemberCalculationClient:   loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		pointsLedgerCalculationClient:    loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		meteredApiCredentialNotification: meteredApiCredentialNotification{ApiCredentialNotificationServiceClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		bus:                              deps.Bus,
		conn:                             deps.Conn,
		loyaltyMemberCalculationPort:     newLoyaltyMemberCalculationPort(deps),
		rewardCalculationClient:          loyaltyv1.NewRewardCalculationServiceClient(deps.Conn),
		sellerScorecardCalculationClient: sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
		pointsLedgerValidationClient:     loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn),
		sellerScorecardValidationClient:  sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
	}
}

// CalculateTier calculates a tier.
func (s *TierCalculationServer) CalculateTier(ctx context.Context, req *loyaltyv1.CalculateTierRequest) (*loyaltyv1.CalculateTierResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "calculate tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	source := req.GetTier().GetOwner().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	recalculateReviewVotesRequest := newRecalculateReviewVotesRequest(withRecalculateReviewVotesContext(req.GetContext()), withRecalculateReviewVotesReviewVote(&reviewsv1.ReviewVote{Subject: &customersv1.CustomerRef{DisplayName: displayName}}), withRecalculateReviewVotesDetails(&reviewsv1.RecalculateReviewVotesRequest_Details{
		Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
	}))
	recalculateReviewVotesReply, err := s.reviewVoteCalculationClient.RecalculateReviewVotes(ctx, recalculateReviewVotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/RecalculateReviewVotes")
	}
	encoded, err := protojson.Marshal(req.GetTier())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded loyaltyv1.Tier
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	explainLoyaltyMemberCalculationReply, err := s.loyaltyMemberCalculationClient.ExplainLoyaltyMemberCalculation(ctx, &loyaltyv1.ExplainLoyaltyMemberCalculationRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{CreatedBy: (&decoded).GetAudit().GetCreatedBy()},
		},
		Details: &loyaltyv1.ExplainLoyaltyMemberCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetTier().GetOwner().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/ExplainLoyaltyMemberCalculation")
	}
	recalculatePointsLedgersReply, err := s.pointsLedgerCalculationClient.RecalculatePointsLedgers(ctx, &loyaltyv1.RecalculatePointsLedgersRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Funding: &loyaltyv1.PointsLedger_Card{Card: &loyaltyv1.PointsLedgerCardFunding{
				Token: explainLoyaltyMemberCalculationReply.GetLoyaltyMember().GetCard().GetToken(),
			}},
			Holder: &loyaltyv1.RewardRef{
				RewardId:    explainLoyaltyMemberCalculationReply.GetLoyaltyMember().GetHolder().GetRewardId(),
				DisplayName: req.GetTier().GetOwner().GetDisplayName(),
			},
		},
		Details: &loyaltyv1.RecalculatePointsLedgersRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/RecalculatePointsLedgers")
	}
	return &loyaltyv1.CalculateTierResponse{
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				Email:       recalculateReviewVotesReply.GetReviewVote().GetSubject().GetEmail(),
				DisplayName: recalculateReviewVotesReply.GetReviewVote().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainLoyaltyMemberCalculationReply.GetLoyaltyMember().GetAudit().GetCreatedAt(),
				Etag:      recalculatePointsLedgersReply.GetPointsLedger().GetAudit().GetEtag(),
			},
			TierId:  req.GetTierId(),
			Version: req.GetTier().GetVersion(),
		},
		Breakdown: &loyaltyv1.CalculateTierResponse_Breakdown{
			CurrencyCode: explainLoyaltyMemberCalculationReply.GetLoyaltyMember().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EstimateTier estimates a tier.
func (s *TierCalculationServer) EstimateTier(ctx context.Context, req *loyaltyv1.EstimateTierRequest) (*loyaltyv1.EstimateTierResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "estimate tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetTier()).(*loyaltyv1.Tier)
	packed, err := anypb.New(req.GetTier())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifyApiCredentialWatchersRequest := &identityv1.NotifyApiCredentialWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyApiCredentialWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: cloned.GetOwner().GetEmail()},
			Payload:   packed,
		},
	}
	notifyApiCredentialWatchersReply, err := s.meteredApiCredentialNotification.NotifyApiCredentialWatchers(ctx, notifyApiCredentialWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/NotifyApiCredentialWatchers")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "loyalty.estimate_tier.check_ledger_account_eligibility",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"ledger_account.card.holder_name": notifyApiCredentialWatchersReply.GetApiCredential().GetCard().GetHolderName(),
			"details.contact.email":           notifyApiCredentialWatchersReply.GetApiCredential().GetContact().GetEmail(),
			"ledger_account.audit.updated_by": req.GetTier().GetAudit().GetUpdatedBy(),
			"ledger_account.contact.phone":    notifyApiCredentialWatchersReply.GetApiCredential().GetContact().GetPhone(),
			"context.tenant_id":               req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	step := &tierCalculationServerEstimateTierExplainLoyaltyMemberCalculationStep{request: &loyaltyv1.ExplainLoyaltyMemberCalculationRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Funding: &loyaltyv1.LoyaltyMember_Bank{Bank: &loyaltyv1.LoyaltyMemberBankFunding{
				AccountHolder: notifyApiCredentialWatchersReply.GetApiCredential().GetBank().GetAccountHolder(),
			}},
		},
	}}
	if err := s.runEstimateTierExplainLoyaltyMemberCalculation(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/ExplainLoyaltyMemberCalculation")
	}
	explainLoyaltyMemberCalculationReply := step.reply
	recalculateReviewModerationsRequest := &reviewsv1.RecalculateReviewModerationsRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: notifyApiCredentialWatchersReply.GetApiCredential().GetContact().GetPreferredChannel(),
			},
		},
	}
	recalculateReviewModerationsReply, err := s.reviewModerationCalculationLazy().RecalculateReviewModerations(ctx, recalculateReviewModerationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/RecalculateReviewModerations")
	}
	return &loyaltyv1.EstimateTierResponse{
		Result: &commonv1.OperationResult{
			Message: notifyApiCredentialWatchersReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				Email:       explainLoyaltyMemberCalculationReply.GetLoyaltyMember().GetContact().GetEmail(),
				DisplayName: recalculateReviewModerationsReply.GetReviewModeration().GetCustomer().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainLoyaltyMemberCalculationReply.GetLoyaltyMember().GetAudit().GetCreatedBy(),
				UpdatedAt: recalculateReviewModerationsReply.GetReviewModeration().GetAudit().GetUpdatedAt(),
			},
			TierId: req.GetTierId(),
		},
		Breakdown: &loyaltyv1.EstimateTierResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// QuoteTier quotes a tier.
func (s *TierCalculationServer) QuoteTier(ctx context.Context, req *loyaltyv1.QuoteTierRequest) (*loyaltyv1.QuoteTierResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteTier", "error", err)
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "quote tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	compareLoyaltyMemberOptionsRequest := &loyaltyv1.CompareLoyaltyMemberOptionsRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetTier().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetTier().GetAudit().GetCreatedAt(),
			},
			Holder: &loyaltyv1.RewardRef{DisplayName: req.GetTier().GetOwner().GetDisplayName()},
		},
		Details: &loyaltyv1.CompareLoyaltyMemberOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
		},
	}
	compareLoyaltyMemberOptionsReply, err := s.loyaltyMemberCalculationPort.CompareLoyaltyMemberOptions(ctx, compareLoyaltyMemberOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CompareLoyaltyMemberOptions")
	}
	return &loyaltyv1.QuoteTierResponse{
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareLoyaltyMemberOptionsReply.GetLoyaltyMember().GetAudit().GetCreatedAt(),
			},
			TierId:  req.GetTierId(),
			Version: req.GetTier().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareTierOptions compares a tier.
func (s *TierCalculationServer) CompareTierOptions(ctx context.Context, req *loyaltyv1.CompareTierOptionsRequest) (*loyaltyv1.CompareTierOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "compare tier options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetTier()).(*loyaltyv1.Tier)
	compareLoyaltyMemberOptionsRequest := &loyaltyv1.CompareLoyaltyMemberOptionsRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.CompareLoyaltyMemberOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetTier().GetAudit().GetCreatedAt(),
				UpdatedAt: cloned.GetAudit().GetUpdatedAt(),
				Etag:      req.GetTier().GetAudit().GetEtag(),
			},
		},
	}
	compareLoyaltyMemberOptionsReply := new(loyaltyv1.CompareLoyaltyMemberOptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.LoyaltyMemberCalculationService_CompareLoyaltyMemberOptions_FullMethodName, compareLoyaltyMemberOptionsRequest, compareLoyaltyMemberOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CompareLoyaltyMemberOptions")
	}
	compareRewardOptionsRequest := &loyaltyv1.CompareRewardOptionsRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.CompareRewardOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				Etag: compareLoyaltyMemberOptionsReply.GetLoyaltyMember().GetAudit().GetEtag(),
			},
		},
	}
	compareRewardOptionsReply, err := grpcx.Call(ctx, s.rewardCalculationClient.CompareRewardOptions, compareRewardOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/CompareRewardOptions")
	}
	recalculateSellerScorecardsRequest := &sellersv1.RecalculateSellerScorecardsRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Tax: &sellersv1.SellerScorecardLineTax{Amount: req.GetTier().GetDiscount().GetAmount()},
			}},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	}
	type recalculateSellerScorecardsResult struct {
		reply *sellersv1.RecalculateSellerScorecardsResponse
		err   error
	}
	recalculateSellerScorecardsResults := make(chan recalculateSellerScorecardsResult, 1)
	go func() {
		reply, err := s.sellerScorecardCalculationClient.RecalculateSellerScorecards(ctx, recalculateSellerScorecardsRequest)
		recalculateSellerScorecardsResults <- recalculateSellerScorecardsResult{reply, err}
	}()
	var recalculateSellerScorecardsReply *sellersv1.RecalculateSellerScorecardsResponse
	select {
	case result := <-recalculateSellerScorecardsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerScorecardCalculationService/RecalculateSellerScorecards")
		}
		recalculateSellerScorecardsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	scorePointsLedgerReply, err := s.pointsLedgerValidationClient.ScorePointsLedger(ctx, &loyaltyv1.ScorePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetTier().GetAudit().GetUpdatedAt()},
			Funding: &loyaltyv1.PointsLedger_Bank{Bank: &loyaltyv1.PointsLedgerBankFunding{
				Bic: recalculateSellerScorecardsReply.GetSellerScorecard().GetBank().GetBic(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ScorePointsLedger")
	}
	_ = scorePointsLedgerReply
	return &loyaltyv1.CompareTierOptionsResponse{
		Breakdown: &loyaltyv1.CompareTierOptionsResponse_Breakdown{
			Total: compareLoyaltyMemberOptionsReply.GetBreakdown().GetTotal(),
			Lines: []*loyaltyv1.CompareTierOptionsResponse_Breakdown_Line{&loyaltyv1.CompareTierOptionsResponse_Breakdown_Line{
				Amount: compareRewardOptionsReply.GetReward().GetDiscount().GetAmount(),
			}},
		},
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				DisplayName: compareRewardOptionsReply.GetReward().GetOwner().GetDisplayName(),
				Email:       recalculateSellerScorecardsReply.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
			TierId:     req.GetTierId(),
			Parameters: req.GetTier().GetParameters(),
		},
		Result: &commonv1.OperationResult{
			Status: recalculateSellerScorecardsReply.GetResult().GetStatus(),
		},
	}, nil
}

// RecalculateTiers recalculates a tier.
func (s *TierCalculationServer) RecalculateTiers(ctx context.Context, req *loyaltyv1.RecalculateTiersRequest) (*loyaltyv1.RecalculateTiersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "recalculate tiers", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	simulateLoyaltyMemberRequest := &loyaltyv1.SimulateLoyaltyMemberRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.SimulateLoyaltyMemberRequest_Details{
			Destination: &commonv1.PostalAddress{Recipient: req.GetDetails().GetDestination().GetRecipient()},
		},
	}
	simulateLoyaltyMemberReply, err := grpcx.Call(ctx, s.loyaltyMemberCalculationClient.SimulateLoyaltyMember, simulateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/SimulateLoyaltyMember")
	}
	return &loyaltyv1.RecalculateTiersResponse{
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulateLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
				CreatedAt: simulateLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetCreatedAt(),
			},
			TierId:      req.GetTierId(),
			Description: req.GetTier().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SimulateTier simulates a tier.
func (s *TierCalculationServer) SimulateTier(ctx context.Context, req *loyaltyv1.SimulateTierRequest) (*loyaltyv1.SimulateTierResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SimulateTier", "error", err)
		return nil, err
	}
	if err := s.store.SaveTier(ctx, req.GetContext().GetTenantId(), req.GetTier()); err != nil {
		return nil, grpcx.Wrap(err, "save tier")
	}
	s.log.InfoContext(ctx, "simulate tier", "tenant", req.GetContext().GetTenantId(), "etag", req.GetTier().GetAudit().GetEtag())
	tierCache := cache.New[string, *loyaltyv1.Tier](1)
	tierCache.Put(req.GetContext().GetTraceId(), req.GetTier())
	cachedTier, _ := tierCache.Get(req.GetContext().GetTraceId())
	scoreSellerScorecardRequest := &sellersv1.ScoreSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
				Tax: &sellersv1.SellerScorecardLineTax{Amount: req.GetTier().GetDiscount().GetAmount()},
			}},
			BillingAddress: &commonv1.PostalAddress{
				Locality:    req.GetDetails().GetDestination().GetLocality(),
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{CreatedBy: cachedTier.GetAudit().GetCreatedBy()},
		},
	}
	scoreSellerScorecardReply, err := sellersv1.SellerScorecardValidationServiceClient.ScoreSellerScorecard(s.sellerScorecardValidationClient, ctx, scoreSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ScoreSellerScorecard")
	}
	recalculateGiftCardBatchesRequest := &giftcardsv1.RecalculateGiftCardBatchesRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Settlement: &giftcardsv1.GiftCardBatch_Bank{Bank: &giftcardsv1.GiftCardBatchBankSettlement{
				AccountHolder: scoreSellerScorecardReply.GetSellerScorecard().GetBank().GetAccountHolder(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: scoreSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetPostalCode(),
			},
		},
		Details: &giftcardsv1.RecalculateGiftCardBatchesRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	}
	recalculateGiftCardBatchesReply := new(giftcardsv1.RecalculateGiftCardBatchesResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardBatchCalculationService_RecalculateGiftCardBatches_FullMethodName, recalculateGiftCardBatchesRequest, recalculateGiftCardBatchesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/RecalculateGiftCardBatches")
	}
	return &loyaltyv1.SimulateTierResponse{
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{
				Email:       scoreSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetEmail(),
				DisplayName: recalculateGiftCardBatchesReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
			TierId: req.GetTierId(),
			Status: req.GetTier().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: recalculateGiftCardBatchesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetTierRates returns a tier.
func (s *TierCalculationServer) GetTierRates(ctx context.Context, req *loyaltyv1.GetTierRatesRequest) (*loyaltyv1.GetTierRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetTierRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tier", req.GetTierId(), "get_tier_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get tier rates", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	recalculateLoyaltyMembersRequest := &loyaltyv1.RecalculateLoyaltyMembersRequest{}
	recalculateLoyaltyMembersRequest.Context = req.GetContext()
	recalculateLoyaltyMembersRequest.LoyaltyMember = &loyaltyv1.LoyaltyMember{
		Contact: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
		Funding: &loyaltyv1.LoyaltyMember_ExternalAccountId{ExternalAccountId: req.GetTierId()},
	}
	recalculateLoyaltyMembersReply, err := s.loyaltyMemberCalculationClient.RecalculateLoyaltyMembers(ctx, recalculateLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/RecalculateLoyaltyMembers")
	}
	return &loyaltyv1.GetTierRatesResponse{
		Result: &commonv1.OperationResult{
			Status:        recalculateLoyaltyMembersReply.GetResult().GetStatus(),
			CorrelationId: recalculateLoyaltyMembersReply.GetResult().GetCorrelationId(),
		},
		Tier: &loyaltyv1.Tier{TierId: req.GetTierId()},
	}, nil
}

// tierCalculationServerEstimateTierExplainLoyaltyMemberCalculationStep carries the request of one step of EstimateTier
// and, once it ran, its reply.
type tierCalculationServerEstimateTierExplainLoyaltyMemberCalculationStep struct {
	request *loyaltyv1.ExplainLoyaltyMemberCalculationRequest
	reply   *loyaltyv1.ExplainLoyaltyMemberCalculationResponse
}

func (s *TierCalculationServer) runEstimateTierExplainLoyaltyMemberCalculation(ctx context.Context, step *tierCalculationServerEstimateTierExplainLoyaltyMemberCalculationStep) error {
	reply, err := s.loyaltyMemberCalculationClient.ExplainLoyaltyMemberCalculation(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// reviewModerationCalculationLazy dials acme.reviews.v1.ReviewModerationCalculationService the first time a handler
// needs it.
func (s *TierCalculationServer) reviewModerationCalculationLazy() reviewsv1.ReviewModerationCalculationServiceClient {
	s.reviewModerationCalculationOnce.Do(func() {
		s.reviewModerationCalculationCached = reviewsv1.NewReviewModerationCalculationServiceClient(s.conn)
	})
	return s.reviewModerationCalculationCached
}
