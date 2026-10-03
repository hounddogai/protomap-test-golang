package loyalty

import (
	"context"
	"fmt"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	riskv1 "github.com/hounddogai/protomap-test-golang/gen/acme/risk/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// LoyaltyMemberServer implements `acme.loyalty.v1.LoyaltyMemberService`.
// Manages loyalty members: creates, reads, changes, and lists them.
type LoyaltyMemberServer struct {
	*loyaltyMemberHandlers
}

// loyaltyMemberHandlers holds the handlers of LoyaltyMemberServer, with what they use.
type loyaltyMemberHandlers struct {
	loyaltyv1.UnimplementedLoyaltyMemberServiceServer
	store                               *Store
	log                                 *slog.Logger
	conn                                *grpc.ClientConn
	tierCalculationClient               loyaltyv1.TierCalculationServiceClient
	rewardCalculationClient             loyaltyv1.RewardCalculationServiceClient
	simulatePointsLedgerFn              func(context.Context, *loyaltyv1.SimulatePointsLedgerRequest, ...grpc.CallOption) (*loyaltyv1.SimulatePointsLedgerResponse, error)
	sellerScorecardValidationClient     sellersv1.SellerScorecardValidationServiceClient
	clients                             *registry.Registry
	riskRegisterClient                  riskv1.RiskRegisterServiceClient
	tenantExportClient                  identityv1.TenantExportServiceClient
	rewardValidationClient              loyaltyv1.RewardValidationServiceClient
	tierValidationClient                loyaltyv1.TierValidationServiceClient
	giftCardTransactionValidationClient giftcardsv1.GiftCardTransactionValidationServiceClient
	rewardValidationPort                rewardValidationPort
}

// NewLoyaltyMemberServer wires a LoyaltyMemberServer.
func NewLoyaltyMemberServer(deps platform.Deps) *LoyaltyMemberServer {
	pointsLedgerCalculation := loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn)
	return &LoyaltyMemberServer{loyaltyMemberHandlers: &loyaltyMemberHandlers{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.loyalty.v1.LoyaltyMemberService"),
		conn:                                deps.Conn,
		tierCalculationClient:               loyaltyv1.NewTierCalculationServiceClient(deps.Conn),
		rewardCalculationClient:             loyaltyv1.NewRewardCalculationServiceClient(deps.Conn),
		simulatePointsLedgerFn:              pointsLedgerCalculation.SimulatePointsLedger,
		sellerScorecardValidationClient:     sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		clients:                             deps.Clients,
		riskRegisterClient:                  riskv1.NewRiskRegisterServiceClient(deps.Conn),
		tenantExportClient:                  identityv1.NewTenantExportServiceClient(deps.Conn),
		rewardValidationClient:              loyaltyv1.NewRewardValidationServiceClient(deps.Conn),
		tierValidationClient:                loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		giftCardTransactionValidationClient: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
		rewardValidationPort:                newRewardValidationPort(deps),
	}}
}

// CreateLoyaltyMember creates a loyalty member.
func (s *loyaltyMemberHandlers) CreateLoyaltyMember(ctx context.Context, req *loyaltyv1.CreateLoyaltyMemberRequest) (*loyaltyv1.CreateLoyaltyMemberResponse, error) {
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "create loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	lookupLoyaltyMemberRequest := &loyaltyv1.LookupLoyaltyMemberRequest{
		Context:         req.GetContext(),
		LoyaltyMemberId: req.GetLoyaltyMember().GetLoyaltyMemberId(),
	}
	lookupLoyaltyMemberReply := new(loyaltyv1.LookupLoyaltyMemberResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", loyaltyLoyaltyMemberQueryServiceName, "LookupLoyaltyMember"), lookupLoyaltyMemberRequest, lookupLoyaltyMemberReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/LookupLoyaltyMember")
	}
	recalculateTiersReply, err := s.tierCalculationClient.RecalculateTiers(ctx, &loyaltyv1.RecalculateTiersRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetLoyaltyMember().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierCalculationService/RecalculateTiers")
	}
	quoteRewardRequest := &loyaltyv1.QuoteRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				DisplayName: lookupLoyaltyMemberReply.GetLoyaltyMember().GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: lookupLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetCreatedBy(),
				CreatedAt: recalculateTiersReply.GetTier().GetAudit().GetCreatedAt(),
			},
		},
	}
	quoteRewardReply, err := loyaltyv1.RewardCalculationServiceClient.QuoteReward(s.rewardCalculationClient, ctx, quoteRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/QuoteReward")
	}
	return &loyaltyv1.CreateLoyaltyMemberResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Balance: lookupLoyaltyMemberReply.GetLoyaltyMember().GetBalance(),
			Holder: &loyaltyv1.RewardRef{
				Version: lookupLoyaltyMemberReply.GetLoyaltyMember().GetHolder().GetVersion(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: recalculateTiersReply.GetTier().GetAudit().GetCreatedAt(),
				Etag:      quoteRewardReply.GetReward().GetAudit().GetEtag(),
			},
			CurrencyCode: recalculateTiersReply.GetBreakdown().GetCurrencyCode(),
			Status:       req.GetLoyaltyMember().GetStatus(),
			Available:    req.GetLoyaltyMember().GetAvailable(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetLoyaltyMember returns a loyalty member.
func (s *loyaltyMemberHandlers) GetLoyaltyMember(ctx context.Context, req *loyaltyv1.GetLoyaltyMemberRequest) (*loyaltyv1.GetLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetLoyaltyMember", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "loyalty_member", req.GetLoyaltyMemberId(), "get_loyalty_member"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get loyalty member", "tenant", req.GetContext().GetTenantId())
	simulatePointsLedgerRequest := &loyaltyv1.SimulatePointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
	}
	simulatePointsLedgerReply, err := s.simulatePointsLedgerFn(ctx, simulatePointsLedgerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/SimulatePointsLedger")
	}
	lintLoyaltyMembersRequest := &loyaltyv1.LintLoyaltyMembersRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{
				Token: simulatePointsLedgerReply.GetPointsLedger().GetCard().GetToken(),
			}},
			Holder: &loyaltyv1.RewardRef{
				Version: simulatePointsLedgerReply.GetPointsLedger().GetHolder().GetVersion(),
			},
		},
		Details: &loyaltyv1.LintLoyaltyMembersRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: simulatePointsLedgerReply.GetPointsLedger().GetContact().GetPreferredChannel(),
			},
		},
	}
	lintLoyaltyMembersReply := new(loyaltyv1.LintLoyaltyMembersResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.LoyaltyMemberValidationService_LintLoyaltyMembers_FullMethodName, lintLoyaltyMembersRequest, lintLoyaltyMembersReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/LintLoyaltyMembers")
	}
	return &loyaltyv1.GetLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: simulatePointsLedgerReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: lintLoyaltyMembersReply.GetLoyaltyMember().GetContact().GetPreferredChannel(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
		},
	}, nil
}

// UpdateLoyaltyMember changes a loyalty member.
func (s *loyaltyMemberHandlers) UpdateLoyaltyMember(ctx context.Context, req *loyaltyv1.UpdateLoyaltyMemberRequest) (*loyaltyv1.UpdateLoyaltyMemberResponse, error) {
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "update loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*sellersv1.ScoreSellerScorecardResponse, len(req.GetLoyaltyMember().GetLimits()))
	for i, limit := range req.GetLoyaltyMember().GetLimits() {
		group.Go(func() error {
			reply, err := s.sellerScorecardValidationClient.ScoreSellerScorecard(groupCtx, &sellersv1.ScoreSellerScorecardRequest{
				Context: req.GetContext(),
				SellerScorecard: &sellersv1.SellerScorecard{
					Lines: []*sellersv1.SellerScorecardLine{&sellersv1.SellerScorecardLine{
						Tax: &sellersv1.SellerScorecardLineTax{Amount: limit.GetAmount()},
					}},
					Settlement: &sellersv1.SellerScorecard_Bank{Bank: &sellersv1.SellerScorecardBankSettlement{Iban: req.GetLoyaltyMember().GetBank().GetIban()}},
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ScoreSellerScorecard")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	recalculateRewardsRequest := &loyaltyv1.RecalculateRewardsRequest{}
	recalculateRewardsRequest.Context = req.GetContext()
	recalculateRewardsReply, err := s.rewardCalculationClient.RecalculateRewards(ctx, recalculateRewardsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/RecalculateRewards")
	}
	sessionQuery, err := registry.Lookup[identityv1.SessionQueryServiceClient](s.clients, "acme.identity.v1.SessionQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	findSessionsRequest := &identityv1.FindSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.FindSessionsRequest_Filter{
			Labels:       req.GetLoyaltyMember().GetLabels(),
			CreatedAfter: recalculateRewardsReply.GetReward().GetAudit().GetCreatedAt(),
		},
	}
	findSessionsReply, err := sessionQuery.FindSessions(ctx, findSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/FindSessions")
	}
	return &loyaltyv1.UpdateLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			Message:      recalculateRewardsReply.GetResult().GetMessage(),
			ReferenceIds: findSessionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: recalculateRewardsReply.GetReward().GetOwner().GetDisplayName(),
			},
			OpenedAt: req.GetLoyaltyMember().GetOpenedAt(),
			Contact:  req.GetLoyaltyMember().GetContact(),
		},
	}, nil
}

// DeleteLoyaltyMember deletes a loyalty member.
func (s *loyaltyMemberHandlers) DeleteLoyaltyMember(ctx context.Context, req *loyaltyv1.DeleteLoyaltyMemberRequest) (*loyaltyv1.DeleteLoyaltyMemberResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "loyalty_member", req.GetLoyaltyMemberId(), "delete_loyalty_member"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete loyalty member", "tenant", req.GetContext().GetTenantId())
	previewSellerPayoutScheduleRequest := &sellersv1.PreviewSellerPayoutScheduleRequest{
		Context:              req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
		Reason:               req.GetReason(),
	}
	previewSellerPayoutScheduleReply := new(sellersv1.PreviewSellerPayoutScheduleResponse)
	if err := s.conn.Invoke(ctx, sellersSellerPayoutScheduleValidationServicePreviewSellerPayoutScheduleMethod, previewSellerPayoutScheduleRequest, previewSellerPayoutScheduleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/PreviewSellerPayoutSchedule")
	}
	simulateLoyaltyMemberRequest := &loyaltyv1.SimulateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				CreatedAt: previewSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetAudit().GetCreatedAt(),
			},
			Holder: &loyaltyv1.RewardRef{
				DisplayName: previewSellerPayoutScheduleReply.GetSellerPayoutSchedule().GetOrganizer().GetDisplayName(),
			},
		},
	}
	simulateLoyaltyMemberReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewLoyaltyMemberCalculationServiceClient).SimulateLoyaltyMember(ctx, simulateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/SimulateLoyaltyMember")
	}
	_ = simulateLoyaltyMemberReply
	return &loyaltyv1.DeleteLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			Message: previewSellerPayoutScheduleReply.GetResult().GetMessage(),
			Status:  previewSellerPayoutScheduleReply.GetResult().GetStatus(),
		},
	}, nil
}

// ListLoyaltyMembers lists loyalty members a page at a time.
func (s *loyaltyMemberHandlers) ListLoyaltyMembers(ctx context.Context, req *loyaltyv1.ListLoyaltyMembersRequest) (*loyaltyv1.ListLoyaltyMembersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list loyalty members", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	countAccessPoliciesRequest := &authorizationv1.CountAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.CountAccessPoliciesRequest_Filter{
			OwnerId: req.GetFilter().GetOwnerId(),
			Region:  tenancy.Value(ctx, "region"),
			Labels:  req.GetFilter().GetLabels(),
		},
	}
	countAccessPoliciesReply := new(authorizationv1.CountAccessPoliciesResponse)
	if err := grpcx.Invoke(ctx, s.conn, authorizationv1.AccessPolicyQueryService_CountAccessPolicies_FullMethodName, countAccessPoliciesRequest, countAccessPoliciesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyQueryService/CountAccessPolicies")
	}
	batchGetRiskRegistersRequest := &riskv1.BatchGetRiskRegistersRequest{Context: req.GetContext()}
	type batchGetRiskRegistersResult struct {
		reply *riskv1.BatchGetRiskRegistersResponse
		err   error
	}
	batchGetRiskRegistersResults := make(chan batchGetRiskRegistersResult, 1)
	go func() {
		reply, err := s.riskRegisterClient.BatchGetRiskRegisters(ctx, batchGetRiskRegistersRequest)
		batchGetRiskRegistersResults <- batchGetRiskRegistersResult{reply, err}
	}()
	var batchGetRiskRegistersReply *riskv1.BatchGetRiskRegistersResponse
	select {
	case result := <-batchGetRiskRegistersResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.risk.v1.RiskRegisterService/BatchGetRiskRegisters")
		}
		batchGetRiskRegistersReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &loyaltyv1.ListLoyaltyMembersResponse{
		Result: &commonv1.OperationResult{
			Status:  countAccessPoliciesReply.GetResult().GetStatus(),
			Message: countAccessPoliciesReply.GetResult().GetMessage(),
		},
		PageInfo: &commonv1.PageInfo{
			NextPageToken: batchGetRiskRegistersReply.GetPageInfo().GetNextPageToken(),
		},
	}, nil
}

// BatchGetLoyaltyMembers returns several loyalty members at once.
func (s *loyaltyMemberHandlers) BatchGetLoyaltyMembers(ctx context.Context, req *loyaltyv1.BatchGetLoyaltyMembersRequest) (*loyaltyv1.BatchGetLoyaltyMembersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "batch get loyalty members", "tenant", req.GetContext().GetTenantId())
	retryTenantExportRequest := buildRetryTenantExportRequestForBatchGetLoyaltyMembers(req)
	retryTenantExportReply, err := s.tenantExportClient.RetryTenantExport(ctx, retryTenantExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/RetryTenantExport")
	}
	return &loyaltyv1.BatchGetLoyaltyMembersResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: retryTenantExportReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// SearchLoyaltyMembers lists loyalty members a page at a time.
func (s *loyaltyMemberHandlers) SearchLoyaltyMembers(ctx context.Context, req *loyaltyv1.SearchLoyaltyMembersRequest) (*loyaltyv1.SearchLoyaltyMembersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "search loyalty members", "tenant", req.GetContext().GetTenantId())
	return &loyaltyv1.SearchLoyaltyMembersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// WatchLoyaltyMembers streams the changes of loyalty members as they happen.
func (s *loyaltyMemberHandlers) WatchLoyaltyMembers(req *loyaltyv1.WatchLoyaltyMembersRequest, stream grpc.ServerStreamingServer[loyaltyv1.WatchLoyaltyMembersResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected WatchLoyaltyMembers", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "watch loyalty members", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetSubscriber())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.ContactInfo
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	ctx = tenancy.With(ctx, "email", req.GetSubscriber().GetEmail())
	normalizeSellerPayoutScheduleRequest := &sellersv1.NormalizeSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Name:             (&decoded).GetName(),
					Email:            tenancy.Value(ctx, "email"),
					PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
				},
			}},
		},
	}
	normalizeSellerPayoutScheduleReply := new(sellersv1.NormalizeSellerPayoutScheduleResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerPayoutScheduleValidationServiceName, "NormalizeSellerPayoutSchedule"), normalizeSellerPayoutScheduleRequest, normalizeSellerPayoutScheduleReply); err != nil {
		return grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleValidationService/NormalizeSellerPayoutSchedule")
	}
	if err := stream.Send(&loyaltyv1.WatchLoyaltyMembersResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: normalizeSellerPayoutScheduleReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetSubscriber().GetName(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
			},
		},
	}); err != nil {
		return err
	}
	return nil
}

// CloneLoyaltyMember creates a loyalty member.
func (s *loyaltyMemberHandlers) CloneLoyaltyMember(ctx context.Context, req *loyaltyv1.CloneLoyaltyMemberRequest) (*loyaltyv1.CloneLoyaltyMemberResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "clone loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	normalizeRewardRequest := &loyaltyv1.NormalizeRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetLoyaltyMember().GetAudit().GetEtag(),
				CreatedBy: req.GetLoyaltyMember().GetAudit().GetCreatedBy(),
			},
		},
	}
	var normalizeRewardReply *loyaltyv1.NormalizeRewardResponse
	if err := pipeline.Run(ctx, "acme.loyalty.v1.RewardValidationService/NormalizeReward", func(ctx context.Context) error {
		answer, err := s.rewardValidationClient.NormalizeReward(ctx, normalizeRewardRequest)
		normalizeRewardReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/NormalizeReward")
	}
	explainTierDecisionRequest := &loyaltyv1.ExplainTierDecisionRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetLoyaltyMember().GetAudit().GetCreatedBy()},
			Action: &loyaltyv1.Tier_Discount{Discount: &loyaltyv1.TierDiscountAction{
				Percent: normalizeRewardReply.GetReward().GetDiscount().GetPercent(),
			}},
		},
		Details: &loyaltyv1.ExplainTierDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetLoyaltyMember().GetContact().GetPreferredChannel(),
			},
		},
	}
	explainTierDecisionReply, err := grpcx.Call(ctx, s.tierValidationClient.ExplainTierDecision, explainTierDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/ExplainTierDecision")
	}
	packed, err := anypb.New(req.GetLoyaltyMember())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	checkGiftCardTransactionEligibilityRequest := &giftcardsv1.CheckGiftCardTransactionEligibilityRequest{}
	checkGiftCardTransactionEligibilityRequest.Context = req.GetContext()
	checkGiftCardTransactionEligibilityRequest.Details = &giftcardsv1.CheckGiftCardTransactionEligibilityRequest_Details{
		Contact: &commonv1.ContactInfo{
			Name:  req.GetLoyaltyMember().GetContact().GetName(),
			Email: explainTierDecisionReply.GetTier().GetOwner().GetEmail(),
			Phone: req.GetLoyaltyMember().GetContact().GetPhone(),
		},
	}
	checkGiftCardTransactionEligibilityRequest.GiftCardTransaction = &giftcardsv1.GiftCardTransaction{Detail: packed}
	checkGiftCardTransactionEligibilityReply, err := s.giftCardTransactionValidationClient.CheckGiftCardTransactionEligibility(ctx, checkGiftCardTransactionEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/CheckGiftCardTransactionEligibility")
	}
	previewRewardRequest := &loyaltyv1.PreviewRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				Email: normalizeRewardReply.GetReward().GetOwner().GetEmail(),
			},
			Action: &loyaltyv1.Reward_Block{Block: &loyaltyv1.RewardBlockAction{
				Reason: normalizeRewardReply.GetReward().GetBlock().GetReason(),
			}},
		},
	}
	previewRewardReply, err := s.rewardValidationPort.PreviewReward(ctx, previewRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/PreviewReward")
	}
	return &loyaltyv1.CloneLoyaltyMemberResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{Version: normalizeRewardReply.GetReward().GetVersion()},
			Audit: &commonv1.AuditInfo{
				Etag: explainTierDecisionReply.GetTier().GetAudit().GetEtag(),
			},
			Labels:  req.GetLoyaltyMember().GetLabels(),
			Balance: req.GetLoyaltyMember().GetBalance(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: checkGiftCardTransactionEligibilityReply.GetGiftCardTransaction().GetCorrelationId(),
			Message:       previewRewardReply.GetOutcome().GetMessage(),
			Status:        "ok",
		},
	}, nil
}
