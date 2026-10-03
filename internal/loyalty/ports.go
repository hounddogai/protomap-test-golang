package loyalty

import (
	"context"
	"log/slog"

	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// giftCardBatchValidationPort is what the loyalty domain needs of acme.giftcards.v1.GiftCardBatchValidationService.
type giftCardBatchValidationPort interface {
	ExplainGiftCardBatchDecision(ctx context.Context, request *giftcardsv1.ExplainGiftCardBatchDecisionRequest) (*giftcardsv1.ExplainGiftCardBatchDecisionResponse, error)
}

// grpcGiftCardBatchValidationPort reaches acme.giftcards.v1.GiftCardBatchValidationService over gRPC.
type grpcGiftCardBatchValidationPort struct {
	client giftcardsv1.GiftCardBatchValidationServiceClient
}

// sandboxGiftCardBatchValidationPort answers in the sandbox environment, where
// acme.giftcards.v1.GiftCardBatchValidationService is not deployed.
type sandboxGiftCardBatchValidationPort struct{ log *slog.Logger }

func (p grpcGiftCardBatchValidationPort) ExplainGiftCardBatchDecision(ctx context.Context, request *giftcardsv1.ExplainGiftCardBatchDecisionRequest) (*giftcardsv1.ExplainGiftCardBatchDecisionResponse, error) {
	return p.client.ExplainGiftCardBatchDecision(ctx, request)
}

func (p sandboxGiftCardBatchValidationPort) ExplainGiftCardBatchDecision(ctx context.Context, request *giftcardsv1.ExplainGiftCardBatchDecisionRequest) (*giftcardsv1.ExplainGiftCardBatchDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainGiftCardBatchDecision")
	return &giftcardsv1.ExplainGiftCardBatchDecisionResponse{}, nil
}

func newGiftCardBatchValidationPort(deps platform.Deps) giftCardBatchValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxGiftCardBatchValidationPort{log: deps.Logger}
	}
	return grpcGiftCardBatchValidationPort{client: giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn)}
}

// loyaltyMemberCalculationPort is what the loyalty domain needs of acme.loyalty.v1.LoyaltyMemberCalculationService.
type loyaltyMemberCalculationPort interface {
	CalculateLoyaltyMember(ctx context.Context, request *loyaltyv1.CalculateLoyaltyMemberRequest) (*loyaltyv1.CalculateLoyaltyMemberResponse, error)
	CompareLoyaltyMemberOptions(ctx context.Context, request *loyaltyv1.CompareLoyaltyMemberOptionsRequest) (*loyaltyv1.CompareLoyaltyMemberOptionsResponse, error)
}

// grpcLoyaltyMemberCalculationPort reaches acme.loyalty.v1.LoyaltyMemberCalculationService over gRPC.
type grpcLoyaltyMemberCalculationPort struct {
	client loyaltyv1.LoyaltyMemberCalculationServiceClient
}

// sandboxLoyaltyMemberCalculationPort answers in the sandbox environment, where
// acme.loyalty.v1.LoyaltyMemberCalculationService is not deployed.
type sandboxLoyaltyMemberCalculationPort struct{ log *slog.Logger }

func (p grpcLoyaltyMemberCalculationPort) CalculateLoyaltyMember(ctx context.Context, request *loyaltyv1.CalculateLoyaltyMemberRequest) (*loyaltyv1.CalculateLoyaltyMemberResponse, error) {
	return p.client.CalculateLoyaltyMember(ctx, request)
}

func (p sandboxLoyaltyMemberCalculationPort) CalculateLoyaltyMember(ctx context.Context, request *loyaltyv1.CalculateLoyaltyMemberRequest) (*loyaltyv1.CalculateLoyaltyMemberResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateLoyaltyMember")
	return &loyaltyv1.CalculateLoyaltyMemberResponse{}, nil
}

func (p grpcLoyaltyMemberCalculationPort) CompareLoyaltyMemberOptions(ctx context.Context, request *loyaltyv1.CompareLoyaltyMemberOptionsRequest) (*loyaltyv1.CompareLoyaltyMemberOptionsResponse, error) {
	return p.client.CompareLoyaltyMemberOptions(ctx, request)
}

func (p sandboxLoyaltyMemberCalculationPort) CompareLoyaltyMemberOptions(ctx context.Context, request *loyaltyv1.CompareLoyaltyMemberOptionsRequest) (*loyaltyv1.CompareLoyaltyMemberOptionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CompareLoyaltyMemberOptions")
	return &loyaltyv1.CompareLoyaltyMemberOptionsResponse{}, nil
}

func newLoyaltyMemberCalculationPort(deps platform.Deps) loyaltyMemberCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxLoyaltyMemberCalculationPort{log: deps.Logger}
	}
	return grpcLoyaltyMemberCalculationPort{client: loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn)}
}

// metricSeriesQueryPort is what the loyalty domain needs of acme.metrics.v1.MetricSeriesQueryService.
type metricSeriesQueryPort interface {
	SuggestMetricSerieses(ctx context.Context, request *metricsv1.SuggestMetricSeriesesRequest) (*metricsv1.SuggestMetricSeriesesResponse, error)
}

// grpcMetricSeriesQueryPort reaches acme.metrics.v1.MetricSeriesQueryService over gRPC.
type grpcMetricSeriesQueryPort struct {
	client metricsv1.MetricSeriesQueryServiceClient
}

// sandboxMetricSeriesQueryPort answers in the sandbox environment, where acme.metrics.v1.MetricSeriesQueryService is
// not deployed.
type sandboxMetricSeriesQueryPort struct{ log *slog.Logger }

func (p grpcMetricSeriesQueryPort) SuggestMetricSerieses(ctx context.Context, request *metricsv1.SuggestMetricSeriesesRequest) (*metricsv1.SuggestMetricSeriesesResponse, error) {
	return p.client.SuggestMetricSerieses(ctx, request)
}

func (p sandboxMetricSeriesQueryPort) SuggestMetricSerieses(ctx context.Context, request *metricsv1.SuggestMetricSeriesesRequest) (*metricsv1.SuggestMetricSeriesesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SuggestMetricSerieses")
	return &metricsv1.SuggestMetricSeriesesResponse{}, nil
}

func newMetricSeriesQueryPort(deps platform.Deps) metricSeriesQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxMetricSeriesQueryPort{log: deps.Logger}
	}
	return grpcMetricSeriesQueryPort{client: metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn)}
}

// pointsLedgerCalculationPort is what the loyalty domain needs of acme.loyalty.v1.PointsLedgerCalculationService.
type pointsLedgerCalculationPort interface {
	ExplainPointsLedgerCalculation(ctx context.Context, request *loyaltyv1.ExplainPointsLedgerCalculationRequest) (*loyaltyv1.ExplainPointsLedgerCalculationResponse, error)
}

// grpcPointsLedgerCalculationPort reaches acme.loyalty.v1.PointsLedgerCalculationService over gRPC.
type grpcPointsLedgerCalculationPort struct {
	client loyaltyv1.PointsLedgerCalculationServiceClient
}

// sandboxPointsLedgerCalculationPort answers in the sandbox environment, where
// acme.loyalty.v1.PointsLedgerCalculationService is not deployed.
type sandboxPointsLedgerCalculationPort struct{ log *slog.Logger }

func (p grpcPointsLedgerCalculationPort) ExplainPointsLedgerCalculation(ctx context.Context, request *loyaltyv1.ExplainPointsLedgerCalculationRequest) (*loyaltyv1.ExplainPointsLedgerCalculationResponse, error) {
	return p.client.ExplainPointsLedgerCalculation(ctx, request)
}

func (p sandboxPointsLedgerCalculationPort) ExplainPointsLedgerCalculation(ctx context.Context, request *loyaltyv1.ExplainPointsLedgerCalculationRequest) (*loyaltyv1.ExplainPointsLedgerCalculationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainPointsLedgerCalculation")
	return &loyaltyv1.ExplainPointsLedgerCalculationResponse{}, nil
}

func newPointsLedgerCalculationPort(deps platform.Deps) pointsLedgerCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPointsLedgerCalculationPort{log: deps.Logger}
	}
	return grpcPointsLedgerCalculationPort{client: loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn)}
}

// pointsLedgerValidationPort is what the loyalty domain needs of acme.loyalty.v1.PointsLedgerValidationService.
type pointsLedgerValidationPort interface {
	ExplainPointsLedgerDecision(ctx context.Context, request *loyaltyv1.ExplainPointsLedgerDecisionRequest) (*loyaltyv1.ExplainPointsLedgerDecisionResponse, error)
}

// grpcPointsLedgerValidationPort reaches acme.loyalty.v1.PointsLedgerValidationService over gRPC.
type grpcPointsLedgerValidationPort struct {
	client loyaltyv1.PointsLedgerValidationServiceClient
}

// sandboxPointsLedgerValidationPort answers in the sandbox environment, where
// acme.loyalty.v1.PointsLedgerValidationService is not deployed.
type sandboxPointsLedgerValidationPort struct{ log *slog.Logger }

func (p grpcPointsLedgerValidationPort) ExplainPointsLedgerDecision(ctx context.Context, request *loyaltyv1.ExplainPointsLedgerDecisionRequest) (*loyaltyv1.ExplainPointsLedgerDecisionResponse, error) {
	return p.client.ExplainPointsLedgerDecision(ctx, request)
}

func (p sandboxPointsLedgerValidationPort) ExplainPointsLedgerDecision(ctx context.Context, request *loyaltyv1.ExplainPointsLedgerDecisionRequest) (*loyaltyv1.ExplainPointsLedgerDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainPointsLedgerDecision")
	return &loyaltyv1.ExplainPointsLedgerDecisionResponse{}, nil
}

func newPointsLedgerValidationPort(deps platform.Deps) pointsLedgerValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPointsLedgerValidationPort{log: deps.Logger}
	}
	return grpcPointsLedgerValidationPort{client: loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn)}
}

// rewardApprovalPort is what the loyalty domain needs of acme.loyalty.v1.RewardApprovalService.
type rewardApprovalPort interface {
	RecallReward(ctx context.Context, request *loyaltyv1.RecallRewardRequest) (*loyaltyv1.RecallRewardResponse, error)
}

// grpcRewardApprovalPort reaches acme.loyalty.v1.RewardApprovalService over gRPC.
type grpcRewardApprovalPort struct {
	client loyaltyv1.RewardApprovalServiceClient
}

// sandboxRewardApprovalPort answers in the sandbox environment, where acme.loyalty.v1.RewardApprovalService is not
// deployed.
type sandboxRewardApprovalPort struct{ log *slog.Logger }

func (p grpcRewardApprovalPort) RecallReward(ctx context.Context, request *loyaltyv1.RecallRewardRequest) (*loyaltyv1.RecallRewardResponse, error) {
	return p.client.RecallReward(ctx, request)
}

func (p sandboxRewardApprovalPort) RecallReward(ctx context.Context, request *loyaltyv1.RecallRewardRequest) (*loyaltyv1.RecallRewardResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecallReward")
	return &loyaltyv1.RecallRewardResponse{}, nil
}

func newRewardApprovalPort(deps platform.Deps) rewardApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRewardApprovalPort{log: deps.Logger}
	}
	return grpcRewardApprovalPort{client: loyaltyv1.NewRewardApprovalServiceClient(deps.Conn)}
}

// rewardLifecyclePort is what the loyalty domain needs of acme.loyalty.v1.RewardLifecycleService.
type rewardLifecyclePort interface {
	GetRewardLifecycle(ctx context.Context, request *loyaltyv1.GetRewardLifecycleRequest) (*loyaltyv1.GetRewardLifecycleResponse, error)
}

// grpcRewardLifecyclePort reaches acme.loyalty.v1.RewardLifecycleService over gRPC.
type grpcRewardLifecyclePort struct {
	client loyaltyv1.RewardLifecycleServiceClient
}

// sandboxRewardLifecyclePort answers in the sandbox environment, where acme.loyalty.v1.RewardLifecycleService is not
// deployed.
type sandboxRewardLifecyclePort struct{ log *slog.Logger }

func (p grpcRewardLifecyclePort) GetRewardLifecycle(ctx context.Context, request *loyaltyv1.GetRewardLifecycleRequest) (*loyaltyv1.GetRewardLifecycleResponse, error) {
	return p.client.GetRewardLifecycle(ctx, request)
}

func (p sandboxRewardLifecyclePort) GetRewardLifecycle(ctx context.Context, request *loyaltyv1.GetRewardLifecycleRequest) (*loyaltyv1.GetRewardLifecycleResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetRewardLifecycle")
	return &loyaltyv1.GetRewardLifecycleResponse{}, nil
}

func newRewardLifecyclePort(deps platform.Deps) rewardLifecyclePort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRewardLifecyclePort{log: deps.Logger}
	}
	return grpcRewardLifecyclePort{client: loyaltyv1.NewRewardLifecycleServiceClient(deps.Conn)}
}

// rewardValidationPort is what the loyalty domain needs of acme.loyalty.v1.RewardValidationService.
type rewardValidationPort interface {
	PreviewReward(ctx context.Context, request *loyaltyv1.PreviewRewardRequest) (*loyaltyv1.PreviewRewardResponse, error)
}

// grpcRewardValidationPort reaches acme.loyalty.v1.RewardValidationService over gRPC.
type grpcRewardValidationPort struct {
	client loyaltyv1.RewardValidationServiceClient
}

// sandboxRewardValidationPort answers in the sandbox environment, where acme.loyalty.v1.RewardValidationService is not
// deployed.
type sandboxRewardValidationPort struct{ log *slog.Logger }

func (p grpcRewardValidationPort) PreviewReward(ctx context.Context, request *loyaltyv1.PreviewRewardRequest) (*loyaltyv1.PreviewRewardResponse, error) {
	return p.client.PreviewReward(ctx, request)
}

func (p sandboxRewardValidationPort) PreviewReward(ctx context.Context, request *loyaltyv1.PreviewRewardRequest) (*loyaltyv1.PreviewRewardResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewReward")
	return &loyaltyv1.PreviewRewardResponse{}, nil
}

func newRewardValidationPort(deps platform.Deps) rewardValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRewardValidationPort{log: deps.Logger}
	}
	return grpcRewardValidationPort{client: loyaltyv1.NewRewardValidationServiceClient(deps.Conn)}
}

// userHistoryPort is what the loyalty domain needs of acme.identity.v1.UserHistoryService.
type userHistoryPort interface {
	PruneUserVersions(ctx context.Context, request *identityv1.PruneUserVersionsRequest) (*identityv1.PruneUserVersionsResponse, error)
}

// grpcUserHistoryPort reaches acme.identity.v1.UserHistoryService over gRPC.
type grpcUserHistoryPort struct {
	client identityv1.UserHistoryServiceClient
}

// sandboxUserHistoryPort answers in the sandbox environment, where acme.identity.v1.UserHistoryService is not deployed.
type sandboxUserHistoryPort struct{ log *slog.Logger }

func (p grpcUserHistoryPort) PruneUserVersions(ctx context.Context, request *identityv1.PruneUserVersionsRequest) (*identityv1.PruneUserVersionsResponse, error) {
	return p.client.PruneUserVersions(ctx, request)
}

func (p sandboxUserHistoryPort) PruneUserVersions(ctx context.Context, request *identityv1.PruneUserVersionsRequest) (*identityv1.PruneUserVersionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PruneUserVersions")
	return &identityv1.PruneUserVersionsResponse{}, nil
}

func newUserHistoryPort(deps platform.Deps) userHistoryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxUserHistoryPort{log: deps.Logger}
	}
	return grpcUserHistoryPort{client: identityv1.NewUserHistoryServiceClient(deps.Conn)}
}

// userPort is what the loyalty domain needs of acme.identity.v1.UserService.
type userPort interface {
	SearchUsers(ctx context.Context, request *identityv1.SearchUsersRequest) (*identityv1.SearchUsersResponse, error)
}

// grpcUserPort reaches acme.identity.v1.UserService over gRPC.
type grpcUserPort struct{ client identityv1.UserServiceClient }

// sandboxUserPort answers in the sandbox environment, where acme.identity.v1.UserService is not deployed.
type sandboxUserPort struct{ log *slog.Logger }

func (p grpcUserPort) SearchUsers(ctx context.Context, request *identityv1.SearchUsersRequest) (*identityv1.SearchUsersResponse, error) {
	return p.client.SearchUsers(ctx, request)
}

func (p sandboxUserPort) SearchUsers(ctx context.Context, request *identityv1.SearchUsersRequest) (*identityv1.SearchUsersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SearchUsers")
	return &identityv1.SearchUsersResponse{}, nil
}

func newUserPort(deps platform.Deps) userPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxUserPort{log: deps.Logger}
	}
	return grpcUserPort{client: identityv1.NewUserServiceClient(deps.Conn)}
}
