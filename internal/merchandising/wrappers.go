package merchandising

import (
	"context"
	"time"

	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredGiftCardValidation records how long each call to acme.giftcards.v1.GiftCardValidationService takes.
type meteredGiftCardValidation struct {
	giftcardsv1.GiftCardValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredGiftCardValidation) ValidateGiftCard(ctx context.Context, request *giftcardsv1.ValidateGiftCardRequest, options ...grpc.CallOption) (*giftcardsv1.ValidateGiftCardResponse, error) {
	defer c.recorder.Observe("acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard", time.Now())
	return c.GiftCardValidationServiceClient.ValidateGiftCard(ctx, request, options...)
}

// meteredLedgerAccountValidation records how long each call to acme.ledger.v1.LedgerAccountValidationService takes.
type meteredLedgerAccountValidation struct {
	ledgerv1.LedgerAccountValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredLedgerAccountValidation) LintLedgerAccounts(ctx context.Context, request *ledgerv1.LintLedgerAccountsRequest, options ...grpc.CallOption) (*ledgerv1.LintLedgerAccountsResponse, error) {
	defer c.recorder.Observe("acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts", time.Now())
	return c.LedgerAccountValidationServiceClient.LintLedgerAccounts(ctx, request, options...)
}

// meteredMerchandisingRuleValidation records how long each call to
// acme.merchandising.v1.MerchandisingRuleValidationService takes.
type meteredMerchandisingRuleValidation struct {
	merchandisingv1.MerchandisingRuleValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredMerchandisingRuleValidation) LintMerchandisingRules(ctx context.Context, request *merchandisingv1.LintMerchandisingRulesRequest, options ...grpc.CallOption) (*merchandisingv1.LintMerchandisingRulesResponse, error) {
	defer c.recorder.Observe("acme.merchandising.v1.MerchandisingRuleValidationService/LintMerchandisingRules", time.Now())
	return c.MerchandisingRuleValidationServiceClient.LintMerchandisingRules(ctx, request, options...)
}

// meteredPlacementApproval records how long each call to acme.merchandising.v1.PlacementApprovalService takes.
type meteredPlacementApproval struct {
	merchandisingv1.PlacementApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredPlacementApproval) DelegatePlacementApproval(ctx context.Context, request *merchandisingv1.DelegatePlacementApprovalRequest, options ...grpc.CallOption) (*merchandisingv1.DelegatePlacementApprovalResponse, error) {
	defer c.recorder.Observe("acme.merchandising.v1.PlacementApprovalService/DelegatePlacementApproval", time.Now())
	return c.PlacementApprovalServiceClient.DelegatePlacementApproval(ctx, request, options...)
}

func (c meteredPlacementApproval) ListPendingPlacements(ctx context.Context, request *merchandisingv1.ListPendingPlacementsRequest, options ...grpc.CallOption) (*merchandisingv1.ListPendingPlacementsResponse, error) {
	defer c.recorder.Observe("acme.merchandising.v1.PlacementApprovalService/ListPendingPlacements", time.Now())
	return c.PlacementApprovalServiceClient.ListPendingPlacements(ctx, request, options...)
}
