package promotions

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// auditEventValidationPort is what the promotions domain needs of acme.audit.v1.AuditEventValidationService.
type auditEventValidationPort interface {
	ValidateAuditEvent(ctx context.Context, request *auditv1.ValidateAuditEventRequest) (*auditv1.ValidateAuditEventResponse, error)
}

// grpcAuditEventValidationPort reaches acme.audit.v1.AuditEventValidationService over gRPC.
type grpcAuditEventValidationPort struct {
	client auditv1.AuditEventValidationServiceClient
}

// sandboxAuditEventValidationPort answers in the sandbox environment, where acme.audit.v1.AuditEventValidationService
// is not deployed.
type sandboxAuditEventValidationPort struct{ log *slog.Logger }

func (p grpcAuditEventValidationPort) ValidateAuditEvent(ctx context.Context, request *auditv1.ValidateAuditEventRequest) (*auditv1.ValidateAuditEventResponse, error) {
	return p.client.ValidateAuditEvent(ctx, request)
}

func (p sandboxAuditEventValidationPort) ValidateAuditEvent(ctx context.Context, request *auditv1.ValidateAuditEventRequest) (*auditv1.ValidateAuditEventResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidateAuditEvent")
	return &auditv1.ValidateAuditEventResponse{}, nil
}

func newAuditEventValidationPort(deps platform.Deps) auditEventValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxAuditEventValidationPort{log: deps.Logger}
	}
	return grpcAuditEventValidationPort{client: auditv1.NewAuditEventValidationServiceClient(deps.Conn)}
}

// couponCalculationPort is what the promotions domain needs of acme.promotions.v1.CouponCalculationService.
type couponCalculationPort interface {
	EstimateCoupon(ctx context.Context, request *promotionsv1.EstimateCouponRequest) (*promotionsv1.EstimateCouponResponse, error)
}

// grpcCouponCalculationPort reaches acme.promotions.v1.CouponCalculationService over gRPC.
type grpcCouponCalculationPort struct {
	client promotionsv1.CouponCalculationServiceClient
}

// sandboxCouponCalculationPort answers in the sandbox environment, where acme.promotions.v1.CouponCalculationService is
// not deployed.
type sandboxCouponCalculationPort struct{ log *slog.Logger }

func (p grpcCouponCalculationPort) EstimateCoupon(ctx context.Context, request *promotionsv1.EstimateCouponRequest) (*promotionsv1.EstimateCouponResponse, error) {
	return p.client.EstimateCoupon(ctx, request)
}

func (p sandboxCouponCalculationPort) EstimateCoupon(ctx context.Context, request *promotionsv1.EstimateCouponRequest) (*promotionsv1.EstimateCouponResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateCoupon")
	return &promotionsv1.EstimateCouponResponse{}, nil
}

func newCouponCalculationPort(deps platform.Deps) couponCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCouponCalculationPort{log: deps.Logger}
	}
	return grpcCouponCalculationPort{client: promotionsv1.NewCouponCalculationServiceClient(deps.Conn)}
}

// couponQueryPort is what the promotions domain needs of acme.promotions.v1.CouponQueryService.
type couponQueryPort interface {
	ResolveCoupons(ctx context.Context, request *promotionsv1.ResolveCouponsRequest) (*promotionsv1.ResolveCouponsResponse, error)
}

// grpcCouponQueryPort reaches acme.promotions.v1.CouponQueryService over gRPC.
type grpcCouponQueryPort struct {
	client promotionsv1.CouponQueryServiceClient
}

// sandboxCouponQueryPort answers in the sandbox environment, where acme.promotions.v1.CouponQueryService is not
// deployed.
type sandboxCouponQueryPort struct{ log *slog.Logger }

func (p grpcCouponQueryPort) ResolveCoupons(ctx context.Context, request *promotionsv1.ResolveCouponsRequest) (*promotionsv1.ResolveCouponsResponse, error) {
	return p.client.ResolveCoupons(ctx, request)
}

func (p sandboxCouponQueryPort) ResolveCoupons(ctx context.Context, request *promotionsv1.ResolveCouponsRequest) (*promotionsv1.ResolveCouponsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ResolveCoupons")
	return &promotionsv1.ResolveCouponsResponse{}, nil
}

func newCouponQueryPort(deps platform.Deps) couponQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCouponQueryPort{log: deps.Logger}
	}
	return grpcCouponQueryPort{client: promotionsv1.NewCouponQueryServiceClient(deps.Conn)}
}

// couponValidationPort is what the promotions domain needs of acme.promotions.v1.CouponValidationService.
type couponValidationPort interface {
	ExplainCouponDecision(ctx context.Context, request *promotionsv1.ExplainCouponDecisionRequest) (*promotionsv1.ExplainCouponDecisionResponse, error)
	ScoreCoupon(ctx context.Context, request *promotionsv1.ScoreCouponRequest) (*promotionsv1.ScoreCouponResponse, error)
}

// grpcCouponValidationPort reaches acme.promotions.v1.CouponValidationService over gRPC.
type grpcCouponValidationPort struct {
	client promotionsv1.CouponValidationServiceClient
}

// sandboxCouponValidationPort answers in the sandbox environment, where acme.promotions.v1.CouponValidationService is
// not deployed.
type sandboxCouponValidationPort struct{ log *slog.Logger }

func (p grpcCouponValidationPort) ExplainCouponDecision(ctx context.Context, request *promotionsv1.ExplainCouponDecisionRequest) (*promotionsv1.ExplainCouponDecisionResponse, error) {
	return p.client.ExplainCouponDecision(ctx, request)
}

func (p sandboxCouponValidationPort) ExplainCouponDecision(ctx context.Context, request *promotionsv1.ExplainCouponDecisionRequest) (*promotionsv1.ExplainCouponDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainCouponDecision")
	return &promotionsv1.ExplainCouponDecisionResponse{}, nil
}

func (p grpcCouponValidationPort) ScoreCoupon(ctx context.Context, request *promotionsv1.ScoreCouponRequest) (*promotionsv1.ScoreCouponResponse, error) {
	return p.client.ScoreCoupon(ctx, request)
}

func (p sandboxCouponValidationPort) ScoreCoupon(ctx context.Context, request *promotionsv1.ScoreCouponRequest) (*promotionsv1.ScoreCouponResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScoreCoupon")
	return &promotionsv1.ScoreCouponResponse{}, nil
}

func newCouponValidationPort(deps platform.Deps) couponValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCouponValidationPort{log: deps.Logger}
	}
	return grpcCouponValidationPort{client: promotionsv1.NewCouponValidationServiceClient(deps.Conn)}
}

// fraudSignalCalculationPort is what the promotions domain needs of acme.fraud.v1.FraudSignalCalculationService.
type fraudSignalCalculationPort interface {
	ExplainFraudSignalCalculation(ctx context.Context, request *fraudv1.ExplainFraudSignalCalculationRequest) (*fraudv1.ExplainFraudSignalCalculationResponse, error)
}

// grpcFraudSignalCalculationPort reaches acme.fraud.v1.FraudSignalCalculationService over gRPC.
type grpcFraudSignalCalculationPort struct {
	client fraudv1.FraudSignalCalculationServiceClient
}

// sandboxFraudSignalCalculationPort answers in the sandbox environment, where
// acme.fraud.v1.FraudSignalCalculationService is not deployed.
type sandboxFraudSignalCalculationPort struct{ log *slog.Logger }

func (p grpcFraudSignalCalculationPort) ExplainFraudSignalCalculation(ctx context.Context, request *fraudv1.ExplainFraudSignalCalculationRequest) (*fraudv1.ExplainFraudSignalCalculationResponse, error) {
	return p.client.ExplainFraudSignalCalculation(ctx, request)
}

func (p sandboxFraudSignalCalculationPort) ExplainFraudSignalCalculation(ctx context.Context, request *fraudv1.ExplainFraudSignalCalculationRequest) (*fraudv1.ExplainFraudSignalCalculationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainFraudSignalCalculation")
	return &fraudv1.ExplainFraudSignalCalculationResponse{}, nil
}

func newFraudSignalCalculationPort(deps platform.Deps) fraudSignalCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFraudSignalCalculationPort{log: deps.Logger}
	}
	return grpcFraudSignalCalculationPort{client: fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn)}
}

// fraudSignalWorkflowPort is what the promotions domain needs of acme.fraud.v1.FraudSignalWorkflowService.
type fraudSignalWorkflowPort interface {
	ResumeFraudSignalWorkflow(ctx context.Context, request *fraudv1.ResumeFraudSignalWorkflowRequest) (*fraudv1.ResumeFraudSignalWorkflowResponse, error)
}

// grpcFraudSignalWorkflowPort reaches acme.fraud.v1.FraudSignalWorkflowService over gRPC.
type grpcFraudSignalWorkflowPort struct {
	client fraudv1.FraudSignalWorkflowServiceClient
}

// sandboxFraudSignalWorkflowPort answers in the sandbox environment, where acme.fraud.v1.FraudSignalWorkflowService is
// not deployed.
type sandboxFraudSignalWorkflowPort struct{ log *slog.Logger }

func (p grpcFraudSignalWorkflowPort) ResumeFraudSignalWorkflow(ctx context.Context, request *fraudv1.ResumeFraudSignalWorkflowRequest) (*fraudv1.ResumeFraudSignalWorkflowResponse, error) {
	return p.client.ResumeFraudSignalWorkflow(ctx, request)
}

func (p sandboxFraudSignalWorkflowPort) ResumeFraudSignalWorkflow(ctx context.Context, request *fraudv1.ResumeFraudSignalWorkflowRequest) (*fraudv1.ResumeFraudSignalWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ResumeFraudSignalWorkflow")
	return &fraudv1.ResumeFraudSignalWorkflowResponse{}, nil
}

func newFraudSignalWorkflowPort(deps platform.Deps) fraudSignalWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFraudSignalWorkflowPort{log: deps.Logger}
	}
	return grpcFraudSignalWorkflowPort{client: fraudv1.NewFraudSignalWorkflowServiceClient(deps.Conn)}
}

// merchandisingRuleValidationPort is what the promotions domain needs of
// acme.merchandising.v1.MerchandisingRuleValidationService.
type merchandisingRuleValidationPort interface {
	NormalizeMerchandisingRule(ctx context.Context, request *merchandisingv1.NormalizeMerchandisingRuleRequest) (*merchandisingv1.NormalizeMerchandisingRuleResponse, error)
}

// grpcMerchandisingRuleValidationPort reaches acme.merchandising.v1.MerchandisingRuleValidationService over gRPC.
type grpcMerchandisingRuleValidationPort struct {
	client merchandisingv1.MerchandisingRuleValidationServiceClient
}

// sandboxMerchandisingRuleValidationPort answers in the sandbox environment, where
// acme.merchandising.v1.MerchandisingRuleValidationService is not deployed.
type sandboxMerchandisingRuleValidationPort struct{ log *slog.Logger }

func (p grpcMerchandisingRuleValidationPort) NormalizeMerchandisingRule(ctx context.Context, request *merchandisingv1.NormalizeMerchandisingRuleRequest) (*merchandisingv1.NormalizeMerchandisingRuleResponse, error) {
	return p.client.NormalizeMerchandisingRule(ctx, request)
}

func (p sandboxMerchandisingRuleValidationPort) NormalizeMerchandisingRule(ctx context.Context, request *merchandisingv1.NormalizeMerchandisingRuleRequest) (*merchandisingv1.NormalizeMerchandisingRuleResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizeMerchandisingRule")
	return &merchandisingv1.NormalizeMerchandisingRuleResponse{}, nil
}

func newMerchandisingRuleValidationPort(deps platform.Deps) merchandisingRuleValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxMerchandisingRuleValidationPort{log: deps.Logger}
	}
	return grpcMerchandisingRuleValidationPort{client: merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn)}
}

// merchandisingRuleWorkflowPort is what the promotions domain needs of
// acme.merchandising.v1.MerchandisingRuleWorkflowService.
type merchandisingRuleWorkflowPort interface {
	SignalMerchandisingRuleWorkflow(ctx context.Context, request *merchandisingv1.SignalMerchandisingRuleWorkflowRequest) (*merchandisingv1.SignalMerchandisingRuleWorkflowResponse, error)
}

// grpcMerchandisingRuleWorkflowPort reaches acme.merchandising.v1.MerchandisingRuleWorkflowService over gRPC.
type grpcMerchandisingRuleWorkflowPort struct {
	client merchandisingv1.MerchandisingRuleWorkflowServiceClient
}

// sandboxMerchandisingRuleWorkflowPort answers in the sandbox environment, where
// acme.merchandising.v1.MerchandisingRuleWorkflowService is not deployed.
type sandboxMerchandisingRuleWorkflowPort struct{ log *slog.Logger }

func (p grpcMerchandisingRuleWorkflowPort) SignalMerchandisingRuleWorkflow(ctx context.Context, request *merchandisingv1.SignalMerchandisingRuleWorkflowRequest) (*merchandisingv1.SignalMerchandisingRuleWorkflowResponse, error) {
	return p.client.SignalMerchandisingRuleWorkflow(ctx, request)
}

func (p sandboxMerchandisingRuleWorkflowPort) SignalMerchandisingRuleWorkflow(ctx context.Context, request *merchandisingv1.SignalMerchandisingRuleWorkflowRequest) (*merchandisingv1.SignalMerchandisingRuleWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SignalMerchandisingRuleWorkflow")
	return &merchandisingv1.SignalMerchandisingRuleWorkflowResponse{}, nil
}

func newMerchandisingRuleWorkflowPort(deps platform.Deps) merchandisingRuleWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxMerchandisingRuleWorkflowPort{log: deps.Logger}
	}
	return grpcMerchandisingRuleWorkflowPort{client: merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn)}
}

// pointsLedgerApprovalPort is what the promotions domain needs of acme.loyalty.v1.PointsLedgerApprovalService.
type pointsLedgerApprovalPort interface {
	ListPendingPointsLedgers(ctx context.Context, request *loyaltyv1.ListPendingPointsLedgersRequest) (*loyaltyv1.ListPendingPointsLedgersResponse, error)
}

// grpcPointsLedgerApprovalPort reaches acme.loyalty.v1.PointsLedgerApprovalService over gRPC.
type grpcPointsLedgerApprovalPort struct {
	client loyaltyv1.PointsLedgerApprovalServiceClient
}

// sandboxPointsLedgerApprovalPort answers in the sandbox environment, where acme.loyalty.v1.PointsLedgerApprovalService
// is not deployed.
type sandboxPointsLedgerApprovalPort struct{ log *slog.Logger }

func (p grpcPointsLedgerApprovalPort) ListPendingPointsLedgers(ctx context.Context, request *loyaltyv1.ListPendingPointsLedgersRequest) (*loyaltyv1.ListPendingPointsLedgersResponse, error) {
	return p.client.ListPendingPointsLedgers(ctx, request)
}

func (p sandboxPointsLedgerApprovalPort) ListPendingPointsLedgers(ctx context.Context, request *loyaltyv1.ListPendingPointsLedgersRequest) (*loyaltyv1.ListPendingPointsLedgersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListPendingPointsLedgers")
	return &loyaltyv1.ListPendingPointsLedgersResponse{}, nil
}

func newPointsLedgerApprovalPort(deps platform.Deps) pointsLedgerApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPointsLedgerApprovalPort{log: deps.Logger}
	}
	return grpcPointsLedgerApprovalPort{client: loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn)}
}

// productReviewApprovalPort is what the promotions domain needs of acme.reviews.v1.ProductReviewApprovalService.
type productReviewApprovalPort interface {
	ApproveProductReview(ctx context.Context, request *reviewsv1.ApproveProductReviewRequest) (*reviewsv1.ApproveProductReviewResponse, error)
}

// grpcProductReviewApprovalPort reaches acme.reviews.v1.ProductReviewApprovalService over gRPC.
type grpcProductReviewApprovalPort struct {
	client reviewsv1.ProductReviewApprovalServiceClient
}

// sandboxProductReviewApprovalPort answers in the sandbox environment, where
// acme.reviews.v1.ProductReviewApprovalService is not deployed.
type sandboxProductReviewApprovalPort struct{ log *slog.Logger }

func (p grpcProductReviewApprovalPort) ApproveProductReview(ctx context.Context, request *reviewsv1.ApproveProductReviewRequest) (*reviewsv1.ApproveProductReviewResponse, error) {
	return p.client.ApproveProductReview(ctx, request)
}

func (p sandboxProductReviewApprovalPort) ApproveProductReview(ctx context.Context, request *reviewsv1.ApproveProductReviewRequest) (*reviewsv1.ApproveProductReviewResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ApproveProductReview")
	return &reviewsv1.ApproveProductReviewResponse{}, nil
}

func newProductReviewApprovalPort(deps platform.Deps) productReviewApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxProductReviewApprovalPort{log: deps.Logger}
	}
	return grpcProductReviewApprovalPort{client: reviewsv1.NewProductReviewApprovalServiceClient(deps.Conn)}
}

// promotionApprovalPort is what the promotions domain needs of acme.promotions.v1.PromotionApprovalService.
type promotionApprovalPort interface {
	DelegatePromotionApproval(ctx context.Context, request *promotionsv1.DelegatePromotionApprovalRequest) (*promotionsv1.DelegatePromotionApprovalResponse, error)
}

// grpcPromotionApprovalPort reaches acme.promotions.v1.PromotionApprovalService over gRPC.
type grpcPromotionApprovalPort struct {
	client promotionsv1.PromotionApprovalServiceClient
}

// sandboxPromotionApprovalPort answers in the sandbox environment, where acme.promotions.v1.PromotionApprovalService is
// not deployed.
type sandboxPromotionApprovalPort struct{ log *slog.Logger }

func (p grpcPromotionApprovalPort) DelegatePromotionApproval(ctx context.Context, request *promotionsv1.DelegatePromotionApprovalRequest) (*promotionsv1.DelegatePromotionApprovalResponse, error) {
	return p.client.DelegatePromotionApproval(ctx, request)
}

func (p sandboxPromotionApprovalPort) DelegatePromotionApproval(ctx context.Context, request *promotionsv1.DelegatePromotionApprovalRequest) (*promotionsv1.DelegatePromotionApprovalResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers DelegatePromotionApproval")
	return &promotionsv1.DelegatePromotionApprovalResponse{}, nil
}

func newPromotionApprovalPort(deps platform.Deps) promotionApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPromotionApprovalPort{log: deps.Logger}
	}
	return grpcPromotionApprovalPort{client: promotionsv1.NewPromotionApprovalServiceClient(deps.Conn)}
}

// promotionQueryPort is what the promotions domain needs of acme.promotions.v1.PromotionQueryService.
type promotionQueryPort interface {
	SuggestPromotions(ctx context.Context, request *promotionsv1.SuggestPromotionsRequest) (*promotionsv1.SuggestPromotionsResponse, error)
}

// grpcPromotionQueryPort reaches acme.promotions.v1.PromotionQueryService over gRPC.
type grpcPromotionQueryPort struct {
	client promotionsv1.PromotionQueryServiceClient
}

// sandboxPromotionQueryPort answers in the sandbox environment, where acme.promotions.v1.PromotionQueryService is not
// deployed.
type sandboxPromotionQueryPort struct{ log *slog.Logger }

func (p grpcPromotionQueryPort) SuggestPromotions(ctx context.Context, request *promotionsv1.SuggestPromotionsRequest) (*promotionsv1.SuggestPromotionsResponse, error) {
	return p.client.SuggestPromotions(ctx, request)
}

func (p sandboxPromotionQueryPort) SuggestPromotions(ctx context.Context, request *promotionsv1.SuggestPromotionsRequest) (*promotionsv1.SuggestPromotionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SuggestPromotions")
	return &promotionsv1.SuggestPromotionsResponse{}, nil
}

func newPromotionQueryPort(deps platform.Deps) promotionQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPromotionQueryPort{log: deps.Logger}
	}
	return grpcPromotionQueryPort{client: promotionsv1.NewPromotionQueryServiceClient(deps.Conn)}
}

// redemptionImportPort is what the promotions domain needs of acme.promotions.v1.RedemptionImportService.
type redemptionImportPort interface {
	CancelRedemptionImport(ctx context.Context, request *promotionsv1.CancelRedemptionImportRequest) (*promotionsv1.CancelRedemptionImportResponse, error)
}

// grpcRedemptionImportPort reaches acme.promotions.v1.RedemptionImportService over gRPC.
type grpcRedemptionImportPort struct {
	client promotionsv1.RedemptionImportServiceClient
}

// sandboxRedemptionImportPort answers in the sandbox environment, where acme.promotions.v1.RedemptionImportService is
// not deployed.
type sandboxRedemptionImportPort struct{ log *slog.Logger }

func (p grpcRedemptionImportPort) CancelRedemptionImport(ctx context.Context, request *promotionsv1.CancelRedemptionImportRequest) (*promotionsv1.CancelRedemptionImportResponse, error) {
	return p.client.CancelRedemptionImport(ctx, request)
}

func (p sandboxRedemptionImportPort) CancelRedemptionImport(ctx context.Context, request *promotionsv1.CancelRedemptionImportRequest) (*promotionsv1.CancelRedemptionImportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CancelRedemptionImport")
	return &promotionsv1.CancelRedemptionImportResponse{}, nil
}

func newRedemptionImportPort(deps platform.Deps) redemptionImportPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRedemptionImportPort{log: deps.Logger}
	}
	return grpcRedemptionImportPort{client: promotionsv1.NewRedemptionImportServiceClient(deps.Conn)}
}

// redemptionValidationPort is what the promotions domain needs of acme.promotions.v1.RedemptionValidationService.
type redemptionValidationPort interface {
	LintRedemptions(ctx context.Context, request *promotionsv1.LintRedemptionsRequest) (*promotionsv1.LintRedemptionsResponse, error)
}

// grpcRedemptionValidationPort reaches acme.promotions.v1.RedemptionValidationService over gRPC.
type grpcRedemptionValidationPort struct {
	client promotionsv1.RedemptionValidationServiceClient
}

// sandboxRedemptionValidationPort answers in the sandbox environment, where
// acme.promotions.v1.RedemptionValidationService is not deployed.
type sandboxRedemptionValidationPort struct{ log *slog.Logger }

func (p grpcRedemptionValidationPort) LintRedemptions(ctx context.Context, request *promotionsv1.LintRedemptionsRequest) (*promotionsv1.LintRedemptionsResponse, error) {
	return p.client.LintRedemptions(ctx, request)
}

func (p sandboxRedemptionValidationPort) LintRedemptions(ctx context.Context, request *promotionsv1.LintRedemptionsRequest) (*promotionsv1.LintRedemptionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers LintRedemptions")
	return &promotionsv1.LintRedemptionsResponse{}, nil
}

func newRedemptionValidationPort(deps platform.Deps) redemptionValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRedemptionValidationPort{log: deps.Logger}
	}
	return grpcRedemptionValidationPort{client: promotionsv1.NewRedemptionValidationServiceClient(deps.Conn)}
}

// rewardLifecyclePort is what the promotions domain needs of acme.loyalty.v1.RewardLifecycleService.
type rewardLifecyclePort interface {
	ScheduleRewardExpiry(ctx context.Context, request *loyaltyv1.ScheduleRewardExpiryRequest) (*loyaltyv1.ScheduleRewardExpiryResponse, error)
}

// grpcRewardLifecyclePort reaches acme.loyalty.v1.RewardLifecycleService over gRPC.
type grpcRewardLifecyclePort struct {
	client loyaltyv1.RewardLifecycleServiceClient
}

// sandboxRewardLifecyclePort answers in the sandbox environment, where acme.loyalty.v1.RewardLifecycleService is not
// deployed.
type sandboxRewardLifecyclePort struct{ log *slog.Logger }

func (p grpcRewardLifecyclePort) ScheduleRewardExpiry(ctx context.Context, request *loyaltyv1.ScheduleRewardExpiryRequest) (*loyaltyv1.ScheduleRewardExpiryResponse, error) {
	return p.client.ScheduleRewardExpiry(ctx, request)
}

func (p sandboxRewardLifecyclePort) ScheduleRewardExpiry(ctx context.Context, request *loyaltyv1.ScheduleRewardExpiryRequest) (*loyaltyv1.ScheduleRewardExpiryResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScheduleRewardExpiry")
	return &loyaltyv1.ScheduleRewardExpiryResponse{}, nil
}

func newRewardLifecyclePort(deps platform.Deps) rewardLifecyclePort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRewardLifecyclePort{log: deps.Logger}
	}
	return grpcRewardLifecyclePort{client: loyaltyv1.NewRewardLifecycleServiceClient(deps.Conn)}
}

// searchIndexExportPort is what the promotions domain needs of acme.search.v1.SearchIndexExportService.
type searchIndexExportPort interface {
	ScheduleSearchIndexExport(ctx context.Context, request *searchv1.ScheduleSearchIndexExportRequest) (*searchv1.ScheduleSearchIndexExportResponse, error)
}

// grpcSearchIndexExportPort reaches acme.search.v1.SearchIndexExportService over gRPC.
type grpcSearchIndexExportPort struct {
	client searchv1.SearchIndexExportServiceClient
}

// sandboxSearchIndexExportPort answers in the sandbox environment, where acme.search.v1.SearchIndexExportService is not
// deployed.
type sandboxSearchIndexExportPort struct{ log *slog.Logger }

func (p grpcSearchIndexExportPort) ScheduleSearchIndexExport(ctx context.Context, request *searchv1.ScheduleSearchIndexExportRequest) (*searchv1.ScheduleSearchIndexExportResponse, error) {
	return p.client.ScheduleSearchIndexExport(ctx, request)
}

func (p sandboxSearchIndexExportPort) ScheduleSearchIndexExport(ctx context.Context, request *searchv1.ScheduleSearchIndexExportRequest) (*searchv1.ScheduleSearchIndexExportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScheduleSearchIndexExport")
	return &searchv1.ScheduleSearchIndexExportResponse{}, nil
}

func newSearchIndexExportPort(deps platform.Deps) searchIndexExportPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSearchIndexExportPort{log: deps.Logger}
	}
	return grpcSearchIndexExportPort{client: searchv1.NewSearchIndexExportServiceClient(deps.Conn)}
}
