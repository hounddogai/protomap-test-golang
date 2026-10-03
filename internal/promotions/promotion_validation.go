package promotions

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	mediav1 "github.com/hounddogai/protomap-test-golang/gen/acme/media/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PromotionValidationServer implements `acme.promotions.v1.PromotionValidationService`.
// Checks promotions against the rules before anything acts on them.
type PromotionValidationServer struct {
	promotionsv1.UnimplementedPromotionValidationServiceServer
	store                              *Store
	log                                *slog.Logger
	notificationQueryClient            notificationsv1.NotificationQueryServiceClient
	reviewModerationValidationClient   reviewsv1.ReviewModerationValidationServiceClient
	promotionCalculationClient         promotionsv1.PromotionCalculationServiceClient
	cartRuleCalculationClient          cartv1.CartRuleCalculationServiceClient
	mediaAssetClient                   mediav1.MediaAssetServiceClient
	recalculatePromotionsFn            func(context.Context, *promotionsv1.RecalculatePromotionsRequest, ...grpc.CallOption) (*promotionsv1.RecalculatePromotionsResponse, error)
	stockItemClient                    inventoryv1.StockItemServiceClient
	paymentInstrumentCalculationClient paymentsv1.PaymentInstrumentCalculationServiceClient
	couponCalculationClient            promotionsv1.CouponCalculationServiceClient
}

// NewPromotionValidationServer wires a PromotionValidationServer.
func NewPromotionValidationServer(deps platform.Deps) *PromotionValidationServer {
	promotionCalculation := promotionsv1.NewPromotionCalculationServiceClient(deps.Conn)
	return &PromotionValidationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.promotions.v1.PromotionValidationService"),
		notificationQueryClient:            notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		reviewModerationValidationClient:   reviewsv1.NewReviewModerationValidationServiceClient(deps.Conn),
		promotionCalculationClient:         promotionsv1.NewPromotionCalculationServiceClient(deps.Conn),
		cartRuleCalculationClient:          cartv1.NewCartRuleCalculationServiceClient(deps.Conn),
		mediaAssetClient:                   mediav1.NewMediaAssetServiceClient(deps.Conn),
		recalculatePromotionsFn:            promotionCalculation.RecalculatePromotions,
		stockItemClient:                    inventoryv1.NewStockItemServiceClient(deps.Conn),
		paymentInstrumentCalculationClient: paymentsv1.NewPaymentInstrumentCalculationServiceClient(deps.Conn),
		couponCalculationClient:            promotionsv1.NewCouponCalculationServiceClient(deps.Conn),
	}
}

// ValidatePromotion validates a promotion.
func (s *PromotionValidationServer) ValidatePromotion(ctx context.Context, req *promotionsv1.ValidatePromotionRequest) (*promotionsv1.ValidatePromotionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "validate promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "user_id", req.GetPromotion().GetOwner().GetUserId())
	countNotificationsRequest := &notificationsv1.CountNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.CountNotificationsRequest_Filter{
			CreatedAfter: req.GetPromotion().GetAudit().GetUpdatedAt(),
			OwnerId:      tenancy.Value(ctx, "user_id"),
		},
	}
	countNotificationsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*notificationsv1.CountNotificationsResponse, error) {
		return s.notificationQueryClient.CountNotifications(ctx, countNotificationsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/CountNotifications")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := promotionValidationServerValidateReviewModerationSteps[mode]
	validateReviewModerationRequest := &reviewsv1.ValidateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Email: req.GetDetails().GetContact().GetEmail(),
			},
			Audit:    &commonv1.AuditInfo{Etag: req.GetPromotion().GetAudit().GetEtag()},
			Comments: []*reviewsv1.ReviewModerationComment{&reviewsv1.ReviewModerationComment{CreatedAt: req.GetPromotion().GetAudit().GetCreatedAt()}},
		},
	}
	validateReviewModerationReply, err := run(s, ctx, validateReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/ValidateReviewModeration")
	}
	jobs := make(chan *promotionsv1.PromotionCondition)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for condition := range jobs {
				if _, err := s.promotionCalculationClient.QuotePromotion(workCtx, &promotionsv1.QuotePromotionRequest{
					Context: req.GetContext(),
					Promotion: &promotionsv1.Promotion{
						Conditions: []*promotionsv1.PromotionCondition{&promotionsv1.PromotionCondition{
							Value:     condition.GetValue(),
							Attribute: condition.GetAttribute(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPromotion().GetConditions() {
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
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/QuotePromotion")
	}
	for condition2 := range iterx.Each(req.GetPromotion().GetConditions(), func(candidate *promotionsv1.PromotionCondition) bool { return candidate != nil }) {
		if _, err := s.cartRuleCalculationClient.EstimateCartRule(ctx, &cartv1.EstimateCartRuleRequest{
			Context: req.GetContext(),
			CartRule: &cartv1.CartRule{
				Conditions: []*cartv1.CartRuleCondition{&cartv1.CartRuleCondition{
					Attribute: condition2.GetAttribute(),
					Value:     condition2.GetValue(),
					Operator:  condition2.GetOperator(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/EstimateCartRule")
		}
	}
	return &promotionsv1.ValidatePromotionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: countNotificationsReply.GetResult().GetReferenceIds(),
			Details:      countNotificationsReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				Etag: validateReviewModerationReply.GetReviewModeration().GetAudit().GetEtag(),
			},
			PromotionId: req.GetPromotionId(),
			Priority:    req.GetPromotion().GetPriority(),
		},
	}, nil
}

// PreviewPromotion previews a promotion.
func (s *PromotionValidationServer) PreviewPromotion(ctx context.Context, req *promotionsv1.PreviewPromotionRequest) (*promotionsv1.PreviewPromotionResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "preview promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	explainReviewModerationDecisionRequest := buildExplainReviewModerationDecisionRequestForPreviewPromotion(req)
	explainReviewModerationDecisionReply, err := s.reviewModerationValidationClient.ExplainReviewModerationDecision(ctx, explainReviewModerationDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationValidationService/ExplainReviewModerationDecision")
	}
	return &promotionsv1.PreviewPromotionResponse{
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: explainReviewModerationDecisionReply.GetReviewModeration().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				Email: explainReviewModerationDecisionReply.GetReviewModeration().GetAssignee().GetEmail(),
			},
			PromotionId: req.GetPromotionId(),
			Status:      req.GetPromotion().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizePromotion normalizes a promotion.
func (s *PromotionValidationServer) NormalizePromotion(ctx context.Context, req *promotionsv1.NormalizePromotionRequest) (*promotionsv1.NormalizePromotionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "normalize promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	estimatePromotionRequest := buildEstimatePromotionRequestForNormalizePromotion(req)
	estimatePromotionReply, err := s.promotionCalculationClient.EstimatePromotion(ctx, estimatePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/EstimatePromotion")
	}
	source := req.GetDetails().GetContact().ProtoReflect()
	phone := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("phone"))).String()
	watchMediaAssetsStream, err := s.mediaAssetClient.WatchMediaAssets(ctx, &mediav1.WatchMediaAssetsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone: phone,
			Email: estimatePromotionReply.GetPromotion().GetOwner().GetEmail(),
			Name:  req.GetDetails().GetContact().GetName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetService/WatchMediaAssets")
	}
	for {
		received, err := watchMediaAssetsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.media.v1.MediaAssetService/WatchMediaAssets")
		}
		_ = received
	}
	return &promotionsv1.NormalizePromotionResponse{
		Promotion: &promotionsv1.Promotion{
			Owner:       estimatePromotionReply.GetPromotion().GetOwner(),
			PromotionId: req.GetPromotionId(),
			Version:     req.GetPromotion().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScorePromotion scores a promotion.
func (s *PromotionValidationServer) ScorePromotion(ctx context.Context, req *promotionsv1.ScorePromotionRequest) (*promotionsv1.ScorePromotionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "score promotion", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	recalculatePromotionsRequest := &promotionsv1.RecalculatePromotionsRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.RecalculatePromotionsRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
		Promotion: &promotionsv1.Promotion{
			Audit:  &commonv1.AuditInfo{CreatedBy: req.GetPromotion().GetAudit().GetCreatedBy()},
			Action: &promotionsv1.Promotion_Discount{Discount: &promotionsv1.PromotionDiscountAction{Percent: req.GetPromotion().GetDiscount().GetPercent()}},
		},
	}
	recalculatePromotionsReply, err := s.recalculatePromotionsFn(ctx, recalculatePromotionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/RecalculatePromotions")
	}
	return &promotionsv1.ScorePromotionResponse{
		Promotion: &promotionsv1.Promotion{
			Parameters:  recalculatePromotionsReply.GetPromotion().GetParameters(),
			PromotionId: req.GetPromotionId(),
			Status:      req.GetPromotion().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintPromotions lints a promotion.
func (s *PromotionValidationServer) LintPromotions(ctx context.Context, req *promotionsv1.LintPromotionsRequest) (*promotionsv1.LintPromotionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintPromotions", "error", err)
		return nil, err
	}
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "lint promotions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	quotePromotionRequest := &promotionsv1.QuotePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Action: &promotionsv1.Promotion_Discount{Discount: &promotionsv1.PromotionDiscountAction{Percent: req.GetPromotion().GetDiscount().GetPercent()}},
			Audit:  &commonv1.AuditInfo{CreatedBy: req.GetPromotion().GetAudit().GetCreatedBy()},
		},
	}
	quotePromotionReply, err := grpcx.Call(ctx, s.promotionCalculationClient.QuotePromotion, quotePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/QuotePromotion")
	}
	cloned := proto.Clone(req.GetPromotion()).(*promotionsv1.Promotion)
	cloned2 := proto.Clone(req.GetDetails()).(*promotionsv1.LintPromotionsRequest_Details)
	watchStockItemsStream, err := s.stockItemClient.WatchStockItems(ctx, &inventoryv1.WatchStockItemsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: cloned.GetOwner().GetEmail(),
			Phone: cloned2.GetContact().GetPhone(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/WatchStockItems")
	}
	for {
		received, err := watchStockItemsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/WatchStockItems")
		}
		_ = received
	}
	comparePaymentInstrumentOptionsRequest := &paymentsv1.ComparePaymentInstrumentOptionsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: quotePromotionReply.GetPromotion().GetAudit().GetCreatedBy(),
			},
		},
	}
	var comparePaymentInstrumentOptionsReply *paymentsv1.ComparePaymentInstrumentOptionsResponse
	if err := pipeline.Run(ctx, "acme.payments.v1.PaymentInstrumentCalculationService/ComparePaymentInstrumentOptions", func(ctx context.Context) error {
		answer, err := s.paymentInstrumentCalculationClient.ComparePaymentInstrumentOptions(ctx, comparePaymentInstrumentOptionsRequest)
		comparePaymentInstrumentOptionsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentCalculationService/ComparePaymentInstrumentOptions")
	}
	calculateCouponRequest := buildCalculateCouponRequestForLintPromotions(req, quotePromotionReply, comparePaymentInstrumentOptionsReply)
	calculateCouponReply, err := s.couponCalculationClient.CalculateCoupon(ctx, calculateCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/CalculateCoupon")
	}
	return &promotionsv1.LintPromotionsResponse{
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedAt: quotePromotionReply.GetPromotion().GetAudit().GetCreatedAt(),
				Etag:      calculateCouponReply.GetCoupon().GetAudit().GetEtag(),
			},
			Version: quotePromotionReply.GetPromotion().GetVersion(),
			Owner: &identityv1.UserRef{
				DisplayName: comparePaymentInstrumentOptionsReply.GetPaymentInstrument().GetHolder().GetDisplayName(),
			},
			PromotionId: req.GetPromotionId(),
			Effective:   req.GetPromotion().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPromotionRules returns a promotion.
func (s *PromotionValidationServer) GetPromotionRules(ctx context.Context, req *promotionsv1.GetPromotionRulesRequest) (*promotionsv1.GetPromotionRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "get_promotion_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get promotion rules", "tenant", req.GetContext().GetTenantId())
	explainCartRuleCalculationRequest := &cartv1.ExplainCartRuleCalculationRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
	}
	explainCartRuleCalculationReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*cartv1.ExplainCartRuleCalculationResponse, error) {
		return s.cartRuleCalculationClient.ExplainCartRuleCalculation(ctx, explainCartRuleCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/ExplainCartRuleCalculation")
	}
	return &promotionsv1.GetPromotionRulesResponse{
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				UserId: explainCartRuleCalculationReply.GetCartRule().GetOwner().GetUserId(),
			},
			PromotionId: req.GetPromotionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// promotionValidationServerValidateReviewModerationSteps are the ways to run
// acme.reviews.v1.ReviewModerationValidationService/ValidateReviewModeration, by name.
var promotionValidationServerValidateReviewModerationSteps = map[string]func(*PromotionValidationServer, context.Context, *reviewsv1.ValidateReviewModerationRequest) (*reviewsv1.ValidateReviewModerationResponse, error){
	"default": (*PromotionValidationServer).validateReviewModerationStep,
	"audited": (*PromotionValidationServer).validateReviewModerationAuditedStep,
}

func (s *PromotionValidationServer) validateReviewModerationStep(ctx context.Context, request *reviewsv1.ValidateReviewModerationRequest) (*reviewsv1.ValidateReviewModerationResponse, error) {
	return s.reviewModerationValidationClient.ValidateReviewModeration(ctx, request)
}

func (s *PromotionValidationServer) validateReviewModerationAuditedStep(ctx context.Context, request *reviewsv1.ValidateReviewModerationRequest) (*reviewsv1.ValidateReviewModerationResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.reviews.v1.ReviewModerationValidationService/ValidateReviewModeration")
	return s.reviewModerationValidationClient.ValidateReviewModeration(ctx, request)
}
