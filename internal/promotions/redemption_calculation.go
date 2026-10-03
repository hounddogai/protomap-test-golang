package promotions

import (
	"context"
	"log/slog"
	"sync"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	telehealthv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telehealth/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RedemptionCalculationServer implements `acme.promotions.v1.RedemptionCalculationService`.
// Computes the figures of a redemption, such as totals, taxes, and estimates.
type RedemptionCalculationServer struct {
	promotionsv1.UnsafeRedemptionCalculationServiceServer
	store                         *Store
	log                           *slog.Logger
	promotionCalculationClient    promotionsv1.PromotionCalculationServiceClient
	conn                          *grpc.ClientConn
	paymentIntentValidationOnce   sync.Once
	paymentIntentValidationCached checkoutv1.PaymentIntentValidationServiceClient
	couponValidationClient        promotionsv1.CouponValidationServiceClient
	couponCalculationClient       promotionsv1.CouponCalculationServiceClient
	promotionValidationClient     promotionsv1.PromotionValidationServiceClient
	documentValidationClient      documentsv1.DocumentValidationServiceClient
	videoVisitQueryClient         telehealthv1.VideoVisitQueryServiceClient
}

// NewRedemptionCalculationServer wires a RedemptionCalculationServer.
func NewRedemptionCalculationServer(deps platform.Deps) *RedemptionCalculationServer {
	return &RedemptionCalculationServer{
		store:                      NewStore(deps.DB),
		log:                        deps.Logger.With("service", "acme.promotions.v1.RedemptionCalculationService"),
		promotionCalculationClient: promotionsv1.NewPromotionCalculationServiceClient(deps.Conn),
		conn:                       deps.Conn,
		couponValidationClient:     promotionsv1.NewCouponValidationServiceClient(deps.Conn),
		couponCalculationClient:    promotionsv1.NewCouponCalculationServiceClient(deps.Conn),
		promotionValidationClient:  promotionsv1.NewPromotionValidationServiceClient(deps.Conn),
		documentValidationClient:   documentsv1.NewDocumentValidationServiceClient(deps.Conn),
		videoVisitQueryClient:      telehealthv1.NewVideoVisitQueryServiceClient(deps.Conn),
	}
}

// CalculateRedemption calculates a redemption.
func (s *RedemptionCalculationServer) CalculateRedemption(ctx context.Context, req *promotionsv1.CalculateRedemptionRequest) (*promotionsv1.CalculateRedemptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateRedemption", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "calculate redemption", "tenant", req.GetContext().GetTenantId())
	simulatePromotionRequest := &promotionsv1.SimulatePromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
		},
		Details: &promotionsv1.SimulatePromotionRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	}
	simulatePromotionReply, err := grpcx.Call(ctx, s.promotionCalculationClient.SimulatePromotion, simulatePromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionCalculationService/SimulatePromotion")
	}
	validatePaymentIntentRequest := &checkoutv1.ValidatePaymentIntentRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulatePromotionReply.GetPromotion().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				UserId: simulatePromotionReply.GetPromotion().GetOwner().GetUserId(),
			},
		},
	}
	validatePaymentIntentReply, err := s.paymentIntentValidationLazy().ValidatePaymentIntent(ctx, validatePaymentIntentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.PaymentIntentValidationService/ValidatePaymentIntent")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	previewCouponRequest := &promotionsv1.PreviewCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: validatePaymentIntentReply.GetPaymentIntent().GetAudit().GetUpdatedBy(),
				UpdatedAt: validatePaymentIntentReply.GetPaymentIntent().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &promotionsv1.PreviewCouponRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: validatePaymentIntentReply.GetPaymentIntent().GetRequester().GetPhone(),
				Email: validatePaymentIntentReply.GetPaymentIntent().GetRequester().GetEmail(),
			},
		},
		CouponId: envelope.GetDisplayName(),
	}
	previewCouponReply := new(promotionsv1.PreviewCouponResponse)
	if err := grpcx.Invoke(ctx, s.conn, promotionsv1.CouponValidationService_PreviewCoupon_FullMethodName, previewCouponRequest, previewCouponReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/PreviewCoupon")
	}
	return &promotionsv1.CalculateRedemptionResponse{
		Result: &commonv1.OperationResult{
			Status:       simulatePromotionReply.GetResult().GetStatus(),
			Message:      validatePaymentIntentReply.GetOutcome().GetMessage(),
			ReferenceIds: validatePaymentIntentReply.GetResult().GetReferenceIds(),
		},
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName: simulatePromotionReply.GetPromotion().GetOwner().GetDisplayName(),
			},
			RedemptionId: req.GetRedemptionId(),
			Source:       req.GetRedemption().GetSource(),
		},
		Breakdown: &promotionsv1.CalculateRedemptionResponse_Breakdown{
			CurrencyCode: previewCouponReply.GetCoupon().GetCurrencyCode(),
		},
	}, nil
}

// EstimateRedemption estimates a redemption.
func (s *RedemptionCalculationServer) EstimateRedemption(ctx context.Context, req *promotionsv1.EstimateRedemptionRequest) (*promotionsv1.EstimateRedemptionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateRedemption", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "estimate redemption", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "currency_code", req.GetRedemption().GetSubject().GetCurrencyCode())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	explainCouponDecisionRequest := &promotionsv1.ExplainCouponDecisionRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
			CurrencyCode: tenancy.Value(ctx, "currency_code"),
		},
		CouponId:    req.GetRedemption().GetSubject().GetCouponId(),
		Annotations: annotations,
	}
	var explainCouponDecisionReply *promotionsv1.ExplainCouponDecisionResponse
	if err := pipeline.Run(ctx, "acme.promotions.v1.CouponValidationService/ExplainCouponDecision", func(ctx context.Context) error {
		answer, err := s.couponValidationClient.ExplainCouponDecision(ctx, explainCouponDecisionRequest)
		explainCouponDecisionReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ExplainCouponDecision")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*promotionsv1.ValidateCouponResponse, len(req.GetRedemption().GetMeasurements()))
	for i, measurement := range req.GetRedemption().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.couponValidationClient.ValidateCoupon(groupCtx, &promotionsv1.ValidateCouponRequest{
				Context: req.GetContext(),
				Coupon: &promotionsv1.Coupon{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: explainCouponDecisionReply.GetCoupon().GetAudit().GetUpdatedBy(),
						UpdatedAt: measurement.GetObservedAt(),
					},
					Funding: &promotionsv1.Coupon_Bank{Bank: &promotionsv1.CouponBankFunding{
						Bic: explainCouponDecisionReply.GetCoupon().GetBank().GetBic(),
					}},
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
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ValidateCoupon")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &promotionsv1.EstimateRedemptionResponse{
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				CurrencyCode: explainCouponDecisionReply.GetCoupon().GetCurrencyCode(),
			},
			RedemptionId: req.GetRedemptionId(),
			Source:       req.GetRedemption().GetSource(),
		},
		Result: &commonv1.OperationResult{
			Message: explainCouponDecisionReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// QuoteRedemption quotes a redemption.
func (s *RedemptionCalculationServer) QuoteRedemption(ctx context.Context, req *promotionsv1.QuoteRedemptionRequest) (*promotionsv1.QuoteRedemptionResponse, error) {
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "quote redemption", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "locality", req.GetDetails().GetDestination().GetLocality())
	updateUserRequest := &identityv1.UpdateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: req.GetDetails().GetDestination().GetRecipient(),
				Lines:     req.GetDetails().GetDestination().GetLines(),
				Locality:  tenancy.Value(ctx, "locality"),
			},
		},
	}
	updateUserReply := new(identityv1.UpdateUserResponse)
	if err := s.conn.Invoke(ctx, identityUserServiceUpdateUserMethod, updateUserRequest, updateUserReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/UpdateUser")
	}
	return &promotionsv1.QuoteRedemptionResponse{
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				DisplayName:  updateUserReply.GetUser().GetRelationshipManager().GetDisplayName(),
				CurrencyCode: req.GetDetails().GetCurrencyCode(),
			},
			RedemptionId: req.GetRedemptionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareRedemptionOptions compares a redemption.
func (s *RedemptionCalculationServer) CompareRedemptionOptions(ctx context.Context, req *promotionsv1.CompareRedemptionOptionsRequest) (*promotionsv1.CompareRedemptionOptionsResponse, error) {
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "compare redemption options", "tenant", req.GetContext().GetTenantId())
	compareReturnLabelOptionsRequest := &returnsv1.CompareReturnLabelOptionsRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
		},
		Details: &returnsv1.CompareReturnLabelOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:  req.GetDetails().GetDestination().GetRecipient(),
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	}
	compareReturnLabelOptionsReply := new(returnsv1.CompareReturnLabelOptionsResponse)
	if err := s.conn.Invoke(ctx, returnsReturnLabelCalculationServiceCompareReturnLabelOptionsMethod, compareReturnLabelOptionsRequest, compareReturnLabelOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/CompareReturnLabelOptions")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	estimateCouponReply, err := s.couponCalculationClient.EstimateCoupon(ctx, &promotionsv1.EstimateCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareReturnLabelOptionsReply.GetReturnLabel().GetAudit().GetCreatedAt(),
			},
			Funding: &promotionsv1.Coupon_Card{Card: &promotionsv1.CouponCardFunding{
				Token: compareReturnLabelOptionsReply.GetReturnLabel().GetCard().GetToken(),
			}},
		},
		CouponId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/EstimateCoupon")
	}
	previewPromotionRequest := &promotionsv1.PreviewPromotionRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedBy: compareReturnLabelOptionsReply.GetReturnLabel().GetAudit().GetCreatedBy(),
				CreatedAt: compareReturnLabelOptionsReply.GetReturnLabel().GetAudit().GetCreatedAt(),
			},
		},
		Details: &promotionsv1.PreviewPromotionRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: estimateCouponReply.GetCoupon().GetContact().GetPreferredChannel(),
				Email:            estimateCouponReply.GetCoupon().GetContact().GetEmail(),
			},
		},
	}
	previewPromotionReply, err := promotionsv1.PromotionValidationServiceClient.PreviewPromotion(s.promotionValidationClient, ctx, previewPromotionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/PreviewPromotion")
	}
	checkDocumentEligibilityRequest := &documentsv1.CheckDocumentEligibilityRequest{}
	checkDocumentEligibilityRequest.Context = req.GetContext()
	checkDocumentEligibilityRequest.Document = &documentsv1.Document{
		Settlement: &documentsv1.Document_Bank{Bank: &documentsv1.DocumentBankSettlement{
			Iban:          estimateCouponReply.GetCoupon().GetBank().GetIban(),
			AccountHolder: compareReturnLabelOptionsReply.GetReturnLabel().GetBank().GetAccountHolder(),
		}},
		Audit: &commonv1.AuditInfo{
			CreatedBy: compareReturnLabelOptionsReply.GetReturnLabel().GetAudit().GetCreatedBy(),
		},
	}
	checkDocumentEligibilityReply, err := s.documentValidationClient.CheckDocumentEligibility(ctx, checkDocumentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/CheckDocumentEligibility")
	}
	_ = checkDocumentEligibilityReply
	return &promotionsv1.CompareRedemptionOptionsResponse{
		Result: &commonv1.OperationResult{
			Message: compareReturnLabelOptionsReply.GetResult().GetMessage(),
			Status:  previewPromotionReply.GetOutcome().GetStatus(),
		},
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				CurrencyCode: estimateCouponReply.GetCoupon().GetCurrencyCode(),
			},
			RedemptionId: req.GetRedemptionId(),
			OccurredAt:   req.GetRedemption().GetOccurredAt(),
		},
		Breakdown: &promotionsv1.CompareRedemptionOptionsResponse_Breakdown{
			CurrencyCode: estimateCouponReply.GetBreakdown().GetCurrencyCode(),
		},
	}, nil
}

// RecalculateRedemptions recalculates a redemption.
func (s *RedemptionCalculationServer) RecalculateRedemptions(ctx context.Context, req *promotionsv1.RecalculateRedemptionsRequest) (*promotionsv1.RecalculateRedemptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateRedemptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "recalculate redemptions", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*promotionsv1.RecalculateRedemptionsRequest_Details)
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	findOrderAmendmentsRequest := &ordersv1.FindOrderAmendmentsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.FindOrderAmendmentsRequest_Filter{
			Region:       cloned.GetDestination().GetRegion(),
			OwnerId:      req.GetRedemption().GetRedemptionId(),
			CreatedAfter: req.GetRedemption().GetOccurredAt(),
			Labels:       annotations,
		},
	}
	findOrderAmendmentsReply, err := grpcx.NewClient(s.conn, ordersv1.NewOrderAmendmentQueryServiceClient).FindOrderAmendments(ctx, findOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentQueryService/FindOrderAmendments")
	}
	scoreCouponRequest := &promotionsv1.ScoreCouponRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ScoreCouponRequest_Details{
			CurrencyCode: req.GetRedemption().GetSubject().GetCurrencyCode(),
		},
	}
	scoreCouponReply := new(promotionsv1.ScoreCouponResponse)
	if err := grpcx.Invoke(ctx, s.conn, promotionsv1.CouponValidationService_ScoreCoupon_FullMethodName, scoreCouponRequest, scoreCouponReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ScoreCoupon")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintPromotionsReply, err := s.promotionValidationClient.LintPromotions(ctx, &promotionsv1.LintPromotionsRequest{
		Context: req.GetContext(),
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreCouponReply.GetCoupon().GetAudit().GetCreatedBy(),
				UpdatedBy: scoreCouponReply.GetCoupon().GetAudit().GetUpdatedBy(),
			},
		},
		PromotionId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionValidationService/LintPromotions")
	}
	cloned2 := proto.Clone(req.GetDetails()).(*promotionsv1.RecalculateRedemptionsRequest_Details)
	var envelope2 promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	browseVideoVisitsReply, err := s.videoVisitQueryClient.BrowseVideoVisits(ctx, &telehealthv1.BrowseVideoVisitsRequest{
		Context: req.GetContext(),
		Filter: &telehealthv1.BrowseVideoVisitsRequest_Filter{
			Region:       cloned2.GetDestination().GetRegion(),
			Labels:       scoreCouponReply.GetCoupon().GetLabels(),
			CreatedAfter: scoreCouponReply.GetCoupon().GetAudit().GetUpdatedAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telehealth.v1.VideoVisitQueryService/BrowseVideoVisits")
	}
	_ = lintPromotionsReply
	return &promotionsv1.RecalculateRedemptionsResponse{
		Redemption: &promotionsv1.Redemption{
			CorrelationId: findOrderAmendmentsReply.GetResult().GetCorrelationId(),
			RedemptionId:  req.GetRedemptionId(),
			OccurredAt:    req.GetRedemption().GetOccurredAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: findOrderAmendmentsReply.GetResult().GetReferenceIds(),
			Status:       scoreCouponReply.GetResult().GetStatus(),
			Details:      browseVideoVisitsReply.GetResult().GetDetails(),
		},
		Breakdown: &promotionsv1.RecalculateRedemptionsResponse_Breakdown{
			CurrencyCode: scoreCouponReply.GetCoupon().GetCurrencyCode(),
		},
	}, nil
}

// SimulateRedemption simulates a redemption.
func (s *RedemptionCalculationServer) SimulateRedemption(ctx context.Context, req *promotionsv1.SimulateRedemptionRequest) (*promotionsv1.SimulateRedemptionResponse, error) {
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "simulate redemption", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetRedemption())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.Redemption
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	normalizeCouponRequest := &promotionsv1.NormalizeCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
		},
		CouponId: (&decoded).GetSubject().GetCouponId(),
	}
	normalizeCouponReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*promotionsv1.NormalizeCouponResponse, error) {
		return s.couponValidationClient.NormalizeCoupon(ctx, normalizeCouponRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/NormalizeCoupon")
	}
	var envelope promotionsv1.PromotionRef
	if err := req.GetRedemption().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainCouponDecisionReply, err := s.couponValidationClient.ExplainCouponDecision(ctx, &promotionsv1.ExplainCouponDecisionRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{
				Phone: normalizeCouponReply.GetCoupon().GetContact().GetPhone(),
			},
			Holder: &promotionsv1.PromotionRef{
				DisplayName: req.GetRedemption().GetSubject().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizeCouponReply.GetCoupon().GetAudit().GetUpdatedBy(),
			},
			Funding: &promotionsv1.Coupon_Card{Card: &promotionsv1.CouponCardFunding{
				Token: normalizeCouponReply.GetCoupon().GetCard().GetToken(),
			}},
		},
		CouponId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ExplainCouponDecision")
	}
	return &promotionsv1.SimulateRedemptionResponse{
		Result: &commonv1.OperationResult{Status: normalizeCouponReply.GetResult().GetStatus()},
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				CurrencyCode: normalizeCouponReply.GetCoupon().GetCurrencyCode(),
			},
			RedemptionId: req.GetRedemptionId(),
			Attributes:   req.GetRedemption().GetAttributes(),
		},
		Breakdown: &promotionsv1.SimulateRedemptionResponse_Breakdown{
			CurrencyCode: explainCouponDecisionReply.GetCoupon().GetCurrencyCode(),
		},
	}, nil
}

// ExplainRedemptionCalculation explains a redemption.
func (s *RedemptionCalculationServer) ExplainRedemptionCalculation(ctx context.Context, req *promotionsv1.ExplainRedemptionCalculationRequest) (*promotionsv1.ExplainRedemptionCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRedemption(ctx, req.GetContext().GetTenantId(), req.GetRedemption()); err != nil {
		return nil, grpcx.Wrap(err, "save redemption")
	}
	s.log.InfoContext(ctx, "explain redemption calculation", "tenant", req.GetContext().GetTenantId())
	source := req.GetDetails().GetContact().ProtoReflect()
	preferredChannel := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("preferred_channel"))).String()
	validateCouponRequest := &promotionsv1.ValidateCouponRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
		},
		Details: &promotionsv1.ValidateCouponRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: preferredChannel,
				Email:            req.GetDetails().GetContact().GetEmail(),
			},
		},
	}
	validateCouponReply := new(promotionsv1.ValidateCouponResponse)
	if err := s.conn.Invoke(ctx, promotionsCouponValidationServiceValidateCouponMethod, validateCouponRequest, validateCouponReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ValidateCoupon")
	}
	scoreCouponRequest := buildScoreCouponRequestForExplainRedemptionCalculation(req, validateCouponReply)
	scoreCouponReply, err := s.couponValidationClient.ScoreCoupon(ctx, scoreCouponRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/ScoreCoupon")
	}
	return &promotionsv1.ExplainRedemptionCalculationResponse{
		Redemption: &promotionsv1.Redemption{
			Subject: &promotionsv1.CouponRef{
				CurrencyCode: validateCouponReply.GetCoupon().GetCurrencyCode(),
			},
			RedemptionId: req.GetRedemptionId(),
		},
		Outcome: &promotionsv1.ExplainRedemptionCalculationResponse_Outcome{
			Message: validateCouponReply.GetOutcome().GetMessage(),
			Status:  scoreCouponReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetRedemption().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// GetRedemptionRates returns a redemption.
func (s *RedemptionCalculationServer) GetRedemptionRates(ctx context.Context, req *promotionsv1.GetRedemptionRatesRequest) (*promotionsv1.GetRedemptionRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetRedemptionRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "redemption", req.GetRedemptionId(), "get_redemption_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get redemption rates", "tenant", req.GetContext().GetTenantId())
	lintCouponsReply, err := s.couponValidationClient.LintCoupons(ctx, &promotionsv1.LintCouponsRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponValidationService/LintCoupons")
	}
	return &promotionsv1.GetRedemptionRatesResponse{
		Redemption: &promotionsv1.Redemption{
			Subject:      &promotionsv1.CouponRef{CouponId: lintCouponsReply.GetCoupon().GetCouponId()},
			RedemptionId: req.GetRedemptionId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// paymentIntentValidationLazy dials acme.checkout.v1.PaymentIntentValidationService the first time a handler needs it.
func (s *RedemptionCalculationServer) paymentIntentValidationLazy() checkoutv1.PaymentIntentValidationServiceClient {
	s.paymentIntentValidationOnce.Do(func() { s.paymentIntentValidationCached = checkoutv1.NewPaymentIntentValidationServiceClient(s.conn) })
	return s.paymentIntentValidationCached
}
