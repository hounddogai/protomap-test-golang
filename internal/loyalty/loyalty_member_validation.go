package loyalty

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	benefitsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/benefits/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	modelregistryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/modelregistry/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// LoyaltyMemberValidationServer implements `acme.loyalty.v1.LoyaltyMemberValidationService`.
// Checks loyalty members against the rules before anything acts on them.
type LoyaltyMemberValidationServer struct {
	loyaltyv1.UnimplementedLoyaltyMemberValidationServiceServer
	store                           *Store
	log                             *slog.Logger
	registeredModelQueryClient      modelregistryv1.RegisteredModelQueryServiceClient
	conn                            *grpc.ClientConn
	giftCardBatchCalculationClient  giftcardsv1.GiftCardBatchCalculationServiceClient
	accessPolicyValidationClient    authorizationv1.AccessPolicyValidationServiceClient
	placementQueryClient            merchandisingv1.PlacementQueryServiceClient
	loyaltyMemberCalculationClient  loyaltyv1.LoyaltyMemberCalculationServiceClient
	benefitPlanQueryClient          benefitsv1.BenefitPlanQueryServiceClient
	meteredReviewVoteCalculation    meteredReviewVoteCalculation
	meteredSellerCalculation        meteredSellerCalculation
	loyaltyMemberCalculationPort    loyaltyMemberCalculationPort
	apiCredentialNotificationClient identityv1.ApiCredentialNotificationServiceClient
}

// NewLoyaltyMemberValidationServer wires a LoyaltyMemberValidationServer.
func NewLoyaltyMemberValidationServer(deps platform.Deps) *LoyaltyMemberValidationServer {
	return &LoyaltyMemberValidationServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.loyalty.v1.LoyaltyMemberValidationService"),
		registeredModelQueryClient:      modelregistryv1.NewRegisteredModelQueryServiceClient(deps.Conn),
		conn:                            deps.Conn,
		giftCardBatchCalculationClient:  giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		accessPolicyValidationClient:    authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		placementQueryClient:            merchandisingv1.NewPlacementQueryServiceClient(deps.Conn),
		loyaltyMemberCalculationClient:  loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		benefitPlanQueryClient:          benefitsv1.NewBenefitPlanQueryServiceClient(deps.Conn),
		meteredReviewVoteCalculation:    meteredReviewVoteCalculation{ReviewVoteCalculationServiceClient: reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		meteredSellerCalculation:        meteredSellerCalculation{SellerCalculationServiceClient: sellersv1.NewSellerCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		loyaltyMemberCalculationPort:    newLoyaltyMemberCalculationPort(deps),
		apiCredentialNotificationClient: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn),
	}
}

// ValidateLoyaltyMember validates a loyalty member.
func (s *LoyaltyMemberValidationServer) ValidateLoyaltyMember(ctx context.Context, req *loyaltyv1.ValidateLoyaltyMemberRequest) (*loyaltyv1.ValidateLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidateLoyaltyMember", "error", err)
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "validate loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	countRegisteredModelsReply, err := s.registeredModelQueryClient.CountRegisteredModels(ctx, &modelregistryv1.CountRegisteredModelsRequest{
		Context: req.GetContext(),
		Filter: &modelregistryv1.CountRegisteredModelsRequest_Filter{
			Labels:       req.GetLoyaltyMember().GetLabels(),
			CreatedAfter: req.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.modelregistry.v1.RegisteredModelQueryService/CountRegisteredModels")
	}
	recalculateLoyaltyMembersRequest := &loyaltyv1.RecalculateLoyaltyMembersRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetLoyaltyMember().GetContact().GetPreferredChannel(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetLoyaltyMember().GetAudit().GetEtag()},
		},
	}
	recalculateLoyaltyMembersReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewLoyaltyMemberCalculationServiceClient).RecalculateLoyaltyMembers(ctx, recalculateLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/RecalculateLoyaltyMembers")
	}
	for _, limit := range req.GetLoyaltyMember().GetLimits() {
		if _, err := s.giftCardBatchCalculationClient.QuoteGiftCardBatch(ctx, &giftcardsv1.QuoteGiftCardBatchRequest{
			Context: req.GetContext(),
			GiftCardBatch: &giftcardsv1.GiftCardBatch{
				Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{
					Tax: &giftcardsv1.GiftCardBatchLineTax{Amount: limit.GetAmount()},
				}},
				Audit: &commonv1.AuditInfo{UpdatedAt: req.GetLoyaltyMember().GetAudit().GetUpdatedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/QuoteGiftCardBatch")
		}
	}
	return &loyaltyv1.ValidateLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			Message: countRegisteredModelsReply.GetResult().GetMessage(),
			Status:  countRegisteredModelsReply.GetResult().GetStatus(),
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				Name: recalculateLoyaltyMembersReply.GetLoyaltyMember().GetContact().GetName(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Balance:         req.GetLoyaltyMember().GetBalance(),
		},
	}, nil
}

// PreviewLoyaltyMember previews a loyalty member.
func (s *LoyaltyMemberValidationServer) PreviewLoyaltyMember(ctx context.Context, req *loyaltyv1.PreviewLoyaltyMemberRequest) (*loyaltyv1.PreviewLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "preview loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	source := req.GetLoyaltyMember().GetContact().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	normalizeAccessPolicyRequest := &authorizationv1.NormalizeAccessPolicyRequest{}
	normalizeAccessPolicyRequest.Context = req.GetContext()
	normalizeAccessPolicyRequest.Details = &authorizationv1.NormalizeAccessPolicyRequest_Details{
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
		},
	}
	normalizeAccessPolicyRequest.AccessPolicy = &authorizationv1.AccessPolicy{
		Audit: &commonv1.AuditInfo{
			UpdatedBy: req.GetLoyaltyMember().GetAudit().GetUpdatedBy(),
			Etag:      req.GetLoyaltyMember().GetAudit().GetEtag(),
		},
		Owner: &identityv1.UserRef{Email: email},
	}
	normalizeAccessPolicyReply, err := s.accessPolicyValidationClient.NormalizeAccessPolicy(ctx, normalizeAccessPolicyRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/NormalizeAccessPolicy")
	}
	labels := make(map[string]string, len(req.GetLoyaltyMember().GetLabels()))
	for key, value := range req.GetLoyaltyMember().GetLabels() {
		labels[key] = value
	}
	exportPlacementsStream, err := s.placementQueryClient.ExportPlacements(ctx, &merchandisingv1.ExportPlacementsRequest{
		Context: req.GetContext(),
		Filter:  &merchandisingv1.ExportPlacementsRequest_Filter{Labels: labels},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
	}
	for {
		received, err := exportPlacementsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementQueryService/ExportPlacements")
		}
		_ = received
	}
	loyaltyMemberCache := cache.New[string, *loyaltyv1.LoyaltyMember](1)
	loyaltyMemberCache.Put(req.GetContext().GetTraceId(), req.GetLoyaltyMember())
	cachedLoyaltyMember, _ := loyaltyMemberCache.Get(req.GetContext().GetTraceId())
	compareLoyaltyMemberOptionsRequest := &loyaltyv1.CompareLoyaltyMemberOptionsRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Funding: &loyaltyv1.LoyaltyMember_Bank{Bank: &loyaltyv1.LoyaltyMemberBankFunding{Iban: req.GetLoyaltyMember().GetBank().GetIban()}},
			Contact: &commonv1.ContactInfo{Email: cachedLoyaltyMember.GetContact().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: normalizeAccessPolicyReply.GetAccessPolicy().GetAudit().GetCreatedAt(),
			},
			Holder: &loyaltyv1.RewardRef{
				DisplayName: normalizeAccessPolicyReply.GetAccessPolicy().GetOwner().GetDisplayName(),
			},
		},
	}
	compareLoyaltyMemberOptionsReply, err := grpcx.Call(ctx, s.loyaltyMemberCalculationClient.CompareLoyaltyMemberOptions, compareLoyaltyMemberOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CompareLoyaltyMemberOptions")
	}
	return &loyaltyv1.PreviewLoyaltyMemberResponse{
		Outcome: &loyaltyv1.PreviewLoyaltyMemberResponse_Outcome{
			ReferenceId: normalizeAccessPolicyReply.GetOutcome().GetReferenceId(),
			Message:     compareLoyaltyMemberOptionsReply.GetResult().GetMessage(),
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Contact:         req.GetLoyaltyMember().GetContact(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckLoyaltyMemberEligibility checks a loyalty member.
func (s *LoyaltyMemberValidationServer) CheckLoyaltyMemberEligibility(ctx context.Context, req *loyaltyv1.CheckLoyaltyMemberEligibilityRequest) (*loyaltyv1.CheckLoyaltyMemberEligibilityResponse, error) {
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "check loyalty member eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	getLoyaltyMemberRatesRequest := &loyaltyv1.GetLoyaltyMemberRatesRequest{
		Context:         req.GetContext(),
		LoyaltyMemberId: req.GetLoyaltyMemberId(),
	}
	getLoyaltyMemberRatesReply := new(loyaltyv1.GetLoyaltyMemberRatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.LoyaltyMemberCalculationService_GetLoyaltyMemberRates_FullMethodName, getLoyaltyMemberRatesRequest, getLoyaltyMemberRatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/GetLoyaltyMemberRates")
	}
	simulateLoyaltyMemberRequest := newSimulateLoyaltyMemberRequest(withSimulateLoyaltyMemberContext(req.GetContext()), withSimulateLoyaltyMemberLoyaltyMember(&loyaltyv1.LoyaltyMember{
		Contact: &commonv1.ContactInfo{
			Email:            req.GetDetails().GetContact().GetEmail(),
			PreferredChannel: getLoyaltyMemberRatesReply.GetLoyaltyMember().GetContact().GetPreferredChannel(),
		},
		Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{
			HolderName: req.GetLoyaltyMember().GetCard().GetHolderName(),
		}},
	}))
	simulateLoyaltyMemberReply, err := s.loyaltyMemberCalculationClient.SimulateLoyaltyMember(ctx, simulateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/SimulateLoyaltyMember")
	}
	return &loyaltyv1.CheckLoyaltyMemberEligibilityResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Status: getLoyaltyMemberRatesReply.GetLoyaltyMember().GetStatus(),
			Holder: getLoyaltyMemberRatesReply.GetLoyaltyMember().GetHolder(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: simulateLoyaltyMemberReply.GetLoyaltyMember().GetAudit().GetUpdatedBy(),
			},
			Available:       simulateLoyaltyMemberReply.GetLoyaltyMember().GetAvailable(),
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			CurrencyCode:    req.GetLoyaltyMember().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeLoyaltyMember normalizes a loyalty member.
func (s *LoyaltyMemberValidationServer) NormalizeLoyaltyMember(ctx context.Context, req *loyaltyv1.NormalizeLoyaltyMemberRequest) (*loyaltyv1.NormalizeLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "normalize loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeBenefitPlansRequest := &benefitsv1.SummarizeBenefitPlansRequest{}
	summarizeBenefitPlansRequest.Context = req.GetContext()
	summarizeBenefitPlansRequest.Annotations = annotations
	summarizeBenefitPlansRequest.Reason = req.GetReason()
	summarizeBenefitPlansRequest.Details = &benefitsv1.SummarizeBenefitPlansRequest_Details{Filters: req.GetLoyaltyMember().GetLabels()}
	summarizeBenefitPlansReply, err := s.benefitPlanQueryClient.SummarizeBenefitPlans(ctx, summarizeBenefitPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.benefits.v1.BenefitPlanQueryService/SummarizeBenefitPlans")
	}
	cloned := proto.Clone(req.GetDetails()).(*loyaltyv1.NormalizeLoyaltyMemberRequest_Details)
	source := req.GetLoyaltyMember().GetHolder().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	estimateReviewVoteRequest := &reviewsv1.EstimateReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				Email:       cloned.GetContact().GetEmail(),
				DisplayName: displayName,
			},
			CorrelationId: summarizeBenefitPlansReply.GetResult().GetCorrelationId(),
		},
	}
	estimateReviewVoteReply, err := s.meteredReviewVoteCalculation.EstimateReviewVote(ctx, estimateReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/EstimateReviewVote")
	}
	return &loyaltyv1.NormalizeLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeBenefitPlansReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &loyaltyv1.NormalizeLoyaltyMemberResponse_Outcome{Status: estimateReviewVoteReply.GetResult().GetStatus()},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: estimateReviewVoteReply.GetReviewVote().GetSubject().GetDisplayName(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Contact:         req.GetLoyaltyMember().GetContact(),
		},
	}, nil
}

// ExplainLoyaltyMemberDecision explains a loyalty member.
func (s *LoyaltyMemberValidationServer) ExplainLoyaltyMemberDecision(ctx context.Context, req *loyaltyv1.ExplainLoyaltyMemberDecisionRequest) (*loyaltyv1.ExplainLoyaltyMemberDecisionResponse, error) {
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "explain loyalty member decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "updated_by", req.GetLoyaltyMember().GetAudit().GetUpdatedBy())
	var iban string
	switch choice := req.GetLoyaltyMember().GetFunding().(type) {
	case *loyaltyv1.LoyaltyMember_Card:
		iban = choice.Card.GetHolderName()
	case *loyaltyv1.LoyaltyMember_Bank:
		iban = choice.Bank.GetIban()
	case *loyaltyv1.LoyaltyMember_ExternalAccountId:
		iban = choice.ExternalAccountId
	}
	calculateLoyaltyMemberRequest := &loyaltyv1.CalculateLoyaltyMemberRequest{}
	calculateLoyaltyMemberRequest.Context = req.GetContext()
	calculateLoyaltyMemberRequest.LoyaltyMember = &loyaltyv1.LoyaltyMember{
		Audit:   &commonv1.AuditInfo{UpdatedBy: tenancy.Value(ctx, "updated_by")},
		Funding: &loyaltyv1.LoyaltyMember_Bank{Bank: &loyaltyv1.LoyaltyMemberBankFunding{Iban: iban}},
	}
	calculateLoyaltyMemberReply, err := s.loyaltyMemberCalculationClient.CalculateLoyaltyMember(ctx, calculateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	return &loyaltyv1.ExplainLoyaltyMemberDecisionResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder:          calculateLoyaltyMemberReply.GetLoyaltyMember().GetHolder(),
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Labels:          req.GetLoyaltyMember().GetLabels(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreLoyaltyMember scores a loyalty member.
func (s *LoyaltyMemberValidationServer) ScoreLoyaltyMember(ctx context.Context, req *loyaltyv1.ScoreLoyaltyMemberRequest) (*loyaltyv1.ScoreLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreLoyaltyMember", "error", err)
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "score loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	explainSellerCalculationRequest := &sellersv1.ExplainSellerCalculationRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ExplainSellerCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetLoyaltyMember().GetContact().GetPreferredChannel(),
			},
		},
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{Phone: req.GetLoyaltyMember().GetContact().GetPhone()},
		},
	}
	explainSellerCalculationReply, err := s.meteredSellerCalculation.ExplainSellerCalculation(ctx, explainSellerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/ExplainSellerCalculation")
	}
	calculateLoyaltyMemberRequest := &loyaltyv1.CalculateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{UpdatedBy: req.GetLoyaltyMember().GetAudit().GetUpdatedBy()},
			Contact: &commonv1.ContactInfo{
				Phone: explainSellerCalculationReply.GetSeller().GetContact().GetPhone(),
			},
			Holder: &loyaltyv1.RewardRef{Version: req.GetLoyaltyMember().GetHolder().GetVersion()},
		},
	}
	calculateLoyaltyMemberReply, err := s.loyaltyMemberCalculationPort.CalculateLoyaltyMember(ctx, calculateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/CalculateLoyaltyMember")
	}
	return &loyaltyv1.ScoreLoyaltyMemberResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainSellerCalculationReply.GetSeller().GetAudit().GetUpdatedBy(),
				CreatedAt: explainSellerCalculationReply.GetSeller().GetAudit().GetCreatedAt(),
			},
			Labels:          calculateLoyaltyMemberReply.GetLoyaltyMember().GetLabels(),
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Status:          req.GetLoyaltyMember().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintLoyaltyMembers lints a loyalty member.
func (s *LoyaltyMemberValidationServer) LintLoyaltyMembers(ctx context.Context, req *loyaltyv1.LintLoyaltyMembersRequest) (*loyaltyv1.LintLoyaltyMembersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintLoyaltyMembers", "error", err)
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "lint loyalty members", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*authorizationv1.LintAccessPoliciesResponse, len(req.GetLoyaltyMember().GetLimits()))
	for i, limit := range req.GetLoyaltyMember().GetLimits() {
		group.Go(func() error {
			reply, err := s.accessPolicyValidationClient.LintAccessPolicies(groupCtx, &authorizationv1.LintAccessPoliciesRequest{
				Context: req.GetContext(),
				AccessPolicy: &authorizationv1.AccessPolicy{
					Action: &authorizationv1.AccessPolicy_Discount{Discount: &authorizationv1.AccessPolicyDiscountAction{Amount: limit.GetAmount()}},
					Name:   limit.GetName(),
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
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	getLoyaltyMemberRatesRequest := &loyaltyv1.GetLoyaltyMemberRatesRequest{
		Context:         req.GetContext(),
		LoyaltyMemberId: req.GetLoyaltyMember().GetLoyaltyMemberId(),
	}
	getLoyaltyMemberRatesReply := new(loyaltyv1.GetLoyaltyMemberRatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.LoyaltyMemberCalculationService_GetLoyaltyMemberRates_FullMethodName, getLoyaltyMemberRatesRequest, getLoyaltyMemberRatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/GetLoyaltyMemberRates")
	}
	return &loyaltyv1.LintLoyaltyMembersResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact:         getLoyaltyMemberRatesReply.GetLoyaltyMember().GetContact(),
			Labels:          getLoyaltyMemberRatesReply.GetLoyaltyMember().GetLabels(),
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			OpenedAt:        req.GetLoyaltyMember().GetOpenedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetLoyaltyMemberRules returns a loyalty member.
func (s *LoyaltyMemberValidationServer) GetLoyaltyMemberRules(ctx context.Context, req *loyaltyv1.GetLoyaltyMemberRulesRequest) (*loyaltyv1.GetLoyaltyMemberRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "loyalty_member", req.GetLoyaltyMemberId(), "get_loyalty_member_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get loyalty member rules", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	listApiCredentialSubscriptionsReply, err := s.apiCredentialNotificationClient.ListApiCredentialSubscriptions(ctx, &identityv1.ListApiCredentialSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListApiCredentialSubscriptionsRequest_Filter{
			OwnerId:      req.GetContext().GetActor().GetUserId(),
			Labels:       baggage,
			CreatedAfter: req.GetContext().GetDeadline(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/ListApiCredentialSubscriptions")
	}
	return &loyaltyv1.GetLoyaltyMemberRulesResponse{
		Result: &commonv1.OperationResult{
			Details: listApiCredentialSubscriptionsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{LoyaltyMemberId: req.GetLoyaltyMemberId()},
	}, nil
}
