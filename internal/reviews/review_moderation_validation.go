package reviews

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
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
	"google.golang.org/protobuf/types/known/anypb"
)

// ReviewModerationValidationServer implements `acme.reviews.v1.ReviewModerationValidationService`.
// Checks review moderations against the rules before anything acts on them.
type ReviewModerationValidationServer struct {
	reviewsv1.UnimplementedReviewModerationValidationServiceServer
	store                                *Store
	log                                  *slog.Logger
	apiCredentialAdminClient             identityv1.ApiCredentialAdminServiceClient
	sellerScorecardValidationClient      sellersv1.SellerScorecardValidationServiceClient
	conn                                 *grpc.ClientConn
	reviewVoteCalculationOnce            sync.Once
	reviewVoteCalculationCached          reviewsv1.ReviewVoteCalculationServiceClient
	reviewVoteCalculationClient          reviewsv1.ReviewVoteCalculationServiceClient
	sessionQueryClient                   identityv1.SessionQueryServiceClient
	ledgerAccountClient                  ledgerv1.LedgerAccountServiceClient
	ledgerAccountClientFallback          ledgerv1.LedgerAccountServiceClient
	metricSeriesClient                   metricsv1.MetricSeriesServiceClient
	merchandisingRuleValidationClient    merchandisingv1.MerchandisingRuleValidationServiceClient
	reviewModerationCalculationOnce      sync.Once
	reviewModerationCalculationCached    reviewsv1.ReviewModerationCalculationServiceClient
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	timesheetClient                      timekeepingv1.TimesheetServiceClient
	giftCardBatchValidationClient        giftcardsv1.GiftCardBatchValidationServiceClient
}

// NewReviewModerationValidationServer wires a ReviewModerationValidationServer.
func NewReviewModerationValidationServer(deps platform.Deps) *ReviewModerationValidationServer {
	return &ReviewModerationValidationServer{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.reviews.v1.ReviewModerationValidationService"),
		apiCredentialAdminClient:             identityv1.NewApiCredentialAdminServiceClient(deps.Conn),
		sellerScorecardValidationClient:      sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		reviewVoteCalculationClient:          reviewsv1.NewReviewVoteCalculationServiceClient(deps.Conn),
		sessionQueryClient:                   identityv1.NewSessionQueryServiceClient(deps.Conn),
		ledgerAccountClient:                  ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		ledgerAccountClientFallback:          ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		metricSeriesClient:                   metricsv1.NewMetricSeriesServiceClient(deps.Conn),
		merchandisingRuleValidationClient:    merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		timesheetClient:                      timekeepingv1.NewTimesheetServiceClient(deps.Conn),
		giftCardBatchValidationClient:        giftcardsv1.NewGiftCardBatchValidationServiceClient(deps.Conn),
	}
}

// ValidateReviewModeration validates a review moderation.
func (s *ReviewModerationValidationServer) ValidateReviewModeration(ctx context.Context, req *reviewsv1.ValidateReviewModerationRequest) (*reviewsv1.ValidateReviewModerationResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "validate review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	reviewModerationCache := cache.New[string, *reviewsv1.ReviewModeration](1)
	reviewModerationCache.Put(req.GetContext().GetTraceId(), req.GetReviewModeration())
	cachedReviewModeration, _ := reviewModerationCache.Get(req.GetContext().GetTraceId())
	snapshot, err := protojson.Marshal(req.GetReviewModeration())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairApiCredentialRequest := &identityv1.RepairApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetReviewModeration().GetAudit().GetCreatedBy(),
				UpdatedBy: cachedReviewModeration.GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetReviewModeration().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &identityv1.RepairApiCredentialRequest_Details{Payload: snapshot},
	}
	repairApiCredentialReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.RepairApiCredentialResponse, error) {
		return s.apiCredentialAdminClient.RepairApiCredential(ctx, repairApiCredentialRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialAdminService/RepairApiCredential")
	}
	reviewModerationCache2 := cache.New[string, *reviewsv1.ReviewModeration](1)
	reviewModerationCache2.Put(req.GetContext().GetTraceId(), req.GetReviewModeration())
	cachedReviewModeration2, _ := reviewModerationCache2.Get(req.GetContext().GetTraceId())
	scoreSellerScorecardRequest := &sellersv1.ScoreSellerScorecardRequest{}
	scoreSellerScorecardRequest.Context = req.GetContext()
	scoreSellerScorecardRequest.SellerScorecard = &sellersv1.SellerScorecard{
		Counterparty: &customersv1.CustomerRef{Email: cachedReviewModeration2.GetAssignee().GetEmail()},
		Settlement: &sellersv1.SellerScorecard_Bank{Bank: &sellersv1.SellerScorecardBankSettlement{
			AccountHolder: repairApiCredentialReply.GetApiCredential().GetBank().GetAccountHolder(),
		}},
	}
	scoreSellerScorecardReply, err := s.sellerScorecardValidationClient.ScoreSellerScorecard(ctx, scoreSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ScoreSellerScorecard")
	}
	encoded, err := protojson.Marshal(req.GetReviewModeration())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded reviewsv1.ReviewModeration
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	scoreFeatureFlagRequest := &featureflagsv1.ScoreFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: (&decoded).GetAudit().GetUpdatedAt(),
				CreatedAt: repairApiCredentialReply.GetApiCredential().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{UserId: req.GetReviewModeration().GetAssignee().GetUserId()},
		},
	}
	scoreFeatureFlagReply, err := scoreFeatureFlagCall(ctx, scoreFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag")
	}
	quoteReviewVoteRequest := &reviewsv1.QuoteReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				CustomerId: req.GetReviewModeration().GetCustomer().GetCustomerId(),
				Email:      req.GetDetails().GetContact().GetEmail(),
			},
		},
		Details: &reviewsv1.QuoteReviewVoteRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines:       scoreSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetLines(),
				CountryCode: scoreSellerScorecardReply.GetSellerScorecard().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	quoteReviewVoteReply, err := s.reviewVoteCalculationLazy().QuoteReviewVote(ctx, quoteReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/QuoteReviewVote")
	}
	return &reviewsv1.ValidateReviewModerationResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				PreferredChannel: repairApiCredentialReply.GetApiCredential().GetContact().GetPreferredChannel(),
				Email:            scoreFeatureFlagReply.GetFeatureFlag().GetOwner().GetEmail(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Subject:            req.GetReviewModeration().GetSubject(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  scoreSellerScorecardReply.GetResult().GetReferenceIds(),
			Status:        scoreFeatureFlagReply.GetResult().GetStatus(),
			CorrelationId: quoteReviewVoteReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// PreviewReviewModeration previews a review moderation.
func (s *ReviewModerationValidationServer) PreviewReviewModeration(ctx context.Context, req *reviewsv1.PreviewReviewModerationRequest) (*reviewsv1.PreviewReviewModerationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "preview review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	var customerId string
	switch choice := req.GetReviewModeration().GetOrigin().(type) {
	case *reviewsv1.ReviewModeration_Customer:
		customerId = choice.Customer.GetCustomerId()
	case *reviewsv1.ReviewModeration_ExternalReference:
		customerId = choice.ExternalReference
	case *reviewsv1.ReviewModeration_Channel:
		customerId = choice.Channel.GetChannel()
	}
	packed, err := anypb.New(req.GetReviewModeration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	quoteReviewVoteReply, err := s.reviewVoteCalculationClient.QuoteReviewVote(ctx, &reviewsv1.QuoteReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				CustomerId:  customerId,
				Email:       req.GetDetails().GetContact().GetEmail(),
				DisplayName: req.GetReviewModeration().GetCustomer().GetDisplayName(),
			},
			Detail: packed,
		},
		Reason: req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/QuoteReviewVote")
	}
	for _, item := range req.GetReviewModeration().GetHistory() {
		if _, err := s.sessionQueryClient.BrowseSessions(ctx, &identityv1.BrowseSessionsRequest{
			Context: req.GetContext(),
			Filter: &identityv1.BrowseSessionsRequest_Filter{
				CreatedAfter: item.GetChangedAt(),
				OwnerId:      quoteReviewVoteReply.GetReviewVote().GetSubject().GetCustomerId(),
				Labels:       quoteReviewVoteReply.GetReviewVote().GetAttributes(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/BrowseSessions")
		}
	}
	routes := map[string]func(context.Context, *ledgerv1.SearchLedgerAccountsRequest, ...grpc.CallOption) (*ledgerv1.SearchLedgerAccountsResponse, error){
		"primary":  s.ledgerAccountClient.SearchLedgerAccounts,
		"fallback": s.ledgerAccountClientFallback.SearchLedgerAccounts,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	customFields := make(map[string]string, len(req.GetReviewModeration().GetCustomFields()))
	for key, value := range req.GetReviewModeration().GetCustomFields() {
		customFields[key] = value
	}
	searchLedgerAccountsRequest := &ledgerv1.SearchLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.SearchLedgerAccountsRequest_Filter{
			OwnerId:      req.GetReviewModeration().GetSla().GetPolicyId(),
			CreatedAfter: quoteReviewVoteReply.GetReviewVote().GetOccurredAt(),
			Labels:       customFields,
		},
	}
	searchLedgerAccountsReply, err := route(ctx, searchLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	_ = searchLedgerAccountsReply
	return &reviewsv1.PreviewReviewModerationResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				Email: quoteReviewVoteReply.GetReviewVote().GetSubject().GetEmail(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Priority:           req.GetReviewModeration().GetPriority(),
		},
		Outcome: &reviewsv1.PreviewReviewModerationResponse_Outcome{Message: quoteReviewVoteReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckReviewModerationEligibility checks a review moderation.
func (s *ReviewModerationValidationServer) CheckReviewModerationEligibility(ctx context.Context, req *reviewsv1.CheckReviewModerationEligibilityRequest) (*reviewsv1.CheckReviewModerationEligibilityResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "check review moderation eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "customer_id", req.GetReviewModeration().GetCustomer().GetCustomerId())
	packed, err := anypb.New(req.GetReviewModeration())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateReviewVoteRequest := &reviewsv1.CalculateReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetReviewModeration().GetCustomer().GetDisplayName(),
				Email:       req.GetReviewModeration().GetAssignee().GetEmail(),
				CustomerId:  tenancy.Value(ctx, "customer_id"),
			},
			Detail: packed,
		},
		Annotations: req.GetAnnotations(),
	}
	calculateReviewVoteReply, err := calculateReviewVoteCall(ctx, calculateReviewVoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/CalculateReviewVote")
	}
	updateMetricSeriesRequest := &metricsv1.UpdateMetricSeriesRequest{
		Context: req.GetContext(),
		MetricSeries: &metricsv1.MetricSeries{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetReviewModeration().GetAssignee().GetDisplayName(),
				Email:       req.GetReviewModeration().GetCustomer().GetEmail(),
			},
		},
	}
	updateMetricSeriesReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*metricsv1.UpdateMetricSeriesResponse, error) {
		return s.metricSeriesClient.UpdateMetricSeries(ctx, updateMetricSeriesRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesService/UpdateMetricSeries")
	}
	lintMerchandisingRulesRequest := &merchandisingv1.LintMerchandisingRulesRequest{
		Context: req.GetContext(),
		Details: &merchandisingv1.LintMerchandisingRulesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetReviewModeration().GetRequester().GetEmail()},
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{Email: req.GetReviewModeration().GetAssignee().GetEmail()},
		},
	}
	send := s.lintMerchandisingRulesSender(lintMerchandisingRulesRequest)
	lintMerchandisingRulesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/LintMerchandisingRules")
	}
	return &reviewsv1.CheckReviewModerationEligibilityResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: calculateReviewVoteReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		ReviewModeration: &reviewsv1.ReviewModeration{
			Requester: &commonv1.ContactInfo{
				Email: updateMetricSeriesReply.GetMetricSeries().GetOwner().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: updateMetricSeriesReply.GetMetricSeries().GetAudit().GetCreatedAt(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: lintMerchandisingRulesReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Subject:            req.GetReviewModeration().GetSubject(),
		},
	}, nil
}

// NormalizeReviewModeration normalizes a review moderation.
func (s *ReviewModerationValidationServer) NormalizeReviewModeration(ctx context.Context, req *reviewsv1.NormalizeReviewModerationRequest) (*reviewsv1.NormalizeReviewModerationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "normalize review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	for _, comment := range req.GetReviewModeration().GetComments() {
		if _, err := s.reviewVoteCalculationClient.EstimateReviewVote(ctx, &reviewsv1.EstimateReviewVoteRequest{
			Context: req.GetContext(),
			ReviewVote: &reviewsv1.ReviewVote{
				Subject: &customersv1.CustomerRef{Email: comment.GetAuthorEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/EstimateReviewVote")
		}
	}
	return &reviewsv1.NormalizeReviewModerationResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			ReviewModerationId: req.GetReviewModerationId(),
			Status:             req.GetReviewModeration().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainReviewModerationDecision explains a review moderation.
func (s *ReviewModerationValidationServer) ExplainReviewModerationDecision(ctx context.Context, req *reviewsv1.ExplainReviewModerationDecisionRequest) (*reviewsv1.ExplainReviewModerationDecisionResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "explain review moderation decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetReviewModeration()).(*reviewsv1.ReviewModeration)
	cloned2 := proto.Clone(req.GetReviewModeration()).(*reviewsv1.ReviewModeration)
	estimateReviewModerationRequest := &reviewsv1.EstimateReviewModerationRequest{
		Context: req.GetContext(),
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit:     &commonv1.AuditInfo{Etag: cloned.GetAudit().GetEtag()},
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
			Origin: &reviewsv1.ReviewModeration_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: cloned2.GetAssignee().GetDisplayName(),
				Email:       req.GetReviewModeration().GetRequester().GetEmail(),
			}},
		},
	}
	estimateReviewModerationReply, err := s.reviewModerationCalculationLazy().EstimateReviewModeration(ctx, estimateReviewModerationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewModerationCalculationService/EstimateReviewModeration")
	}
	normalizeSellerScorecardRequest := &sellersv1.NormalizeSellerScorecardRequest{
		Context: req.GetContext(),
		Details: &sellersv1.NormalizeSellerScorecardRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
				Phone:            estimateReviewModerationReply.GetReviewModeration().GetRequester().GetPhone(),
			},
		},
	}
	normalizeSellerScorecardReply, err := grpcx.Call(ctx, s.sellerScorecardValidationClient.NormalizeSellerScorecard, normalizeSellerScorecardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/NormalizeSellerScorecard")
	}
	explainGiftCardTransactionCalculationRequest := &giftcardsv1.ExplainGiftCardTransactionCalculationRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ExplainGiftCardTransactionCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Email: req.GetReviewModeration().GetCustomer().GetEmail(),
			},
		},
	}
	var explainGiftCardTransactionCalculationReply *giftcardsv1.ExplainGiftCardTransactionCalculationResponse
	if err := pipeline.Run(ctx, "acme.giftcards.v1.GiftCardTransactionCalculationService/ExplainGiftCardTransactionCalculation", func(ctx context.Context) error {
		answer, err := s.giftCardTransactionCalculationClient.ExplainGiftCardTransactionCalculation(ctx, explainGiftCardTransactionCalculationRequest)
		explainGiftCardTransactionCalculationReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/ExplainGiftCardTransactionCalculation")
	}
	compareReviewVoteOptionsRequest := &reviewsv1.CompareReviewVoteOptionsRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: estimateReviewModerationReply.GetReviewModeration().GetCustomer().GetDisplayName(),
			},
		},
	}
	compareReviewVoteOptionsReply := new(reviewsv1.CompareReviewVoteOptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ReviewVoteCalculationService_CompareReviewVoteOptions_FullMethodName, compareReviewVoteOptionsRequest, compareReviewVoteOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/CompareReviewVoteOptions")
	}
	return &reviewsv1.ExplainReviewModerationDecisionResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Tags: estimateReviewModerationReply.GetReviewModeration().GetTags(),
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizeSellerScorecardReply.GetSellerScorecard().GetAudit().GetCreatedBy(),
			},
			Requester: &commonv1.ContactInfo{
				Email: compareReviewVoteOptionsReply.GetReviewVote().GetSubject().GetEmail(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Subject:            req.GetReviewModeration().GetSubject(),
		},
		Outcome: &reviewsv1.ExplainReviewModerationDecisionResponse_Outcome{
			Status:      estimateReviewModerationReply.GetResult().GetStatus(),
			EffectiveAt: normalizeSellerScorecardReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: explainGiftCardTransactionCalculationReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ScoreReviewModeration scores a review moderation.
func (s *ReviewModerationValidationServer) ScoreReviewModeration(ctx context.Context, req *reviewsv1.ScoreReviewModerationRequest) (*reviewsv1.ScoreReviewModerationResponse, error) {
	if err := s.store.SaveReviewModeration(ctx, req.GetContext().GetTenantId(), req.GetReviewModeration()); err != nil {
		return nil, grpcx.Wrap(err, "save review moderation")
	}
	s.log.InfoContext(ctx, "score review moderation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReviewModeration().GetAudit().GetEtag())
	var holder struct{ Email string }
	if record := req.GetReviewModeration().GetRequester(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	watchTimesheetsStream, err := s.timesheetClient.WatchTimesheets(ctx, &timekeepingv1.WatchTimesheetsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: req.GetReviewModeration().GetRequester().GetPreferredChannel(),
			Email:            holder.Email,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetService/WatchTimesheets")
	}
	for {
		received, err := watchTimesheetsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetService/WatchTimesheets")
		}
		_ = received
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := reviewModerationValidationServerNormalizeGiftCardBatchSteps[mode]
	normalizeGiftCardBatchRequest := &giftcardsv1.NormalizeGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: req.GetReviewModeration().GetAssignee().GetDisplayName(),
			},
		},
	}
	normalizeGiftCardBatchReply, err := run(s, ctx, normalizeGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/NormalizeGiftCardBatch")
	}
	return &reviewsv1.ScoreReviewModerationResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizeGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetCreatedBy(),
				CreatedAt: normalizeGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
			Requester:          req.GetReviewModeration().GetRequester(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetReviewModerationRules returns a review moderation.
func (s *ReviewModerationValidationServer) GetReviewModerationRules(ctx context.Context, req *reviewsv1.GetReviewModerationRulesRequest) (*reviewsv1.GetReviewModerationRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "review_moderation", req.GetReviewModerationId(), "get_review_moderation_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get review moderation rules", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	estimateReviewVoteReply, err := s.reviewVoteCalculationClient.EstimateReviewVote(ctx, &reviewsv1.EstimateReviewVoteRequest{
		Context:    req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{Subject: &customersv1.CustomerRef{Email: holder.Email}},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/EstimateReviewVote")
	}
	quoteReviewVoteRequest := &reviewsv1.QuoteReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: estimateReviewVoteReply.GetReviewVote().GetSubject().GetDisplayName(),
				CustomerId:  estimateReviewVoteReply.GetReviewVote().GetSubject().GetCustomerId(),
				Email:       estimateReviewVoteReply.GetReviewVote().GetSubject().GetEmail(),
			},
			CorrelationId: estimateReviewVoteReply.GetResult().GetCorrelationId(),
		},
	}
	quoteReviewVoteReply := new(reviewsv1.QuoteReviewVoteResponse)
	if err := grpcx.Invoke(ctx, s.conn, reviewsv1.ReviewVoteCalculationService_QuoteReviewVote_FullMethodName, quoteReviewVoteRequest, quoteReviewVoteReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteCalculationService/QuoteReviewVote")
	}
	return &reviewsv1.GetReviewModerationRulesResponse{
		ReviewModeration: &reviewsv1.ReviewModeration{
			Assignee: &identityv1.UserRef{
				Email: estimateReviewVoteReply.GetReviewVote().GetSubject().GetEmail(),
			},
			ReviewModerationId: req.GetReviewModerationId(),
		},
		Result: &commonv1.OperationResult{Status: quoteReviewVoteReply.GetResult().GetStatus()},
	}, nil
}

// reviewVoteCalculationLazy dials acme.reviews.v1.ReviewVoteCalculationService the first time a handler needs it.
func (s *ReviewModerationValidationServer) reviewVoteCalculationLazy() reviewsv1.ReviewVoteCalculationServiceClient {
	s.reviewVoteCalculationOnce.Do(func() { s.reviewVoteCalculationCached = reviewsv1.NewReviewVoteCalculationServiceClient(s.conn) })
	return s.reviewVoteCalculationCached
}

// lintMerchandisingRulesSender returns a function that sends one request to
// acme.merchandising.v1.MerchandisingRuleValidationService/LintMerchandisingRules.
func (s *ReviewModerationValidationServer) lintMerchandisingRulesSender(request *merchandisingv1.LintMerchandisingRulesRequest) func(context.Context) (*merchandisingv1.LintMerchandisingRulesResponse, error) {
	return func(ctx context.Context) (*merchandisingv1.LintMerchandisingRulesResponse, error) {
		return s.merchandisingRuleValidationClient.LintMerchandisingRules(ctx, request)
	}
}

// reviewModerationCalculationLazy dials acme.reviews.v1.ReviewModerationCalculationService the first time a handler
// needs it.
func (s *ReviewModerationValidationServer) reviewModerationCalculationLazy() reviewsv1.ReviewModerationCalculationServiceClient {
	s.reviewModerationCalculationOnce.Do(func() {
		s.reviewModerationCalculationCached = reviewsv1.NewReviewModerationCalculationServiceClient(s.conn)
	})
	return s.reviewModerationCalculationCached
}

// reviewModerationValidationServerNormalizeGiftCardBatchSteps are the ways to run
// acme.giftcards.v1.GiftCardBatchValidationService/NormalizeGiftCardBatch, by name.
var reviewModerationValidationServerNormalizeGiftCardBatchSteps = map[string]func(*ReviewModerationValidationServer, context.Context, *giftcardsv1.NormalizeGiftCardBatchRequest) (*giftcardsv1.NormalizeGiftCardBatchResponse, error){
	"default": (*ReviewModerationValidationServer).normalizeGiftCardBatchStep,
	"audited": (*ReviewModerationValidationServer).normalizeGiftCardBatchAuditedStep,
}

func (s *ReviewModerationValidationServer) normalizeGiftCardBatchStep(ctx context.Context, request *giftcardsv1.NormalizeGiftCardBatchRequest) (*giftcardsv1.NormalizeGiftCardBatchResponse, error) {
	return s.giftCardBatchValidationClient.NormalizeGiftCardBatch(ctx, request)
}

func (s *ReviewModerationValidationServer) normalizeGiftCardBatchAuditedStep(ctx context.Context, request *giftcardsv1.NormalizeGiftCardBatchRequest) (*giftcardsv1.NormalizeGiftCardBatchResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.giftcards.v1.GiftCardBatchValidationService/NormalizeGiftCardBatch")
	return s.giftCardBatchValidationClient.NormalizeGiftCardBatch(ctx, request)
}
