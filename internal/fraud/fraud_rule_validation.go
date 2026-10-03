package fraud

import (
	"context"
	"log/slog"
	"sync"

	collectionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// FraudRuleValidationServer implements `acme.fraud.v1.FraudRuleValidationService`.
// Checks fraud rules against the rules before anything acts on them.
type FraudRuleValidationServer struct {
	*fraudRuleValidationHandlers
}

// fraudRuleValidationHandlers holds the handlers of FraudRuleValidationServer, with what they use.
type fraudRuleValidationHandlers struct {
	fraudv1.UnimplementedFraudRuleValidationServiceServer
	store                                *Store
	log                                  *slog.Logger
	giftCardTransactionCalculationClient giftcardsv1.GiftCardTransactionCalculationServiceClient
	watchlistCalculationClient           fraudv1.WatchlistCalculationServiceClient
	carrierRateCalculationClient         shippingv1.CarrierRateCalculationServiceClient
	collectionCaseQueryClient            collectionsv1.CollectionCaseQueryServiceClient
	conn                                 *grpc.ClientConn
	userHistoryOnce                      sync.Once
	userHistoryCached                    identityv1.UserHistoryServiceClient
	fraudSignalCalculationClient         fraudv1.FraudSignalCalculationServiceClient
	fraudSignalValidationClient          fraudv1.FraudSignalValidationServiceClient
	searchIndexNotificationClient        searchv1.SearchIndexNotificationServiceClient
	sessionQueryClient                   identityv1.SessionQueryServiceClient
	warehouseClient                      warehousingv1.WarehouseServiceClient
}

// NewFraudRuleValidationServer wires a FraudRuleValidationServer.
func NewFraudRuleValidationServer(deps platform.Deps) *FraudRuleValidationServer {
	return &FraudRuleValidationServer{fraudRuleValidationHandlers: &fraudRuleValidationHandlers{
		store:                                NewStore(deps.DB),
		log:                                  deps.Logger.With("service", "acme.fraud.v1.FraudRuleValidationService"),
		giftCardTransactionCalculationClient: giftcardsv1.NewGiftCardTransactionCalculationServiceClient(deps.Conn),
		watchlistCalculationClient:           fraudv1.NewWatchlistCalculationServiceClient(deps.Conn),
		carrierRateCalculationClient:         shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn),
		collectionCaseQueryClient:            collectionsv1.NewCollectionCaseQueryServiceClient(deps.Conn),
		conn:                                 deps.Conn,
		fraudSignalCalculationClient:         fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn),
		fraudSignalValidationClient:          fraudv1.NewFraudSignalValidationServiceClient(deps.Conn),
		searchIndexNotificationClient:        searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		sessionQueryClient:                   identityv1.NewSessionQueryServiceClient(deps.Conn),
		warehouseClient:                      warehousingv1.NewWarehouseServiceClient(deps.Conn),
	}}
}

// ValidateFraudRule validates a fraud rule.
func (s *fraudRuleValidationHandlers) ValidateFraudRule(ctx context.Context, req *fraudv1.ValidateFraudRuleRequest) (*fraudv1.ValidateFraudRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "validate fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetFraudRule().GetOwner().GetDisplayName())
	packed, err := anypb.New(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareGiftCardTransactionOptionsReply, err := s.giftCardTransactionCalculationClient.CompareGiftCardTransactionOptions(ctx, &giftcardsv1.CompareGiftCardTransactionOptionsRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Detail:  packed,
		},
		Reason: req.GetFraudRule().GetBlock().GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionCalculationService/CompareGiftCardTransactionOptions")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetContact().GetEmail())
	explainWatchlistCalculationReply, err := s.watchlistCalculationClient.ExplainWatchlistCalculation(ctx, &fraudv1.ExplainWatchlistCalculationRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Manager: &identityv1.UserRef{
					UserId: req.GetFraudRule().GetOwner().GetUserId(),
					Email:  tenancy.Value(ctx, "email"),
				},
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{Etag: req.GetFraudRule().GetAudit().GetEtag()},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/ExplainWatchlistCalculation")
	}
	return &fraudv1.ValidateFraudRuleResponse{
		Result: &commonv1.OperationResult{
			Status: compareGiftCardTransactionOptionsReply.GetResult().GetStatus(),
		},
		Outcome: &fraudv1.ValidateFraudRuleResponse_Outcome{
			Status: explainWatchlistCalculationReply.GetOutcome().GetStatus(),
		},
		FraudRule: &fraudv1.FraudRule{
			FraudRuleId: req.GetFraudRuleId(),
			Owner:       req.GetFraudRule().GetOwner(),
		},
	}, nil
}

// PreviewFraudRule previews a fraud rule.
func (s *fraudRuleValidationHandlers) PreviewFraudRule(ctx context.Context, req *fraudv1.PreviewFraudRuleRequest) (*fraudv1.PreviewFraudRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "preview fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	for _, condition := range req.GetFraudRule().GetConditions() {
		if _, err := s.carrierRateCalculationClient.CompareCarrierRateOptions(ctx, &shippingv1.CompareCarrierRateOptionsRequest{
			Context: req.GetContext(),
			CarrierRate: &shippingv1.CarrierRate{
				Conditions: []*shippingv1.CarrierRateCondition{&shippingv1.CarrierRateCondition{
					Attribute: condition.GetAttribute(),
					Negate:    condition.GetNegate(),
					Operator:  condition.GetOperator(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.shipping.v1.CarrierRateCalculationService/CompareCarrierRateOptions")
		}
	}
	ctx = tenancy.With(ctx, "email", req.GetFraudRule().GetOwner().GetEmail())
	scoreRiskAssessmentRequest := &fraudv1.ScoreRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit:     &commonv1.AuditInfo{CreatedBy: req.GetFraudRule().GetAudit().GetCreatedBy()},
			Requester: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	}
	scoreRiskAssessmentReply, err := scoreRiskAssessmentCall(ctx, scoreRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/ScoreRiskAssessment")
	}
	return &fraudv1.PreviewFraudRuleResponse{
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				DisplayName: scoreRiskAssessmentReply.GetRiskAssessment().GetAssignee().GetDisplayName(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Version:     req.GetFraudRule().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeFraudRule normalizes a fraud rule.
func (s *fraudRuleValidationHandlers) NormalizeFraudRule(ctx context.Context, req *fraudv1.NormalizeFraudRuleRequest) (*fraudv1.NormalizeFraudRuleResponse, error) {
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "normalize fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	var reason string
	switch choice := req.GetFraudRule().GetAction().(type) {
	case *fraudv1.FraudRule_Route:
		reason = choice.Route.GetTarget()
	case *fraudv1.FraudRule_Block:
		reason = choice.Block.GetReason()
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	encoded, err := protojson.Marshal(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.FraudRule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	parameters := make(map[string]string, len(req.GetFraudRule().GetParameters()))
	for key2, value2 := range req.GetFraudRule().GetParameters() {
		parameters[key2] = value2
	}
	step := &fraudRuleValidationHandlersNormalizeFraudRuleSummarizeCollectionCasesStep{request: &collectionsv1.SummarizeCollectionCasesRequest{
		Context:     req.GetContext(),
		Reason:      reason,
		Annotations: annotations,
		Details: &collectionsv1.SummarizeCollectionCasesRequest_Details{
			Period:  (&decoded).GetEffective(),
			Filters: parameters,
		},
	}}
	if err := s.runNormalizeFraudRuleSummarizeCollectionCases(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.collections.v1.CollectionCaseQueryService/SummarizeCollectionCases")
	}
	summarizeCollectionCasesReply := step.reply
	annotateUserVersionRequest := &identityv1.AnnotateUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{Email: req.GetFraudRule().GetOwner().GetEmail()},
		},
	}
	annotateUserVersionReply, err := s.userHistoryLazy().AnnotateUserVersion(ctx, annotateUserVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/AnnotateUserVersion")
	}
	return &fraudv1.NormalizeFraudRuleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeCollectionCasesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				Email: annotateUserVersionReply.GetUser().GetRelationshipManager().GetEmail(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Name:        req.GetFraudRule().GetName(),
		},
	}, nil
}

// ExplainFraudRuleDecision explains a fraud rule.
func (s *fraudRuleValidationHandlers) ExplainFraudRuleDecision(ctx context.Context, req *fraudv1.ExplainFraudRuleDecisionRequest) (*fraudv1.ExplainFraudRuleDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "explain fraud rule decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	quoteFraudSignalRequest := &fraudv1.QuoteFraudSignalRequest{}
	quoteFraudSignalRequest.Context = req.GetContext()
	quoteFraudSignalRequest.Reason = req.GetFraudRule().GetBlock().GetReason()
	quoteFraudSignalRequest.FraudSignal = &fraudv1.FraudSignal{Detail: packed}
	quoteFraudSignalReply, err := s.fraudSignalCalculationClient.QuoteFraudSignal(ctx, quoteFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/QuoteFraudSignal")
	}
	recalculateFraudSignalsRequest := &fraudv1.RecalculateFraudSignalsRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				Version:     req.GetFraudRule().GetVersion(),
				DisplayName: quoteFraudSignalReply.GetFraudSignal().GetSubject().GetDisplayName(),
			},
			CorrelationId: quoteFraudSignalReply.GetResult().GetCorrelationId(),
		},
	}
	recalculateFraudSignalsReply := new(fraudv1.RecalculateFraudSignalsResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.fraud.v1.FraudSignalCalculationService/RecalculateFraudSignals", recalculateFraudSignalsRequest, recalculateFraudSignalsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/RecalculateFraudSignals")
	}
	return &fraudv1.ExplainFraudRuleDecisionResponse{
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{
				DisplayName: quoteFraudSignalReply.GetFraudSignal().GetSubject().GetDisplayName(),
			},
			Version:     recalculateFraudSignalsReply.GetFraudSignal().GetSubject().GetVersion(),
			FraudRuleId: req.GetFraudRuleId(),
			Description: req.GetFraudRule().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreFraudRule scores a fraud rule.
func (s *fraudRuleValidationHandlers) ScoreFraudRule(ctx context.Context, req *fraudv1.ScoreFraudRuleRequest) (*fraudv1.ScoreFraudRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreFraudRule", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "score fraud rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetFraudRule().GetOwner().GetDisplayName())
	normalizeFraudSignalReply, err := s.fraudSignalValidationClient.NormalizeFraudSignal(ctx, &fraudv1.NormalizeFraudSignalRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
		Details: &fraudv1.NormalizeFraudSignalRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetFraudRule().GetBlock().GetNotifyEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/NormalizeFraudSignal")
	}
	encoded, err := protojson.Marshal(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.FraudRule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	listSearchIndexSubscriptionsRequest := &searchv1.ListSearchIndexSubscriptionsRequest{}
	listSearchIndexSubscriptionsRequest.Context = req.GetContext()
	listSearchIndexSubscriptionsRequest.Filter = &searchv1.ListSearchIndexSubscriptionsRequest_Filter{
		Region:       req.GetDetails().GetDestination().GetRegion(),
		CreatedAfter: req.GetFraudRule().GetAudit().GetUpdatedAt(),
		OwnerId:      (&decoded).GetOwner().GetUserId(),
	}
	listSearchIndexSubscriptionsReply, err := s.searchIndexNotificationClient.ListSearchIndexSubscriptions(ctx, listSearchIndexSubscriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/ListSearchIndexSubscriptions")
	}
	step := &fraudRuleValidationHandlersScoreFraudRuleBrowseSessionsStep{request: &identityv1.BrowseSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.BrowseSessionsRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetFraudRule().GetAudit().GetUpdatedAt(),
			Labels:       listSearchIndexSubscriptionsReply.GetResult().GetDetails(),
		},
	}}
	if err := s.runScoreFraudRuleBrowseSessions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/BrowseSessions")
	}
	browseSessionsReply := step.reply
	packed, err := anypb.New(req.GetFraudRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recalculateFraudSignalsRequest := &fraudv1.RecalculateFraudSignalsRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: normalizeFraudSignalReply.GetFraudSignal().GetSubject().GetDisplayName(),
				FraudRuleId: normalizeFraudSignalReply.GetFraudSignal().GetSubject().GetFraudRuleId(),
			},
			Detail: packed,
		},
		Details: &fraudv1.RecalculateFraudSignalsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:   req.GetDetails().GetDestination().GetRegion(),
				Locality: req.GetDetails().GetDestination().GetLocality(),
			},
		},
	}
	type recalculateFraudSignalsResult struct {
		reply *fraudv1.RecalculateFraudSignalsResponse
		err   error
	}
	recalculateFraudSignalsResults := make(chan recalculateFraudSignalsResult, 1)
	go func() {
		reply, err := s.fraudSignalCalculationClient.RecalculateFraudSignals(ctx, recalculateFraudSignalsRequest)
		recalculateFraudSignalsResults <- recalculateFraudSignalsResult{reply, err}
	}()
	var recalculateFraudSignalsReply *fraudv1.RecalculateFraudSignalsResponse
	select {
	case result := <-recalculateFraudSignalsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fraud.v1.FraudSignalCalculationService/RecalculateFraudSignals")
		}
		recalculateFraudSignalsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &fraudv1.ScoreFraudRuleResponse{
		Result: &commonv1.OperationResult{
			Message:       normalizeFraudSignalReply.GetResult().GetMessage(),
			CorrelationId: listSearchIndexSubscriptionsReply.GetResult().GetCorrelationId(),
			Details:       browseSessionsReply.GetResult().GetDetails(),
			ReferenceIds:  browseSessionsReply.GetResult().GetReferenceIds(),
			Status:        recalculateFraudSignalsReply.GetResult().GetStatus(),
		},
		FraudRule: &fraudv1.FraudRule{
			FraudRuleId: recalculateFraudSignalsReply.GetFraudSignal().GetSubject().GetFraudRuleId(),
			Priority:    req.GetFraudRule().GetPriority(),
		},
		Breakdown: &fraudv1.ScoreFraudRuleResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// LintFraudRules lints a fraud rule.
func (s *fraudRuleValidationHandlers) LintFraudRules(ctx context.Context, req *fraudv1.LintFraudRulesRequest) (*fraudv1.LintFraudRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintFraudRules", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudRule(ctx, req.GetContext().GetTenantId(), req.GetFraudRule()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud rule")
	}
	s.log.InfoContext(ctx, "lint fraud rules", "tenant", req.GetContext().GetTenantId(), "etag", req.GetFraudRule().GetAudit().GetEtag())
	cloneWarehouseRequest := newCloneWarehouseRequest(withCloneWarehouseContext(req.GetContext()), withCloneWarehouseWarehouse(&warehousingv1.Warehouse{
		Audit: &commonv1.AuditInfo{UpdatedBy: req.GetFraudRule().GetAudit().GetUpdatedBy()},
	}))
	cloneWarehouseReply, err := s.warehouseClient.CloneWarehouse(ctx, cloneWarehouseRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseService/CloneWarehouse")
	}
	return &fraudv1.LintFraudRulesResponse{
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{
				CreatedAt: cloneWarehouseReply.GetWarehouse().GetAudit().GetCreatedAt(),
			},
			FraudRuleId: req.GetFraudRuleId(),
			Priority:    req.GetFraudRule().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetFraudRuleRules returns a fraud rule.
func (s *fraudRuleValidationHandlers) GetFraudRuleRules(ctx context.Context, req *fraudv1.GetFraudRuleRulesRequest) (*fraudv1.GetFraudRuleRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_rule", req.GetFraudRuleId(), "get_fraud_rule_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get fraud rule rules", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	estimateFraudSignalRequest := &fraudv1.EstimateFraudSignalRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject:       &fraudv1.FraudRuleRef{FraudRuleId: req.GetFraudRuleId()},
			Measurements:  []*fraudv1.FraudSignalMeasurement{&fraudv1.FraudSignalMeasurement{ObservedAt: cloned.GetDeadline()}},
			CorrelationId: req.GetContext().GetTraceId(),
			Source:        req.GetContext().GetTenantId(),
		},
	}
	estimateFraudSignalReply := new(fraudv1.EstimateFraudSignalResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.fraud.v1.FraudSignalCalculationService/EstimateFraudSignal", estimateFraudSignalRequest, estimateFraudSignalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/EstimateFraudSignal")
	}
	return &fraudv1.GetFraudRuleRulesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateFraudSignalReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		FraudRule: &fraudv1.FraudRule{FraudRuleId: req.GetFraudRuleId()},
	}, nil
}

// fraudRuleValidationHandlersNormalizeFraudRuleSummarizeCollectionCasesStep carries the request of one step of
// NormalizeFraudRule and, once it ran, its reply.
type fraudRuleValidationHandlersNormalizeFraudRuleSummarizeCollectionCasesStep struct {
	request *collectionsv1.SummarizeCollectionCasesRequest
	reply   *collectionsv1.SummarizeCollectionCasesResponse
}

func (s *fraudRuleValidationHandlers) runNormalizeFraudRuleSummarizeCollectionCases(ctx context.Context, step *fraudRuleValidationHandlersNormalizeFraudRuleSummarizeCollectionCasesStep) error {
	reply, err := s.collectionCaseQueryClient.SummarizeCollectionCases(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// userHistoryLazy dials acme.identity.v1.UserHistoryService the first time a handler needs it.
func (s *fraudRuleValidationHandlers) userHistoryLazy() identityv1.UserHistoryServiceClient {
	s.userHistoryOnce.Do(func() { s.userHistoryCached = identityv1.NewUserHistoryServiceClient(s.conn) })
	return s.userHistoryCached
}

// fraudRuleValidationHandlersScoreFraudRuleBrowseSessionsStep carries the request of one step of ScoreFraudRule and,
// once it ran, its reply.
type fraudRuleValidationHandlersScoreFraudRuleBrowseSessionsStep struct {
	request *identityv1.BrowseSessionsRequest
	reply   *identityv1.BrowseSessionsResponse
}

func (s *fraudRuleValidationHandlers) runScoreFraudRuleBrowseSessions(ctx context.Context, step *fraudRuleValidationHandlersScoreFraudRuleBrowseSessionsStep) error {
	reply, err := s.sessionQueryClient.BrowseSessions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
