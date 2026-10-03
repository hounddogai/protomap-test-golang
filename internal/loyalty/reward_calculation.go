package loyalty

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	deliveriesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/deliveries/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RewardCalculationServer implements `acme.loyalty.v1.RewardCalculationService`.
// Computes the figures of a reward, such as totals, taxes, and estimates.
type RewardCalculationServer struct {
	loyaltyv1.UnimplementedRewardCalculationServiceServer
	store                          *Store
	log                            *slog.Logger
	conn                           *grpc.ClientConn
	loyaltyMemberValidationClient  loyaltyv1.LoyaltyMemberValidationServiceClient
	giftCardBatchValidationPort    giftCardBatchValidationPort
	searchIndexHistoryClient       searchv1.SearchIndexHistoryServiceClient
	giftCardBatchQueryClient       giftcardsv1.GiftCardBatchQueryServiceClient
	clients                        *registry.Registry
	bus                            *eventbus.Bus
	deliveryQueryClient            deliveriesv1.DeliveryQueryServiceClient
	giftCardBatchCalculationClient giftcardsv1.GiftCardBatchCalculationServiceClient
	pointsLedgerCalculationClient  loyaltyv1.PointsLedgerCalculationServiceClient
}

// NewRewardCalculationServer wires a RewardCalculationServer.
func NewRewardCalculationServer(deps platform.Deps) *RewardCalculationServer {
	return &RewardCalculationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.loyalty.v1.RewardCalculationService"),
		conn:                           deps.Conn,
		loyaltyMemberValidationClient:  loyaltyv1.NewLoyaltyMemberValidationServiceClient(deps.Conn),
		giftCardBatchValidationPort:    newGiftCardBatchValidationPort(deps),
		searchIndexHistoryClient:       searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		giftCardBatchQueryClient:       giftcardsv1.NewGiftCardBatchQueryServiceClient(deps.Conn),
		clients:                        deps.Clients,
		bus:                            deps.Bus,
		deliveryQueryClient:            deliveriesv1.NewDeliveryQueryServiceClient(deps.Conn),
		giftCardBatchCalculationClient: giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		pointsLedgerCalculationClient:  loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
	}
}

// CalculateReward calculates a reward.
func (s *RewardCalculationServer) CalculateReward(ctx context.Context, req *loyaltyv1.CalculateRewardRequest) (*loyaltyv1.CalculateRewardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "calculate reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "recipient", req.GetDetails().GetDestination().GetRecipient())
	rewardCache := cache.New[string, *loyaltyv1.Reward](1)
	rewardCache.Put(req.GetContext().GetTraceId(), req.GetReward())
	cachedReward, _ := rewardCache.Get(req.GetContext().GetTraceId())
	var holder struct{ CreatedBy string }
	if record := req.GetReward().GetAudit(); record != nil {
		if err := mapper.CopyFields(&holder, record, "CreatedBy"); err != nil {
			s.log.WarnContext(ctx, "copy created_by", "error", err)
		}
	}
	normalizePlacementRequest := &merchandisingv1.NormalizePlacementRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Location: &commonv1.PostalAddress{Recipient: tenancy.Value(ctx, "recipient")},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cachedReward.GetAudit().GetUpdatedAt(),
				CreatedBy: holder.CreatedBy,
			},
		},
		Details: &merchandisingv1.NormalizePlacementRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetReward().GetOwner().GetEmail()},
		},
	}
	normalizePlacementReply := new(merchandisingv1.NormalizePlacementResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.merchandising.v1.PlacementValidationService/NormalizePlacement", normalizePlacementRequest, normalizePlacementReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementValidationService/NormalizePlacement")
	}
	normalizeLoyaltyMemberReply, err := s.loyaltyMemberValidationClient.NormalizeLoyaltyMember(ctx, &loyaltyv1.NormalizeLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizePlacementReply.GetPlacement().GetAudit().GetUpdatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/NormalizeLoyaltyMember")
	}
	return &loyaltyv1.CalculateRewardResponse{
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: normalizePlacementReply.GetPlacement().GetAudit().GetUpdatedBy(),
			},
			RewardId: req.GetRewardId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: normalizePlacementReply.GetResult().GetCorrelationId(),
			Message:       normalizeLoyaltyMemberReply.GetOutcome().GetMessage(),
			Status:        normalizeLoyaltyMemberReply.GetResult().GetStatus(),
		},
		Breakdown: &loyaltyv1.CalculateRewardResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// EstimateReward estimates a reward.
func (s *RewardCalculationServer) EstimateReward(ctx context.Context, req *loyaltyv1.EstimateRewardRequest) (*loyaltyv1.EstimateRewardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimateReward", "error", err)
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "estimate reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	explainGiftCardBatchDecisionRequest := &giftcardsv1.ExplainGiftCardBatchDecisionRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Counterparty: &giftcardsv1.GiftCardRef{DisplayName: req.GetReward().GetOwner().GetDisplayName()},
		},
		Details: &giftcardsv1.ExplainGiftCardBatchDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetReward().GetOwner().GetEmail()},
		},
	}
	explainGiftCardBatchDecisionReply, err := s.giftCardBatchValidationPort.ExplainGiftCardBatchDecision(ctx, explainGiftCardBatchDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchValidationService/ExplainGiftCardBatchDecision")
	}
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	listSearchIndexVersionsRequest := &searchv1.ListSearchIndexVersionsRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.ListSearchIndexVersionsRequest_Filter{Region: tenancy.Value(ctx, "region")},
	}
	var listSearchIndexVersionsReply *searchv1.ListSearchIndexVersionsResponse
	if err := pipeline.Run(ctx, "acme.search.v1.SearchIndexHistoryService/ListSearchIndexVersions", func(ctx context.Context) error {
		answer, err := s.searchIndexHistoryClient.ListSearchIndexVersions(ctx, listSearchIndexVersionsRequest)
		listSearchIndexVersionsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/ListSearchIndexVersions")
	}
	return &loyaltyv1.EstimateRewardResponse{
		Breakdown: &loyaltyv1.EstimateRewardResponse_Breakdown{
			CurrencyCode: explainGiftCardBatchDecisionReply.GetGiftCardBatch().GetCounterparty().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: listSearchIndexVersionsReply.GetResult().GetCorrelationId(),
			Status:        listSearchIndexVersionsReply.GetResult().GetStatus(),
		},
		Reward: &loyaltyv1.Reward{
			RewardId:    req.GetRewardId(),
			Description: req.GetReward().GetDescription(),
		},
	}, nil
}

// QuoteReward quotes a reward.
func (s *RewardCalculationServer) QuoteReward(ctx context.Context, req *loyaltyv1.QuoteRewardRequest) (*loyaltyv1.QuoteRewardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected QuoteReward", "error", err)
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "quote reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	var reason string
	switch choice := req.GetReward().GetAction().(type) {
	case *loyaltyv1.Reward_Route:
		reason = choice.Route.GetTarget()
	case *loyaltyv1.Reward_Block:
		reason = choice.Block.GetReason()
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeGiftCardBatchesRequest := &giftcardsv1.SummarizeGiftCardBatchesRequest{}
	summarizeGiftCardBatchesRequest.Context = req.GetContext()
	summarizeGiftCardBatchesRequest.Reason = reason
	summarizeGiftCardBatchesRequest.Annotations = annotations
	summarizeGiftCardBatchesRequest.Details = &giftcardsv1.SummarizeGiftCardBatchesRequest_Details{Period: req.GetReward().GetEffective()}
	summarizeGiftCardBatchesReply, err := s.giftCardBatchQueryClient.SummarizeGiftCardBatches(ctx, summarizeGiftCardBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchQueryService/SummarizeGiftCardBatches")
	}
	lintLoyaltyMembersRequest := &loyaltyv1.LintLoyaltyMembersRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder: &loyaltyv1.RewardRef{DisplayName: req.GetReward().GetOwner().GetDisplayName()},
			Audit:  &commonv1.AuditInfo{UpdatedBy: req.GetReward().GetAudit().GetUpdatedBy()},
		},
	}
	lintLoyaltyMembersReply, err := grpcx.NewClient(s.conn, loyaltyv1.NewLoyaltyMemberValidationServiceClient).LintLoyaltyMembers(ctx, lintLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/LintLoyaltyMembers")
	}
	giftCardBatchCalculation, ok := s.clients.Get("acme.giftcards.v1.GiftCardBatchCalculationService").(giftcardsv1.GiftCardBatchCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardBatchCalculationService")
	}
	cloned := proto.Clone(req.GetDetails()).(*loyaltyv1.QuoteRewardRequest_Details)
	estimateGiftCardBatchRequest := &giftcardsv1.EstimateGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{
				Tax: &giftcardsv1.GiftCardBatchLineTax{Amount: req.GetReward().GetDiscount().GetAmount()},
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: lintLoyaltyMembersReply.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient:   req.GetDetails().GetDestination().GetRecipient(),
				CountryCode: cloned.GetDestination().GetCountryCode(),
			},
		},
	}
	estimateGiftCardBatchReply, err := giftCardBatchCalculation.EstimateGiftCardBatch(ctx, estimateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/EstimateGiftCardBatch")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "loyalty.quote_reward.explain_loyalty_member_decision",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"loyalty_member.audit.created_by": req.GetReward().GetAudit().GetCreatedBy(),
			"context.tenant_id":               req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &loyaltyv1.QuoteRewardResponse{
		Result: &commonv1.OperationResult{
			Status:  summarizeGiftCardBatchesReply.GetResult().GetStatus(),
			Details: summarizeGiftCardBatchesReply.GetResult().GetDetails(),
		},
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: estimateGiftCardBatchReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
			},
			RewardId: req.GetRewardId(),
			Status:   req.GetReward().GetStatus(),
		},
	}, nil
}

// CompareRewardOptions compares a reward.
func (s *RewardCalculationServer) CompareRewardOptions(ctx context.Context, req *loyaltyv1.CompareRewardOptionsRequest) (*loyaltyv1.CompareRewardOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "compare reward options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	countDeliveriesRequest := &deliveriesv1.CountDeliveriesRequest{
		Context: req.GetContext(),
		Filter: &deliveriesv1.CountDeliveriesRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: req.GetReward().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetRewardId(),
			Labels:       req.GetAnnotations(),
		},
	}
	countDeliveriesReply, err := deliveriesv1.DeliveryQueryServiceClient.CountDeliveries(s.deliveryQueryClient, ctx, countDeliveriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.deliveries.v1.DeliveryQueryService/CountDeliveries")
	}
	return &loyaltyv1.CompareRewardOptionsResponse{
		Result: &commonv1.OperationResult{
			Message: countDeliveriesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Reward: &loyaltyv1.Reward{
			RewardId: req.GetRewardId(),
			Owner:    req.GetReward().GetOwner(),
		},
	}, nil
}

// RecalculateRewards recalculates a reward.
func (s *RewardCalculationServer) RecalculateRewards(ctx context.Context, req *loyaltyv1.RecalculateRewardsRequest) (*loyaltyv1.RecalculateRewardsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "recalculate rewards", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	loyaltyMemberValidation, ok := s.clients.Get("acme.loyalty.v1.LoyaltyMemberValidationService").(loyaltyv1.LoyaltyMemberValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.loyalty.v1.LoyaltyMemberValidationService")
	}
	explainLoyaltyMemberDecisionRequest := &loyaltyv1.ExplainLoyaltyMemberDecisionRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: req.GetReward().GetOwner().GetEmail()},
		},
	}
	explainLoyaltyMemberDecisionReply, err := loyaltyMemberValidation.ExplainLoyaltyMemberDecision(ctx, explainLoyaltyMemberDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberValidationService/ExplainLoyaltyMemberDecision")
	}
	source := req.GetReward().GetOwner().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	recalculateGiftCardBatchesRequest := &giftcardsv1.RecalculateGiftCardBatchesRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{
				Lines:       req.GetDetails().GetDestination().GetLines(),
				CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			},
			Counterparty: &giftcardsv1.GiftCardRef{DisplayName: displayName},
			Lines:        []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{Amount: req.GetReward().GetDiscount().GetAmount()}},
		},
	}
	type recalculateGiftCardBatchesResult struct {
		reply *giftcardsv1.RecalculateGiftCardBatchesResponse
		err   error
	}
	recalculateGiftCardBatchesResults := make(chan recalculateGiftCardBatchesResult, 1)
	go func() {
		reply, err := s.giftCardBatchCalculationClient.RecalculateGiftCardBatches(ctx, recalculateGiftCardBatchesRequest)
		recalculateGiftCardBatchesResults <- recalculateGiftCardBatchesResult{reply, err}
	}()
	var recalculateGiftCardBatchesReply *giftcardsv1.RecalculateGiftCardBatchesResponse
	select {
	case result := <-recalculateGiftCardBatchesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.giftcards.v1.GiftCardBatchCalculationService/RecalculateGiftCardBatches")
		}
		recalculateGiftCardBatchesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &loyaltyv1.RecalculateRewardsResponse{
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{
				Email:       explainLoyaltyMemberDecisionReply.GetLoyaltyMember().GetContact().GetEmail(),
				DisplayName: recalculateGiftCardBatchesReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
			RewardId: req.GetRewardId(),
			Priority: req.GetReward().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			Message: explainLoyaltyMemberDecisionReply.GetResult().GetMessage(),
			Status:  recalculateGiftCardBatchesReply.GetResult().GetStatus(),
		},
	}, nil
}

// SimulateReward simulates a reward.
func (s *RewardCalculationServer) SimulateReward(ctx context.Context, req *loyaltyv1.SimulateRewardRequest) (*loyaltyv1.SimulateRewardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "simulate reward", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	comparePointsLedgerOptionsRequest := &loyaltyv1.ComparePointsLedgerOptionsRequest{}
	comparePointsLedgerOptionsRequest.Context = req.GetContext()
	comparePointsLedgerOptionsRequest.Details = &loyaltyv1.ComparePointsLedgerOptionsRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: req.GetDetails().GetDestination().GetCountryCode(),
			Recipient:   req.GetDetails().GetDestination().GetRecipient(),
		},
	}
	comparePointsLedgerOptionsRequest.PointsLedger = &loyaltyv1.PointsLedger{
		Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{Amount: req.GetReward().GetDiscount().GetAmount()}},
	}
	comparePointsLedgerOptionsReply, err := s.pointsLedgerCalculationClient.ComparePointsLedgerOptions(ctx, comparePointsLedgerOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/ComparePointsLedgerOptions")
	}
	return &loyaltyv1.SimulateRewardResponse{
		Breakdown: &loyaltyv1.SimulateRewardResponse_Breakdown{
			Total: comparePointsLedgerOptionsReply.GetBreakdown().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			Status: comparePointsLedgerOptionsReply.GetResult().GetStatus(),
		},
		Reward: &loyaltyv1.Reward{
			RewardId: req.GetRewardId(),
			Status:   req.GetReward().GetStatus(),
		},
	}, nil
}

// GetRewardRates returns a reward.
func (s *RewardCalculationServer) GetRewardRates(ctx context.Context, req *loyaltyv1.GetRewardRatesRequest) (*loyaltyv1.GetRewardRatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "get_reward_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get reward rates", "tenant", req.GetContext().GetTenantId())
	annotateSearchIndexVersionReply, err := s.searchIndexHistoryClient.AnnotateSearchIndexVersion(ctx, &searchv1.AnnotateSearchIndexVersionRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Components: map[string]*searchv1.SearchIndexComponent{"primary": &searchv1.SearchIndexComponent{ComponentId: req.GetRewardId()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/AnnotateSearchIndexVersion")
	}
	return &loyaltyv1.GetRewardRatesResponse{
		Reward: &loyaltyv1.Reward{
			Description: annotateSearchIndexVersionReply.GetSearchIndex().GetComponents()["primary"].GetDescription(),
			RewardId:    req.GetRewardId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
