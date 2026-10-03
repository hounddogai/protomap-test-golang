package giftcards

import (
	"context"
	"errors"
	"io"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	chatv1 "github.com/hounddogai/protomap-test-golang/gen/acme/chat/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// GiftCardTransactionCalculationServer implements `acme.giftcards.v1.GiftCardTransactionCalculationService`.
// Computes the figures of a gift card transaction, such as totals, taxes, and estimates.
type GiftCardTransactionCalculationServer struct {
	giftcardsv1.UnimplementedGiftCardTransactionCalculationServiceServer
	store                          *Store
	log                            *slog.Logger
	giftCardValidationClient       giftcardsv1.GiftCardValidationServiceClient
	searchIndexQueryClient         searchv1.SearchIndexQueryServiceClient
	featureFlagQueryClient         featureflagsv1.FeatureFlagQueryServiceClient
	meteredGiftCardValidation      meteredGiftCardValidation
	auditEventClient               auditv1.AuditEventServiceClient
	featureFlagValidationClient    featureflagsv1.FeatureFlagValidationServiceClient
	conn                           *grpc.ClientConn
	giftCardBatchCalculationClient giftcardsv1.GiftCardBatchCalculationServiceClient
	jobOpeningCalculationClient    recruitingv1.JobOpeningCalculationServiceClient
	synonymExportClient            searchv1.SynonymExportServiceClient
	clients                        *registry.Registry
}

// NewGiftCardTransactionCalculationServer wires a GiftCardTransactionCalculationServer.
func NewGiftCardTransactionCalculationServer(deps platform.Deps) *GiftCardTransactionCalculationServer {
	return &GiftCardTransactionCalculationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.giftcards.v1.GiftCardTransactionCalculationService"),
		giftCardValidationClient:       giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		searchIndexQueryClient:         searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		featureFlagQueryClient:         featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		meteredGiftCardValidation:      meteredGiftCardValidation{GiftCardValidationServiceClient: giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		auditEventClient:               auditv1.NewAuditEventServiceClient(deps.Conn),
		featureFlagValidationClient:    featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		giftCardBatchCalculationClient: giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		jobOpeningCalculationClient:    recruitingv1.NewJobOpeningCalculationServiceClient(deps.Conn),
		synonymExportClient:            searchv1.NewSynonymExportServiceClient(deps.Conn),
		clients:                        deps.Clients,
	}
}

// CalculateGiftCardTransaction calculates a gift card transaction.
func (s *GiftCardTransactionCalculationServer) CalculateGiftCardTransaction(ctx context.Context, req *giftcardsv1.CalculateGiftCardTransactionRequest) (*giftcardsv1.CalculateGiftCardTransactionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "calculate gift card transaction", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "gift_card_transaction_id", req.GetGiftCardTransaction().GetGiftCardTransactionId())
	explainGiftCardDecisionReply, err := s.giftCardValidationClient.ExplainGiftCardDecision(ctx, &giftcardsv1.ExplainGiftCardDecisionRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				GiftCardTransactionId: tenancy.Value(ctx, "gift_card_transaction_id"),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ExplainGiftCardDecision")
	}
	exportSearchIndexesStream, err := s.searchIndexQueryClient.ExportSearchIndexes(ctx, &searchv1.ExportSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchIndexesRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			Labels:       explainGiftCardDecisionReply.GetGiftCard().GetLabels(),
			CreatedAfter: explainGiftCardDecisionReply.GetGiftCard().GetAudit().GetUpdatedAt(),
			OwnerId:      explainGiftCardDecisionReply.GetGiftCard().GetGiftCardId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
	}
	for {
		received, err := exportSearchIndexesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/ExportSearchIndexes")
		}
		_ = received
	}
	return &giftcardsv1.CalculateGiftCardTransactionResponse{
		Result: &commonv1.OperationResult{
			Status: explainGiftCardDecisionReply.GetResult().GetStatus(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Kind:                  explainGiftCardDecisionReply.GetGiftCard().GetHolder().GetKind(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
		},
		Breakdown: &giftcardsv1.CalculateGiftCardTransactionResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
	}, nil
}

// EstimateGiftCardTransaction estimates a gift card transaction.
func (s *GiftCardTransactionCalculationServer) EstimateGiftCardTransaction(ctx context.Context, req *giftcardsv1.EstimateGiftCardTransactionRequest) (*giftcardsv1.EstimateGiftCardTransactionResponse, error) {
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "estimate gift card transaction", "tenant", req.GetContext().GetTenantId())
	for measurement := range iterx.Each(req.GetGiftCardTransaction().GetMeasurements(), func(candidate *giftcardsv1.GiftCardTransactionMeasurement) bool { return candidate != nil }) {
		if _, err := s.giftCardValidationClient.ValidateGiftCard(ctx, &giftcardsv1.ValidateGiftCardRequest{
			Context: req.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
				Holder: &giftcardsv1.GiftCardTransactionRef{
					DisplayName:           req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
					Kind:                  req.GetGiftCardTransaction().GetKind(),
					GiftCardTransactionId: req.GetGiftCardTransactionId(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
		}
	}
	return &giftcardsv1.EstimateGiftCardTransactionResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Source:                req.GetGiftCardTransaction().GetSource(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteGiftCardTransaction quotes a gift card transaction.
func (s *GiftCardTransactionCalculationServer) QuoteGiftCardTransaction(ctx context.Context, req *giftcardsv1.QuoteGiftCardTransactionRequest) (*giftcardsv1.QuoteGiftCardTransactionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "quote gift card transaction", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*giftcardsv1.PreviewGiftCardResponse, len(req.GetGiftCardTransaction().GetMeasurements()))
	for i, measurement := range req.GetGiftCardTransaction().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.giftCardValidationClient.PreviewGiftCard(groupCtx, &giftcardsv1.PreviewGiftCardRequest{
				Context: req.GetContext(),
				GiftCard: &giftcardsv1.GiftCard{
					Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
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
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/PreviewGiftCard")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	cloned := proto.Clone(req.GetDetails()).(*giftcardsv1.QuoteGiftCardTransactionRequest_Details)
	exportFeatureFlagsStream, err := s.featureFlagQueryClient.ExportFeatureFlags(ctx, &featureflagsv1.ExportFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.ExportFeatureFlagsRequest_Filter{
			Region:  cloned.GetDestination().GetRegion(),
			OwnerId: req.GetGiftCardTransaction().GetGiftCardTransactionId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/ExportFeatureFlags")
	}
	for {
		received, err := exportFeatureFlagsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/ExportFeatureFlags")
		}
		_ = received
	}
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateGiftCardRequest := &giftcardsv1.ValidateGiftCardRequest{
		Context:    req.GetContext(),
		GiftCardId: envelope.GetDisplayName(),
	}
	validateGiftCardReply, err := s.meteredGiftCardValidation.ValidateGiftCard(ctx, validateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	}
	return &giftcardsv1.QuoteGiftCardTransactionResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			GiftCardTransactionId: validateGiftCardReply.GetGiftCard().GetHolder().GetGiftCardTransactionId(),
			Subject:               req.GetGiftCardTransaction().GetSubject(),
		},
		Breakdown: &giftcardsv1.QuoteGiftCardTransactionResponse_Breakdown{CurrencyCode: req.GetDetails().GetCurrencyCode()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareGiftCardTransactionOptions compares a gift card transaction.
func (s *GiftCardTransactionCalculationServer) CompareGiftCardTransactionOptions(ctx context.Context, req *giftcardsv1.CompareGiftCardTransactionOptionsRequest) (*giftcardsv1.CompareGiftCardTransactionOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CompareGiftCardTransactionOptions", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "compare gift card transaction options", "tenant", req.GetContext().GetTenantId())
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	watchAuditEventsStream, err := s.auditEventClient.WatchAuditEvents(ctx, &auditv1.WatchAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.WatchAuditEventsRequest_Filter{
			Region: req.GetDetails().GetDestination().GetRegion(),
			Query:  envelope.GetDisplayName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
	}
	for {
		received, err := watchAuditEventsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
		}
		_ = received
	}
	explainFeatureFlagDecisionRequest := &featureflagsv1.ExplainFeatureFlagDecisionRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Action: &featureflagsv1.FeatureFlag_Block{Block: &featureflagsv1.FeatureFlagBlockAction{Reason: req.GetReason()}},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
		},
	}
	type explainFeatureFlagDecisionResult struct {
		reply *featureflagsv1.ExplainFeatureFlagDecisionResponse
		err   error
	}
	explainFeatureFlagDecisionResults := make(chan explainFeatureFlagDecisionResult, 1)
	go func() {
		reply, err := s.featureFlagValidationClient.ExplainFeatureFlagDecision(ctx, explainFeatureFlagDecisionRequest)
		explainFeatureFlagDecisionResults <- explainFeatureFlagDecisionResult{reply, err}
	}()
	var explainFeatureFlagDecisionReply *featureflagsv1.ExplainFeatureFlagDecisionResponse
	select {
	case result := <-explainFeatureFlagDecisionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision")
		}
		explainFeatureFlagDecisionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	source := req.GetDetails().GetDestination().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	var envelope2 giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	searchChatRoomsRequest := &chatv1.SearchChatRoomsRequest{
		Context: req.GetContext(),
		Filter: &chatv1.SearchChatRoomsRequest_Filter{
			Region:       region,
			CreatedAfter: explainFeatureFlagDecisionReply.GetFeatureFlag().GetAudit().GetCreatedAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	}
	searchChatRoomsReply, err := grpcx.NewClient(s.conn, chatv1.NewChatRoomServiceClient).SearchChatRooms(ctx, searchChatRoomsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.chat.v1.ChatRoomService/SearchChatRooms")
	}
	return &giftcardsv1.CompareGiftCardTransactionOptionsResponse{
		Result: &commonv1.OperationResult{
			Status:        explainFeatureFlagDecisionReply.GetOutcome().GetStatus(),
			Message:       explainFeatureFlagDecisionReply.GetOutcome().GetMessage(),
			CorrelationId: searchChatRoomsReply.GetResult().GetCorrelationId(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId:         searchChatRoomsReply.GetResult().GetCorrelationId(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Subject:               req.GetGiftCardTransaction().GetSubject(),
		},
	}, nil
}

// RecalculateGiftCardTransactions recalculates a gift card transaction.
func (s *GiftCardTransactionCalculationServer) RecalculateGiftCardTransactions(ctx context.Context, req *giftcardsv1.RecalculateGiftCardTransactionsRequest) (*giftcardsv1.RecalculateGiftCardTransactionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "recalculate gift card transactions", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := giftCardTransactionCalculationServerValidateGiftCardSteps[mode]
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateGiftCardRequest := &giftcardsv1.ValidateGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName:           req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
				GiftCardTransactionId: req.GetGiftCardTransaction().GetGiftCardTransactionId(),
				Kind:                  req.GetGiftCardTransaction().GetKind(),
			},
		},
		GiftCardId: envelope.GetDisplayName(),
	}
	validateGiftCardReply, err := run(s, ctx, validateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	}
	ctx = tenancy.With(ctx, "gift_card_batch_id", req.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId())
	getGiftCardBatchRatesReply, err := s.giftCardBatchCalculationClient.GetGiftCardBatchRates(ctx, &giftcardsv1.GetGiftCardBatchRatesRequest{
		Context:         req.GetContext(),
		GiftCardBatchId: tenancy.Value(ctx, "gift_card_batch_id"),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/GetGiftCardBatchRates")
	}
	return &giftcardsv1.RecalculateGiftCardTransactionsResponse{
		Result: &commonv1.OperationResult{Status: validateGiftCardReply.GetResult().GetStatus()},
		Breakdown: &giftcardsv1.RecalculateGiftCardTransactionsResponse_Breakdown{
			Total:        getGiftCardBatchRatesReply.GetGiftCardBatch().GetTotal(),
			CurrencyCode: req.GetDetails().GetCurrencyCode(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{GiftCardTransactionId: req.GetGiftCardTransactionId()},
	}, nil
}

// SimulateGiftCardTransaction simulates a gift card transaction.
func (s *GiftCardTransactionCalculationServer) SimulateGiftCardTransaction(ctx context.Context, req *giftcardsv1.SimulateGiftCardTransactionRequest) (*giftcardsv1.SimulateGiftCardTransactionResponse, error) {
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "simulate gift card transaction", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *giftcardsv1.SimulateGiftCardTransactionRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &giftCardTransactionCalculationServerSimulateGiftCardTransactionScoreGiftCardStep{request: &giftcardsv1.ScoreGiftCardRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ScoreGiftCardRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: cachedDetails.GetDestination().GetRecipient(),
				Region:    tenancy.Value(ctx, "region"),
				Locality:  req.GetDetails().GetDestination().GetLocality(),
			},
		},
		GiftCardId: envelope.GetDisplayName(),
	}}
	if err := s.runSimulateGiftCardTransactionScoreGiftCard(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ScoreGiftCard")
	}
	scoreGiftCardReply := step.reply
	detailsCache2 := cache.New[string, *giftcardsv1.SimulateGiftCardTransactionRequest_Details](1)
	detailsCache2.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails2, _ := detailsCache2.Get(req.GetContext().GetTraceId())
	var envelope2 giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	quoteJobOpeningRequest := &recruitingv1.QuoteJobOpeningRequest{
		Context: req.GetContext(),
		JobOpening: &recruitingv1.JobOpening{
			Settlement: &recruitingv1.JobOpening_Card{Card: &recruitingv1.JobOpeningCardSettlement{
				Network: scoreGiftCardReply.GetGiftCard().GetCard().GetNetwork(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: scoreGiftCardReply.GetGiftCard().GetAudit().GetCreatedAt(),
				UpdatedBy: scoreGiftCardReply.GetGiftCard().GetAudit().GetUpdatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{Recipient: cachedDetails2.GetDestination().GetRecipient()},
		},
		JobOpeningId: envelope2.GetDisplayName(),
	}
	quoteJobOpeningReply, err := recruitingv1.JobOpeningCalculationServiceClient.QuoteJobOpening(s.jobOpeningCalculationClient, ctx, quoteJobOpeningRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.recruiting.v1.JobOpeningCalculationService/QuoteJobOpening")
	}
	_ = quoteJobOpeningReply
	return &giftcardsv1.SimulateGiftCardTransactionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreGiftCardReply.GetResult().GetReferenceIds(),
			Status:       scoreGiftCardReply.GetResult().GetStatus(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			CorrelationId:         req.GetGiftCardTransaction().GetCorrelationId(),
		},
	}, nil
}

// ExplainGiftCardTransactionCalculation explains a gift card transaction.
func (s *GiftCardTransactionCalculationServer) ExplainGiftCardTransactionCalculation(ctx context.Context, req *giftcardsv1.ExplainGiftCardTransactionCalculationRequest) (*giftcardsv1.ExplainGiftCardTransactionCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainGiftCardTransactionCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "explain gift card transaction calculation", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*searchv1.RetrySynonymExportResponse, len(req.GetGiftCardTransaction().GetMeasurements()))
	for i, measurement := range req.GetGiftCardTransaction().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.synonymExportClient.RetrySynonymExport(groupCtx, &searchv1.RetrySynonymExportRequest{
				Context: req.GetContext(),
				Synonym: &searchv1.Synonym{
					Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
					Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
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
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/RetrySynonymExport")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	synonymSync, ok := s.clients.Get("acme.search.v1.SynonymSyncService").(searchv1.SynonymSyncServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SynonymSyncService")
	}
	snapshot, err := protojson.Marshal(req.GetGiftCardTransaction())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullSynonymsRequest := &searchv1.PullSynonymsRequest{
		Context: req.GetContext(),
		Details: &searchv1.PullSynonymsRequest_Details{Payload: snapshot},
	}
	pullSynonymsReply, err := synonymSync.PullSynonyms(ctx, pullSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PullSynonyms")
	}
	return &giftcardsv1.ExplainGiftCardTransactionCalculationResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: pullSynonymsReply.GetSynonym().GetOwner().GetDisplayName(),
			},
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Attributes:            req.GetGiftCardTransaction().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetGiftCardTransactionRates returns a gift card transaction.
func (s *GiftCardTransactionCalculationServer) GetGiftCardTransactionRates(ctx context.Context, req *giftcardsv1.GetGiftCardTransactionRatesRequest) (*giftcardsv1.GetGiftCardTransactionRatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetGiftCardTransactionRates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "get_gift_card_transaction_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get gift card transaction rates", "tenant", req.GetContext().GetTenantId())
	countSearchIndexesRequest := &searchv1.CountSearchIndexesRequest{}
	countSearchIndexesRequest.Context = req.GetContext()
	countSearchIndexesRequest.Filter = &searchv1.CountSearchIndexesRequest_Filter{
		OwnerId:      req.GetGiftCardTransactionId(),
		CreatedAfter: req.GetContext().GetDeadline(),
	}
	countSearchIndexesReply, err := s.searchIndexQueryClient.CountSearchIndexes(ctx, countSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/CountSearchIndexes")
	}
	return &giftcardsv1.GetGiftCardTransactionRatesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: countSearchIndexesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{GiftCardTransactionId: req.GetGiftCardTransactionId()},
	}, nil
}

// giftCardTransactionCalculationServerValidateGiftCardSteps are the ways to run
// acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard, by name.
var giftCardTransactionCalculationServerValidateGiftCardSteps = map[string]func(*GiftCardTransactionCalculationServer, context.Context, *giftcardsv1.ValidateGiftCardRequest) (*giftcardsv1.ValidateGiftCardResponse, error){
	"default": (*GiftCardTransactionCalculationServer).validateGiftCardStep,
	"audited": (*GiftCardTransactionCalculationServer).validateGiftCardAuditedStep,
}

func (s *GiftCardTransactionCalculationServer) validateGiftCardStep(ctx context.Context, request *giftcardsv1.ValidateGiftCardRequest) (*giftcardsv1.ValidateGiftCardResponse, error) {
	return s.giftCardValidationClient.ValidateGiftCard(ctx, request)
}

func (s *GiftCardTransactionCalculationServer) validateGiftCardAuditedStep(ctx context.Context, request *giftcardsv1.ValidateGiftCardRequest) (*giftcardsv1.ValidateGiftCardResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	return s.giftCardValidationClient.ValidateGiftCard(ctx, request)
}

// giftCardTransactionCalculationServerSimulateGiftCardTransactionScoreGiftCardStep carries the request of one step of
// SimulateGiftCardTransaction and, once it ran, its reply.
type giftCardTransactionCalculationServerSimulateGiftCardTransactionScoreGiftCardStep struct {
	request *giftcardsv1.ScoreGiftCardRequest
	reply   *giftcardsv1.ScoreGiftCardResponse
}

func (s *GiftCardTransactionCalculationServer) runSimulateGiftCardTransactionScoreGiftCard(ctx context.Context, step *giftCardTransactionCalculationServerSimulateGiftCardTransactionScoreGiftCardStep) error {
	reply, err := s.giftCardValidationClient.ScoreGiftCard(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
