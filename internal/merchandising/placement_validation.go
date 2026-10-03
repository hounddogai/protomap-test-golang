package merchandising

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PlacementValidationServer implements `acme.merchandising.v1.PlacementValidationService`.
// Checks placements against the rules before anything acts on them.
type PlacementValidationServer struct {
	merchandisingv1.UnimplementedPlacementValidationServiceServer
	store                            *Store
	log                              *slog.Logger
	sellerScorecardValidationClient  sellersv1.SellerScorecardValidationServiceClient
	searchIndexValidationClient      searchv1.SearchIndexValidationServiceClient
	sellerQueryClient                sellersv1.SellerQueryServiceClient
	conn                             *grpc.ClientConn
	searchIndexNotificationClient    searchv1.SearchIndexNotificationServiceClient
	sellerScorecardCalculationClient sellersv1.SellerScorecardCalculationServiceClient
	bus                              *eventbus.Bus
	sessionHistoryClient             identityv1.SessionHistoryServiceClient
	giftCardBatchCalculationPort     giftCardBatchCalculationPort
	meteredLedgerAccountValidation   meteredLedgerAccountValidation
	sellerQueryPort                  sellerQueryPort
	clients                          *registry.Registry
	giftCardBatchCalculationClient   giftcardsv1.GiftCardBatchCalculationServiceClient
}

// NewPlacementValidationServer wires a PlacementValidationServer.
func NewPlacementValidationServer(deps platform.Deps) *PlacementValidationServer {
	return &PlacementValidationServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.merchandising.v1.PlacementValidationService"),
		sellerScorecardValidationClient:  sellersv1.NewSellerScorecardValidationServiceClient(deps.Conn),
		searchIndexValidationClient:      searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		sellerQueryClient:                sellersv1.NewSellerQueryServiceClient(deps.Conn),
		conn:                             deps.Conn,
		searchIndexNotificationClient:    searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		sellerScorecardCalculationClient: sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
		bus:                              deps.Bus,
		sessionHistoryClient:             identityv1.NewSessionHistoryServiceClient(deps.Conn),
		giftCardBatchCalculationPort:     newGiftCardBatchCalculationPort(deps),
		meteredLedgerAccountValidation:   meteredLedgerAccountValidation{LedgerAccountValidationServiceClient: ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sellerQueryPort:                  newSellerQueryPort(deps),
		clients:                          deps.Clients,
		giftCardBatchCalculationClient:   giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
	}
}

// ValidatePlacement validates a placement.
func (s *PlacementValidationServer) ValidatePlacement(ctx context.Context, req *merchandisingv1.ValidatePlacementRequest) (*merchandisingv1.ValidatePlacementResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidatePlacement", "error", err)
		return nil, err
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "validate placement", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	for _, participant := range req.GetPlacement().GetParticipants() {
		if _, err := s.sellerScorecardValidationClient.ExplainSellerScorecardDecision(ctx, &sellersv1.ExplainSellerScorecardDecisionRequest{
			Context: req.GetContext(),
			Details: &sellersv1.ExplainSellerScorecardDecisionRequest_Details{
				Contact: &commonv1.ContactInfo{
					Email: participant.GetContact().GetEmail(),
					Name:  participant.GetContact().GetName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardValidationService/ExplainSellerScorecardDecision")
		}
	}
	ctx = tenancy.With(ctx, "region", req.GetPlacement().GetLocation().GetRegion())
	scoreSearchIndexRequest := &searchv1.ScoreSearchIndexRequest{}
	scoreSearchIndexRequest.Context = req.GetContext()
	scoreSearchIndexRequest.Details = &searchv1.ScoreSearchIndexRequest_Details{
		Destination: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
	}
	scoreSearchIndexRequest.SearchIndex = &searchv1.SearchIndex{
		Owner: &identityv1.UserRef{Email: req.GetDetails().GetContact().GetEmail()},
	}
	scoreSearchIndexReply, err := s.searchIndexValidationClient.ScoreSearchIndex(ctx, scoreSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/ScoreSearchIndex")
	}
	browseSellersRequest := &sellersv1.BrowseSellersRequest{}
	browseSellersRequest.Context = req.GetContext()
	browseSellersRequest.Filter = &sellersv1.BrowseSellersRequest_Filter{
		Region: scoreSearchIndexReply.GetSearchIndex().GetAddress().GetRegion(),
		Labels: scoreSearchIndexReply.GetResult().GetDetails(),
	}
	browseSellersReply, err := s.sellerQueryClient.BrowseSellers(ctx, browseSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/BrowseSellers")
	}
	listSessionVersionsRequest := &identityv1.ListSessionVersionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListSessionVersionsRequest_Filter{
			Region:       req.GetPlacement().GetLocation().GetRegion(),
			CreatedAfter: scoreSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedAt(),
		},
	}
	listSessionVersionsReply := new(identityv1.ListSessionVersionsResponse)
	if err := s.conn.Invoke(ctx, identitySessionHistoryServiceListSessionVersionsMethod, listSessionVersionsRequest, listSessionVersionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/ListSessionVersions")
	}
	return &merchandisingv1.ValidatePlacementResponse{
		Placement: &merchandisingv1.Placement{
			Location: &commonv1.PostalAddress{
				Lines: scoreSearchIndexReply.GetSearchIndex().GetAddress().GetLines(),
			},
			PlacementId: req.GetPlacementId(),
			Title:       req.GetPlacement().GetTitle(),
		},
		Result: &commonv1.OperationResult{
			Status:        browseSellersReply.GetResult().GetStatus(),
			CorrelationId: listSessionVersionsReply.GetResult().GetCorrelationId(),
		},
		Outcome: &merchandisingv1.ValidatePlacementResponse_Outcome{Message: listSessionVersionsReply.GetResult().GetMessage()},
	}, nil
}

// PreviewPlacement previews a placement.
func (s *PlacementValidationServer) PreviewPlacement(ctx context.Context, req *merchandisingv1.PreviewPlacementRequest) (*merchandisingv1.PreviewPlacementResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "preview placement", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	recalculateGiftCardBatchesRequest := &giftcardsv1.RecalculateGiftCardBatchesRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetPlacement().GetLocation().GetLines()},
			Audit:          &commonv1.AuditInfo{CreatedAt: req.GetPlacement().GetAudit().GetCreatedAt()},
		},
	}
	recalculateGiftCardBatchesReply := new(giftcardsv1.RecalculateGiftCardBatchesResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardBatchCalculationService_RecalculateGiftCardBatches_FullMethodName, recalculateGiftCardBatchesRequest, recalculateGiftCardBatchesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/RecalculateGiftCardBatches")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*searchv1.SendSearchIndexTestNoticeResponse, len(req.GetPlacement().GetParticipants()))
	for i, participant := range req.GetPlacement().GetParticipants() {
		group.Go(func() error {
			reply, err := s.searchIndexNotificationClient.SendSearchIndexTestNotice(groupCtx, &searchv1.SendSearchIndexTestNoticeRequest{
				Context: req.GetContext(),
				Details: &searchv1.SendSearchIndexTestNoticeRequest_Details{
					Recipient: &commonv1.ContactInfo{
						Name:             participant.GetContact().GetName(),
						PreferredChannel: participant.GetContact().GetPreferredChannel(),
					},
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
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/SendSearchIndexTestNotice")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	calculateSellerScorecardRequest := &sellersv1.CalculateSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			Audit: &commonv1.AuditInfo{
				Etag: recalculateGiftCardBatchesReply.GetGiftCardBatch().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetPlacement().GetLocation().GetCountryCode(),
			},
		},
	}
	calculateSellerScorecardReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*sellersv1.CalculateSellerScorecardResponse, error) {
		return s.sellerScorecardCalculationClient.CalculateSellerScorecard(ctx, calculateSellerScorecardRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/CalculateSellerScorecard")
	}
	return &merchandisingv1.PreviewPlacementResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: recalculateGiftCardBatchesReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &merchandisingv1.PreviewPlacementResponse_Outcome{
			Status: recalculateGiftCardBatchesReply.GetResult().GetStatus(),
		},
		Placement: &merchandisingv1.Placement{
			Organizer: &identityv1.UserRef{
				Email: calculateSellerScorecardReply.GetSellerScorecard().GetCounterparty().GetEmail(),
			},
			PlacementId: req.GetPlacementId(),
			Title:       req.GetPlacement().GetTitle(),
		},
	}, nil
}

// CheckPlacementEligibility checks a placement.
func (s *PlacementValidationServer) CheckPlacementEligibility(ctx context.Context, req *merchandisingv1.CheckPlacementEligibilityRequest) (*merchandisingv1.CheckPlacementEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CheckPlacementEligibility", "error", err)
		return nil, err
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "check placement eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "merchandising.check_placement_eligibility.subscribe_synonym_updates",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.recipient.preferred_channel": req.GetDetails().GetContact().GetPreferredChannel(),
			"context.tenant_id":                   req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	for _, participant := range req.GetPlacement().GetParticipants() {
		if _, err := s.sessionHistoryClient.RecordSessionVersion(ctx, &identityv1.RecordSessionVersionRequest{
			Context: req.GetContext(),
			Session: &identityv1.Session{
				Assignee: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
				Requester: &commonv1.ContactInfo{
					Name:             participant.GetContact().GetName(),
					Phone:            participant.GetContact().GetPhone(),
					PreferredChannel: participant.GetContact().GetPreferredChannel(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/RecordSessionVersion")
		}
	}
	estimateGiftCardBatchRequest := &giftcardsv1.EstimateGiftCardBatchRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.EstimateGiftCardBatchRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetPlacement().GetLocation().GetRegion()},
		},
	}
	estimateGiftCardBatchReply, err := s.giftCardBatchCalculationPort.EstimateGiftCardBatch(ctx, estimateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/EstimateGiftCardBatch")
	}
	lintLedgerAccountsRequest := &ledgerv1.LintLedgerAccountsRequest{Context: req.GetContext()}
	lintLedgerAccountsReply, err := s.meteredLedgerAccountValidation.LintLedgerAccounts(ctx, lintLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts")
	}
	return &merchandisingv1.CheckPlacementEligibilityResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: estimateGiftCardBatchReply.GetResult().GetCorrelationId(),
			Message:       estimateGiftCardBatchReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Placement: &merchandisingv1.Placement{
			Organizer: &identityv1.UserRef{
				DisplayName: lintLedgerAccountsReply.GetLedgerAccount().GetHolder().GetDisplayName(),
			},
			PlacementId: req.GetPlacementId(),
			Location:    req.GetPlacement().GetLocation(),
		},
	}, nil
}

// NormalizePlacement normalizes a placement.
func (s *PlacementValidationServer) NormalizePlacement(ctx context.Context, req *merchandisingv1.NormalizePlacementRequest) (*merchandisingv1.NormalizePlacementResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizePlacement", "error", err)
		return nil, err
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "normalize placement", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetPlacement().GetOrganizer().GetDisplayName())
	cloneAuditEventRequest := &auditv1.CloneAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	}
	cloneAuditEventReply := new(auditv1.CloneAuditEventResponse)
	if err := grpcx.Invoke(ctx, s.conn, auditv1.AuditEventService_CloneAuditEvent_FullMethodName, cloneAuditEventRequest, cloneAuditEventReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/CloneAuditEvent")
	}
	return &merchandisingv1.NormalizePlacementResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: cloneAuditEventReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Organizer:   req.GetPlacement().GetOrganizer(),
		},
	}, nil
}

// ExplainPlacementDecision explains a placement.
func (s *PlacementValidationServer) ExplainPlacementDecision(ctx context.Context, req *merchandisingv1.ExplainPlacementDecisionRequest) (*merchandisingv1.ExplainPlacementDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "explain placement decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	summarizeSellersRequest := &sellersv1.SummarizeSellersRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
	}
	summarizeSellersReply, err := s.sellerQueryPort.SummarizeSellers(ctx, summarizeSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerQueryService/SummarizeSellers")
	}
	return &merchandisingv1.ExplainPlacementDecisionResponse{
		Outcome: &merchandisingv1.ExplainPlacementDecisionResponse_Outcome{Status: summarizeSellersReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeSellersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Window:      req.GetPlacement().GetWindow(),
		},
	}, nil
}

// ScorePlacement scores a placement.
func (s *PlacementValidationServer) ScorePlacement(ctx context.Context, req *merchandisingv1.ScorePlacementRequest) (*merchandisingv1.ScorePlacementResponse, error) {
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "score placement", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	searchQueryExport, ok := s.clients.Get("acme.search.v1.SearchQueryExportService").(searchv1.SearchQueryExportServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.search.v1.SearchQueryExportService")
	}
	listSearchQueryExportsRequest := &searchv1.ListSearchQueryExportsRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.ListSearchQueryExportsRequest_Filter{Region: req.GetPlacement().GetLocation().GetRegion()},
	}
	listSearchQueryExportsReply, err := searchQueryExport.ListSearchQueryExports(ctx, listSearchQueryExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ListSearchQueryExports")
	}
	return &merchandisingv1.ScorePlacementResponse{
		Result: &commonv1.OperationResult{
			Message: listSearchQueryExportsReply.GetResult().GetMessage(),
			Status:  listSearchQueryExportsReply.GetResult().GetStatus(),
		},
		Placement: &merchandisingv1.Placement{
			PlacementId: req.GetPlacementId(),
			Location:    req.GetPlacement().GetLocation(),
		},
	}, nil
}

// LintPlacements lints a placement.
func (s *PlacementValidationServer) LintPlacements(ctx context.Context, req *merchandisingv1.LintPlacementsRequest) (*merchandisingv1.LintPlacementsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintPlacements", "error", err)
		return nil, err
	}
	if err := s.store.SavePlacement(ctx, req.GetContext().GetTenantId(), req.GetPlacement()); err != nil {
		return nil, grpcx.Wrap(err, "save placement")
	}
	s.log.InfoContext(ctx, "lint placements", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPlacement().GetAudit().GetEtag())
	estimateGiftCardBatchRequest := newEstimateGiftCardBatchRequest(withEstimateGiftCardBatchContext(req.GetContext()), withEstimateGiftCardBatchGiftCardBatch(&giftcardsv1.GiftCardBatch{
		BillingAddress: &commonv1.PostalAddress{
			Recipient: req.GetPlacement().GetLocation().GetRecipient(),
			Lines:     req.GetPlacement().GetLocation().GetLines(),
		},
	}))
	estimateGiftCardBatchReply, err := s.giftCardBatchCalculationClient.EstimateGiftCardBatch(ctx, estimateGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/EstimateGiftCardBatch")
	}
	return &merchandisingv1.LintPlacementsResponse{
		Placement: &merchandisingv1.Placement{
			Location: &commonv1.PostalAddress{
				CountryCode: estimateGiftCardBatchReply.GetGiftCardBatch().GetBillingAddress().GetCountryCode(),
			},
			PlacementId: req.GetPlacementId(),
			Title:       req.GetPlacement().GetTitle(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
