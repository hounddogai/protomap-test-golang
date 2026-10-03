package fraud

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	communityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/community/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	showsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shows/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// WatchlistValidationServer implements `acme.fraud.v1.WatchlistValidationService`.
// Checks watchlists against the rules before anything acts on them.
type WatchlistValidationServer struct {
	fraudv1.UnimplementedWatchlistValidationServiceServer
	store                          *Store
	log                            *slog.Logger
	stockMovementValidationClient  inventoryv1.StockMovementValidationServiceClient
	conn                           *grpc.ClientConn
	synonymExportClient            searchv1.SynonymExportServiceClient
	searchIndexClient              searchv1.SearchIndexServiceClient
	documentQueryClient            documentsv1.DocumentQueryServiceClient
	fraudSignalCalculationClient   fraudv1.FraudSignalCalculationServiceClient
	watchlistCalculationClient     fraudv1.WatchlistCalculationServiceClient
	synonymExportPort              synonymExportPort
	forumClient                    communityv1.ForumServiceClient
	riskAssessmentValidationClient fraudv1.RiskAssessmentValidationServiceClient
	searchSearchIndexesFn          func(context.Context, *searchv1.SearchSearchIndexesRequest, ...grpc.CallOption) (*searchv1.SearchSearchIndexesResponse, error)
	meteredWatchlistCalculation    meteredWatchlistCalculation
	clients                        *registry.Registry
	giftCardValidationClient       giftcardsv1.GiftCardValidationServiceClient
	shipmentValidationClient       shippingv1.ShipmentValidationServiceClient
	simulateWatchlistFn            func(context.Context, *fraudv1.SimulateWatchlistRequest, ...grpc.CallOption) (*fraudv1.SimulateWatchlistResponse, error)
	searchQueryNotificationClient  searchv1.SearchQueryNotificationServiceClient
}

// NewWatchlistValidationServer wires a WatchlistValidationServer.
func NewWatchlistValidationServer(deps platform.Deps) *WatchlistValidationServer {
	searchIndex := searchv1.NewSearchIndexServiceClient(deps.Conn)
	watchlistCalculation := fraudv1.NewWatchlistCalculationServiceClient(deps.Conn)
	return &WatchlistValidationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.fraud.v1.WatchlistValidationService"),
		stockMovementValidationClient:  inventoryv1.NewStockMovementValidationServiceClient(deps.Conn),
		conn:                           deps.Conn,
		synonymExportClient:            searchv1.NewSynonymExportServiceClient(deps.Conn),
		searchIndexClient:              searchv1.NewSearchIndexServiceClient(deps.Conn),
		documentQueryClient:            documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		fraudSignalCalculationClient:   fraudv1.NewFraudSignalCalculationServiceClient(deps.Conn),
		watchlistCalculationClient:     fraudv1.NewWatchlistCalculationServiceClient(deps.Conn),
		synonymExportPort:              newSynonymExportPort(deps),
		forumClient:                    communityv1.NewForumServiceClient(deps.Conn),
		riskAssessmentValidationClient: fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn),
		searchSearchIndexesFn:          searchIndex.SearchSearchIndexes,
		meteredWatchlistCalculation:    meteredWatchlistCalculation{WatchlistCalculationServiceClient: fraudv1.NewWatchlistCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                        deps.Clients,
		giftCardValidationClient:       giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		shipmentValidationClient:       shippingv1.NewShipmentValidationServiceClient(deps.Conn),
		simulateWatchlistFn:            watchlistCalculation.SimulateWatchlist,
		searchQueryNotificationClient:  searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
	}
}

// ValidateWatchlist validates a watchlist.
func (s *WatchlistValidationServer) ValidateWatchlist(ctx context.Context, req *fraudv1.ValidateWatchlistRequest) (*fraudv1.ValidateWatchlistResponse, error) {
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "validate watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	explainReturnLabelDecisionRequest := &returnsv1.ExplainReturnLabelDecisionRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit:          &commonv1.AuditInfo{UpdatedBy: req.GetWatchlist().GetAudit().GetUpdatedBy()},
			BillingAddress: &commonv1.PostalAddress{Lines: req.GetWatchlist().GetLocation().GetLines()},
		},
	}
	explainReturnLabelDecisionReply, err := explainReturnLabelDecisionCall(ctx, explainReturnLabelDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelValidationService/ExplainReturnLabelDecision")
	}
	checkStockMovementEligibilityRequest := &inventoryv1.CheckStockMovementEligibilityRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.CheckStockMovementEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetContact().GetPhone(),
				Name:  req.GetDetails().GetContact().GetName(),
				Email: req.GetDetails().GetContact().GetEmail(),
			},
		},
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: explainReturnLabelDecisionReply.GetReturnLabel().GetCounterparty().GetDisplayName(),
			},
		},
	}
	var checkStockMovementEligibilityReply *inventoryv1.CheckStockMovementEligibilityResponse
	if err := pipeline.Run(ctx, "acme.inventory.v1.StockMovementValidationService/CheckStockMovementEligibility", func(ctx context.Context) error {
		answer, err := s.stockMovementValidationClient.CheckStockMovementEligibility(ctx, checkStockMovementEligibilityRequest)
		checkStockMovementEligibilityReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/CheckStockMovementEligibility")
	}
	calculateShowRequest := &showsv1.CalculateShowRequest{
		Context: req.GetContext(),
		Show: &showsv1.Show{
			Participants: []*showsv1.ShowParticipant{&showsv1.ShowParticipant{
				Contact: &commonv1.ContactInfo{
					Name:  req.GetDetails().GetContact().GetName(),
					Phone: req.GetDetails().GetContact().GetPhone(),
				},
			}},
		},
	}
	calculateShowReply := new(showsv1.CalculateShowResponse)
	if err := s.conn.Invoke(ctx, showsShowCalculationServiceCalculateShowMethod, calculateShowRequest, calculateShowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shows.v1.ShowCalculationService/CalculateShow")
	}
	return &fraudv1.ValidateWatchlistResponse{
		Watchlist: &fraudv1.Watchlist{
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainReturnLabelDecisionReply.GetReturnLabel().GetAudit().GetCreatedAt(),
			},
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						Etag: explainReturnLabelDecisionReply.GetReturnLabel().GetAudit().GetEtag(),
					},
				}},
			}},
			Attributes: checkStockMovementEligibilityReply.GetStockMovement().GetAttributes(),
			Location: &commonv1.PostalAddress{
				PostalCode: calculateShowReply.GetShow().GetLocation().GetPostalCode(),
			},
			WatchlistId: req.GetWatchlistId(),
			ParentId:    req.GetWatchlist().GetParentId(),
		},
		Outcome: &fraudv1.ValidateWatchlistResponse_Outcome{
			Status: checkStockMovementEligibilityReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: calculateShowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PreviewWatchlist previews a watchlist.
func (s *WatchlistValidationServer) PreviewWatchlist(ctx context.Context, req *fraudv1.PreviewWatchlistRequest) (*fraudv1.PreviewWatchlistResponse, error) {
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "preview watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	scheduleSynonymExportRequest := &searchv1.ScheduleSynonymExportRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Details:     &searchv1.ScheduleSynonymExportRequest_Details{RecipientEmails: names},
	}
	scheduleSynonymExportReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*searchv1.ScheduleSynonymExportResponse, error) {
		return s.synonymExportClient.ScheduleSynonymExport(ctx, scheduleSynonymExportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ScheduleSynonymExport")
	}
	names2 := collectWatchlistNames(req.GetWatchlist(), nil)
	batchGetSearchIndexesRequest := &searchv1.BatchGetSearchIndexesRequest{
		Context:        req.GetContext(),
		SearchIndexIds: names2,
	}
	type batchGetSearchIndexesResult struct {
		reply *searchv1.BatchGetSearchIndexesResponse
		err   error
	}
	batchGetSearchIndexesResults := make(chan batchGetSearchIndexesResult, 1)
	go func() {
		reply, err := s.searchIndexClient.BatchGetSearchIndexes(ctx, batchGetSearchIndexesRequest)
		batchGetSearchIndexesResults <- batchGetSearchIndexesResult{reply, err}
	}()
	var batchGetSearchIndexesReply *searchv1.BatchGetSearchIndexesResponse
	select {
	case result := <-batchGetSearchIndexesResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.search.v1.SearchIndexService/BatchGetSearchIndexes")
		}
		batchGetSearchIndexesReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	encoded, err := protojson.Marshal(req.GetWatchlist())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.Watchlist
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	findDocumentsRequest := &documentsv1.FindDocumentsRequest{
		Context: req.GetContext(),
		Filter:  &documentsv1.FindDocumentsRequest_Filter{Region: (&decoded).GetLocation().GetRegion()},
	}
	findDocumentsReply, err := grpcx.Call(ctx, s.documentQueryClient.FindDocuments, findDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/FindDocuments")
	}
	_ = batchGetSearchIndexesReply
	_ = findDocumentsReply
	return &fraudv1.PreviewWatchlistResponse{
		Outcome: &fraudv1.PreviewWatchlistResponse_Outcome{Status: scheduleSynonymExportReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: scheduleSynonymExportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Watchlist: &fraudv1.Watchlist{
			WatchlistId: req.GetWatchlistId(),
			Name:        req.GetWatchlist().GetName(),
		},
	}, nil
}

// CheckWatchlistEligibility checks a watchlist.
func (s *WatchlistValidationServer) CheckWatchlistEligibility(ctx context.Context, req *fraudv1.CheckWatchlistEligibilityRequest) (*fraudv1.CheckWatchlistEligibilityResponse, error) {
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "check watchlist eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetWatchlist()).(*fraudv1.Watchlist)
	source := req.GetWatchlist().GetLocation().ProtoReflect()
	locality := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("locality"))).String()
	packed, err := anypb.New(req.GetWatchlist())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	quoteFraudSignalRequest := &fraudv1.QuoteFraudSignalRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{DisplayName: cloned.GetManager().GetDisplayName()},
			Detail:  packed,
		},
		Details: &fraudv1.QuoteFraudSignalRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: locality,
				Lines:    names,
			},
		},
	}
	quoteFraudSignalReply, err := grpcx.Call(ctx, s.fraudSignalCalculationClient.QuoteFraudSignal, quoteFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalCalculationService/QuoteFraudSignal")
	}
	return &fraudv1.CheckWatchlistEligibilityResponse{
		Outcome: &fraudv1.CheckWatchlistEligibilityResponse_Outcome{Message: quoteFraudSignalReply.GetResult().GetMessage()},
		Watchlist: &fraudv1.Watchlist{
			WatchlistId: req.GetWatchlistId(),
			Depth:       req.GetWatchlist().GetDepth(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeWatchlist normalizes a watchlist.
func (s *WatchlistValidationServer) NormalizeWatchlist(ctx context.Context, req *fraudv1.NormalizeWatchlistRequest) (*fraudv1.NormalizeWatchlistResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeWatchlist", "error", err)
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "normalize watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	step := &watchlistValidationServerNormalizeWatchlistSimulateWatchlistStep{request: &fraudv1.SimulateWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: req.GetWatchlist().GetAudit().GetUpdatedAt(),
						Etag:      req.GetWatchlist().GetAudit().GetEtag(),
					},
				}},
			}},
			Path: names,
		},
	}}
	if err := s.runNormalizeWatchlistSimulateWatchlist(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/SimulateWatchlist")
	}
	simulateWatchlistReply := step.reply
	retrySynonymExportRequest := &searchv1.RetrySynonymExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.RetrySynonymExportRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			},
		},
	}
	retrySynonymExportReply, err := s.synonymExportPort.RetrySynonymExport(ctx, retrySynonymExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/RetrySynonymExport")
	}
	return &fraudv1.NormalizeWatchlistResponse{
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					WatchlistId: simulateWatchlistReply.GetWatchlist().GetWatchlistId(),
					Path:        simulateWatchlistReply.GetWatchlist().GetPath(),
					Audit: &commonv1.AuditInfo{
						UpdatedBy: retrySynonymExportReply.GetSynonym().GetAudit().GetUpdatedBy(),
					},
				}},
				Audit: &commonv1.AuditInfo{
					Etag: retrySynonymExportReply.GetSynonym().GetAudit().GetEtag(),
				},
			}},
			WatchlistId: req.GetWatchlistId(),
			Status:      req.GetWatchlist().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainWatchlistDecision explains a watchlist.
func (s *WatchlistValidationServer) ExplainWatchlistDecision(ctx context.Context, req *fraudv1.ExplainWatchlistDecisionRequest) (*fraudv1.ExplainWatchlistDecisionResponse, error) {
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "explain watchlist decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	source := req.GetWatchlist().GetLocation().ProtoReflect()
	countryCode := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("country_code"))).String()
	cloned := proto.Clone(req.GetWatchlist()).(*fraudv1.Watchlist)
	ctx = tenancy.With(ctx, "created_by", req.GetWatchlist().GetAudit().GetCreatedBy())
	updateForumReply, err := s.forumClient.UpdateForum(ctx, &communityv1.UpdateForumRequest{
		Context: req.GetContext(),
		Forum: &communityv1.Forum{
			Children: []*communityv1.Forum{&communityv1.Forum{
				Children: []*communityv1.Forum{&communityv1.Forum{
					Location: &commonv1.PostalAddress{CountryCode: countryCode},
					Children: []*communityv1.Forum{&communityv1.Forum{Manager: cloned.GetManager()}},
					Audit:    &commonv1.AuditInfo{CreatedBy: tenancy.Value(ctx, "created_by")},
				}},
				Manager: &identityv1.UserRef{UserId: req.GetWatchlist().GetManager().GetUserId()},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.community.v1.ForumService/UpdateForum")
	}
	source2 := req.GetWatchlist().GetManager().ProtoReflect()
	displayName := source2.Get(source2.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	checkRiskAssessmentEligibilityRequest := &fraudv1.CheckRiskAssessmentEligibilityRequest{}
	checkRiskAssessmentEligibilityRequest.Context = req.GetContext()
	checkRiskAssessmentEligibilityRequest.RiskAssessment = &fraudv1.RiskAssessment{
		Audit: &commonv1.AuditInfo{
			CreatedAt: updateForumReply.GetForum().GetAudit().GetCreatedAt(),
			CreatedBy: req.GetWatchlist().GetAudit().GetCreatedBy(),
		},
		Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{CreatedAt: req.GetWatchlist().GetAudit().GetCreatedAt()}},
		Origin:   &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{DisplayName: displayName}},
		Tags:     names,
	}
	checkRiskAssessmentEligibilityReply, err := s.riskAssessmentValidationClient.CheckRiskAssessmentEligibility(ctx, checkRiskAssessmentEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/CheckRiskAssessmentEligibility")
	}
	searchSearchIndexesRequest := &searchv1.SearchSearchIndexesRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.SearchSearchIndexesRequest_Filter{Region: req.GetWatchlist().GetLocation().GetRegion()},
	}
	searchSearchIndexesReply, err := s.searchSearchIndexesFn(ctx, searchSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/SearchSearchIndexes")
	}
	getWatchlistRatesRequest := &fraudv1.GetWatchlistRatesRequest{
		Context:     req.GetContext(),
		WatchlistId: req.GetWatchlist().GetWatchlistId(),
	}
	getWatchlistRatesReply, err := s.meteredWatchlistCalculation.GetWatchlistRates(ctx, getWatchlistRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/GetWatchlistRates")
	}
	return &fraudv1.ExplainWatchlistDecisionResponse{
		Watchlist: &fraudv1.Watchlist{
			Audit: &commonv1.AuditInfo{
				CreatedBy: updateForumReply.GetForum().GetAudit().GetCreatedBy(),
			},
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Manager: &identityv1.UserRef{
					DisplayName: updateForumReply.GetForum().GetManager().GetDisplayName(),
					Email:       checkRiskAssessmentEligibilityReply.GetRiskAssessment().GetRequester().GetEmail(),
				},
			}},
			Manager: &identityv1.UserRef{
				UserId: checkRiskAssessmentEligibilityReply.GetRiskAssessment().GetAssignee().GetUserId(),
				Email:  getWatchlistRatesReply.GetWatchlist().GetManager().GetEmail(),
			},
			WatchlistId: req.GetWatchlistId(),
			ParentId:    req.GetWatchlist().GetParentId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: searchSearchIndexesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &fraudv1.ExplainWatchlistDecisionResponse_Outcome{Message: searchSearchIndexesReply.GetResult().GetMessage()},
	}, nil
}

// ScoreWatchlist scores a watchlist.
func (s *WatchlistValidationServer) ScoreWatchlist(ctx context.Context, req *fraudv1.ScoreWatchlistRequest) (*fraudv1.ScoreWatchlistResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "score watchlist", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetWatchlist())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded fraudv1.Watchlist
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	encoded2, err := protojson.Marshal(req.GetWatchlist())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 fraudv1.Watchlist
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	previewRiskAssessmentRequest := &fraudv1.PreviewRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{Email: req.GetWatchlist().GetManager().GetEmail()}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: (&decoded).GetAudit().GetCreatedBy(),
				UpdatedBy: (&decoded2).GetAudit().GetUpdatedBy(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetWatchlist().GetManager().GetDisplayName(),
			},
		},
	}
	previewRiskAssessmentReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*fraudv1.PreviewRiskAssessmentResponse, error) {
		return s.riskAssessmentValidationClient.PreviewRiskAssessment(ctx, previewRiskAssessmentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/PreviewRiskAssessment")
	}
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	scoreSessionRequest := &identityv1.ScoreSessionRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetWatchlist().GetAudit().GetCreatedAt()},
			Requester: &commonv1.ContactInfo{
				Phone: previewRiskAssessmentReply.GetRiskAssessment().GetRequester().GetPhone(),
			},
			Tags: names,
		},
	}
	scoreSessionReply := new(identityv1.ScoreSessionResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.SessionValidationService_ScoreSession_FullMethodName, scoreSessionRequest, scoreSessionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ScoreSession")
	}
	sellerListingQuery, err := registry.Lookup[sellersv1.SellerListingQueryServiceClient](s.clients, "acme.sellers.v1.SellerListingQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "region", req.GetWatchlist().GetLocation().GetRegion())
	findSellerListingsRequest := &sellersv1.FindSellerListingsRequest{
		Context: req.GetContext(),
		Filter:  &sellersv1.FindSellerListingsRequest_Filter{Region: tenancy.Value(ctx, "region")},
	}
	findSellerListingsReply, err := sellerListingQuery.FindSellerListings(ctx, findSellerListingsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingQueryService/FindSellerListings")
	}
	validateGiftCardRequest := buildValidateGiftCardRequestForScoreWatchlist(req, previewRiskAssessmentReply, scoreSessionReply)
	validateGiftCardReply, err := s.giftCardValidationClient.ValidateGiftCard(ctx, validateGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ValidateGiftCard")
	}
	return &fraudv1.ScoreWatchlistResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: previewRiskAssessmentReply.GetResult().GetCorrelationId(),
			Details:       previewRiskAssessmentReply.GetResult().GetDetails(),
			Message:       findSellerListingsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Audit: &commonv1.AuditInfo{
					UpdatedBy: scoreSessionReply.GetSession().GetAudit().GetUpdatedBy(),
				},
				Manager: &identityv1.UserRef{
					DisplayName: scoreSessionReply.GetSession().GetAssignee().GetDisplayName(),
				},
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						Etag: validateGiftCardReply.GetGiftCard().GetAudit().GetEtag(),
					},
				}},
			}},
			WatchlistId: req.GetWatchlistId(),
			ParentId:    req.GetWatchlist().GetParentId(),
		},
	}, nil
}

// LintWatchlists lints a watchlist.
func (s *WatchlistValidationServer) LintWatchlists(ctx context.Context, req *fraudv1.LintWatchlistsRequest) (*fraudv1.LintWatchlistsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintWatchlists", "error", err)
		return nil, err
	}
	if err := s.store.SaveWatchlist(ctx, req.GetContext().GetTenantId(), req.GetWatchlist()); err != nil {
		return nil, grpcx.Wrap(err, "save watchlist")
	}
	s.log.InfoContext(ctx, "lint watchlists", "tenant", req.GetContext().GetTenantId(), "etag", req.GetWatchlist().GetAudit().GetEtag())
	names := collectWatchlistNames(req.GetWatchlist(), nil)
	previewShipmentRequest := &shippingv1.PreviewShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			Audit:          &commonv1.AuditInfo{Etag: req.GetWatchlist().GetAudit().GetEtag()},
			BillingAddress: &commonv1.PostalAddress{Lines: names},
		},
	}
	previewShipmentReply, err := grpcx.Call(ctx, s.shipmentValidationClient.PreviewShipment, previewShipmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentValidationService/PreviewShipment")
	}
	names2 := collectWatchlistNames(req.GetWatchlist(), nil)
	simulateWatchlistRequest := &fraudv1.SimulateWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{Etag: req.GetWatchlist().GetAudit().GetEtag()},
					Manager: &identityv1.UserRef{
						DisplayName: previewShipmentReply.GetShipment().GetCounterparty().GetDisplayName(),
					},
				}},
			}},
			Path: names2,
		},
	}
	simulateWatchlistReply, err := s.simulateWatchlistFn(ctx, simulateWatchlistRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistCalculationService/SimulateWatchlist")
	}
	riskAssessmentCalculation, err := registry.Lookup[fraudv1.RiskAssessmentCalculationServiceClient](s.clients, "acme.fraud.v1.RiskAssessmentCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	names3 := collectWatchlistNames(req.GetWatchlist(), nil)
	estimateRiskAssessmentRequest := &fraudv1.EstimateRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Comments: []*fraudv1.RiskAssessmentComment{&fraudv1.RiskAssessmentComment{
				CreatedAt: previewShipmentReply.GetShipment().GetAudit().GetCreatedAt(),
			}},
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{Email: req.GetWatchlist().GetManager().GetEmail()}},
			Tags:   names3,
		},
		Details: &fraudv1.EstimateRiskAssessmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: previewShipmentReply.GetShipment().GetBillingAddress().GetPostalCode(),
			},
		},
	}
	estimateRiskAssessmentReply, err := riskAssessmentCalculation.EstimateRiskAssessment(ctx, estimateRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/EstimateRiskAssessment")
	}
	document, err := registry.Lookup[documentsv1.DocumentServiceClient](s.clients, "acme.documents.v1.DocumentService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	createDocumentRequest := &documentsv1.CreateDocumentRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			BillingAddress: &commonv1.PostalAddress{
				Region: simulateWatchlistReply.GetWatchlist().GetLocation().GetRegion(),
				Lines:  simulateWatchlistReply.GetWatchlist().GetLocation().GetLines(),
			},
		},
	}
	createDocumentReply, err := document.CreateDocument(ctx, createDocumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/CreateDocument")
	}
	return &fraudv1.LintWatchlistsResponse{
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Audit: &commonv1.AuditInfo{
					CreatedAt: previewShipmentReply.GetShipment().GetAudit().GetCreatedAt(),
				},
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Location: &commonv1.PostalAddress{
						Recipient: simulateWatchlistReply.GetWatchlist().GetLocation().GetRecipient(),
					},
				}},
				Manager: &identityv1.UserRef{
					UserId: estimateRiskAssessmentReply.GetRiskAssessment().GetAssignee().GetUserId(),
				},
				Location: &commonv1.PostalAddress{
					Recipient: createDocumentReply.GetDocument().GetBillingAddress().GetRecipient(),
				},
			}},
			Location: &commonv1.PostalAddress{
				Region: previewShipmentReply.GetShipment().GetBillingAddress().GetRegion(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: createDocumentReply.GetDocument().GetAudit().GetCreatedBy(),
			},
			WatchlistId: req.GetWatchlistId(),
			Name:        req.GetWatchlist().GetName(),
		},
		Result: &commonv1.OperationResult{Status: estimateRiskAssessmentReply.GetResult().GetStatus()},
	}, nil
}

// GetWatchlistRules returns a watchlist.
func (s *WatchlistValidationServer) GetWatchlistRules(ctx context.Context, req *fraudv1.GetWatchlistRulesRequest) (*fraudv1.GetWatchlistRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "watchlist", req.GetWatchlistId(), "get_watchlist_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get watchlist rules", "tenant", req.GetContext().GetTenantId())
	notifySearchQueryWatchersRequest := &searchv1.NotifySearchQueryWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchQueryWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		Annotations: req.GetContext().GetBaggage(),
	}
	notifySearchQueryWatchersReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*searchv1.NotifySearchQueryWatchersResponse, error) {
		return s.searchQueryNotificationClient.NotifySearchQueryWatchers(ctx, notifySearchQueryWatchersRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/NotifySearchQueryWatchers")
	}
	return &fraudv1.GetWatchlistRulesResponse{
		Result: &commonv1.OperationResult{
			Message: notifySearchQueryWatchersReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Watchlist: &fraudv1.Watchlist{WatchlistId: req.GetWatchlistId()},
	}, nil
}

// watchlistValidationServerNormalizeWatchlistSimulateWatchlistStep carries the request of one step of
// NormalizeWatchlist and, once it ran, its reply.
type watchlistValidationServerNormalizeWatchlistSimulateWatchlistStep struct {
	request *fraudv1.SimulateWatchlistRequest
	reply   *fraudv1.SimulateWatchlistResponse
}

func (s *WatchlistValidationServer) runNormalizeWatchlistSimulateWatchlist(ctx context.Context, step *watchlistValidationServerNormalizeWatchlistSimulateWatchlistStep) error {
	reply, err := s.watchlistCalculationClient.SimulateWatchlist(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
