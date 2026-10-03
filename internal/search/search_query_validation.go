package search

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SearchQueryValidationServer implements `acme.search.v1.SearchQueryValidationService`.
// Checks search queries against the rules before anything acts on them.
type SearchQueryValidationServer struct {
	searchv1.UnimplementedSearchQueryValidationServiceServer
	store                         *Store
	log                           *slog.Logger
	searchIndexNotificationClient searchv1.SearchIndexNotificationServiceClient
}

// NewSearchQueryValidationServer wires a SearchQueryValidationServer.
func NewSearchQueryValidationServer(deps platform.Deps) *SearchQueryValidationServer {
	return &SearchQueryValidationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.search.v1.SearchQueryValidationService"),
		searchIndexNotificationClient: searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
	}
}

// ValidateSearchQuery validates a search query.
func (s *SearchQueryValidationServer) ValidateSearchQuery(ctx context.Context, req *searchv1.ValidateSearchQueryRequest) (*searchv1.ValidateSearchQueryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "validate search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ValidateSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetSearchQuery().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PreviewSearchQuery previews a search query.
func (s *SearchQueryValidationServer) PreviewSearchQuery(ctx context.Context, req *searchv1.PreviewSearchQueryRequest) (*searchv1.PreviewSearchQueryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "preview search query", "tenant", req.GetContext().GetTenantId())
	unsubscribeSearchIndexUpdatesRequest := buildUnsubscribeSearchIndexUpdatesRequestForPreviewSearchQuery(req)
	unsubscribeSearchIndexUpdatesReply, err := s.searchIndexNotificationClient.UnsubscribeSearchIndexUpdates(ctx, unsubscribeSearchIndexUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/UnsubscribeSearchIndexUpdates")
	}
	return &searchv1.PreviewSearchQueryResponse{
		Outcome: &searchv1.PreviewSearchQueryResponse_Outcome{
			EffectiveAt: unsubscribeSearchIndexUpdatesReply.GetOutcome().GetEffectiveAt(),
		},
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			CorrelationId: req.GetSearchQuery().GetCorrelationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeSearchQuery normalizes a search query.
func (s *SearchQueryValidationServer) NormalizeSearchQuery(ctx context.Context, req *searchv1.NormalizeSearchQueryRequest) (*searchv1.NormalizeSearchQueryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "normalize search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.NormalizeSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			OccurredAt:    req.GetSearchQuery().GetOccurredAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainSearchQueryDecision explains a search query.
func (s *SearchQueryValidationServer) ExplainSearchQueryDecision(ctx context.Context, req *searchv1.ExplainSearchQueryDecisionRequest) (*searchv1.ExplainSearchQueryDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainSearchQueryDecision", "error", err)
		return nil, err
	}
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "explain search query decision", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ExplainSearchQueryDecisionResponse{
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			Source:        req.GetSearchQuery().GetSource(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreSearchQuery scores a search query.
func (s *SearchQueryValidationServer) ScoreSearchQuery(ctx context.Context, req *searchv1.ScoreSearchQueryRequest) (*searchv1.ScoreSearchQueryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "score search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ScoreSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			Subject:       req.GetSearchQuery().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintSearchQueries lints a search query.
func (s *SearchQueryValidationServer) LintSearchQueries(ctx context.Context, req *searchv1.LintSearchQueriesRequest) (*searchv1.LintSearchQueriesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "lint search queries", "tenant", req.GetContext().GetTenantId())
	return &searchv1.LintSearchQueriesResponse{
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			OccurredAt:    req.GetSearchQuery().GetOccurredAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSearchQueryRules returns a search query.
func (s *SearchQueryValidationServer) GetSearchQueryRules(ctx context.Context, req *searchv1.GetSearchQueryRulesRequest) (*searchv1.GetSearchQueryRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "get_search_query_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get search query rules", "tenant", req.GetContext().GetTenantId())
	return &searchv1.GetSearchQueryRulesResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}
