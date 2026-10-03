package search

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// SearchIndexQueryServer implements `acme.search.v1.SearchIndexQueryService`.
// Answers read-only questions about search indexes, for screens and reports.
type SearchIndexQueryServer struct {
	searchv1.UnimplementedSearchIndexQueryServiceServer
	store   *Store
	log     *slog.Logger
	clients *registry.Registry
}

// NewSearchIndexQueryServer wires a SearchIndexQueryServer.
func NewSearchIndexQueryServer(deps platform.Deps) *SearchIndexQueryServer {
	return &SearchIndexQueryServer{
		store:   NewStore(deps.DB),
		log:     deps.Logger.With("service", "acme.search.v1.SearchIndexQueryService"),
		clients: deps.Clients,
	}
}

// LookupSearchIndex returns a search index.
func (s *SearchIndexQueryServer) LookupSearchIndex(ctx context.Context, req *searchv1.LookupSearchIndexRequest) (*searchv1.LookupSearchIndexResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "lookup_search_index"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup search index", "tenant", req.GetContext().GetTenantId())
	return &searchv1.LookupSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseSearchIndexes lists search indexes a page at a time.
func (s *SearchIndexQueryServer) BrowseSearchIndexes(ctx context.Context, req *searchv1.BrowseSearchIndexesRequest) (*searchv1.BrowseSearchIndexesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowseSearchIndexes", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse search indexes", "tenant", req.GetContext().GetTenantId())
	return &searchv1.BrowseSearchIndexesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindSearchIndexes lists search indexes a page at a time.
func (s *SearchIndexQueryServer) FindSearchIndexes(ctx context.Context, req *searchv1.FindSearchIndexesRequest) (*searchv1.FindSearchIndexesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "find search indexes", "tenant", req.GetContext().GetTenantId())
	return &searchv1.FindSearchIndexesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountSearchIndexes counts search indexes by status.
func (s *SearchIndexQueryServer) CountSearchIndexes(ctx context.Context, req *searchv1.CountSearchIndexesRequest) (*searchv1.CountSearchIndexesResponse, error) {
	s.log.InfoContext(ctx, "count search indexes", "tenant", req.GetContext().GetTenantId())
	searchQueryHistory, err := registry.Lookup[searchv1.SearchQueryHistoryServiceClient](s.clients, "acme.search.v1.SearchQueryHistoryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareSearchQueryVersionsRequest := &searchv1.CompareSearchQueryVersionsRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Measurements: []*searchv1.SearchQueryMeasurement{&searchv1.SearchQueryMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Detail:       packed,
		},
	}
	compareSearchQueryVersionsReply, err := searchQueryHistory.CompareSearchQueryVersions(ctx, compareSearchQueryVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/CompareSearchQueryVersions")
	}
	return &searchv1.CountSearchIndexesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: compareSearchQueryVersionsReply.GetSearchQuery().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ResolveSearchIndexes returns several search indexes at once.
func (s *SearchIndexQueryServer) ResolveSearchIndexes(ctx context.Context, req *searchv1.ResolveSearchIndexesRequest) (*searchv1.ResolveSearchIndexesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve search indexes", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ResolveSearchIndexesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportSearchIndexes streams every matching search index.
func (s *SearchIndexQueryServer) ExportSearchIndexes(req *searchv1.ExportSearchIndexesRequest, stream grpc.ServerStreamingServer[searchv1.ExportSearchIndexesResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export search indexes", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&searchv1.ExportSearchIndexesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
		return err
	}
	return nil
}

// SummarizeSearchIndexes summarizes a search index.
func (s *SearchIndexQueryServer) SummarizeSearchIndexes(ctx context.Context, req *searchv1.SummarizeSearchIndexesRequest) (*searchv1.SummarizeSearchIndexesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "summarize_search_indexes"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize search indexes", "tenant", req.GetContext().GetTenantId())
	return &searchv1.SummarizeSearchIndexesResponse{
		Period: req.GetDetails().GetPeriod(),
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuggestSearchIndexes lists search indexes a page at a time.
func (s *SearchIndexQueryServer) SuggestSearchIndexes(ctx context.Context, req *searchv1.SuggestSearchIndexesRequest) (*searchv1.SuggestSearchIndexesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestSearchIndexes", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest search indexes", "tenant", req.GetContext().GetTenantId())
	return &searchv1.SuggestSearchIndexesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}
