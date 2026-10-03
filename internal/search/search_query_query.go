package search

import (
	"context"
	"log/slog"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// SearchQueryQueryServer implements `acme.search.v1.SearchQueryQueryService`.
// Answers read-only questions about search queries, for screens and reports.
type SearchQueryQueryServer struct {
	searchv1.UnimplementedSearchQueryQueryServiceServer
	store                  *Store
	log                    *slog.Logger
	searchIndexQueryClient searchv1.SearchIndexQueryServiceClient
	auditEventQueryClient  auditv1.AuditEventQueryServiceClient
	conn                   *grpc.ClientConn
}

// NewSearchQueryQueryServer wires a SearchQueryQueryServer.
func NewSearchQueryQueryServer(deps platform.Deps) *SearchQueryQueryServer {
	return &SearchQueryQueryServer{
		store:                  NewStore(deps.DB),
		log:                    deps.Logger.With("service", "acme.search.v1.SearchQueryQueryService"),
		searchIndexQueryClient: searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
		auditEventQueryClient:  auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		conn:                   deps.Conn,
	}
}

// LookupSearchQuery returns a search query.
func (s *SearchQueryQueryServer) LookupSearchQuery(ctx context.Context, req *searchv1.LookupSearchQueryRequest) (*searchv1.LookupSearchQueryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "lookup_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup search query", "tenant", req.GetContext().GetTenantId())
	browseSearchIndexesRequest := &searchv1.BrowseSearchIndexesRequest{}
	browseSearchIndexesRequest.Context = req.GetContext()
	browseSearchIndexesRequest.Filter = &searchv1.BrowseSearchIndexesRequest_Filter{OwnerId: req.GetSearchQueryId()}
	browseSearchIndexesReply, err := s.searchIndexQueryClient.BrowseSearchIndexes(ctx, browseSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/BrowseSearchIndexes")
	}
	return &searchv1.LookupSearchQueryResponse{
		Result:      &commonv1.OperationResult{Status: browseSearchIndexesReply.GetResult().GetStatus()},
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
	}, nil
}

// BrowseSearchQueries lists search queries a page at a time.
func (s *SearchQueryQueryServer) BrowseSearchQueries(ctx context.Context, req *searchv1.BrowseSearchQueriesRequest) (*searchv1.BrowseSearchQueriesResponse, error) {
	s.log.InfoContext(ctx, "browse search queries", "tenant", req.GetContext().GetTenantId())
	countAuditEventsReply, err := s.auditEventQueryClient.CountAuditEvents(ctx, &auditv1.CountAuditEventsRequest{
		Context: req.GetContext(),
		Filter: &auditv1.CountAuditEventsRequest_Filter{
			Query:  req.GetFilter().GetQuery(),
			Region: req.GetFilter().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/CountAuditEvents")
	}
	return &searchv1.BrowseSearchQueriesResponse{
		Result: &commonv1.OperationResult{
			Message: countAuditEventsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// FindSearchQueries lists search queries a page at a time.
func (s *SearchQueryQueryServer) FindSearchQueries(ctx context.Context, req *searchv1.FindSearchQueriesRequest) (*searchv1.FindSearchQueriesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindSearchQueries", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find search queries", "tenant", req.GetContext().GetTenantId())
	return &searchv1.FindSearchQueriesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountSearchQueries counts search queries by status.
func (s *SearchQueryQueryServer) CountSearchQueries(ctx context.Context, req *searchv1.CountSearchQueriesRequest) (*searchv1.CountSearchQueriesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count search queries", "tenant", req.GetContext().GetTenantId())
	return &searchv1.CountSearchQueriesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportSearchQueries streams every matching search query.
func (s *SearchQueryQueryServer) ExportSearchQueries(req *searchv1.ExportSearchQueriesRequest, stream grpc.ServerStreamingServer[searchv1.ExportSearchQueriesResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export search queries", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&searchv1.ExportSearchQueriesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
		return err
	}
	return nil
}

// SummarizeSearchQueries summarizes a search query.
func (s *SearchQueryQueryServer) SummarizeSearchQueries(ctx context.Context, req *searchv1.SummarizeSearchQueriesRequest) (*searchv1.SummarizeSearchQueriesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SummarizeSearchQueries", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "summarize_search_queries"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize search queries", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateSearchIndexVersionRequest := &searchv1.AnnotateSearchIndexVersionRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Details: &searchv1.AnnotateSearchIndexVersionRequest_Details{Snapshot: snapshot},
	}
	annotateSearchIndexVersionReply := new(searchv1.AnnotateSearchIndexVersionResponse)
	if err := s.conn.Invoke(ctx, searchSearchIndexHistoryServiceAnnotateSearchIndexVersionMethod, annotateSearchIndexVersionRequest, annotateSearchIndexVersionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/AnnotateSearchIndexVersion")
	}
	return &searchv1.SummarizeSearchQueriesResponse{
		Result: &commonv1.OperationResult{
			Details:       annotateSearchIndexVersionReply.GetResult().GetDetails(),
			CorrelationId: annotateSearchIndexVersionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestSearchQueries lists search queries a page at a time.
func (s *SearchQueryQueryServer) SuggestSearchQueries(ctx context.Context, req *searchv1.SuggestSearchQueriesRequest) (*searchv1.SuggestSearchQueriesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuggestSearchQueries", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest search queries", "tenant", req.GetContext().GetTenantId())
	return &searchv1.SuggestSearchQueriesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekSearchQuery returns a search query.
func (s *SearchQueryQueryServer) PeekSearchQuery(ctx context.Context, req *searchv1.PeekSearchQueryRequest) (*searchv1.PeekSearchQueryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "peek_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.PeekSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}
