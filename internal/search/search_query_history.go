package search

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SearchQueryHistoryServer implements `acme.search.v1.SearchQueryHistoryService`.
// Keeps every version of a search query and restores earlier ones.
type SearchQueryHistoryServer struct {
	searchv1.UnimplementedSearchQueryHistoryServiceServer
	store             *Store
	log               *slog.Logger
	synonymSyncClient searchv1.SynonymSyncServiceClient
	conn              *grpc.ClientConn
}

// NewSearchQueryHistoryServer wires a SearchQueryHistoryServer.
func NewSearchQueryHistoryServer(deps platform.Deps) *SearchQueryHistoryServer {
	return &SearchQueryHistoryServer{
		store:             NewStore(deps.DB),
		log:               deps.Logger.With("service", "acme.search.v1.SearchQueryHistoryService"),
		synonymSyncClient: searchv1.NewSynonymSyncServiceClient(deps.Conn),
		conn:              deps.Conn,
	}
}

// RecordSearchQueryVersion records a search query.
func (s *SearchQueryHistoryServer) RecordSearchQueryVersion(ctx context.Context, req *searchv1.RecordSearchQueryVersionRequest) (*searchv1.RecordSearchQueryVersionResponse, error) {
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "record search query version", "tenant", req.GetContext().GetTenantId())
	return &searchv1.RecordSearchQueryVersionResponse{
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			Attributes:    req.GetSearchQuery().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSearchQueryVersions lists search queries a page at a time.
func (s *SearchQueryHistoryServer) ListSearchQueryVersions(ctx context.Context, req *searchv1.ListSearchQueryVersionsRequest) (*searchv1.ListSearchQueryVersionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list search query versions", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ListSearchQueryVersionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// GetSearchQueryVersion returns a search query.
func (s *SearchQueryHistoryServer) GetSearchQueryVersion(ctx context.Context, req *searchv1.GetSearchQueryVersionRequest) (*searchv1.GetSearchQueryVersionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "get_search_query_version"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get search query version", "tenant", req.GetContext().GetTenantId())
	source := req.GetContext().GetActor().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	syncSynonymsRequest := &searchv1.SyncSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{Owner: &identityv1.UserRef{Email: email}},
	}
	type syncSynonymsResult struct {
		reply *searchv1.SyncSynonymsResponse
		err   error
	}
	syncSynonymsResults := make(chan syncSynonymsResult, 1)
	go func() {
		reply, err := s.synonymSyncClient.SyncSynonyms(ctx, syncSynonymsRequest)
		syncSynonymsResults <- syncSynonymsResult{reply, err}
	}()
	var syncSynonymsReply *searchv1.SyncSynonymsResponse
	select {
	case result := <-syncSynonymsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.search.v1.SynonymSyncService/SyncSynonyms")
		}
		syncSynonymsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &searchv1.GetSearchQueryVersionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: syncSynonymsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
	}, nil
}

// CompareSearchQueryVersions compares a search query.
func (s *SearchQueryHistoryServer) CompareSearchQueryVersions(ctx context.Context, req *searchv1.CompareSearchQueryVersionsRequest) (*searchv1.CompareSearchQueryVersionsResponse, error) {
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "compare search query versions", "tenant", req.GetContext().GetTenantId())
	return &searchv1.CompareSearchQueryVersionsResponse{
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			Subject:       req.GetSearchQuery().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RevertSearchQuery reverts a search query.
func (s *SearchQueryHistoryServer) RevertSearchQuery(ctx context.Context, req *searchv1.RevertSearchQueryRequest) (*searchv1.RevertSearchQueryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "revert search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.RevertSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			CorrelationId: req.GetSearchQuery().GetCorrelationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PruneSearchQueryVersions deletes a search query.
func (s *SearchQueryHistoryServer) PruneSearchQueryVersions(ctx context.Context, req *searchv1.PruneSearchQueryVersionsRequest) (*searchv1.PruneSearchQueryVersionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PruneSearchQueryVersions", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "prune_search_query_versions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "prune search query versions", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	listSearchIndexExportsRequest := &searchv1.ListSearchIndexExportsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSearchIndexExportsRequest_Filter{
			OwnerId:      req.GetSearchQueryId(),
			Labels:       req.GetContext().GetBaggage(),
			CreatedAfter: cloned.GetDeadline(),
		},
	}
	listSearchIndexExportsReply, err := grpcx.NewClient(s.conn, searchv1.NewSearchIndexExportServiceClient).ListSearchIndexExports(ctx, listSearchIndexExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ListSearchIndexExports")
	}
	return &searchv1.PruneSearchQueryVersionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listSearchIndexExportsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}
