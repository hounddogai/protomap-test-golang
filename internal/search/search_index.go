package search

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SearchIndexServer implements `acme.search.v1.SearchIndexService`.
// Manages search indexes: creates, reads, changes, and lists them.
type SearchIndexServer struct {
	searchv1.UnimplementedSearchIndexServiceServer
	store                  *Store
	log                    *slog.Logger
	searchIndexQueryClient searchv1.SearchIndexQueryServiceClient
}

// NewSearchIndexServer wires a SearchIndexServer.
func NewSearchIndexServer(deps platform.Deps) *SearchIndexServer {
	return &SearchIndexServer{
		store:                  NewStore(deps.DB),
		log:                    deps.Logger.With("service", "acme.search.v1.SearchIndexService"),
		searchIndexQueryClient: searchv1.NewSearchIndexQueryServiceClient(deps.Conn),
	}
}

// CreateSearchIndex creates a search index.
func (s *SearchIndexServer) CreateSearchIndex(ctx context.Context, req *searchv1.CreateSearchIndexRequest) (*searchv1.CreateSearchIndexResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "create search index", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.CreateSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{
			Capacity: req.GetSearchIndex().GetCapacity(),
			Code:     req.GetSearchIndex().GetCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSearchIndex returns a search index.
func (s *SearchIndexServer) GetSearchIndex(ctx context.Context, req *searchv1.GetSearchIndexRequest) (*searchv1.GetSearchIndexResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "get_search_index"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get search index", "tenant", req.GetContext().GetTenantId())
	return &searchv1.GetSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdateSearchIndex changes a search index.
func (s *SearchIndexServer) UpdateSearchIndex(ctx context.Context, req *searchv1.UpdateSearchIndexRequest) (*searchv1.UpdateSearchIndexResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "update search index", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.UpdateSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{
			Specs: req.GetSearchIndex().GetSpecs(),
			Name:  req.GetSearchIndex().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DeleteSearchIndex deletes a search index.
func (s *SearchIndexServer) DeleteSearchIndex(ctx context.Context, req *searchv1.DeleteSearchIndexRequest) (*searchv1.DeleteSearchIndexResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "delete_search_index"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete search index", "tenant", req.GetContext().GetTenantId())
	return &searchv1.DeleteSearchIndexResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// BatchGetSearchIndexes returns several search indexes at once.
func (s *SearchIndexServer) BatchGetSearchIndexes(ctx context.Context, req *searchv1.BatchGetSearchIndexesRequest) (*searchv1.BatchGetSearchIndexesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BatchGetSearchIndexes", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get search indexes", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	suggestSearchIndexesReply, err := s.searchIndexQueryClient.SuggestSearchIndexes(ctx, &searchv1.SuggestSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.SuggestSearchIndexesRequest_Filter{
			OwnerId: req.GetContext().GetTenantId(),
			Labels:  baggage,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexQueryService/SuggestSearchIndexes")
	}
	return &searchv1.BatchGetSearchIndexesResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestSearchIndexesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// SearchSearchIndexes lists search indexes a page at a time.
func (s *SearchIndexServer) SearchSearchIndexes(ctx context.Context, req *searchv1.SearchSearchIndexesRequest) (*searchv1.SearchSearchIndexesResponse, error) {
	s.log.InfoContext(ctx, "search search indexes", "tenant", req.GetContext().GetTenantId())
	return &searchv1.SearchSearchIndexesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// BatchCreateSearchIndexes takes a stream of search indexes and answers once at the end.
func (s *SearchIndexServer) BatchCreateSearchIndexes(stream grpc.ClientStreamingServer[searchv1.BatchCreateSearchIndexesRequest, searchv1.BatchCreateSearchIndexesResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create search indexes", "tenant", msg.GetContext().GetTenantId())
		accepted++
	}
	return stream.SendAndClose(&searchv1.BatchCreateSearchIndexesResponse{Accepted: accepted})
}
