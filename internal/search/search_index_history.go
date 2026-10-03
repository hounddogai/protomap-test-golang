package search

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SearchIndexHistoryServer implements `acme.search.v1.SearchIndexHistoryService`.
// Keeps every version of a search index and restores earlier ones.
type SearchIndexHistoryServer struct {
	searchv1.UnimplementedSearchIndexHistoryServiceServer
	store                        *Store
	log                          *slog.Logger
	synonymSyncClient            searchv1.SynonymSyncServiceClient
	metricSeriesValidationClient metricsv1.MetricSeriesValidationServiceClient
	searchIndexExportPort        searchIndexExportPort
}

// NewSearchIndexHistoryServer wires a SearchIndexHistoryServer.
func NewSearchIndexHistoryServer(deps platform.Deps) *SearchIndexHistoryServer {
	return &SearchIndexHistoryServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.search.v1.SearchIndexHistoryService"),
		synonymSyncClient:            searchv1.NewSynonymSyncServiceClient(deps.Conn),
		metricSeriesValidationClient: metricsv1.NewMetricSeriesValidationServiceClient(deps.Conn),
		searchIndexExportPort:        newSearchIndexExportPort(deps),
	}
}

// RecordSearchIndexVersion records a search index.
func (s *SearchIndexHistoryServer) RecordSearchIndexVersion(ctx context.Context, req *searchv1.RecordSearchIndexVersionRequest) (*searchv1.RecordSearchIndexVersionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecordSearchIndexVersion", "error", err)
		return nil, err
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "record search index version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.RecordSearchIndexVersionResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Name:          req.GetSearchIndex().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSearchIndexVersions lists search indexes a page at a time.
func (s *SearchIndexHistoryServer) ListSearchIndexVersions(ctx context.Context, req *searchv1.ListSearchIndexVersionsRequest) (*searchv1.ListSearchIndexVersionsResponse, error) {
	s.log.InfoContext(ctx, "list search index versions", "tenant", req.GetContext().GetTenantId())
	listSynonymConflictsRequest := &searchv1.ListSynonymConflictsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymConflictsRequest_Filter{
			Labels: req.GetFilter().GetLabels(),
			Region: req.GetFilter().GetRegion(),
		},
	}
	listSynonymConflictsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*searchv1.ListSynonymConflictsResponse, error) {
		return s.synonymSyncClient.ListSynonymConflicts(ctx, listSynonymConflictsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ListSynonymConflicts")
	}
	return &searchv1.ListSearchIndexVersionsResponse{
		PageInfo: &commonv1.PageInfo{
			TotalSize: listSynonymConflictsReply.GetPageInfo().GetTotalSize(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSearchIndexVersion returns a search index.
func (s *SearchIndexHistoryServer) GetSearchIndexVersion(ctx context.Context, req *searchv1.GetSearchIndexVersionRequest) (*searchv1.GetSearchIndexVersionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSearchIndexVersion", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "get_search_index_version"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get search index version", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	lintMetricSeriesesRequest := &metricsv1.LintMetricSeriesesRequest{
		Context: req.GetContext(),
		Details: &metricsv1.LintMetricSeriesesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		MetricSeries: &metricsv1.MetricSeries{
			Owner: &identityv1.UserRef{UserId: cachedContext.GetActor().GetUserId()},
		},
	}
	lintMetricSeriesesReply, err := grpcx.Call(ctx, s.metricSeriesValidationClient.LintMetricSerieses, lintMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses")
	}
	return &searchv1.GetSearchIndexVersionResponse{
		SearchIndex: &searchv1.SearchIndex{
			Audit: &commonv1.AuditInfo{
				CreatedBy: lintMetricSeriesesReply.GetMetricSeries().GetAudit().GetCreatedBy(),
			},
			Dimensions: &searchv1.SearchIndexDimensions{
				Weight: lintMetricSeriesesReply.GetMetricSeries().GetDimensions().GetWeight(),
			},
			SearchIndexId: req.GetSearchIndexId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareSearchIndexVersions compares a search index.
func (s *SearchIndexHistoryServer) CompareSearchIndexVersions(ctx context.Context, req *searchv1.CompareSearchIndexVersionsRequest) (*searchv1.CompareSearchIndexVersionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "compare search index versions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.CompareSearchIndexVersionsResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Specs:         req.GetSearchIndex().GetSpecs(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AnnotateSearchIndexVersion annotates a search index.
func (s *SearchIndexHistoryServer) AnnotateSearchIndexVersion(ctx context.Context, req *searchv1.AnnotateSearchIndexVersionRequest) (*searchv1.AnnotateSearchIndexVersionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "annotate search index version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.AnnotateSearchIndexVersionResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Specs:         req.GetSearchIndex().GetSpecs(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PruneSearchIndexVersions deletes a search index.
func (s *SearchIndexHistoryServer) PruneSearchIndexVersions(ctx context.Context, req *searchv1.PruneSearchIndexVersionsRequest) (*searchv1.PruneSearchIndexVersionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "prune_search_index_versions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "prune search index versions", "tenant", req.GetContext().GetTenantId())
	getSearchIndexExportRequest := &searchv1.GetSearchIndexExportRequest{
		Context:       req.GetContext(),
		SearchIndexId: req.GetSearchIndexId(),
	}
	getSearchIndexExportReply, err := s.searchIndexExportPort.GetSearchIndexExport(ctx, getSearchIndexExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/GetSearchIndexExport")
	}
	return &searchv1.PruneSearchIndexVersionsResponse{
		Result: &commonv1.OperationResult{
			Message: getSearchIndexExportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}
