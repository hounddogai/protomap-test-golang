package search

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// SearchQueryExportServer implements `acme.search.v1.SearchQueryExportService`.
// Exports search queries to files and other systems.
type SearchQueryExportServer struct {
	searchv1.UnimplementedSearchQueryExportServiceServer
	store                  *Store
	log                    *slog.Logger
	featureFlagQueryClient featureflagsv1.FeatureFlagQueryServiceClient
	synonymSyncClient      searchv1.SynonymSyncServiceClient
}

// NewSearchQueryExportServer wires a SearchQueryExportServer.
func NewSearchQueryExportServer(deps platform.Deps) *SearchQueryExportServer {
	return &SearchQueryExportServer{
		store:                  NewStore(deps.DB),
		log:                    deps.Logger.With("service", "acme.search.v1.SearchQueryExportService"),
		featureFlagQueryClient: featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		synonymSyncClient:      searchv1.NewSynonymSyncServiceClient(deps.Conn),
	}
}

// StreamSearchQueryExport streams every matching search query.
func (s *SearchQueryExportServer) StreamSearchQueryExport(req *searchv1.StreamSearchQueryExportRequest, stream grpc.ServerStreamingServer[searchv1.StreamSearchQueryExportResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream search query export", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&searchv1.StreamSearchQueryExportResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
		return err
	}
	return nil
}

// GetSearchQueryExport returns a search query.
func (s *SearchQueryExportServer) GetSearchQueryExport(ctx context.Context, req *searchv1.GetSearchQueryExportRequest) (*searchv1.GetSearchQueryExportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "get_search_query_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get search query export", "tenant", req.GetContext().GetTenantId())
	browseFeatureFlagsRequest := buildBrowseFeatureFlagsRequestForGetSearchQueryExport(req)
	browseFeatureFlagsReply, err := s.featureFlagQueryClient.BrowseFeatureFlags(ctx, browseFeatureFlagsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/BrowseFeatureFlags")
	}
	return &searchv1.GetSearchQueryExportResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseFeatureFlagsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
	}, nil
}

// CancelSearchQueryExport cancels a search query.
func (s *SearchQueryExportServer) CancelSearchQueryExport(ctx context.Context, req *searchv1.CancelSearchQueryExportRequest) (*searchv1.CancelSearchQueryExportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelSearchQueryExport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "cancel_search_query_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "cancel search query export", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullSynonymsReply, err := s.synonymSyncClient.PullSynonyms(ctx, &searchv1.PullSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
		Details: &searchv1.PullSynonymsRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/PullSynonyms")
	}
	return &searchv1.CancelSearchQueryExportResponse{
		Outcome: &searchv1.CancelSearchQueryExportResponse_Outcome{
			Message:     pullSynonymsReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSearchQueryExports lists search queries a page at a time.
func (s *SearchQueryExportServer) ListSearchQueryExports(ctx context.Context, req *searchv1.ListSearchQueryExportsRequest) (*searchv1.ListSearchQueryExportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list search query exports", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := searchQueryExportServerSyncSynonymsSteps[mode]
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	syncSynonymsRequest := &searchv1.SyncSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email:  tenancy.Value(ctx, "email"),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit:      &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
			Parameters: req.GetFilter().GetLabels(),
		},
	}
	syncSynonymsReply, err := run(s, ctx, syncSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/SyncSynonyms")
	}
	return &searchv1.ListSearchQueryExportsResponse{
		SearchQueries: []*searchv1.SearchQuery{&searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{
				DisplayName: syncSynonymsReply.GetSynonym().GetOwner().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{
			ReferenceIds: syncSynonymsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ScheduleSearchQueryExport schedules a search query.
func (s *SearchQueryExportServer) ScheduleSearchQueryExport(ctx context.Context, req *searchv1.ScheduleSearchQueryExportRequest) (*searchv1.ScheduleSearchQueryExportResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "schedule_search_query_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule search query export", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ScheduleSearchQueryExportResponse{
		Period: req.GetDetails().GetPeriod(),
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetrySearchQueryExport retrys a search query.
func (s *SearchQueryExportServer) RetrySearchQueryExport(ctx context.Context, req *searchv1.RetrySearchQueryExportRequest) (*searchv1.RetrySearchQueryExportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "retry search query export", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := searchQueryExportServerListSynonymConflictsSteps[mode]
	searchQueryCache := cache.New[string, *searchv1.SearchQuery](1)
	searchQueryCache.Put(req.GetContext().GetTraceId(), req.GetSearchQuery())
	cachedSearchQuery, _ := searchQueryCache.Get(req.GetContext().GetTraceId())
	var envelope searchv1.SearchIndexRef
	if err := req.GetSearchQuery().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listSynonymConflictsRequest := &searchv1.ListSynonymConflictsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymConflictsRequest_Filter{
			OwnerId:      cachedSearchQuery.GetSubject().GetSearchIndexId(),
			Labels:       req.GetDetails().GetParameters(),
			CreatedAfter: req.GetSearchQuery().GetOccurredAt(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listSynonymConflictsReply, err := run(s, ctx, listSynonymConflictsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ListSynonymConflicts")
	}
	return &searchv1.RetrySearchQueryExportResponse{
		SearchQuery: &searchv1.SearchQuery{
			CorrelationId: listSynonymConflictsReply.GetResult().GetCorrelationId(),
			SearchQueryId: req.GetSearchQueryId(),
			Kind:          req.GetSearchQuery().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ShareSearchQueryExport shares a search query.
func (s *SearchQueryExportServer) ShareSearchQueryExport(ctx context.Context, req *searchv1.ShareSearchQueryExportRequest) (*searchv1.ShareSearchQueryExportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "share_search_query_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share search query export", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ShareSearchQueryExportResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// searchQueryExportServerSyncSynonymsSteps are the ways to run acme.search.v1.SynonymSyncService/SyncSynonyms, by name.
var searchQueryExportServerSyncSynonymsSteps = map[string]func(*SearchQueryExportServer, context.Context, *searchv1.SyncSynonymsRequest) (*searchv1.SyncSynonymsResponse, error){
	"default": (*SearchQueryExportServer).syncSynonymsStep,
	"audited": (*SearchQueryExportServer).syncSynonymsAuditedStep,
}

func (s *SearchQueryExportServer) syncSynonymsStep(ctx context.Context, request *searchv1.SyncSynonymsRequest) (*searchv1.SyncSynonymsResponse, error) {
	return s.synonymSyncClient.SyncSynonyms(ctx, request)
}

func (s *SearchQueryExportServer) syncSynonymsAuditedStep(ctx context.Context, request *searchv1.SyncSynonymsRequest) (*searchv1.SyncSynonymsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SynonymSyncService/SyncSynonyms")
	return s.synonymSyncClient.SyncSynonyms(ctx, request)
}

// searchQueryExportServerListSynonymConflictsSteps are the ways to run
// acme.search.v1.SynonymSyncService/ListSynonymConflicts, by name.
var searchQueryExportServerListSynonymConflictsSteps = map[string]func(*SearchQueryExportServer, context.Context, *searchv1.ListSynonymConflictsRequest) (*searchv1.ListSynonymConflictsResponse, error){
	"default": (*SearchQueryExportServer).listSynonymConflictsStep,
	"audited": (*SearchQueryExportServer).listSynonymConflictsAuditedStep,
}

func (s *SearchQueryExportServer) listSynonymConflictsStep(ctx context.Context, request *searchv1.ListSynonymConflictsRequest) (*searchv1.ListSynonymConflictsResponse, error) {
	return s.synonymSyncClient.ListSynonymConflicts(ctx, request)
}

func (s *SearchQueryExportServer) listSynonymConflictsAuditedStep(ctx context.Context, request *searchv1.ListSynonymConflictsRequest) (*searchv1.ListSynonymConflictsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.search.v1.SynonymSyncService/ListSynonymConflicts")
	return s.synonymSyncClient.ListSynonymConflicts(ctx, request)
}
