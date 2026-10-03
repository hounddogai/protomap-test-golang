package search

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SearchIndexAdminServer implements `acme.search.v1.SearchIndexAdminService`.
// Operator actions on search indexes that support staff run by hand.
type SearchIndexAdminServer struct {
	searchv1.UnimplementedSearchIndexAdminServiceServer
	store             *Store
	log               *slog.Logger
	synonymSyncClient searchv1.SynonymSyncServiceClient
}

// NewSearchIndexAdminServer wires a SearchIndexAdminServer.
func NewSearchIndexAdminServer(deps platform.Deps) *SearchIndexAdminServer {
	return &SearchIndexAdminServer{
		store:             NewStore(deps.DB),
		log:               deps.Logger.With("service", "acme.search.v1.SearchIndexAdminService"),
		synonymSyncClient: searchv1.NewSynonymSyncServiceClient(deps.Conn),
	}
}

// ArchiveSearchIndex archives a search index.
func (s *SearchIndexAdminServer) ArchiveSearchIndex(ctx context.Context, req *searchv1.ArchiveSearchIndexRequest) (*searchv1.ArchiveSearchIndexResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "archive_search_index"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "archive search index", "tenant", req.GetContext().GetTenantId())
	syncSynonymsRequest := buildSyncSynonymsRequestForArchiveSearchIndex(req)
	syncSynonymsReply, err := s.synonymSyncClient.SyncSynonyms(ctx, syncSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/SyncSynonyms")
	}
	return &searchv1.ArchiveSearchIndexResponse{
		Outcome: &searchv1.ArchiveSearchIndexResponse_Outcome{
			Status:      syncSynonymsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RestoreSearchIndex restores a search index.
func (s *SearchIndexAdminServer) RestoreSearchIndex(ctx context.Context, req *searchv1.RestoreSearchIndexRequest) (*searchv1.RestoreSearchIndexResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "restore_search_index"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "restore search index", "tenant", req.GetContext().GetTenantId())
	return &searchv1.RestoreSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Outcome:     &searchv1.RestoreSearchIndexResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReassignSearchIndex reassigns a search index.
func (s *SearchIndexAdminServer) ReassignSearchIndex(ctx context.Context, req *searchv1.ReassignSearchIndexRequest) (*searchv1.ReassignSearchIndexResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReassignSearchIndex", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "reassign_search_index"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reassign search index", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ReassignSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Capacity:      int64(req.GetDetails().GetCapacity()),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnlockSearchIndex unlocks a search index.
func (s *SearchIndexAdminServer) UnlockSearchIndex(ctx context.Context, req *searchv1.UnlockSearchIndexRequest) (*searchv1.UnlockSearchIndexResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "unlock_search_index"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unlock search index", "tenant", req.GetContext().GetTenantId())
	return &searchv1.UnlockSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Outcome:     &searchv1.UnlockSearchIndexResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TransferSearchIndex transfers a search index.
func (s *SearchIndexAdminServer) TransferSearchIndex(ctx context.Context, req *searchv1.TransferSearchIndexRequest) (*searchv1.TransferSearchIndexResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected TransferSearchIndex", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "transfer_search_index"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "transfer search index", "tenant", req.GetContext().GetTenantId())
	reconcileSynonymsRequest := &searchv1.ReconcileSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	reconcileSynonymsReply, err := reconcileSynonymsCall(ctx, reconcileSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ReconcileSynonyms")
	}
	return &searchv1.TransferSearchIndexResponse{
		Result: &commonv1.OperationResult{
			Message:       reconcileSynonymsReply.GetResult().GetMessage(),
			CorrelationId: reconcileSynonymsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Outcome:     &searchv1.TransferSearchIndexResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RepairSearchIndex repairs a search index.
func (s *SearchIndexAdminServer) RepairSearchIndex(ctx context.Context, req *searchv1.RepairSearchIndexRequest) (*searchv1.RepairSearchIndexResponse, error) {
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "repair search index", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.RepairSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Capacity:      req.GetSearchIndex().GetCapacity(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectSearchIndex returns a search index.
func (s *SearchIndexAdminServer) InspectSearchIndex(ctx context.Context, req *searchv1.InspectSearchIndexRequest) (*searchv1.InspectSearchIndexResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectSearchIndex", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "inspect_search_index"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect search index", "tenant", req.GetContext().GetTenantId())
	return &searchv1.InspectSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}
