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
	"google.golang.org/protobuf/proto"
)

// SynonymSyncServer implements `acme.search.v1.SynonymSyncService`.
// Keeps synonyms in step with the systems of record in other regions.
type SynonymSyncServer struct {
	searchv1.UnimplementedSynonymSyncServiceServer
	store                   *Store
	log                     *slog.Logger
	searchIndexExportClient searchv1.SearchIndexExportServiceClient
}

// NewSynonymSyncServer wires a SynonymSyncServer.
func NewSynonymSyncServer(deps platform.Deps) *SynonymSyncServer {
	return &SynonymSyncServer{
		store:                   NewStore(deps.DB),
		log:                     deps.Logger.With("service", "acme.search.v1.SynonymSyncService"),
		searchIndexExportClient: searchv1.NewSearchIndexExportServiceClient(deps.Conn),
	}
}

// SyncSynonyms syncs a synonym.
func (s *SynonymSyncServer) SyncSynonyms(ctx context.Context, req *searchv1.SyncSynonymsRequest) (*searchv1.SyncSynonymsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "sync synonyms", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.SyncSynonymsResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Status:    req.GetSynonym().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PushSynonym pushs a synonym.
func (s *SynonymSyncServer) PushSynonym(ctx context.Context, req *searchv1.PushSynonymRequest) (*searchv1.PushSynonymResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "push synonym", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.PushSynonymResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Priority:  req.GetSynonym().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PullSynonyms pulls a synonym.
func (s *SynonymSyncServer) PullSynonyms(ctx context.Context, req *searchv1.PullSynonymsRequest) (*searchv1.PullSynonymsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PullSynonyms", "error", err)
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "pull synonyms", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.PullSynonymsResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Version:   req.GetSynonym().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReconcileSynonyms reconciles a synonym.
func (s *SynonymSyncServer) ReconcileSynonyms(ctx context.Context, req *searchv1.ReconcileSynonymsRequest) (*searchv1.ReconcileSynonymsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReconcileSynonyms", "error", err)
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "reconcile synonyms", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.ReconcileSynonymsResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Effective: req.GetSynonym().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamSynonymChanges exchanges changes of synonyms with a peer.
func (s *SynonymSyncServer) StreamSynonymChanges(stream grpc.BidiStreamingServer[searchv1.StreamSynonymChangesRequest, searchv1.StreamSynonymChangesResponse]) error {
	ctx := stream.Context()
	incoming := make(chan *searchv1.StreamSynonymChangesRequest)
	failed := make(chan error, 1)
	go func() {
		defer close(incoming)
		for {
			received, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					failed <- err
				}
				return
			}
			select {
			case incoming <- received:
			case <-ctx.Done():
				return
			}
		}
	}()
	for msg := range incoming {
		s.log.InfoContext(ctx, "stream synonym changes", "tenant", msg.GetContext().GetTenantId())
		parameters := make(map[string]string, len(msg.GetChange().GetParameters()))
		for key, value := range msg.GetChange().GetParameters() {
			parameters[key] = value
		}
		listSearchIndexExportsRequest := &searchv1.ListSearchIndexExportsRequest{
			Context: msg.GetContext(),
			Filter: &searchv1.ListSearchIndexExportsRequest_Filter{
				CreatedAfter: msg.GetChange().GetAudit().GetUpdatedAt(),
				Labels:       parameters,
				OwnerId:      msg.GetChange().GetOwner().GetUserId(),
			},
		}
		if _, err := grpcx.Call(ctx, s.searchIndexExportClient.ListSearchIndexExports, listSearchIndexExportsRequest); err != nil {
			return grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ListSearchIndexExports")
		}
		if err := stream.Send(&searchv1.StreamSynonymChangesResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
			return err
		}
	}
	select {
	case err := <-failed:
		return err
	default:
		return nil
	}
}

// ResolveSynonymConflict resolves a synonym.
func (s *SynonymSyncServer) ResolveSynonymConflict(ctx context.Context, req *searchv1.ResolveSynonymConflictRequest) (*searchv1.ResolveSynonymConflictResponse, error) {
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "resolve synonym conflict", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleSearchIndexExportRequest := newScheduleSearchIndexExportRequest(withScheduleSearchIndexExportContext(req.GetContext()), withScheduleSearchIndexExportAnnotations(annotations))
	scheduleSearchIndexExportReply, err := s.searchIndexExportClient.ScheduleSearchIndexExport(ctx, scheduleSearchIndexExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ScheduleSearchIndexExport")
	}
	return &searchv1.ResolveSynonymConflictResponse{
		Outcome: &searchv1.ResolveSynonymConflictResponse_Outcome{
			Status: scheduleSearchIndexExportReply.GetResult().GetStatus(),
		},
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Priority:  req.GetSynonym().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSynonymSyncState returns a synonym.
func (s *SynonymSyncServer) GetSynonymSyncState(ctx context.Context, req *searchv1.GetSynonymSyncStateRequest) (*searchv1.GetSynonymSyncStateResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "get_synonym_sync_state"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get synonym sync state", "tenant", req.GetContext().GetTenantId())
	return &searchv1.GetSynonymSyncStateResponse{
		Synonym: &searchv1.Synonym{SynonymId: req.GetSynonymId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSynonymConflicts lists synonyms a page at a time.
func (s *SynonymSyncServer) ListSynonymConflicts(ctx context.Context, req *searchv1.ListSynonymConflictsRequest) (*searchv1.ListSynonymConflictsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list synonym conflicts", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*searchv1.ListSynonymConflictsRequest_Filter)
	streamSearchIndexExportStream, err := s.searchIndexExportClient.StreamSearchIndexExport(ctx, &searchv1.StreamSearchIndexExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSearchIndexExportRequest_Filter{
			Region:       cloned.GetRegion(),
			OwnerId:      req.GetFilter().GetOwnerId(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
	}
	for {
		received, err := streamSearchIndexExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
		}
		_ = received
	}
	return &searchv1.ListSynonymConflictsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}
