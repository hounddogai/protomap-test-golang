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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SynonymExportServer implements `acme.search.v1.SynonymExportService`.
// Exports synonyms to files and other systems.
type SynonymExportServer struct {
	searchv1.UnimplementedSynonymExportServiceServer
	store                   *Store
	log                     *slog.Logger
	searchQueryExportClient searchv1.SearchQueryExportServiceClient
}

// NewSynonymExportServer wires a SynonymExportServer.
func NewSynonymExportServer(deps platform.Deps) *SynonymExportServer {
	return &SynonymExportServer{
		store:                   NewStore(deps.DB),
		log:                     deps.Logger.With("service", "acme.search.v1.SynonymExportService"),
		searchQueryExportClient: searchv1.NewSearchQueryExportServiceClient(deps.Conn),
	}
}

// StreamSynonymExport streams every matching synonym.
func (s *SynonymExportServer) StreamSynonymExport(req *searchv1.StreamSynonymExportRequest, stream grpc.ServerStreamingServer[searchv1.StreamSynonymExportResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream synonym export", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&searchv1.StreamSynonymExportResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
		return err
	}
	return nil
}

// GetSynonymExport returns a synonym.
func (s *SynonymExportServer) GetSynonymExport(ctx context.Context, req *searchv1.GetSynonymExportRequest) (*searchv1.GetSynonymExportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "get_synonym_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get synonym export", "tenant", req.GetContext().GetTenantId())
	return &searchv1.GetSynonymExportResponse{
		Synonym: &searchv1.Synonym{SynonymId: req.GetSynonymId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSynonymExports lists synonyms a page at a time.
func (s *SynonymExportServer) ListSynonymExports(ctx context.Context, req *searchv1.ListSynonymExportsRequest) (*searchv1.ListSynonymExportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list synonym exports", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	scheduleSearchQueryExportRequest := &searchv1.ScheduleSearchQueryExportRequest{}
	scheduleSearchQueryExportRequest.Context = req.GetContext()
	scheduleSearchQueryExportRequest.Details = &searchv1.ScheduleSearchQueryExportRequest_Details{Filters: labels}
	scheduleSearchQueryExportRequest.Annotations = req.GetContext().GetBaggage()
	scheduleSearchQueryExportReply, err := s.searchQueryExportClient.ScheduleSearchQueryExport(ctx, scheduleSearchQueryExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ScheduleSearchQueryExport")
	}
	return &searchv1.ListSynonymExportsResponse{
		Result: &commonv1.OperationResult{
			Details: scheduleSearchQueryExportReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ScheduleSynonymExport schedules a synonym.
func (s *SynonymExportServer) ScheduleSynonymExport(ctx context.Context, req *searchv1.ScheduleSynonymExportRequest) (*searchv1.ScheduleSynonymExportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "schedule_synonym_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule synonym export", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ScheduleSynonymExportResponse{
		Period: req.GetDetails().GetPeriod(),
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetrySynonymExport retrys a synonym.
func (s *SynonymExportServer) RetrySynonymExport(ctx context.Context, req *searchv1.RetrySynonymExportRequest) (*searchv1.RetrySynonymExportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "retry synonym export", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.RetrySynonymExportResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Name:      req.GetSynonym().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ShareSynonymExport shares a synonym.
func (s *SynonymExportServer) ShareSynonymExport(ctx context.Context, req *searchv1.ShareSynonymExportRequest) (*searchv1.ShareSynonymExportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "share_synonym_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share synonym export", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ShareSynonymExportResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Owner:     &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
