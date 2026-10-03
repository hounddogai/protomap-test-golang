package search

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SearchIndexExportServer implements `acme.search.v1.SearchIndexExportService`.
// Exports search indexes to files and other systems.
type SearchIndexExportServer struct {
	searchv1.UnimplementedSearchIndexExportServiceServer
	store                    *Store
	log                      *slog.Logger
	conn                     *grpc.ClientConn
	documentValidationOnce   sync.Once
	documentValidationCached documentsv1.DocumentValidationServiceClient
	auditEventClient         auditv1.AuditEventServiceClient
}

// NewSearchIndexExportServer wires a SearchIndexExportServer.
func NewSearchIndexExportServer(deps platform.Deps) *SearchIndexExportServer {
	return &SearchIndexExportServer{
		store:            NewStore(deps.DB),
		log:              deps.Logger.With("service", "acme.search.v1.SearchIndexExportService"),
		conn:             deps.Conn,
		auditEventClient: auditv1.NewAuditEventServiceClient(deps.Conn),
	}
}

// StreamSearchIndexExport streams every matching search index.
func (s *SearchIndexExportServer) StreamSearchIndexExport(req *searchv1.StreamSearchIndexExportRequest, stream grpc.ServerStreamingServer[searchv1.StreamSearchIndexExportResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream search index export", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&searchv1.StreamSearchIndexExportResponse{Result: &commonv1.OperationResult{Status: "ok"}}); err != nil {
		return err
	}
	return nil
}

// GetSearchIndexExport returns a search index.
func (s *SearchIndexExportServer) GetSearchIndexExport(ctx context.Context, req *searchv1.GetSearchIndexExportRequest) (*searchv1.GetSearchIndexExportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetSearchIndexExport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "get_search_index_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get search index export", "tenant", req.GetContext().GetTenantId())
	return &searchv1.GetSearchIndexExportResponse{
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelSearchIndexExport cancels a search index.
func (s *SearchIndexExportServer) CancelSearchIndexExport(ctx context.Context, req *searchv1.CancelSearchIndexExportRequest) (*searchv1.CancelSearchIndexExportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelSearchIndexExport", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "cancel_search_index_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "cancel search index export", "tenant", req.GetContext().GetTenantId())
	return &searchv1.CancelSearchIndexExportResponse{
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Outcome:     &searchv1.CancelSearchIndexExportResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSearchIndexExports lists search indexes a page at a time.
func (s *SearchIndexExportServer) ListSearchIndexExports(ctx context.Context, req *searchv1.ListSearchIndexExportsRequest) (*searchv1.ListSearchIndexExportsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list search index exports", "tenant", req.GetContext().GetTenantId())
	lintDocumentsRequest := &documentsv1.LintDocumentsRequest{
		Context: req.GetContext(),
		Document: &documentsv1.Document{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	lintDocumentsReply, err := s.documentValidationLazy().LintDocuments(ctx, lintDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentValidationService/LintDocuments")
	}
	return &searchv1.ListSearchIndexExportsResponse{
		Result: &commonv1.OperationResult{
			Message: lintDocumentsReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// ScheduleSearchIndexExport schedules a search index.
func (s *SearchIndexExportServer) ScheduleSearchIndexExport(ctx context.Context, req *searchv1.ScheduleSearchIndexExportRequest) (*searchv1.ScheduleSearchIndexExportResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "schedule_search_index_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "schedule search index export", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	watchAuditEventsStream, err := s.auditEventClient.WatchAuditEvents(ctx, &auditv1.WatchAuditEventsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: cachedContext.GetActor().GetEmail()},
		Filter:     &auditv1.WatchAuditEventsRequest_Filter{Labels: annotations},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
	}
	for {
		received, err := watchAuditEventsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/WatchAuditEvents")
		}
		_ = received
	}
	return &searchv1.ScheduleSearchIndexExportResponse{
		Period: req.GetDetails().GetPeriod(),
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ShareSearchIndexExport shares a search index.
func (s *SearchIndexExportServer) ShareSearchIndexExport(ctx context.Context, req *searchv1.ShareSearchIndexExportRequest) (*searchv1.ShareSearchIndexExportResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "share_search_index_export"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "share search index export", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ShareSearchIndexExportResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Owner:         &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// documentValidationLazy dials acme.documents.v1.DocumentValidationService the first time a handler needs it.
func (s *SearchIndexExportServer) documentValidationLazy() documentsv1.DocumentValidationServiceClient {
	s.documentValidationOnce.Do(func() { s.documentValidationCached = documentsv1.NewDocumentValidationServiceClient(s.conn) })
	return s.documentValidationCached
}
