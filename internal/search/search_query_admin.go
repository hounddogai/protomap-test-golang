package search

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SearchQueryAdminServer implements `acme.search.v1.SearchQueryAdminService`.
// Operator actions on search queries that support staff run by hand.
type SearchQueryAdminServer struct {
	searchv1.UnimplementedSearchQueryAdminServiceServer
	store                   *Store
	log                     *slog.Logger
	searchQueryExportClient searchv1.SearchQueryExportServiceClient
	synonymExportClient     searchv1.SynonymExportServiceClient
}

// NewSearchQueryAdminServer wires a SearchQueryAdminServer.
func NewSearchQueryAdminServer(deps platform.Deps) SearchQueryAdminServer {
	return SearchQueryAdminServer{
		store:                   NewStore(deps.DB),
		log:                     deps.Logger.With("service", "acme.search.v1.SearchQueryAdminService"),
		searchQueryExportClient: searchv1.NewSearchQueryExportServiceClient(deps.Conn),
		synonymExportClient:     searchv1.NewSynonymExportServiceClient(deps.Conn),
	}
}

// ArchiveSearchQuery archives a search query.
func (s SearchQueryAdminServer) ArchiveSearchQuery(ctx context.Context, req *searchv1.ArchiveSearchQueryRequest) (*searchv1.ArchiveSearchQueryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "archive_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "archive search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ArchiveSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Outcome:     &searchv1.ArchiveSearchQueryResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RestoreSearchQuery restores a search query.
func (s SearchQueryAdminServer) RestoreSearchQuery(ctx context.Context, req *searchv1.RestoreSearchQueryRequest) (*searchv1.RestoreSearchQueryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "restore_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "restore search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.RestoreSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Outcome:     &searchv1.RestoreSearchQueryResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PurgeSearchQuery deletes a search query.
func (s SearchQueryAdminServer) PurgeSearchQuery(ctx context.Context, req *searchv1.PurgeSearchQueryRequest) (*searchv1.PurgeSearchQueryResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "purge_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "purge search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.PurgeSearchQueryResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ReassignSearchQuery reassigns a search query.
func (s SearchQueryAdminServer) ReassignSearchQuery(ctx context.Context, req *searchv1.ReassignSearchQueryRequest) (*searchv1.ReassignSearchQueryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReassignSearchQuery", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "reassign_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reassign search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ReassignSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			Subject: &searchv1.SearchIndexRef{
				DisplayName: req.GetDetails().GetAssignee().GetDisplayName(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LockSearchQuery locks a search query.
func (s SearchQueryAdminServer) LockSearchQuery(ctx context.Context, req *searchv1.LockSearchQueryRequest) (*searchv1.LockSearchQueryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "lock_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lock search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.LockSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Outcome:     &searchv1.LockSearchQueryResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnlockSearchQuery unlocks a search query.
func (s SearchQueryAdminServer) UnlockSearchQuery(ctx context.Context, req *searchv1.UnlockSearchQueryRequest) (*searchv1.UnlockSearchQueryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "unlock_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unlock search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.UnlockSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Outcome:     &searchv1.UnlockSearchQueryResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TransferSearchQuery transfers a search query.
func (s SearchQueryAdminServer) TransferSearchQuery(ctx context.Context, req *searchv1.TransferSearchQueryRequest) (*searchv1.TransferSearchQueryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "transfer_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "transfer search query", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *searchv1.TransferSearchQueryRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	shareSearchQueryExportRequest := &searchv1.ShareSearchQueryExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.ShareSearchQueryExportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email:            cachedDetails.GetNotifyContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
		},
	}
	shareSearchQueryExportReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*searchv1.ShareSearchQueryExportResponse, error) {
		return s.searchQueryExportClient.ShareSearchQueryExport(ctx, shareSearchQueryExportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryExportService/ShareSearchQueryExport")
	}
	return &searchv1.TransferSearchQueryResponse{
		Result: &commonv1.OperationResult{
			Message: shareSearchQueryExportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Outcome:     &searchv1.TransferSearchQueryResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RepairSearchQuery repairs a search query.
func (s SearchQueryAdminServer) RepairSearchQuery(ctx context.Context, req *searchv1.RepairSearchQueryRequest) (*searchv1.RepairSearchQueryResponse, error) {
	if err := s.store.SaveSearchQuery(ctx, req.GetContext().GetTenantId(), req.GetSearchQuery()); err != nil {
		return nil, grpcx.Wrap(err, "save search query")
	}
	s.log.InfoContext(ctx, "repair search query", "tenant", req.GetContext().GetTenantId())
	return &searchv1.RepairSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{
			SearchQueryId: req.GetSearchQueryId(),
			Kind:          req.GetSearchQuery().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectSearchQuery returns a search query.
func (s SearchQueryAdminServer) InspectSearchQuery(ctx context.Context, req *searchv1.InspectSearchQueryRequest) (*searchv1.InspectSearchQueryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_query", req.GetSearchQueryId(), "inspect_search_query"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect search query", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	streamSynonymExportStream, err := s.synonymExportClient.StreamSynonymExport(ctx, &searchv1.StreamSynonymExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSynonymExportRequest_Filter{
			OwnerId:      req.GetSearchQueryId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
	}
	for {
		received, err := streamSynonymExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
		}
		_ = received
	}
	return &searchv1.InspectSearchQueryResponse{
		SearchQuery: &searchv1.SearchQuery{SearchQueryId: req.GetSearchQueryId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}
