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
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// SearchIndexValidationServer implements `acme.search.v1.SearchIndexValidationService`.
// Checks search indexes against the rules before anything acts on them.
type SearchIndexValidationServer struct {
	searchv1.UnimplementedSearchIndexValidationServiceServer
	store                         *Store
	log                           *slog.Logger
	searchQueryHistoryClient      searchv1.SearchQueryHistoryServiceClient
	searchQueryNotificationClient searchv1.SearchQueryNotificationServiceClient
	searchIndexNotificationClient searchv1.SearchIndexNotificationServiceClient
}

// NewSearchIndexValidationServer wires a SearchIndexValidationServer.
func NewSearchIndexValidationServer(deps platform.Deps) *SearchIndexValidationServer {
	return &SearchIndexValidationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.search.v1.SearchIndexValidationService"),
		searchQueryHistoryClient:      searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		searchQueryNotificationClient: searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		searchIndexNotificationClient: searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
	}
}

// ValidateSearchIndex validates a search index.
func (s *SearchIndexValidationServer) ValidateSearchIndex(ctx context.Context, req *searchv1.ValidateSearchIndexRequest) (*searchv1.ValidateSearchIndexResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ValidateSearchIndex", "error", err)
		return nil, err
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "validate search index", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.ValidateSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Specs:         req.GetSearchIndex().GetSpecs(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewSearchIndex previews a search index.
func (s *SearchIndexValidationServer) PreviewSearchIndex(ctx context.Context, req *searchv1.PreviewSearchIndexRequest) (*searchv1.PreviewSearchIndexResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "preview search index", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	listSearchQueryVersionsRequest := &searchv1.ListSearchQueryVersionsRequest{}
	listSearchQueryVersionsRequest.Context = req.GetContext()
	listSearchQueryVersionsRequest.Filter = &searchv1.ListSearchQueryVersionsRequest_Filter{
		Region:       req.GetSearchIndex().GetAddress().GetRegion(),
		CreatedAfter: req.GetSearchIndex().GetAudit().GetCreatedAt(),
		OwnerId:      req.GetSearchIndex().GetComponents()["primary"].GetComponentId(),
		Labels:       req.GetSearchIndex().GetSpecs(),
	}
	listSearchQueryVersionsReply, err := s.searchQueryHistoryClient.ListSearchQueryVersions(ctx, listSearchQueryVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/ListSearchQueryVersions")
	}
	return &searchv1.PreviewSearchIndexResponse{
		Result: &commonv1.OperationResult{
			Status: listSearchQueryVersionsReply.GetResult().GetStatus(),
		},
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Status:        req.GetSearchIndex().GetStatus(),
		},
	}, nil
}

// CheckSearchIndexEligibility checks a search index.
func (s *SearchIndexValidationServer) CheckSearchIndexEligibility(ctx context.Context, req *searchv1.CheckSearchIndexEligibilityRequest) (*searchv1.CheckSearchIndexEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "check search index eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.CheckSearchIndexEligibilityResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Name:          req.GetSearchIndex().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeSearchIndex normalizes a search index.
func (s *SearchIndexValidationServer) NormalizeSearchIndex(ctx context.Context, req *searchv1.NormalizeSearchIndexRequest) (*searchv1.NormalizeSearchIndexResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "normalize search index", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	var holder struct{ Email string }
	if record := req.GetDetails().GetContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	streamSearchQueryNoticesStream, err := s.searchQueryNotificationClient.StreamSearchQueryNotices(ctx, &searchv1.StreamSearchQueryNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
			Email:            holder.Email,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
	}
	for {
		received, err := streamSearchQueryNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
		}
		_ = received
	}
	return &searchv1.NormalizeSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Name:          req.GetSearchIndex().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainSearchIndexDecision explains a search index.
func (s *SearchIndexValidationServer) ExplainSearchIndexDecision(ctx context.Context, req *searchv1.ExplainSearchIndexDecisionRequest) (*searchv1.ExplainSearchIndexDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "explain search index decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetSearchIndex())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeSearchIndexUpdatesRequest := newUnsubscribeSearchIndexUpdatesRequest(withUnsubscribeSearchIndexUpdatesContext(req.GetContext()), withUnsubscribeSearchIndexUpdatesDetails(&searchv1.UnsubscribeSearchIndexUpdatesRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Email:            req.GetDetails().GetContact().GetEmail(),
			Phone:            req.GetDetails().GetContact().GetPhone(),
			PreferredChannel: req.GetDetails().GetContact().GetPreferredChannel(),
		},
		Payload: packed,
	}), withUnsubscribeSearchIndexUpdatesSearchIndexId(req.GetSearchIndexId()))
	unsubscribeSearchIndexUpdatesReply, err := s.searchIndexNotificationClient.UnsubscribeSearchIndexUpdates(ctx, unsubscribeSearchIndexUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/UnsubscribeSearchIndexUpdates")
	}
	return &searchv1.ExplainSearchIndexDecisionResponse{
		SearchIndex: &searchv1.SearchIndex{
			Components: map[string]*searchv1.SearchIndexComponent{"primary": &searchv1.SearchIndexComponent{
				Description: unsubscribeSearchIndexUpdatesReply.GetSearchIndex().GetComponents()["primary"].GetDescription(),
			}},
			Owner:         unsubscribeSearchIndexUpdatesReply.GetSearchIndex().GetOwner(),
			SearchIndexId: req.GetSearchIndexId(),
			Code:          req.GetSearchIndex().GetCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreSearchIndex scores a search index.
func (s *SearchIndexValidationServer) ScoreSearchIndex(ctx context.Context, req *searchv1.ScoreSearchIndexRequest) (*searchv1.ScoreSearchIndexResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "score search index", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.ScoreSearchIndexResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Code:          req.GetSearchIndex().GetCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintSearchIndexes lints a search index.
func (s *SearchIndexValidationServer) LintSearchIndexes(ctx context.Context, req *searchv1.LintSearchIndexesRequest) (*searchv1.LintSearchIndexesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSearchIndex(ctx, req.GetContext().GetTenantId(), req.GetSearchIndex()); err != nil {
		return nil, grpcx.Wrap(err, "save search index")
	}
	s.log.InfoContext(ctx, "lint search indexes", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSearchIndex().GetAudit().GetEtag())
	return &searchv1.LintSearchIndexesResponse{
		SearchIndex: &searchv1.SearchIndex{
			SearchIndexId: req.GetSearchIndexId(),
			Name:          req.GetSearchIndex().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSearchIndexRules returns a search index.
func (s *SearchIndexValidationServer) GetSearchIndexRules(ctx context.Context, req *searchv1.GetSearchIndexRulesRequest) (*searchv1.GetSearchIndexRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "search_index", req.GetSearchIndexId(), "get_search_index_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get search index rules", "tenant", req.GetContext().GetTenantId())
	return &searchv1.GetSearchIndexRulesResponse{
		SearchIndex: &searchv1.SearchIndex{SearchIndexId: req.GetSearchIndexId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}
