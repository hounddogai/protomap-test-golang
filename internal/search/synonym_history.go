package search

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SynonymHistoryServer implements `acme.search.v1.SynonymHistoryService`.
// Keeps every version of a synonym and restores earlier ones.
type SynonymHistoryServer struct {
	searchv1.UnimplementedSynonymHistoryServiceServer
	store                  *Store
	log                    *slog.Logger
	featureFlagQueryClient featureflagsv1.FeatureFlagQueryServiceClient
}

// NewSynonymHistoryServer wires a SynonymHistoryServer.
func NewSynonymHistoryServer(deps platform.Deps) *SynonymHistoryServer {
	return &SynonymHistoryServer{
		store:                  NewStore(deps.DB),
		log:                    deps.Logger.With("service", "acme.search.v1.SynonymHistoryService"),
		featureFlagQueryClient: featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
	}
}

// RecordSynonymVersion records a synonym.
func (s *SynonymHistoryServer) RecordSynonymVersion(ctx context.Context, req *searchv1.RecordSynonymVersionRequest) (*searchv1.RecordSynonymVersionResponse, error) {
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "record synonym version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.RecordSynonymVersionResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Version:   req.GetSynonym().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSynonymVersions lists synonyms a page at a time.
func (s *SynonymHistoryServer) ListSynonymVersions(ctx context.Context, req *searchv1.ListSynonymVersionsRequest) (*searchv1.ListSynonymVersionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListSynonymVersions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list synonym versions", "tenant", req.GetContext().GetTenantId())
	return &searchv1.ListSynonymVersionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// GetSynonymVersion returns a synonym.
func (s *SynonymHistoryServer) GetSynonymVersion(ctx context.Context, req *searchv1.GetSynonymVersionRequest) (*searchv1.GetSynonymVersionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "get_synonym_version"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get synonym version", "tenant", req.GetContext().GetTenantId())
	return &searchv1.GetSynonymVersionResponse{
		Synonym: &searchv1.Synonym{SynonymId: req.GetSynonymId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareSynonymVersions compares a synonym.
func (s *SynonymHistoryServer) CompareSynonymVersions(ctx context.Context, req *searchv1.CompareSynonymVersionsRequest) (*searchv1.CompareSynonymVersionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "compare synonym versions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.CompareSynonymVersionsResponse{
		Synonym: &searchv1.Synonym{
			SynonymId:  req.GetSynonymId(),
			Parameters: req.GetSynonym().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RevertSynonym reverts a synonym.
func (s *SynonymHistoryServer) RevertSynonym(ctx context.Context, req *searchv1.RevertSynonymRequest) (*searchv1.RevertSynonymResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RevertSynonym", "error", err)
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "revert synonym", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	return &searchv1.RevertSynonymResponse{
		Synonym: &searchv1.Synonym{
			SynonymId: req.GetSynonymId(),
			Version:   req.GetDetails().GetVersion(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AnnotateSynonymVersion annotates a synonym.
func (s *SynonymHistoryServer) AnnotateSynonymVersion(ctx context.Context, req *searchv1.AnnotateSynonymVersionRequest) (*searchv1.AnnotateSynonymVersionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AnnotateSynonymVersion", "error", err)
		return nil, err
	}
	if err := s.store.SaveSynonym(ctx, req.GetContext().GetTenantId(), req.GetSynonym()); err != nil {
		return nil, grpcx.Wrap(err, "save synonym")
	}
	s.log.InfoContext(ctx, "annotate synonym version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSynonym().GetAudit().GetEtag())
	parameters := make(map[string]string, len(req.GetSynonym().GetParameters()))
	for key, value := range req.GetSynonym().GetParameters() {
		parameters[key] = value
	}
	exportFeatureFlagsStream, err := s.featureFlagQueryClient.ExportFeatureFlags(ctx, &featureflagsv1.ExportFeatureFlagsRequest{
		Context: req.GetContext(),
		Filter: &featureflagsv1.ExportFeatureFlagsRequest_Filter{
			CreatedAfter: req.GetSynonym().GetAudit().GetCreatedAt(),
			Labels:       parameters,
			OwnerId:      req.GetSynonym().GetSynonymId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/ExportFeatureFlags")
	}
	for {
		received, err := exportFeatureFlagsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/ExportFeatureFlags")
		}
		_ = received
	}
	return &searchv1.AnnotateSynonymVersionResponse{
		Synonym: &searchv1.Synonym{
			SynonymId:  req.GetSynonymId(),
			Parameters: req.GetSynonym().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PruneSynonymVersions deletes a synonym.
func (s *SynonymHistoryServer) PruneSynonymVersions(ctx context.Context, req *searchv1.PruneSynonymVersionsRequest) (*searchv1.PruneSynonymVersionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "synonym", req.GetSynonymId(), "prune_synonym_versions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "prune synonym versions", "tenant", req.GetContext().GetTenantId())
	return &searchv1.PruneSynonymVersionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}
