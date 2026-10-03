package identity

import (
	"context"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SessionHistoryServer implements `acme.identity.v1.SessionHistoryService`.
// Keeps every version of a session and restores earlier ones.
type SessionHistoryServer struct {
	identityv1.UnimplementedSessionHistoryServiceServer
	store                        *Store
	log                          *slog.Logger
	accessPolicyValidationClient authorizationv1.AccessPolicyValidationServiceClient
}

// NewSessionHistoryServer wires a SessionHistoryServer.
func NewSessionHistoryServer(deps platform.Deps) *SessionHistoryServer {
	return &SessionHistoryServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.identity.v1.SessionHistoryService"),
		accessPolicyValidationClient: authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
	}
}

// RecordSessionVersion records a session.
func (s *SessionHistoryServer) RecordSessionVersion(ctx context.Context, req *identityv1.RecordSessionVersionRequest) (*identityv1.RecordSessionVersionResponse, error) {
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "record session version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	return &identityv1.RecordSessionVersionResponse{
		Session: &identityv1.Session{
			SessionId:    req.GetSessionId(),
			CustomFields: req.GetSession().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListSessionVersions lists sessions a page at a time.
func (s *SessionHistoryServer) ListSessionVersions(ctx context.Context, req *identityv1.ListSessionVersionsRequest) (*identityv1.ListSessionVersionsResponse, error) {
	s.log.InfoContext(ctx, "list session versions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ListSessionVersionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// GetSessionVersion returns a session.
func (s *SessionHistoryServer) GetSessionVersion(ctx context.Context, req *identityv1.GetSessionVersionRequest) (*identityv1.GetSessionVersionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "get_session_version"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get session version", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetSessionVersionResponse{
		Session: &identityv1.Session{SessionId: req.GetSessionId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareSessionVersions compares a session.
func (s *SessionHistoryServer) CompareSessionVersions(ctx context.Context, req *identityv1.CompareSessionVersionsRequest) (*identityv1.CompareSessionVersionsResponse, error) {
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "compare session versions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	return &identityv1.CompareSessionVersionsResponse{
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Assignee:  req.GetSession().GetAssignee(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RevertSession reverts a session.
func (s *SessionHistoryServer) RevertSession(ctx context.Context, req *identityv1.RevertSessionRequest) (*identityv1.RevertSessionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RevertSession", "error", err)
		return nil, err
	}
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "revert session", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	return &identityv1.RevertSessionResponse{
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Assignee:  req.GetSession().GetAssignee(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AnnotateSessionVersion annotates a session.
func (s *SessionHistoryServer) AnnotateSessionVersion(ctx context.Context, req *identityv1.AnnotateSessionVersionRequest) (*identityv1.AnnotateSessionVersionResponse, error) {
	if err := s.store.SaveSession(ctx, req.GetContext().GetTenantId(), req.GetSession()); err != nil {
		return nil, grpcx.Wrap(err, "save session")
	}
	s.log.InfoContext(ctx, "annotate session version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetSession().GetAudit().GetEtag())
	return &identityv1.AnnotateSessionVersionResponse{
		Session: &identityv1.Session{
			SessionId:    req.GetSessionId(),
			CustomFields: req.GetSession().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PruneSessionVersions deletes a session.
func (s *SessionHistoryServer) PruneSessionVersions(ctx context.Context, req *identityv1.PruneSessionVersionsRequest) (*identityv1.PruneSessionVersionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "prune_session_versions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "prune session versions", "tenant", req.GetContext().GetTenantId())
	lintAccessPoliciesReply, err := s.accessPolicyValidationClient.LintAccessPolicies(ctx, &authorizationv1.LintAccessPoliciesRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Audit: &commonv1.AuditInfo{Etag: req.GetEtag()},
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
		},
		Reason: req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/LintAccessPolicies")
	}
	return &identityv1.PruneSessionVersionsResponse{
		Result: &commonv1.OperationResult{Status: lintAccessPoliciesReply.GetOutcome().GetStatus()},
	}, nil
}
