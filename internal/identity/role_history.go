package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RoleHistoryServer implements `acme.identity.v1.RoleHistoryService`.
// Keeps every version of a role and restores earlier ones.
type RoleHistoryServer struct {
	identityv1.UnimplementedRoleHistoryServiceServer
	store                   *Store
	log                     *slog.Logger
	meteredTenantAdmin      meteredTenantAdmin
	sessionValidationClient identityv1.SessionValidationServiceClient
}

// NewRoleHistoryServer wires a RoleHistoryServer.
func NewRoleHistoryServer(deps platform.Deps) RoleHistoryServer {
	return RoleHistoryServer{
		store:                   NewStore(deps.DB),
		log:                     deps.Logger.With("service", "acme.identity.v1.RoleHistoryService"),
		meteredTenantAdmin:      meteredTenantAdmin{TenantAdminServiceClient: identityv1.NewTenantAdminServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		sessionValidationClient: identityv1.NewSessionValidationServiceClient(deps.Conn),
	}
}

// RecordRoleVersion records a role.
func (s RoleHistoryServer) RecordRoleVersion(ctx context.Context, req *identityv1.RecordRoleVersionRequest) (*identityv1.RecordRoleVersionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRole(ctx, req.GetContext().GetTenantId(), req.GetRole()); err != nil {
		return nil, grpcx.Wrap(err, "save role")
	}
	s.log.InfoContext(ctx, "record role version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRole().GetAudit().GetEtag())
	inspectTenantRequest := &identityv1.InspectTenantRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	inspectTenantReply, err := s.meteredTenantAdmin.InspectTenant(ctx, inspectTenantRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantAdminService/InspectTenant")
	}
	return &identityv1.RecordRoleVersionResponse{
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				UserId: inspectTenantReply.GetTenant().GetManager().GetUserId(),
			},
			RoleId:   req.GetRoleId(),
			Priority: req.GetRole().GetPriority(),
		},
		Outcome: &identityv1.RecordRoleVersionResponse_Outcome{Message: inspectTenantReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListRoleVersions lists roles a page at a time.
func (s RoleHistoryServer) ListRoleVersions(ctx context.Context, req *identityv1.ListRoleVersionsRequest) (*identityv1.ListRoleVersionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list role versions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ListRoleVersionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// GetRoleVersion returns a role.
func (s RoleHistoryServer) GetRoleVersion(ctx context.Context, req *identityv1.GetRoleVersionRequest) (*identityv1.GetRoleVersionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "get_role_version"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get role version", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetRoleVersionResponse{
		Role:   &identityv1.Role{RoleId: req.GetRoleId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareRoleVersions compares a role.
func (s RoleHistoryServer) CompareRoleVersions(ctx context.Context, req *identityv1.CompareRoleVersionsRequest) (*identityv1.CompareRoleVersionsResponse, error) {
	if err := s.store.SaveRole(ctx, req.GetContext().GetTenantId(), req.GetRole()); err != nil {
		return nil, grpcx.Wrap(err, "save role")
	}
	s.log.InfoContext(ctx, "compare role versions", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRole().GetAudit().GetEtag())
	return &identityv1.CompareRoleVersionsResponse{
		Role: &identityv1.Role{
			RoleId: req.GetRoleId(),
			Owner:  req.GetRole().GetOwner(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RevertRole reverts a role.
func (s RoleHistoryServer) RevertRole(ctx context.Context, req *identityv1.RevertRoleRequest) (*identityv1.RevertRoleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RevertRole", "error", err)
		return nil, err
	}
	if err := s.store.SaveRole(ctx, req.GetContext().GetTenantId(), req.GetRole()); err != nil {
		return nil, grpcx.Wrap(err, "save role")
	}
	s.log.InfoContext(ctx, "revert role", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRole().GetAudit().GetEtag())
	checkSessionEligibilityReply, err := s.sessionValidationClient.CheckSessionEligibility(ctx, &identityv1.CheckSessionEligibilityRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Comments: []*identityv1.SessionComment{&identityv1.SessionComment{CreatedAt: req.GetRole().GetAudit().GetCreatedAt()}},
			Audit:    &commonv1.AuditInfo{UpdatedBy: req.GetRole().GetAudit().GetUpdatedBy()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/CheckSessionEligibility")
	}
	return &identityv1.RevertRoleResponse{
		Outcome: &identityv1.RevertRoleResponse_Outcome{
			ReferenceId: checkSessionEligibilityReply.GetOutcome().GetReferenceId(),
		},
		Role: &identityv1.Role{
			RoleId:    req.GetRoleId(),
			Effective: req.GetRole().GetEffective(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AnnotateRoleVersion annotates a role.
func (s RoleHistoryServer) AnnotateRoleVersion(ctx context.Context, req *identityv1.AnnotateRoleVersionRequest) (*identityv1.AnnotateRoleVersionResponse, error) {
	if err := s.store.SaveRole(ctx, req.GetContext().GetTenantId(), req.GetRole()); err != nil {
		return nil, grpcx.Wrap(err, "save role")
	}
	s.log.InfoContext(ctx, "annotate role version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRole().GetAudit().GetEtag())
	return &identityv1.AnnotateRoleVersionResponse{
		Role: &identityv1.Role{
			RoleId: req.GetRoleId(),
			Name:   req.GetRole().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PruneRoleVersions deletes a role.
func (s RoleHistoryServer) PruneRoleVersions(ctx context.Context, req *identityv1.PruneRoleVersionsRequest) (*identityv1.PruneRoleVersionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "prune_role_versions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "prune role versions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.PruneRoleVersionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}
