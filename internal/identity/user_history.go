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
	"google.golang.org/protobuf/encoding/protojson"
)

// UserHistoryServer implements `acme.identity.v1.UserHistoryService`.
// Keeps every version of a user and restores earlier ones.
type UserHistoryServer struct {
	identityv1.UnimplementedUserHistoryServiceServer
	store                     *Store
	log                       *slog.Logger
	apiCredentialAdminPort    apiCredentialAdminPort
	meteredApiCredentialAdmin meteredApiCredentialAdmin
}

// NewUserHistoryServer wires a UserHistoryServer.
func NewUserHistoryServer(deps platform.Deps) *UserHistoryServer {
	return &UserHistoryServer{
		store:                     NewStore(deps.DB),
		log:                       deps.Logger.With("service", "acme.identity.v1.UserHistoryService"),
		apiCredentialAdminPort:    newApiCredentialAdminPort(deps),
		meteredApiCredentialAdmin: meteredApiCredentialAdmin{ApiCredentialAdminServiceClient: identityv1.NewApiCredentialAdminServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// RecordUserVersion records a user.
func (s *UserHistoryServer) RecordUserVersion(ctx context.Context, req *identityv1.RecordUserVersionRequest) (*identityv1.RecordUserVersionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecordUserVersion", "error", err)
		return nil, err
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "record user version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.RecordUserVersionResponse{
		User: &identityv1.User{
			UserId:              req.GetUserId(),
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListUserVersions lists users a page at a time.
func (s *UserHistoryServer) ListUserVersions(ctx context.Context, req *identityv1.ListUserVersionsRequest) (*identityv1.ListUserVersionsResponse, error) {
	s.log.InfoContext(ctx, "list user versions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ListUserVersionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// GetUserVersion returns a user.
func (s *UserHistoryServer) GetUserVersion(ctx context.Context, req *identityv1.GetUserVersionRequest) (*identityv1.GetUserVersionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "user", req.GetUserId(), "get_user_version"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get user version", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairApiCredentialRequest := &identityv1.RepairApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
		Details: &identityv1.RepairApiCredentialRequest_Details{Payload: snapshot},
	}
	repairApiCredentialReply, err := s.apiCredentialAdminPort.RepairApiCredential(ctx, repairApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialAdminService/RepairApiCredential")
	}
	return &identityv1.GetUserVersionResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{
				Email:  repairApiCredentialReply.GetApiCredential().GetContact().GetEmail(),
				UserId: req.GetUserId(),
			},
			UserId: req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RevertUser reverts a user.
func (s *UserHistoryServer) RevertUser(ctx context.Context, req *identityv1.RevertUserRequest) (*identityv1.RevertUserResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RevertUser", "error", err)
		return nil, err
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "revert user", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.RevertUserResponse{
		User: &identityv1.User{
			UserId:              req.GetUserId(),
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AnnotateUserVersion annotates a user.
func (s *UserHistoryServer) AnnotateUserVersion(ctx context.Context, req *identityv1.AnnotateUserVersionRequest) (*identityv1.AnnotateUserVersionResponse, error) {
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "annotate user version", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.AnnotateUserVersionResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PruneUserVersions deletes a user.
func (s *UserHistoryServer) PruneUserVersions(ctx context.Context, req *identityv1.PruneUserVersionsRequest) (*identityv1.PruneUserVersionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "user", req.GetUserId(), "prune_user_versions"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "prune user versions", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairApiCredentialRequest := &identityv1.RepairApiCredentialRequest{
		Context:       req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
		Details:       &identityv1.RepairApiCredentialRequest_Details{Payload: snapshot},
	}
	repairApiCredentialReply, err := s.meteredApiCredentialAdmin.RepairApiCredential(ctx, repairApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialAdminService/RepairApiCredential")
	}
	return &identityv1.PruneUserVersionsResponse{
		Result: &commonv1.OperationResult{
			Message: repairApiCredentialReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}
