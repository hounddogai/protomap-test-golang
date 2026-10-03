package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UserValidationServer implements `acme.identity.v1.UserValidationService`.
// Checks users against the rules before anything acts on them.
type UserValidationServer struct {
	identityv1.UnimplementedUserValidationServiceServer
	store *Store
	log   *slog.Logger
}

// NewUserValidationServer wires a UserValidationServer.
func NewUserValidationServer(deps platform.Deps) *UserValidationServer {
	return &UserValidationServer{
		store: NewStore(deps.DB),
		log:   deps.Logger.With("service", "acme.identity.v1.UserValidationService"),
	}
}

// ValidateUser validates a user.
func (s *UserValidationServer) ValidateUser(ctx context.Context, req *identityv1.ValidateUserRequest) (*identityv1.ValidateUserResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "validate user", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.ValidateUserResponse{
		User: &identityv1.User{
			UserId:              req.GetUserId(),
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewUser previews a user.
func (s *UserValidationServer) PreviewUser(ctx context.Context, req *identityv1.PreviewUserRequest) (*identityv1.PreviewUserResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "preview user", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.PreviewUserResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckUserEligibility checks a user.
func (s *UserValidationServer) CheckUserEligibility(ctx context.Context, req *identityv1.CheckUserEligibilityRequest) (*identityv1.CheckUserEligibilityResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "check user eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.CheckUserEligibilityResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeUser normalizes a user.
func (s *UserValidationServer) NormalizeUser(ctx context.Context, req *identityv1.NormalizeUserRequest) (*identityv1.NormalizeUserResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected NormalizeUser", "error", err)
		return nil, err
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "normalize user", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.NormalizeUserResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainUserDecision explains a user.
func (s *UserValidationServer) ExplainUserDecision(ctx context.Context, req *identityv1.ExplainUserDecisionRequest) (*identityv1.ExplainUserDecisionResponse, error) {
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "explain user decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.ExplainUserDecisionResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScoreUser scores a user.
func (s *UserValidationServer) ScoreUser(ctx context.Context, req *identityv1.ScoreUserRequest) (*identityv1.ScoreUserResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "score user", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.ScoreUserResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintUsers lints a user.
func (s *UserValidationServer) LintUsers(ctx context.Context, req *identityv1.LintUsersRequest) (*identityv1.LintUsersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "lint users", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.LintUsersResponse{
		User: &identityv1.User{
			UserId:              req.GetUserId(),
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetUserRules returns a user.
func (s *UserValidationServer) GetUserRules(ctx context.Context, req *identityv1.GetUserRulesRequest) (*identityv1.GetUserRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "user", req.GetUserId(), "get_user_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get user rules", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetUserRulesResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
