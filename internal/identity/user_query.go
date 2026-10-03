package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc"
)

// UserQueryServer implements `acme.identity.v1.UserQueryService`.
// Answers read-only questions about users, for screens and reports.
type UserQueryServer struct {
	identityv1.UnimplementedUserQueryServiceServer
	store                *Store
	log                  *slog.Logger
	userValidationClient identityv1.UserValidationServiceClient
}

// NewUserQueryServer wires a UserQueryServer.
func NewUserQueryServer(deps platform.Deps) *UserQueryServer {
	return &UserQueryServer{
		store:                NewStore(deps.DB),
		log:                  deps.Logger.With("service", "acme.identity.v1.UserQueryService"),
		userValidationClient: identityv1.NewUserValidationServiceClient(deps.Conn),
	}
}

// LookupUser returns a user.
func (s *UserQueryServer) LookupUser(ctx context.Context, req *identityv1.LookupUserRequest) (*identityv1.LookupUserResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LookupUser", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "user", req.GetUserId(), "lookup_user"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup user", "tenant", req.GetContext().GetTenantId())
	return &identityv1.LookupUserResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseUsers lists users a page at a time.
func (s *UserQueryServer) BrowseUsers(ctx context.Context, req *identityv1.BrowseUsersRequest) (*identityv1.BrowseUsersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "browse users", "tenant", req.GetContext().GetTenantId())
	return &identityv1.BrowseUsersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindUsers lists users a page at a time.
func (s *UserQueryServer) FindUsers(ctx context.Context, req *identityv1.FindUsersRequest) (*identityv1.FindUsersResponse, error) {
	s.log.InfoContext(ctx, "find users", "tenant", req.GetContext().GetTenantId())
	return &identityv1.FindUsersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountUsers counts users by status.
func (s *UserQueryServer) CountUsers(ctx context.Context, req *identityv1.CountUsersRequest) (*identityv1.CountUsersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CountUsers", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "count users", "tenant", req.GetContext().GetTenantId())
	return &identityv1.CountUsersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolveUsers returns several users at once.
func (s *UserQueryServer) ResolveUsers(ctx context.Context, req *identityv1.ResolveUsersRequest) (*identityv1.ResolveUsersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve users", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ResolveUsersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ExportUsers streams every matching user.
func (s *UserQueryServer) ExportUsers(req *identityv1.ExportUsersRequest, stream grpc.ServerStreamingServer[identityv1.ExportUsersResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export users", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&identityv1.ExportUsersResponse{
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SuggestUsers lists users a page at a time.
func (s *UserQueryServer) SuggestUsers(ctx context.Context, req *identityv1.SuggestUsersRequest) (*identityv1.SuggestUsersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest users", "tenant", req.GetContext().GetTenantId())
	checkUserEligibilityRequest := &identityv1.CheckUserEligibilityRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress:      &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			RelationshipManager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
		},
		Details: &identityv1.CheckUserEligibilityRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	var checkUserEligibilityReply *identityv1.CheckUserEligibilityResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.UserValidationService/CheckUserEligibility", func(ctx context.Context) error {
		answer, err := s.userValidationClient.CheckUserEligibility(ctx, checkUserEligibilityRequest)
		checkUserEligibilityReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/CheckUserEligibility")
	}
	return &identityv1.SuggestUsersResponse{
		Users: []*identityv1.User{&identityv1.User{
			Contact: checkUserEligibilityReply.GetUser().GetContact(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkUserEligibilityReply.GetUser().GetAudit().GetUpdatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PeekUser returns a user.
func (s *UserQueryServer) PeekUser(ctx context.Context, req *identityv1.PeekUserRequest) (*identityv1.PeekUserResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "user", req.GetUserId(), "peek_user"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek user", "tenant", req.GetContext().GetTenantId())
	return &identityv1.PeekUserResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
