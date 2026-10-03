package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// UserServer implements `acme.identity.v1.UserService`.
// Manages users: creates, reads, changes, and lists them.
type UserServer struct {
	identityv1.UnsafeUserServiceServer
	store                         *Store
	log                           *slog.Logger
	searchIndexHistoryClient      searchv1.SearchIndexHistoryServiceClient
	venueValidationClient         venuesv1.VenueValidationServiceClient
	apiCredentialValidationClient identityv1.ApiCredentialValidationServiceClient
}

// NewUserServer wires a UserServer.
func NewUserServer(deps platform.Deps) *UserServer {
	return &UserServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.identity.v1.UserService"),
		searchIndexHistoryClient:      searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		venueValidationClient:         venuesv1.NewVenueValidationServiceClient(deps.Conn),
		apiCredentialValidationClient: identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
	}
}

// CreateUser creates a user.
func (s *UserServer) CreateUser(ctx context.Context, req *identityv1.CreateUserRequest) (*identityv1.CreateUserResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CreateUser", "error", err)
		return nil, err
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "create user", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.CreateUserResponse{
		User: &identityv1.User{
			Name:   req.GetUser().GetName(),
			UserId: req.GetUser().GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetUser returns a user.
func (s *UserServer) GetUser(ctx context.Context, req *identityv1.GetUserRequest) (*identityv1.GetUserResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "user", req.GetUserId(), "get_user"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get user", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetUserResponse{
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetUserId()},
			UserId:              req.GetUserId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdateUser changes a user.
func (s *UserServer) UpdateUser(ctx context.Context, req *identityv1.UpdateUserRequest) (*identityv1.UpdateUserResponse, error) {
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "update user", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetUser())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareSearchIndexVersionsRequest := newCompareSearchIndexVersionsRequest(withCompareSearchIndexVersionsContext(req.GetContext()), withCompareSearchIndexVersionsSearchIndex(&searchv1.SearchIndex{
		Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{Recipient: req.GetUser().GetPrimaryAddress().GetRecipient()}},
	}), withCompareSearchIndexVersionsDetails(&searchv1.CompareSearchIndexVersionsRequest_Details{Snapshot: snapshot}))
	compareSearchIndexVersionsReply, err := s.searchIndexHistoryClient.CompareSearchIndexVersions(ctx, compareSearchIndexVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions")
	}
	return &identityv1.UpdateUserResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: compareSearchIndexVersionsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{
				Email: compareSearchIndexVersionsReply.GetSearchIndex().GetOwner().GetEmail(),
			},
			RelationshipManager: req.GetUser().GetRelationshipManager(),
			Name:                req.GetUser().GetName(),
		},
	}, nil
}

// ListUsers lists users a page at a time.
func (s *UserServer) ListUsers(ctx context.Context, req *identityv1.ListUsersRequest) (*identityv1.ListUsersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list users", "tenant", req.GetContext().GetTenantId())
	step := &userServerListUsersScoreVenueStep{request: &venuesv1.ScoreVenueRequest{
		Context: req.GetContext(),
		Details: &venuesv1.ScoreVenueRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	}}
	if err := s.runListUsersScoreVenue(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/ScoreVenue")
	}
	scoreVenueReply := step.reply
	return &identityv1.ListUsersResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scoreVenueReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// BatchGetUsers returns several users at once.
func (s *UserServer) BatchGetUsers(ctx context.Context, req *identityv1.BatchGetUsersRequest) (*identityv1.BatchGetUsersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BatchGetUsers", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "batch get users", "tenant", req.GetContext().GetTenantId())
	return &identityv1.BatchGetUsersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SearchUsers lists users a page at a time.
func (s *UserServer) SearchUsers(ctx context.Context, req *identityv1.SearchUsersRequest) (*identityv1.SearchUsersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SearchUsers", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "search users", "tenant", req.GetContext().GetTenantId())
	return &identityv1.SearchUsersResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// WatchUsers streams the changes of users as they happen.
func (s *UserServer) WatchUsers(req *identityv1.WatchUsersRequest, stream grpc.ServerStreamingServer[identityv1.WatchUsersResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "watch users", "tenant", req.GetContext().GetTenantId())
	explainApiCredentialDecisionReply, err := s.apiCredentialValidationClient.ExplainApiCredentialDecision(ctx, &identityv1.ExplainApiCredentialDecisionRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetSubscriber().GetName(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/ExplainApiCredentialDecision")
	}
	if err := stream.Send(&identityv1.WatchUsersResponse{
		User: &identityv1.User{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainApiCredentialDecisionReply.GetApiCredential().GetAudit().GetCreatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Email: explainApiCredentialDecisionReply.GetApiCredential().GetContact().GetEmail(),
				Name:  req.GetSubscriber().GetName(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// CloneUser creates a user.
func (s *UserServer) CloneUser(ctx context.Context, req *identityv1.CloneUserRequest) (*identityv1.CloneUserResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveUser(ctx, req.GetContext().GetTenantId(), req.GetUser()); err != nil {
		return nil, grpcx.Wrap(err, "save user")
	}
	s.log.InfoContext(ctx, "clone user", "tenant", req.GetContext().GetTenantId(), "etag", req.GetUser().GetAudit().GetEtag())
	return &identityv1.CloneUserResponse{
		User: &identityv1.User{
			UserId: req.GetUser().GetUserId(),
			Name:   req.GetUser().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// userServerListUsersScoreVenueStep carries the request of one step of ListUsers and, once it ran, its reply.
type userServerListUsersScoreVenueStep struct {
	request *venuesv1.ScoreVenueRequest
	reply   *venuesv1.ScoreVenueResponse
}

func (s *UserServer) runListUsersScoreVenue(ctx context.Context, step *userServerListUsersScoreVenueStep) error {
	reply, err := s.venueValidationClient.ScoreVenue(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
