package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ApiCredentialNotificationServer implements `acme.identity.v1.ApiCredentialNotificationService`.
// Tells the people who follow an api credential what changed.
type ApiCredentialNotificationServer struct {
	identityv1.UnimplementedApiCredentialNotificationServiceServer
	store                    *Store
	log                      *slog.Logger
	userHistoryClient        identityv1.UserHistoryServiceClient
	conn                     *grpc.ClientConn
	apiCredentialAdminClient identityv1.ApiCredentialAdminServiceClient
}

// NewApiCredentialNotificationServer wires an ApiCredentialNotificationServer.
func NewApiCredentialNotificationServer(deps platform.Deps) *ApiCredentialNotificationServer {
	return &ApiCredentialNotificationServer{
		store:                    NewStore(deps.DB),
		log:                      deps.Logger.With("service", "acme.identity.v1.ApiCredentialNotificationService"),
		userHistoryClient:        identityv1.NewUserHistoryServiceClient(deps.Conn),
		conn:                     deps.Conn,
		apiCredentialAdminClient: identityv1.NewApiCredentialAdminServiceClient(deps.Conn),
	}
}

// SubscribeApiCredentialUpdates subscribes an api credential.
func (s *ApiCredentialNotificationServer) SubscribeApiCredentialUpdates(ctx context.Context, req *identityv1.SubscribeApiCredentialUpdatesRequest) (*identityv1.SubscribeApiCredentialUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubscribeApiCredentialUpdates", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "subscribe_api_credential_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe api credential updates", "tenant", req.GetContext().GetTenantId())
	pruneUserVersionsRequest := &identityv1.PruneUserVersionsRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
		Reason:  req.GetReason(),
	}
	type pruneUserVersionsResult struct {
		reply *identityv1.PruneUserVersionsResponse
		err   error
	}
	pruneUserVersionsResults := make(chan pruneUserVersionsResult, 1)
	go func() {
		reply, err := s.userHistoryClient.PruneUserVersions(ctx, pruneUserVersionsRequest)
		pruneUserVersionsResults <- pruneUserVersionsResult{reply, err}
	}()
	var pruneUserVersionsReply *identityv1.PruneUserVersionsResponse
	select {
	case result := <-pruneUserVersionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.identity.v1.UserHistoryService/PruneUserVersions")
		}
		pruneUserVersionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &identityv1.SubscribeApiCredentialUpdatesResponse{
		Result: &commonv1.OperationResult{Status: pruneUserVersionsReply.GetResult().GetStatus()},
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Contact:         &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
	}, nil
}

// UnsubscribeApiCredentialUpdates unsubscribes an api credential.
func (s *ApiCredentialNotificationServer) UnsubscribeApiCredentialUpdates(ctx context.Context, req *identityv1.UnsubscribeApiCredentialUpdatesRequest) (*identityv1.UnsubscribeApiCredentialUpdatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "unsubscribe_api_credential_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe api credential updates", "tenant", req.GetContext().GetTenantId())
	return &identityv1.UnsubscribeApiCredentialUpdatesResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Contact:         &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NotifyApiCredentialWatchers notifys an api credential.
func (s *ApiCredentialNotificationServer) NotifyApiCredentialWatchers(ctx context.Context, req *identityv1.NotifyApiCredentialWatchersRequest) (*identityv1.NotifyApiCredentialWatchersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "notify_api_credential_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify api credential watchers", "tenant", req.GetContext().GetTenantId())
	return &identityv1.NotifyApiCredentialWatchersResponse{
		ApiCredential: &identityv1.ApiCredential{
			ApiCredentialId: req.GetApiCredentialId(),
			Contact:         &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListApiCredentialSubscriptions lists api credentials a page at a time.
func (s *ApiCredentialNotificationServer) ListApiCredentialSubscriptions(ctx context.Context, req *identityv1.ListApiCredentialSubscriptionsRequest) (*identityv1.ListApiCredentialSubscriptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list api credential subscriptions", "tenant", req.GetContext().GetTenantId())
	revertUserRequest := &identityv1.RevertUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
	}
	revertUserReply := new(identityv1.RevertUserResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.UserHistoryService_RevertUser_FullMethodName, revertUserRequest, revertUserReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RevertUser")
	}
	return &identityv1.ListApiCredentialSubscriptionsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: revertUserReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SendApiCredentialTestNotice sends an api credential.
func (s *ApiCredentialNotificationServer) SendApiCredentialTestNotice(ctx context.Context, req *identityv1.SendApiCredentialTestNoticeRequest) (*identityv1.SendApiCredentialTestNoticeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SendApiCredentialTestNotice", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "send_api_credential_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send api credential test notice", "tenant", req.GetContext().GetTenantId())
	transferApiCredentialRequest := &identityv1.TransferApiCredentialRequest{}
	transferApiCredentialRequest.Context = req.GetContext()
	transferApiCredentialRequest.ApiCredentialId = req.GetApiCredentialId()
	transferApiCredentialRequest.Details = &identityv1.TransferApiCredentialRequest_Details{
		NewOwner: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		NotifyContact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
		},
	}
	transferApiCredentialReply, err := s.apiCredentialAdminClient.TransferApiCredential(ctx, transferApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialAdminService/TransferApiCredential")
	}
	return &identityv1.SendApiCredentialTestNoticeResponse{
		ApiCredential: &identityv1.ApiCredential{
			Holder: &identityv1.SessionRef{
				DisplayName: transferApiCredentialReply.GetApiCredential().GetHolder().GetDisplayName(),
			},
			ApiCredentialId: req.GetApiCredentialId(),
			Contact:         &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetApiCredentialPreferences returns an api credential.
func (s *ApiCredentialNotificationServer) GetApiCredentialPreferences(ctx context.Context, req *identityv1.GetApiCredentialPreferencesRequest) (*identityv1.GetApiCredentialPreferencesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetApiCredentialPreferences", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "api_credential", req.GetApiCredentialId(), "get_api_credential_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get api credential preferences", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetApiCredentialPreferencesResponse{
		ApiCredential: &identityv1.ApiCredential{ApiCredentialId: req.GetApiCredentialId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamApiCredentialNotices streams the changes of api credentials as they happen.
func (s *ApiCredentialNotificationServer) StreamApiCredentialNotices(req *identityv1.StreamApiCredentialNoticesRequest, stream grpc.ServerStreamingServer[identityv1.StreamApiCredentialNoticesResponse]) error {
	ctx := stream.Context()
	s.log.InfoContext(ctx, "stream api credential notices", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&identityv1.StreamApiCredentialNoticesResponse{
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{
				Name:  req.GetSubscriber().GetName(),
				Phone: req.GetSubscriber().GetPhone(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}
