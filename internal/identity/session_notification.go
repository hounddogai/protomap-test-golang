package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// SessionNotificationServer implements `acme.identity.v1.SessionNotificationService`.
// Tells the people who follow a session what changed.
type SessionNotificationServer struct {
	identityv1.UnimplementedSessionNotificationServiceServer
	store                          *Store
	log                            *slog.Logger
	searchQueryQueryClient         searchv1.SearchQueryQueryServiceClient
	searchQueryQueryClientFallback searchv1.SearchQueryQueryServiceClient
	featureFlagClient              featureflagsv1.FeatureFlagServiceClient
}

// NewSessionNotificationServer wires a SessionNotificationServer.
func NewSessionNotificationServer(deps platform.Deps) *SessionNotificationServer {
	return &SessionNotificationServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.identity.v1.SessionNotificationService"),
		searchQueryQueryClient:         searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		searchQueryQueryClientFallback: searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		featureFlagClient:              featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
	}
}

// SubscribeSessionUpdates subscribes a session.
func (s *SessionNotificationServer) SubscribeSessionUpdates(ctx context.Context, req *identityv1.SubscribeSessionUpdatesRequest) (*identityv1.SubscribeSessionUpdatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "subscribe_session_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe session updates", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *searchv1.SummarizeSearchQueriesRequest, ...grpc.CallOption) (*searchv1.SummarizeSearchQueriesResponse, error){
		"primary":  s.searchQueryQueryClient.SummarizeSearchQueries,
		"fallback": s.searchQueryQueryClientFallback.SummarizeSearchQueries,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	summarizeSearchQueriesRequest := &searchv1.SummarizeSearchQueriesRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	summarizeSearchQueriesReply, err := route(ctx, summarizeSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/SummarizeSearchQueries")
	}
	return &identityv1.SubscribeSessionUpdatesResponse{
		Outcome: &identityv1.SubscribeSessionUpdatesResponse_Outcome{Status: summarizeSearchQueriesReply.GetResult().GetStatus()},
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Requester: &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnsubscribeSessionUpdates unsubscribes a session.
func (s *SessionNotificationServer) UnsubscribeSessionUpdates(ctx context.Context, req *identityv1.UnsubscribeSessionUpdatesRequest) (*identityv1.UnsubscribeSessionUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "unsubscribe_session_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe session updates", "tenant", req.GetContext().GetTenantId())
	return &identityv1.UnsubscribeSessionUpdatesResponse{
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Assignee:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NotifySessionWatchers notifys a session.
func (s *SessionNotificationServer) NotifySessionWatchers(ctx context.Context, req *identityv1.NotifySessionWatchersRequest) (*identityv1.NotifySessionWatchersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "notify_session_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify session watchers", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded identityv1.NotifySessionWatchersRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	createFeatureFlagRequest := &featureflagsv1.CreateFeatureFlagRequest{}
	createFeatureFlagRequest.Context = req.GetContext()
	createFeatureFlagRequest.FeatureFlag = &featureflagsv1.FeatureFlag{
		Owner:  &identityv1.UserRef{Email: (&decoded).GetRecipient().GetEmail()},
		Action: &featureflagsv1.FeatureFlag_Block{Block: &featureflagsv1.FeatureFlagBlockAction{Reason: req.GetReason()}},
	}
	createFeatureFlagReply, err := s.featureFlagClient.CreateFeatureFlag(ctx, createFeatureFlagRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/CreateFeatureFlag")
	}
	return &identityv1.NotifySessionWatchersResponse{
		Outcome: &identityv1.NotifySessionWatchersResponse_Outcome{Message: createFeatureFlagReply.GetResult().GetMessage()},
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Requester: &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SendSessionTestNotice sends a session.
func (s *SessionNotificationServer) SendSessionTestNotice(ctx context.Context, req *identityv1.SendSessionTestNoticeRequest) (*identityv1.SendSessionTestNoticeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "send_session_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send session test notice", "tenant", req.GetContext().GetTenantId())
	return &identityv1.SendSessionTestNoticeResponse{
		Session: &identityv1.Session{
			SessionId: req.GetSessionId(),
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetSessionPreferences returns a session.
func (s *SessionNotificationServer) GetSessionPreferences(ctx context.Context, req *identityv1.GetSessionPreferencesRequest) (*identityv1.GetSessionPreferencesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "session", req.GetSessionId(), "get_session_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get session preferences", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetSessionPreferencesResponse{
		Session: &identityv1.Session{SessionId: req.GetSessionId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamSessionNotices streams the changes of sessions as they happen.
func (s *SessionNotificationServer) StreamSessionNotices(req *identityv1.StreamSessionNoticesRequest, stream grpc.ServerStreamingServer[identityv1.StreamSessionNoticesResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "stream session notices", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&identityv1.StreamSessionNoticesResponse{
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{
				Email:            req.GetSubscriber().GetEmail(),
				PreferredChannel: req.GetSubscriber().GetPreferredChannel(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}
