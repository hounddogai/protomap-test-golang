package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RoleNotificationServer implements `acme.identity.v1.RoleNotificationService`.
// Tells the people who follow a role what changed.
type RoleNotificationServer struct {
	identityv1.UnimplementedRoleNotificationServiceServer
	store                *Store
	log                  *slog.Logger
	userValidationClient identityv1.UserValidationServiceClient
	notificationClient   notificationsv1.NotificationServiceClient
}

// NewRoleNotificationServer wires a RoleNotificationServer.
func NewRoleNotificationServer(deps platform.Deps) *RoleNotificationServer {
	return &RoleNotificationServer{
		store:                NewStore(deps.DB),
		log:                  deps.Logger.With("service", "acme.identity.v1.RoleNotificationService"),
		userValidationClient: identityv1.NewUserValidationServiceClient(deps.Conn),
		notificationClient:   notificationsv1.NewNotificationServiceClient(deps.Conn),
	}
}

// SubscribeRoleUpdates subscribes a role.
func (s *RoleNotificationServer) SubscribeRoleUpdates(ctx context.Context, req *identityv1.SubscribeRoleUpdatesRequest) (*identityv1.SubscribeRoleUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "subscribe_role_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe role updates", "tenant", req.GetContext().GetTenantId())
	return &identityv1.SubscribeRoleUpdatesResponse{
		Role: &identityv1.Role{
			RoleId: req.GetRoleId(),
			Owner:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnsubscribeRoleUpdates unsubscribes a role.
func (s *RoleNotificationServer) UnsubscribeRoleUpdates(ctx context.Context, req *identityv1.UnsubscribeRoleUpdatesRequest) (*identityv1.UnsubscribeRoleUpdatesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "unsubscribe_role_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe role updates", "tenant", req.GetContext().GetTenantId())
	return &identityv1.UnsubscribeRoleUpdatesResponse{
		Role: &identityv1.Role{
			RoleId: req.GetRoleId(),
			Owner:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NotifyRoleWatchers notifys a role.
func (s *RoleNotificationServer) NotifyRoleWatchers(ctx context.Context, req *identityv1.NotifyRoleWatchersRequest) (*identityv1.NotifyRoleWatchersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "notify_role_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify role watchers", "tenant", req.GetContext().GetTenantId())
	var envelope identityv1.UserRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainUserDecisionRequest := &identityv1.ExplainUserDecisionRequest{}
	explainUserDecisionRequest.Context = req.GetContext()
	explainUserDecisionRequest.User = &identityv1.User{
		RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetRecipient().GetPreferredChannel(),
		},
	}
	explainUserDecisionRequest.Details = &identityv1.ExplainUserDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetRecipient().GetPhone()},
	}
	explainUserDecisionRequest.UserId = envelope.GetDisplayName()
	explainUserDecisionReply, err := s.userValidationClient.ExplainUserDecision(ctx, explainUserDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/ExplainUserDecision")
	}
	return &identityv1.NotifyRoleWatchersResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: explainUserDecisionReply.GetResult().GetReferenceIds(),
			Details:      explainUserDecisionReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		Role: &identityv1.Role{
			RoleId: req.GetRoleId(),
			Owner:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
	}, nil
}

// ListRoleSubscriptions lists roles a page at a time.
func (s *RoleNotificationServer) ListRoleSubscriptions(ctx context.Context, req *identityv1.ListRoleSubscriptionsRequest) (*identityv1.ListRoleSubscriptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListRoleSubscriptions", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list role subscriptions", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	createNotificationRequest := &notificationsv1.CreateNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Assignee: &identityv1.UserRef{UserId: cachedContext.GetActor().GetUserId()},
		},
	}
	createNotificationReply, err := grpcx.Call(ctx, s.notificationClient.CreateNotification, createNotificationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/CreateNotification")
	}
	return &identityv1.ListRoleSubscriptionsResponse{
		Roles: []*identityv1.Role{&identityv1.Role{
			Audit: &commonv1.AuditInfo{
				Etag: createNotificationReply.GetNotification().GetAudit().GetEtag(),
			},
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: createNotificationReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SendRoleTestNotice sends a role.
func (s *RoleNotificationServer) SendRoleTestNotice(ctx context.Context, req *identityv1.SendRoleTestNoticeRequest) (*identityv1.SendRoleTestNoticeResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "send_role_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send role test notice", "tenant", req.GetContext().GetTenantId())
	return &identityv1.SendRoleTestNoticeResponse{
		Role: &identityv1.Role{
			RoleId: req.GetRoleId(),
			Owner:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetRolePreferences returns a role.
func (s *RoleNotificationServer) GetRolePreferences(ctx context.Context, req *identityv1.GetRolePreferencesRequest) (*identityv1.GetRolePreferencesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "role", req.GetRoleId(), "get_role_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get role preferences", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetRolePreferencesResponse{
		Role:   &identityv1.Role{RoleId: req.GetRoleId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// StreamRoleNotices streams the changes of roles as they happen.
func (s *RoleNotificationServer) StreamRoleNotices(req *identityv1.StreamRoleNoticesRequest, stream grpc.ServerStreamingServer[identityv1.StreamRoleNoticesResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected StreamRoleNotices", "error", err)
		return err
	}
	s.log.InfoContext(ctx, "stream role notices", "tenant", req.GetContext().GetTenantId())
	if err := stream.Send(&identityv1.StreamRoleNoticesResponse{
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{Email: req.GetSubscriber().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}
