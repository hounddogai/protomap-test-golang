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

// TenantNotificationServer implements `acme.identity.v1.TenantNotificationService`.
// Tells the people who follow a tenant what changed.
type TenantNotificationServer struct {
	identityv1.UnimplementedTenantNotificationServiceServer
	store             *Store
	log               *slog.Logger
	userHistoryClient identityv1.UserHistoryServiceClient
}

// NewTenantNotificationServer wires a TenantNotificationServer.
func NewTenantNotificationServer(deps platform.Deps) *TenantNotificationServer {
	return &TenantNotificationServer{
		store:             NewStore(deps.DB),
		log:               deps.Logger.With("service", "acme.identity.v1.TenantNotificationService"),
		userHistoryClient: identityv1.NewUserHistoryServiceClient(deps.Conn),
	}
}

// SubscribeTenantUpdates subscribes a tenant.
func (s *TenantNotificationServer) SubscribeTenantUpdates(ctx context.Context, req *identityv1.SubscribeTenantUpdatesRequest) (*identityv1.SubscribeTenantUpdatesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "subscribe_tenant_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "subscribe tenant updates", "tenant", req.GetContext().GetTenantId())
	var envelope identityv1.UserRef
	if err := req.GetDetails().GetPayload().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pruneUserVersionsReply, err := s.userHistoryClient.PruneUserVersions(ctx, &identityv1.PruneUserVersionsRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
		Reason:  req.GetReason(),
		Etag:    envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/PruneUserVersions")
	}
	return &identityv1.SubscribeTenantUpdatesResponse{
		Outcome: &identityv1.SubscribeTenantUpdatesResponse_Outcome{Status: pruneUserVersionsReply.GetResult().GetStatus()},
		Tenant: &identityv1.Tenant{
			TenantId: req.GetTenantId(),
			Manager:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UnsubscribeTenantUpdates unsubscribes a tenant.
func (s *TenantNotificationServer) UnsubscribeTenantUpdates(ctx context.Context, req *identityv1.UnsubscribeTenantUpdatesRequest) (*identityv1.UnsubscribeTenantUpdatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "unsubscribe_tenant_updates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "unsubscribe tenant updates", "tenant", req.GetContext().GetTenantId())
	return &identityv1.UnsubscribeTenantUpdatesResponse{
		Tenant: &identityv1.Tenant{
			TenantId: req.GetTenantId(),
			Manager:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NotifyTenantWatchers notifys a tenant.
func (s *TenantNotificationServer) NotifyTenantWatchers(ctx context.Context, req *identityv1.NotifyTenantWatchersRequest) (*identityv1.NotifyTenantWatchersResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "notify_tenant_watchers"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "notify tenant watchers", "tenant", req.GetContext().GetTenantId())
	return &identityv1.NotifyTenantWatchersResponse{
		Tenant: &identityv1.Tenant{
			TenantId: req.GetTenantId(),
			Manager:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListTenantSubscriptions lists tenants a page at a time.
func (s *TenantNotificationServer) ListTenantSubscriptions(ctx context.Context, req *identityv1.ListTenantSubscriptionsRequest) (*identityv1.ListTenantSubscriptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list tenant subscriptions", "tenant", req.GetContext().GetTenantId())
	return &identityv1.ListTenantSubscriptionsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SendTenantTestNotice sends a tenant.
func (s *TenantNotificationServer) SendTenantTestNotice(ctx context.Context, req *identityv1.SendTenantTestNoticeRequest) (*identityv1.SendTenantTestNoticeResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SendTenantTestNotice", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "send_tenant_test_notice"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "send tenant test notice", "tenant", req.GetContext().GetTenantId())
	return &identityv1.SendTenantTestNoticeResponse{
		Tenant: &identityv1.Tenant{
			TenantId: req.GetTenantId(),
			Manager:  &identityv1.UserRef{Email: req.GetDetails().GetRecipient().GetEmail()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetTenantPreferences returns a tenant.
func (s *TenantNotificationServer) GetTenantPreferences(ctx context.Context, req *identityv1.GetTenantPreferencesRequest) (*identityv1.GetTenantPreferencesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetTenantPreferences", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "tenant", req.GetTenantId(), "get_tenant_preferences"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get tenant preferences", "tenant", req.GetContext().GetTenantId())
	return &identityv1.GetTenantPreferencesResponse{
		Tenant: &identityv1.Tenant{TenantId: req.GetTenantId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
