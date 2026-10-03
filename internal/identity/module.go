package identity

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"google.golang.org/grpc"
)

// Register registers every service of the identity domain on the server.
func Register(server *grpc.Server, deps platform.Deps) {
	userServer := NewUserServer(deps)
	server.RegisterService(&identityv1.UserService_ServiceDesc, userServer)
	identityv1.RegisterSessionQueryServiceServer(server, NewSessionQueryServer(deps))
	identityv1.RegisterApiCredentialValidationServiceServer(server, NewApiCredentialValidationServer(deps))
	identityv1.RegisterRoleHistoryServiceServer(server, NewRoleHistoryServer(deps))
	identityv1.RegisterTenantNotificationServiceServer(server, NewTenantNotificationServer(deps))
	identityv1.RegisterUserQueryServiceServer(server, NewUserQueryServer(deps))
	identityv1.RegisterSessionValidationServiceServer(server, NewSessionValidationServer(deps))
	identityv1.RegisterApiCredentialHistoryServiceServer(server, NewApiCredentialHistoryServer(deps))
	identityv1.RegisterRoleNotificationServiceServer(server, NewRoleNotificationServer(deps))
	identityv1.RegisterTenantAdminServiceServer(server, NewTenantAdminServer(deps))
	identityv1.RegisterUserValidationServiceServer(server, NewUserValidationServer(deps))
	identityv1.RegisterSessionHistoryServiceServer(server, NewSessionHistoryServer(deps))
	identityv1.RegisterApiCredentialNotificationServiceServer(server, NewApiCredentialNotificationServer(deps))
	identityv1.RegisterRoleAdminServiceServer(server, NewRoleAdminServer(deps))
	identityv1.RegisterTenantExportServiceServer(server, NewTenantExportServer(deps))
	identityv1.RegisterUserHistoryServiceServer(server, NewUserHistoryServer(deps))
	identityv1.RegisterSessionNotificationServiceServer(server, NewSessionNotificationServer(deps))
	identityv1.RegisterApiCredentialAdminServiceServer(server, NewApiCredentialAdminServer(deps))
}

// workers make the calls the domain's events ask for, apart from the requests that published them.
type workers struct {
	log                  *slog.Logger
	sessionHistoryClient identityv1.SessionHistoryServiceClient
}

// StartWorkers subscribes the domain's workers to their topics until the context ends.
func StartWorkers(ctx context.Context, deps platform.Deps) {
	w := &workers{
		log:                  deps.Logger,
		sessionHistoryClient: identityv1.NewSessionHistoryServiceClient(deps.Conn),
	}
	go w.consume(ctx, "identity.score_session.compare_session_versions", deps.Bus.Subscribe("identity.score_session.compare_session_versions"), w.handleIdentityScoreSessionCompareSessionVersions)
}

func (w *workers) consume(ctx context.Context, topic string, events <-chan eventbus.Event, handle func(context.Context, eventbus.Event) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-events:
			if err := handle(ctx, event); err != nil {
				w.log.ErrorContext(ctx, "event failed", "topic", topic, "error", err)
			}
		}
	}
}

// handleIdentityScoreSessionCompareSessionVersions makes the call the identity.score_session.compare_session_versions
// event asks for.
func (w *workers) handleIdentityScoreSessionCompareSessionVersions(ctx context.Context, event eventbus.Event) error {
	_, err := w.sessionHistoryClient.CompareSessionVersions(ctx, &identityv1.CompareSessionVersionsRequest{
		Session: &identityv1.Session{
			Origin:    &identityv1.Session_Channel{Channel: &identityv1.SessionChannelOrigin{Channel: event.Attributes["session.channel.channel"]}},
			Requester: &commonv1.ContactInfo{Email: event.Attributes["session.requester.email"]},
		},
		Context: &commonv1.RequestContext{
			TenantId: event.Attributes["context.tenant_id"],
			Actor: &commonv1.Actor{
				UserId:         "monolith",
				ServiceAccount: true,
			},
		},
	})
	return err
}
