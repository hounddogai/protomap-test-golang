package identity

import (
	"context"
	"time"

	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredApiCredentialAdmin records how long each call to acme.identity.v1.ApiCredentialAdminService takes.
type meteredApiCredentialAdmin struct {
	identityv1.ApiCredentialAdminServiceClient
	recorder *metrics.Recorder
}

// meteredApiCredentialNotification records how long each call to acme.identity.v1.ApiCredentialNotificationService
// takes.
type meteredApiCredentialNotification struct {
	identityv1.ApiCredentialNotificationServiceClient
	recorder *metrics.Recorder
}

func (c meteredApiCredentialNotification) SendApiCredentialTestNotice(ctx context.Context, request *identityv1.SendApiCredentialTestNoticeRequest, options ...grpc.CallOption) (*identityv1.SendApiCredentialTestNoticeResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.ApiCredentialNotificationService/SendApiCredentialTestNotice", time.Now())
	return c.ApiCredentialNotificationServiceClient.SendApiCredentialTestNotice(ctx, request, options...)
}

// meteredTenantAdmin records how long each call to acme.identity.v1.TenantAdminService takes.
type meteredTenantAdmin struct {
	identityv1.TenantAdminServiceClient
	recorder *metrics.Recorder
}

func (c meteredTenantAdmin) InspectTenant(ctx context.Context, request *identityv1.InspectTenantRequest, options ...grpc.CallOption) (*identityv1.InspectTenantResponse, error) {
	defer c.recorder.Observe("acme.identity.v1.TenantAdminService/InspectTenant", time.Now())
	return c.TenantAdminServiceClient.InspectTenant(ctx, request, options...)
}
