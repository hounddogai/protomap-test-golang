package storefront

import (
	"context"
	"time"

	driverappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/driverapp/v1"
	partnerapiv1 "github.com/hounddogai/protomap-test-golang/gen/acme/partnerapi/v1"
	patientappv1 "github.com/hounddogai/protomap-test-golang/gen/acme/patientapp/v1"
	storefrontv1 "github.com/hounddogai/protomap-test-golang/gen/acme/storefront/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredDriverHomeQuery records how long each call to acme.driverapp.v1.DriverHomeQueryService takes.
type meteredDriverHomeQuery struct {
	driverappv1.DriverHomeQueryServiceClient
	recorder *metrics.Recorder
}

func (c meteredDriverHomeQuery) FindDriverHomes(ctx context.Context, request *driverappv1.FindDriverHomesRequest, options ...grpc.CallOption) (*driverappv1.FindDriverHomesResponse, error) {
	defer c.recorder.Observe("acme.driverapp.v1.DriverHomeQueryService/FindDriverHomes", time.Now())
	return c.DriverHomeQueryServiceClient.FindDriverHomes(ctx, request, options...)
}

// meteredPartnerWebhookNotification records how long each call to acme.partnerapi.v1.PartnerWebhookNotificationService
// takes.
type meteredPartnerWebhookNotification struct {
	partnerapiv1.PartnerWebhookNotificationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPartnerWebhookNotification) UnsubscribePartnerWebhookUpdates(ctx context.Context, request *partnerapiv1.UnsubscribePartnerWebhookUpdatesRequest, options ...grpc.CallOption) (*partnerapiv1.UnsubscribePartnerWebhookUpdatesResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerWebhookNotificationService/UnsubscribePartnerWebhookUpdates", time.Now())
	return c.PartnerWebhookNotificationServiceClient.UnsubscribePartnerWebhookUpdates(ctx, request, options...)
}

// meteredPartnerWebhookValidation records how long each call to acme.partnerapi.v1.PartnerWebhookValidationService
// takes.
type meteredPartnerWebhookValidation struct {
	partnerapiv1.PartnerWebhookValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPartnerWebhookValidation) ValidatePartnerWebhook(ctx context.Context, request *partnerapiv1.ValidatePartnerWebhookRequest, options ...grpc.CallOption) (*partnerapiv1.ValidatePartnerWebhookResponse, error) {
	defer c.recorder.Observe("acme.partnerapi.v1.PartnerWebhookValidationService/ValidatePartnerWebhook", time.Now())
	return c.PartnerWebhookValidationServiceClient.ValidatePartnerWebhook(ctx, request, options...)
}

// meteredPatientHome records how long each call to acme.patientapp.v1.PatientHomeService takes.
type meteredPatientHome struct {
	patientappv1.PatientHomeServiceClient
	recorder *metrics.Recorder
}

func (c meteredPatientHome) CreatePatientHome(ctx context.Context, request *patientappv1.CreatePatientHomeRequest, options ...grpc.CallOption) (*patientappv1.CreatePatientHomeResponse, error) {
	defer c.recorder.Observe("acme.patientapp.v1.PatientHomeService/CreatePatientHome", time.Now())
	return c.PatientHomeServiceClient.CreatePatientHome(ctx, request, options...)
}

// meteredShopperViewWorkflow records how long each call to acme.storefront.v1.ShopperViewWorkflowService takes.
type meteredShopperViewWorkflow struct {
	storefrontv1.ShopperViewWorkflowServiceClient
	recorder *metrics.Recorder
}

// meteredStorefrontSessionIntegration records how long each call to
// acme.storefront.v1.StorefrontSessionIntegrationService takes.
type meteredStorefrontSessionIntegration struct {
	storefrontv1.StorefrontSessionIntegrationServiceClient
	recorder *metrics.Recorder
}

func (c meteredStorefrontSessionIntegration) ListStorefrontSessionSources(ctx context.Context, request *storefrontv1.ListStorefrontSessionSourcesRequest, options ...grpc.CallOption) (*storefrontv1.ListStorefrontSessionSourcesResponse, error) {
	defer c.recorder.Observe("acme.storefront.v1.StorefrontSessionIntegrationService/ListStorefrontSessionSources", time.Now())
	return c.StorefrontSessionIntegrationServiceClient.ListStorefrontSessionSources(ctx, request, options...)
}

// meteredStorefrontSessionWorkflow records how long each call to acme.storefront.v1.StorefrontSessionWorkflowService
// takes.
type meteredStorefrontSessionWorkflow struct {
	storefrontv1.StorefrontSessionWorkflowServiceClient
	recorder *metrics.Recorder
}

func (c meteredStorefrontSessionWorkflow) CancelStorefrontSessionWorkflow(ctx context.Context, request *storefrontv1.CancelStorefrontSessionWorkflowRequest, options ...grpc.CallOption) (*storefrontv1.CancelStorefrontSessionWorkflowResponse, error) {
	defer c.recorder.Observe("acme.storefront.v1.StorefrontSessionWorkflowService/CancelStorefrontSessionWorkflow", time.Now())
	return c.StorefrontSessionWorkflowServiceClient.CancelStorefrontSessionWorkflow(ctx, request, options...)
}
