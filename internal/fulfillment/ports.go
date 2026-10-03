package fulfillment

import (
	"context"
	"log/slog"

	campaignsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/campaigns/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// fulfillmentWorkflowPort is what the fulfillment domain needs of acme.fulfillment.v1.FulfillmentWorkflowService.
type fulfillmentWorkflowPort interface {
	ListFulfillmentWorkflows(ctx context.Context, request *fulfillmentv1.ListFulfillmentWorkflowsRequest) (*fulfillmentv1.ListFulfillmentWorkflowsResponse, error)
}

// grpcFulfillmentWorkflowPort reaches acme.fulfillment.v1.FulfillmentWorkflowService over gRPC.
type grpcFulfillmentWorkflowPort struct {
	client fulfillmentv1.FulfillmentWorkflowServiceClient
}

// sandboxFulfillmentWorkflowPort answers in the sandbox environment, where
// acme.fulfillment.v1.FulfillmentWorkflowService is not deployed.
type sandboxFulfillmentWorkflowPort struct{ log *slog.Logger }

func (p grpcFulfillmentWorkflowPort) ListFulfillmentWorkflows(ctx context.Context, request *fulfillmentv1.ListFulfillmentWorkflowsRequest) (*fulfillmentv1.ListFulfillmentWorkflowsResponse, error) {
	return p.client.ListFulfillmentWorkflows(ctx, request)
}

func (p sandboxFulfillmentWorkflowPort) ListFulfillmentWorkflows(ctx context.Context, request *fulfillmentv1.ListFulfillmentWorkflowsRequest) (*fulfillmentv1.ListFulfillmentWorkflowsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListFulfillmentWorkflows")
	return &fulfillmentv1.ListFulfillmentWorkflowsResponse{}, nil
}

func newFulfillmentWorkflowPort(deps platform.Deps) fulfillmentWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFulfillmentWorkflowPort{log: deps.Logger}
	}
	return grpcFulfillmentWorkflowPort{client: fulfillmentv1.NewFulfillmentWorkflowServiceClient(deps.Conn)}
}

// marketingCampaignPort is what the fulfillment domain needs of acme.campaigns.v1.MarketingCampaignService.
type marketingCampaignPort interface {
	UpdateMarketingCampaign(ctx context.Context, request *campaignsv1.UpdateMarketingCampaignRequest) (*campaignsv1.UpdateMarketingCampaignResponse, error)
}

// grpcMarketingCampaignPort reaches acme.campaigns.v1.MarketingCampaignService over gRPC.
type grpcMarketingCampaignPort struct {
	client campaignsv1.MarketingCampaignServiceClient
}

// sandboxMarketingCampaignPort answers in the sandbox environment, where acme.campaigns.v1.MarketingCampaignService is
// not deployed.
type sandboxMarketingCampaignPort struct{ log *slog.Logger }

func (p grpcMarketingCampaignPort) UpdateMarketingCampaign(ctx context.Context, request *campaignsv1.UpdateMarketingCampaignRequest) (*campaignsv1.UpdateMarketingCampaignResponse, error) {
	return p.client.UpdateMarketingCampaign(ctx, request)
}

func (p sandboxMarketingCampaignPort) UpdateMarketingCampaign(ctx context.Context, request *campaignsv1.UpdateMarketingCampaignRequest) (*campaignsv1.UpdateMarketingCampaignResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers UpdateMarketingCampaign")
	return &campaignsv1.UpdateMarketingCampaignResponse{}, nil
}

func newMarketingCampaignPort(deps platform.Deps) marketingCampaignPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxMarketingCampaignPort{log: deps.Logger}
	}
	return grpcMarketingCampaignPort{client: campaignsv1.NewMarketingCampaignServiceClient(deps.Conn)}
}

// parcelValidationPort is what the fulfillment domain needs of acme.shipping.v1.ParcelValidationService.
type parcelValidationPort interface {
	ValidateParcel(ctx context.Context, request *shippingv1.ValidateParcelRequest) (*shippingv1.ValidateParcelResponse, error)
}

// grpcParcelValidationPort reaches acme.shipping.v1.ParcelValidationService over gRPC.
type grpcParcelValidationPort struct {
	client shippingv1.ParcelValidationServiceClient
}

// sandboxParcelValidationPort answers in the sandbox environment, where acme.shipping.v1.ParcelValidationService is not
// deployed.
type sandboxParcelValidationPort struct{ log *slog.Logger }

func (p grpcParcelValidationPort) ValidateParcel(ctx context.Context, request *shippingv1.ValidateParcelRequest) (*shippingv1.ValidateParcelResponse, error) {
	return p.client.ValidateParcel(ctx, request)
}

func (p sandboxParcelValidationPort) ValidateParcel(ctx context.Context, request *shippingv1.ValidateParcelRequest) (*shippingv1.ValidateParcelResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidateParcel")
	return &shippingv1.ValidateParcelResponse{}, nil
}

func newParcelValidationPort(deps platform.Deps) parcelValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxParcelValidationPort{log: deps.Logger}
	}
	return grpcParcelValidationPort{client: shippingv1.NewParcelValidationServiceClient(deps.Conn)}
}
