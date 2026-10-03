package shipping

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// carrierRateApprovalPort is what the shipping domain needs of acme.shipping.v1.CarrierRateApprovalService.
type carrierRateApprovalPort interface {
	SubmitCarrierRate(ctx context.Context, request *shippingv1.SubmitCarrierRateRequest) (*shippingv1.SubmitCarrierRateResponse, error)
}

// grpcCarrierRateApprovalPort reaches acme.shipping.v1.CarrierRateApprovalService over gRPC.
type grpcCarrierRateApprovalPort struct {
	client shippingv1.CarrierRateApprovalServiceClient
}

// sandboxCarrierRateApprovalPort answers in the sandbox environment, where acme.shipping.v1.CarrierRateApprovalService
// is not deployed.
type sandboxCarrierRateApprovalPort struct{ log *slog.Logger }

func (p grpcCarrierRateApprovalPort) SubmitCarrierRate(ctx context.Context, request *shippingv1.SubmitCarrierRateRequest) (*shippingv1.SubmitCarrierRateResponse, error) {
	return p.client.SubmitCarrierRate(ctx, request)
}

func (p sandboxCarrierRateApprovalPort) SubmitCarrierRate(ctx context.Context, request *shippingv1.SubmitCarrierRateRequest) (*shippingv1.SubmitCarrierRateResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SubmitCarrierRate")
	return &shippingv1.SubmitCarrierRateResponse{}, nil
}

func newCarrierRateApprovalPort(deps platform.Deps) carrierRateApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCarrierRateApprovalPort{log: deps.Logger}
	}
	return grpcCarrierRateApprovalPort{client: shippingv1.NewCarrierRateApprovalServiceClient(deps.Conn)}
}

// carrierRateCalculationPort is what the shipping domain needs of acme.shipping.v1.CarrierRateCalculationService.
type carrierRateCalculationPort interface {
	ExplainCarrierRateCalculation(ctx context.Context, request *shippingv1.ExplainCarrierRateCalculationRequest) (*shippingv1.ExplainCarrierRateCalculationResponse, error)
}

// grpcCarrierRateCalculationPort reaches acme.shipping.v1.CarrierRateCalculationService over gRPC.
type grpcCarrierRateCalculationPort struct {
	client shippingv1.CarrierRateCalculationServiceClient
}

// sandboxCarrierRateCalculationPort answers in the sandbox environment, where
// acme.shipping.v1.CarrierRateCalculationService is not deployed.
type sandboxCarrierRateCalculationPort struct{ log *slog.Logger }

func (p grpcCarrierRateCalculationPort) ExplainCarrierRateCalculation(ctx context.Context, request *shippingv1.ExplainCarrierRateCalculationRequest) (*shippingv1.ExplainCarrierRateCalculationResponse, error) {
	return p.client.ExplainCarrierRateCalculation(ctx, request)
}

func (p sandboxCarrierRateCalculationPort) ExplainCarrierRateCalculation(ctx context.Context, request *shippingv1.ExplainCarrierRateCalculationRequest) (*shippingv1.ExplainCarrierRateCalculationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainCarrierRateCalculation")
	return &shippingv1.ExplainCarrierRateCalculationResponse{}, nil
}

func newCarrierRateCalculationPort(deps platform.Deps) carrierRateCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCarrierRateCalculationPort{log: deps.Logger}
	}
	return grpcCarrierRateCalculationPort{client: shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn)}
}

// featureFlagValidationPort is what the shipping domain needs of acme.featureflags.v1.FeatureFlagValidationService.
type featureFlagValidationPort interface {
	CheckFeatureFlagEligibility(ctx context.Context, request *featureflagsv1.CheckFeatureFlagEligibilityRequest) (*featureflagsv1.CheckFeatureFlagEligibilityResponse, error)
	ExplainFeatureFlagDecision(ctx context.Context, request *featureflagsv1.ExplainFeatureFlagDecisionRequest) (*featureflagsv1.ExplainFeatureFlagDecisionResponse, error)
}

// grpcFeatureFlagValidationPort reaches acme.featureflags.v1.FeatureFlagValidationService over gRPC.
type grpcFeatureFlagValidationPort struct {
	client featureflagsv1.FeatureFlagValidationServiceClient
}

// sandboxFeatureFlagValidationPort answers in the sandbox environment, where
// acme.featureflags.v1.FeatureFlagValidationService is not deployed.
type sandboxFeatureFlagValidationPort struct{ log *slog.Logger }

func (p grpcFeatureFlagValidationPort) CheckFeatureFlagEligibility(ctx context.Context, request *featureflagsv1.CheckFeatureFlagEligibilityRequest) (*featureflagsv1.CheckFeatureFlagEligibilityResponse, error) {
	return p.client.CheckFeatureFlagEligibility(ctx, request)
}

func (p sandboxFeatureFlagValidationPort) CheckFeatureFlagEligibility(ctx context.Context, request *featureflagsv1.CheckFeatureFlagEligibilityRequest) (*featureflagsv1.CheckFeatureFlagEligibilityResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CheckFeatureFlagEligibility")
	return &featureflagsv1.CheckFeatureFlagEligibilityResponse{}, nil
}

func (p grpcFeatureFlagValidationPort) ExplainFeatureFlagDecision(ctx context.Context, request *featureflagsv1.ExplainFeatureFlagDecisionRequest) (*featureflagsv1.ExplainFeatureFlagDecisionResponse, error) {
	return p.client.ExplainFeatureFlagDecision(ctx, request)
}

func (p sandboxFeatureFlagValidationPort) ExplainFeatureFlagDecision(ctx context.Context, request *featureflagsv1.ExplainFeatureFlagDecisionRequest) (*featureflagsv1.ExplainFeatureFlagDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainFeatureFlagDecision")
	return &featureflagsv1.ExplainFeatureFlagDecisionResponse{}, nil
}

func newFeatureFlagValidationPort(deps platform.Deps) featureFlagValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxFeatureFlagValidationPort{log: deps.Logger}
	}
	return grpcFeatureFlagValidationPort{client: featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn)}
}

// manifestReportingPort is what the shipping domain needs of acme.shipping.v1.ManifestReportingService.
type manifestReportingPort interface {
	ListManifestReports(ctx context.Context, request *shippingv1.ListManifestReportsRequest) (*shippingv1.ListManifestReportsResponse, error)
	ShareManifestReport(ctx context.Context, request *shippingv1.ShareManifestReportRequest) (*shippingv1.ShareManifestReportResponse, error)
}

// grpcManifestReportingPort reaches acme.shipping.v1.ManifestReportingService over gRPC.
type grpcManifestReportingPort struct {
	client shippingv1.ManifestReportingServiceClient
}

// sandboxManifestReportingPort answers in the sandbox environment, where acme.shipping.v1.ManifestReportingService is
// not deployed.
type sandboxManifestReportingPort struct{ log *slog.Logger }

func (p grpcManifestReportingPort) ListManifestReports(ctx context.Context, request *shippingv1.ListManifestReportsRequest) (*shippingv1.ListManifestReportsResponse, error) {
	return p.client.ListManifestReports(ctx, request)
}

func (p sandboxManifestReportingPort) ListManifestReports(ctx context.Context, request *shippingv1.ListManifestReportsRequest) (*shippingv1.ListManifestReportsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListManifestReports")
	return &shippingv1.ListManifestReportsResponse{}, nil
}

func (p grpcManifestReportingPort) ShareManifestReport(ctx context.Context, request *shippingv1.ShareManifestReportRequest) (*shippingv1.ShareManifestReportResponse, error) {
	return p.client.ShareManifestReport(ctx, request)
}

func (p sandboxManifestReportingPort) ShareManifestReport(ctx context.Context, request *shippingv1.ShareManifestReportRequest) (*shippingv1.ShareManifestReportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ShareManifestReport")
	return &shippingv1.ShareManifestReportResponse{}, nil
}

func newManifestReportingPort(deps platform.Deps) manifestReportingPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxManifestReportingPort{log: deps.Logger}
	}
	return grpcManifestReportingPort{client: shippingv1.NewManifestReportingServiceClient(deps.Conn)}
}

// parcelCalculationPort is what the shipping domain needs of acme.shipping.v1.ParcelCalculationService.
type parcelCalculationPort interface {
	CalculateParcel(ctx context.Context, request *shippingv1.CalculateParcelRequest) (*shippingv1.CalculateParcelResponse, error)
}

// grpcParcelCalculationPort reaches acme.shipping.v1.ParcelCalculationService over gRPC.
type grpcParcelCalculationPort struct {
	client shippingv1.ParcelCalculationServiceClient
}

// sandboxParcelCalculationPort answers in the sandbox environment, where acme.shipping.v1.ParcelCalculationService is
// not deployed.
type sandboxParcelCalculationPort struct{ log *slog.Logger }

func (p grpcParcelCalculationPort) CalculateParcel(ctx context.Context, request *shippingv1.CalculateParcelRequest) (*shippingv1.CalculateParcelResponse, error) {
	return p.client.CalculateParcel(ctx, request)
}

func (p sandboxParcelCalculationPort) CalculateParcel(ctx context.Context, request *shippingv1.CalculateParcelRequest) (*shippingv1.CalculateParcelResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateParcel")
	return &shippingv1.CalculateParcelResponse{}, nil
}

func newParcelCalculationPort(deps platform.Deps) parcelCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxParcelCalculationPort{log: deps.Logger}
	}
	return grpcParcelCalculationPort{client: shippingv1.NewParcelCalculationServiceClient(deps.Conn)}
}

// parcelValidationPort is what the shipping domain needs of acme.shipping.v1.ParcelValidationService.
type parcelValidationPort interface {
	GetParcelRules(ctx context.Context, request *shippingv1.GetParcelRulesRequest) (*shippingv1.GetParcelRulesResponse, error)
}

// grpcParcelValidationPort reaches acme.shipping.v1.ParcelValidationService over gRPC.
type grpcParcelValidationPort struct {
	client shippingv1.ParcelValidationServiceClient
}

// sandboxParcelValidationPort answers in the sandbox environment, where acme.shipping.v1.ParcelValidationService is not
// deployed.
type sandboxParcelValidationPort struct{ log *slog.Logger }

func (p grpcParcelValidationPort) GetParcelRules(ctx context.Context, request *shippingv1.GetParcelRulesRequest) (*shippingv1.GetParcelRulesResponse, error) {
	return p.client.GetParcelRules(ctx, request)
}

func (p sandboxParcelValidationPort) GetParcelRules(ctx context.Context, request *shippingv1.GetParcelRulesRequest) (*shippingv1.GetParcelRulesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetParcelRules")
	return &shippingv1.GetParcelRulesResponse{}, nil
}

func newParcelValidationPort(deps platform.Deps) parcelValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxParcelValidationPort{log: deps.Logger}
	}
	return grpcParcelValidationPort{client: shippingv1.NewParcelValidationServiceClient(deps.Conn)}
}

// preferenceValidationPort is what the shipping domain needs of acme.customers.v1.PreferenceValidationService.
type preferenceValidationPort interface {
	PreviewPreference(ctx context.Context, request *customersv1.PreviewPreferenceRequest) (*customersv1.PreviewPreferenceResponse, error)
}

// grpcPreferenceValidationPort reaches acme.customers.v1.PreferenceValidationService over gRPC.
type grpcPreferenceValidationPort struct {
	client customersv1.PreferenceValidationServiceClient
}

// sandboxPreferenceValidationPort answers in the sandbox environment, where
// acme.customers.v1.PreferenceValidationService is not deployed.
type sandboxPreferenceValidationPort struct{ log *slog.Logger }

func (p grpcPreferenceValidationPort) PreviewPreference(ctx context.Context, request *customersv1.PreviewPreferenceRequest) (*customersv1.PreviewPreferenceResponse, error) {
	return p.client.PreviewPreference(ctx, request)
}

func (p sandboxPreferenceValidationPort) PreviewPreference(ctx context.Context, request *customersv1.PreviewPreferenceRequest) (*customersv1.PreviewPreferenceResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewPreference")
	return &customersv1.PreviewPreferenceResponse{}, nil
}

func newPreferenceValidationPort(deps platform.Deps) preferenceValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPreferenceValidationPort{log: deps.Logger}
	}
	return grpcPreferenceValidationPort{client: customersv1.NewPreferenceValidationServiceClient(deps.Conn)}
}

// reviewVoteApprovalPort is what the shipping domain needs of acme.reviews.v1.ReviewVoteApprovalService.
type reviewVoteApprovalPort interface {
	CommentReviewVoteApproval(ctx context.Context, request *reviewsv1.CommentReviewVoteApprovalRequest) (*reviewsv1.CommentReviewVoteApprovalResponse, error)
}

// grpcReviewVoteApprovalPort reaches acme.reviews.v1.ReviewVoteApprovalService over gRPC.
type grpcReviewVoteApprovalPort struct {
	client reviewsv1.ReviewVoteApprovalServiceClient
}

// sandboxReviewVoteApprovalPort answers in the sandbox environment, where acme.reviews.v1.ReviewVoteApprovalService is
// not deployed.
type sandboxReviewVoteApprovalPort struct{ log *slog.Logger }

func (p grpcReviewVoteApprovalPort) CommentReviewVoteApproval(ctx context.Context, request *reviewsv1.CommentReviewVoteApprovalRequest) (*reviewsv1.CommentReviewVoteApprovalResponse, error) {
	return p.client.CommentReviewVoteApproval(ctx, request)
}

func (p sandboxReviewVoteApprovalPort) CommentReviewVoteApproval(ctx context.Context, request *reviewsv1.CommentReviewVoteApprovalRequest) (*reviewsv1.CommentReviewVoteApprovalResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CommentReviewVoteApproval")
	return &reviewsv1.CommentReviewVoteApprovalResponse{}, nil
}

func newReviewVoteApprovalPort(deps platform.Deps) reviewVoteApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReviewVoteApprovalPort{log: deps.Logger}
	}
	return grpcReviewVoteApprovalPort{client: reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn)}
}

// sessionNotificationPort is what the shipping domain needs of acme.identity.v1.SessionNotificationService.
type sessionNotificationPort interface {
	UnsubscribeSessionUpdates(ctx context.Context, request *identityv1.UnsubscribeSessionUpdatesRequest) (*identityv1.UnsubscribeSessionUpdatesResponse, error)
}

// grpcSessionNotificationPort reaches acme.identity.v1.SessionNotificationService over gRPC.
type grpcSessionNotificationPort struct {
	client identityv1.SessionNotificationServiceClient
}

// sandboxSessionNotificationPort answers in the sandbox environment, where acme.identity.v1.SessionNotificationService
// is not deployed.
type sandboxSessionNotificationPort struct{ log *slog.Logger }

func (p grpcSessionNotificationPort) UnsubscribeSessionUpdates(ctx context.Context, request *identityv1.UnsubscribeSessionUpdatesRequest) (*identityv1.UnsubscribeSessionUpdatesResponse, error) {
	return p.client.UnsubscribeSessionUpdates(ctx, request)
}

func (p sandboxSessionNotificationPort) UnsubscribeSessionUpdates(ctx context.Context, request *identityv1.UnsubscribeSessionUpdatesRequest) (*identityv1.UnsubscribeSessionUpdatesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers UnsubscribeSessionUpdates")
	return &identityv1.UnsubscribeSessionUpdatesResponse{}, nil
}

func newSessionNotificationPort(deps platform.Deps) sessionNotificationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSessionNotificationPort{log: deps.Logger}
	}
	return grpcSessionNotificationPort{client: identityv1.NewSessionNotificationServiceClient(deps.Conn)}
}

// shipmentApprovalPort is what the shipping domain needs of acme.shipping.v1.ShipmentApprovalService.
type shipmentApprovalPort interface {
	ApproveShipment(ctx context.Context, request *shippingv1.ApproveShipmentRequest) (*shippingv1.ApproveShipmentResponse, error)
}

// grpcShipmentApprovalPort reaches acme.shipping.v1.ShipmentApprovalService over gRPC.
type grpcShipmentApprovalPort struct {
	client shippingv1.ShipmentApprovalServiceClient
}

// sandboxShipmentApprovalPort answers in the sandbox environment, where acme.shipping.v1.ShipmentApprovalService is not
// deployed.
type sandboxShipmentApprovalPort struct{ log *slog.Logger }

func (p grpcShipmentApprovalPort) ApproveShipment(ctx context.Context, request *shippingv1.ApproveShipmentRequest) (*shippingv1.ApproveShipmentResponse, error) {
	return p.client.ApproveShipment(ctx, request)
}

func (p sandboxShipmentApprovalPort) ApproveShipment(ctx context.Context, request *shippingv1.ApproveShipmentRequest) (*shippingv1.ApproveShipmentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ApproveShipment")
	return &shippingv1.ApproveShipmentResponse{}, nil
}

func newShipmentApprovalPort(deps platform.Deps) shipmentApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxShipmentApprovalPort{log: deps.Logger}
	}
	return grpcShipmentApprovalPort{client: shippingv1.NewShipmentApprovalServiceClient(deps.Conn)}
}

// shipmentCalculationPort is what the shipping domain needs of acme.shipping.v1.ShipmentCalculationService.
type shipmentCalculationPort interface {
	EstimateShipment(ctx context.Context, request *shippingv1.EstimateShipmentRequest) (*shippingv1.EstimateShipmentResponse, error)
}

// grpcShipmentCalculationPort reaches acme.shipping.v1.ShipmentCalculationService over gRPC.
type grpcShipmentCalculationPort struct {
	client shippingv1.ShipmentCalculationServiceClient
}

// sandboxShipmentCalculationPort answers in the sandbox environment, where acme.shipping.v1.ShipmentCalculationService
// is not deployed.
type sandboxShipmentCalculationPort struct{ log *slog.Logger }

func (p grpcShipmentCalculationPort) EstimateShipment(ctx context.Context, request *shippingv1.EstimateShipmentRequest) (*shippingv1.EstimateShipmentResponse, error) {
	return p.client.EstimateShipment(ctx, request)
}

func (p sandboxShipmentCalculationPort) EstimateShipment(ctx context.Context, request *shippingv1.EstimateShipmentRequest) (*shippingv1.EstimateShipmentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateShipment")
	return &shippingv1.EstimateShipmentResponse{}, nil
}

func newShipmentCalculationPort(deps platform.Deps) shipmentCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxShipmentCalculationPort{log: deps.Logger}
	}
	return grpcShipmentCalculationPort{client: shippingv1.NewShipmentCalculationServiceClient(deps.Conn)}
}

// shipmentValidationPort is what the shipping domain needs of acme.shipping.v1.ShipmentValidationService.
type shipmentValidationPort interface {
	ExplainShipmentDecision(ctx context.Context, request *shippingv1.ExplainShipmentDecisionRequest) (*shippingv1.ExplainShipmentDecisionResponse, error)
	GetShipmentRules(ctx context.Context, request *shippingv1.GetShipmentRulesRequest) (*shippingv1.GetShipmentRulesResponse, error)
}

// grpcShipmentValidationPort reaches acme.shipping.v1.ShipmentValidationService over gRPC.
type grpcShipmentValidationPort struct {
	client shippingv1.ShipmentValidationServiceClient
}

// sandboxShipmentValidationPort answers in the sandbox environment, where acme.shipping.v1.ShipmentValidationService is
// not deployed.
type sandboxShipmentValidationPort struct{ log *slog.Logger }

func (p grpcShipmentValidationPort) ExplainShipmentDecision(ctx context.Context, request *shippingv1.ExplainShipmentDecisionRequest) (*shippingv1.ExplainShipmentDecisionResponse, error) {
	return p.client.ExplainShipmentDecision(ctx, request)
}

func (p sandboxShipmentValidationPort) ExplainShipmentDecision(ctx context.Context, request *shippingv1.ExplainShipmentDecisionRequest) (*shippingv1.ExplainShipmentDecisionResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ExplainShipmentDecision")
	return &shippingv1.ExplainShipmentDecisionResponse{}, nil
}

func (p grpcShipmentValidationPort) GetShipmentRules(ctx context.Context, request *shippingv1.GetShipmentRulesRequest) (*shippingv1.GetShipmentRulesResponse, error) {
	return p.client.GetShipmentRules(ctx, request)
}

func (p sandboxShipmentValidationPort) GetShipmentRules(ctx context.Context, request *shippingv1.GetShipmentRulesRequest) (*shippingv1.GetShipmentRulesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetShipmentRules")
	return &shippingv1.GetShipmentRulesResponse{}, nil
}

func newShipmentValidationPort(deps platform.Deps) shipmentValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxShipmentValidationPort{log: deps.Logger}
	}
	return grpcShipmentValidationPort{client: shippingv1.NewShipmentValidationServiceClient(deps.Conn)}
}

// trackingEventCalculationPort is what the shipping domain needs of acme.shipping.v1.TrackingEventCalculationService.
type trackingEventCalculationPort interface {
	CalculateTrackingEvent(ctx context.Context, request *shippingv1.CalculateTrackingEventRequest) (*shippingv1.CalculateTrackingEventResponse, error)
}

// grpcTrackingEventCalculationPort reaches acme.shipping.v1.TrackingEventCalculationService over gRPC.
type grpcTrackingEventCalculationPort struct {
	client shippingv1.TrackingEventCalculationServiceClient
}

// sandboxTrackingEventCalculationPort answers in the sandbox environment, where
// acme.shipping.v1.TrackingEventCalculationService is not deployed.
type sandboxTrackingEventCalculationPort struct{ log *slog.Logger }

func (p grpcTrackingEventCalculationPort) CalculateTrackingEvent(ctx context.Context, request *shippingv1.CalculateTrackingEventRequest) (*shippingv1.CalculateTrackingEventResponse, error) {
	return p.client.CalculateTrackingEvent(ctx, request)
}

func (p sandboxTrackingEventCalculationPort) CalculateTrackingEvent(ctx context.Context, request *shippingv1.CalculateTrackingEventRequest) (*shippingv1.CalculateTrackingEventResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateTrackingEvent")
	return &shippingv1.CalculateTrackingEventResponse{}, nil
}

func newTrackingEventCalculationPort(deps platform.Deps) trackingEventCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxTrackingEventCalculationPort{log: deps.Logger}
	}
	return grpcTrackingEventCalculationPort{client: shippingv1.NewTrackingEventCalculationServiceClient(deps.Conn)}
}
