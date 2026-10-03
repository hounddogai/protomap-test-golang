package subscriptions

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// collectionApprovalPort is what the subscriptions domain needs of acme.merchandising.v1.CollectionApprovalService.
type collectionApprovalPort interface {
	CommentCollectionApproval(ctx context.Context, request *merchandisingv1.CommentCollectionApprovalRequest) (*merchandisingv1.CommentCollectionApprovalResponse, error)
}

// grpcCollectionApprovalPort reaches acme.merchandising.v1.CollectionApprovalService over gRPC.
type grpcCollectionApprovalPort struct {
	client merchandisingv1.CollectionApprovalServiceClient
}

// sandboxCollectionApprovalPort answers in the sandbox environment, where
// acme.merchandising.v1.CollectionApprovalService is not deployed.
type sandboxCollectionApprovalPort struct{ log *slog.Logger }

func (p grpcCollectionApprovalPort) CommentCollectionApproval(ctx context.Context, request *merchandisingv1.CommentCollectionApprovalRequest) (*merchandisingv1.CommentCollectionApprovalResponse, error) {
	return p.client.CommentCollectionApproval(ctx, request)
}

func (p sandboxCollectionApprovalPort) CommentCollectionApproval(ctx context.Context, request *merchandisingv1.CommentCollectionApprovalRequest) (*merchandisingv1.CommentCollectionApprovalResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CommentCollectionApproval")
	return &merchandisingv1.CommentCollectionApprovalResponse{}, nil
}

func newCollectionApprovalPort(deps platform.Deps) collectionApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCollectionApprovalPort{log: deps.Logger}
	}
	return grpcCollectionApprovalPort{client: merchandisingv1.NewCollectionApprovalServiceClient(deps.Conn)}
}

// consentRecordValidationPort is what the subscriptions domain needs of
// acme.customers.v1.ConsentRecordValidationService.
type consentRecordValidationPort interface {
	ScoreConsentRecord(ctx context.Context, request *customersv1.ScoreConsentRecordRequest) (*customersv1.ScoreConsentRecordResponse, error)
}

// grpcConsentRecordValidationPort reaches acme.customers.v1.ConsentRecordValidationService over gRPC.
type grpcConsentRecordValidationPort struct {
	client customersv1.ConsentRecordValidationServiceClient
}

// sandboxConsentRecordValidationPort answers in the sandbox environment, where
// acme.customers.v1.ConsentRecordValidationService is not deployed.
type sandboxConsentRecordValidationPort struct{ log *slog.Logger }

func (p grpcConsentRecordValidationPort) ScoreConsentRecord(ctx context.Context, request *customersv1.ScoreConsentRecordRequest) (*customersv1.ScoreConsentRecordResponse, error) {
	return p.client.ScoreConsentRecord(ctx, request)
}

func (p sandboxConsentRecordValidationPort) ScoreConsentRecord(ctx context.Context, request *customersv1.ScoreConsentRecordRequest) (*customersv1.ScoreConsentRecordResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScoreConsentRecord")
	return &customersv1.ScoreConsentRecordResponse{}, nil
}

func newConsentRecordValidationPort(deps platform.Deps) consentRecordValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxConsentRecordValidationPort{log: deps.Logger}
	}
	return grpcConsentRecordValidationPort{client: customersv1.NewConsentRecordValidationServiceClient(deps.Conn)}
}

// notificationQueryPort is what the subscriptions domain needs of acme.notifications.v1.NotificationQueryService.
type notificationQueryPort interface {
	CountNotifications(ctx context.Context, request *notificationsv1.CountNotificationsRequest) (*notificationsv1.CountNotificationsResponse, error)
}

// grpcNotificationQueryPort reaches acme.notifications.v1.NotificationQueryService over gRPC.
type grpcNotificationQueryPort struct {
	client notificationsv1.NotificationQueryServiceClient
}

// sandboxNotificationQueryPort answers in the sandbox environment, where acme.notifications.v1.NotificationQueryService
// is not deployed.
type sandboxNotificationQueryPort struct{ log *slog.Logger }

func (p grpcNotificationQueryPort) CountNotifications(ctx context.Context, request *notificationsv1.CountNotificationsRequest) (*notificationsv1.CountNotificationsResponse, error) {
	return p.client.CountNotifications(ctx, request)
}

func (p sandboxNotificationQueryPort) CountNotifications(ctx context.Context, request *notificationsv1.CountNotificationsRequest) (*notificationsv1.CountNotificationsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CountNotifications")
	return &notificationsv1.CountNotificationsResponse{}, nil
}

func newNotificationQueryPort(deps platform.Deps) notificationQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxNotificationQueryPort{log: deps.Logger}
	}
	return grpcNotificationQueryPort{client: notificationsv1.NewNotificationQueryServiceClient(deps.Conn)}
}

// preferenceCalculationPort is what the subscriptions domain needs of acme.customers.v1.PreferenceCalculationService.
type preferenceCalculationPort interface {
	CalculatePreference(ctx context.Context, request *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error)
}

// grpcPreferenceCalculationPort reaches acme.customers.v1.PreferenceCalculationService over gRPC.
type grpcPreferenceCalculationPort struct {
	client customersv1.PreferenceCalculationServiceClient
}

// sandboxPreferenceCalculationPort answers in the sandbox environment, where
// acme.customers.v1.PreferenceCalculationService is not deployed.
type sandboxPreferenceCalculationPort struct{ log *slog.Logger }

func (p grpcPreferenceCalculationPort) CalculatePreference(ctx context.Context, request *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error) {
	return p.client.CalculatePreference(ctx, request)
}

func (p sandboxPreferenceCalculationPort) CalculatePreference(ctx context.Context, request *customersv1.CalculatePreferenceRequest) (*customersv1.CalculatePreferenceResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculatePreference")
	return &customersv1.CalculatePreferenceResponse{}, nil
}

func newPreferenceCalculationPort(deps platform.Deps) preferenceCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPreferenceCalculationPort{log: deps.Logger}
	}
	return grpcPreferenceCalculationPort{client: customersv1.NewPreferenceCalculationServiceClient(deps.Conn)}
}

// renewalScheduleApprovalPort is what the subscriptions domain needs of
// acme.subscriptions.v1.RenewalScheduleApprovalService.
type renewalScheduleApprovalPort interface {
	GetRenewalScheduleApprovalStatus(ctx context.Context, request *subscriptionsv1.GetRenewalScheduleApprovalStatusRequest) (*subscriptionsv1.GetRenewalScheduleApprovalStatusResponse, error)
}

// grpcRenewalScheduleApprovalPort reaches acme.subscriptions.v1.RenewalScheduleApprovalService over gRPC.
type grpcRenewalScheduleApprovalPort struct {
	client subscriptionsv1.RenewalScheduleApprovalServiceClient
}

// sandboxRenewalScheduleApprovalPort answers in the sandbox environment, where
// acme.subscriptions.v1.RenewalScheduleApprovalService is not deployed.
type sandboxRenewalScheduleApprovalPort struct{ log *slog.Logger }

func (p grpcRenewalScheduleApprovalPort) GetRenewalScheduleApprovalStatus(ctx context.Context, request *subscriptionsv1.GetRenewalScheduleApprovalStatusRequest) (*subscriptionsv1.GetRenewalScheduleApprovalStatusResponse, error) {
	return p.client.GetRenewalScheduleApprovalStatus(ctx, request)
}

func (p sandboxRenewalScheduleApprovalPort) GetRenewalScheduleApprovalStatus(ctx context.Context, request *subscriptionsv1.GetRenewalScheduleApprovalStatusRequest) (*subscriptionsv1.GetRenewalScheduleApprovalStatusResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetRenewalScheduleApprovalStatus")
	return &subscriptionsv1.GetRenewalScheduleApprovalStatusResponse{}, nil
}

func newRenewalScheduleApprovalPort(deps platform.Deps) renewalScheduleApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRenewalScheduleApprovalPort{log: deps.Logger}
	}
	return grpcRenewalScheduleApprovalPort{client: subscriptionsv1.NewRenewalScheduleApprovalServiceClient(deps.Conn)}
}

// reviewModerationValidationPort is what the subscriptions domain needs of
// acme.reviews.v1.ReviewModerationValidationService.
type reviewModerationValidationPort interface {
	ValidateReviewModeration(ctx context.Context, request *reviewsv1.ValidateReviewModerationRequest) (*reviewsv1.ValidateReviewModerationResponse, error)
}

// grpcReviewModerationValidationPort reaches acme.reviews.v1.ReviewModerationValidationService over gRPC.
type grpcReviewModerationValidationPort struct {
	client reviewsv1.ReviewModerationValidationServiceClient
}

// sandboxReviewModerationValidationPort answers in the sandbox environment, where
// acme.reviews.v1.ReviewModerationValidationService is not deployed.
type sandboxReviewModerationValidationPort struct{ log *slog.Logger }

func (p grpcReviewModerationValidationPort) ValidateReviewModeration(ctx context.Context, request *reviewsv1.ValidateReviewModerationRequest) (*reviewsv1.ValidateReviewModerationResponse, error) {
	return p.client.ValidateReviewModeration(ctx, request)
}

func (p sandboxReviewModerationValidationPort) ValidateReviewModeration(ctx context.Context, request *reviewsv1.ValidateReviewModerationRequest) (*reviewsv1.ValidateReviewModerationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidateReviewModeration")
	return &reviewsv1.ValidateReviewModerationResponse{}, nil
}

func newReviewModerationValidationPort(deps platform.Deps) reviewModerationValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReviewModerationValidationPort{log: deps.Logger}
	}
	return grpcReviewModerationValidationPort{client: reviewsv1.NewReviewModerationValidationServiceClient(deps.Conn)}
}

// rewardWorkflowPort is what the subscriptions domain needs of acme.loyalty.v1.RewardWorkflowService.
type rewardWorkflowPort interface {
	AdvanceRewardWorkflow(ctx context.Context, request *loyaltyv1.AdvanceRewardWorkflowRequest) (*loyaltyv1.AdvanceRewardWorkflowResponse, error)
}

// grpcRewardWorkflowPort reaches acme.loyalty.v1.RewardWorkflowService over gRPC.
type grpcRewardWorkflowPort struct {
	client loyaltyv1.RewardWorkflowServiceClient
}

// sandboxRewardWorkflowPort answers in the sandbox environment, where acme.loyalty.v1.RewardWorkflowService is not
// deployed.
type sandboxRewardWorkflowPort struct{ log *slog.Logger }

func (p grpcRewardWorkflowPort) AdvanceRewardWorkflow(ctx context.Context, request *loyaltyv1.AdvanceRewardWorkflowRequest) (*loyaltyv1.AdvanceRewardWorkflowResponse, error) {
	return p.client.AdvanceRewardWorkflow(ctx, request)
}

func (p sandboxRewardWorkflowPort) AdvanceRewardWorkflow(ctx context.Context, request *loyaltyv1.AdvanceRewardWorkflowRequest) (*loyaltyv1.AdvanceRewardWorkflowResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers AdvanceRewardWorkflow")
	return &loyaltyv1.AdvanceRewardWorkflowResponse{}, nil
}

func newRewardWorkflowPort(deps platform.Deps) rewardWorkflowPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRewardWorkflowPort{log: deps.Logger}
	}
	return grpcRewardWorkflowPort{client: loyaltyv1.NewRewardWorkflowServiceClient(deps.Conn)}
}

// subscriptionPlanValidationPort is what the subscriptions domain needs of
// acme.subscriptions.v1.SubscriptionPlanValidationService.
type subscriptionPlanValidationPort interface {
	PreviewSubscriptionPlan(ctx context.Context, request *subscriptionsv1.PreviewSubscriptionPlanRequest) (*subscriptionsv1.PreviewSubscriptionPlanResponse, error)
}

// grpcSubscriptionPlanValidationPort reaches acme.subscriptions.v1.SubscriptionPlanValidationService over gRPC.
type grpcSubscriptionPlanValidationPort struct {
	client subscriptionsv1.SubscriptionPlanValidationServiceClient
}

// sandboxSubscriptionPlanValidationPort answers in the sandbox environment, where
// acme.subscriptions.v1.SubscriptionPlanValidationService is not deployed.
type sandboxSubscriptionPlanValidationPort struct{ log *slog.Logger }

func (p grpcSubscriptionPlanValidationPort) PreviewSubscriptionPlan(ctx context.Context, request *subscriptionsv1.PreviewSubscriptionPlanRequest) (*subscriptionsv1.PreviewSubscriptionPlanResponse, error) {
	return p.client.PreviewSubscriptionPlan(ctx, request)
}

func (p sandboxSubscriptionPlanValidationPort) PreviewSubscriptionPlan(ctx context.Context, request *subscriptionsv1.PreviewSubscriptionPlanRequest) (*subscriptionsv1.PreviewSubscriptionPlanResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewSubscriptionPlan")
	return &subscriptionsv1.PreviewSubscriptionPlanResponse{}, nil
}

func newSubscriptionPlanValidationPort(deps platform.Deps) subscriptionPlanValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSubscriptionPlanValidationPort{log: deps.Logger}
	}
	return grpcSubscriptionPlanValidationPort{client: subscriptionsv1.NewSubscriptionPlanValidationServiceClient(deps.Conn)}
}

// tenantAdminPort is what the subscriptions domain needs of acme.identity.v1.TenantAdminService.
type tenantAdminPort interface {
	ArchiveTenant(ctx context.Context, request *identityv1.ArchiveTenantRequest) (*identityv1.ArchiveTenantResponse, error)
}

// grpcTenantAdminPort reaches acme.identity.v1.TenantAdminService over gRPC.
type grpcTenantAdminPort struct {
	client identityv1.TenantAdminServiceClient
}

// sandboxTenantAdminPort answers in the sandbox environment, where acme.identity.v1.TenantAdminService is not deployed.
type sandboxTenantAdminPort struct{ log *slog.Logger }

func (p grpcTenantAdminPort) ArchiveTenant(ctx context.Context, request *identityv1.ArchiveTenantRequest) (*identityv1.ArchiveTenantResponse, error) {
	return p.client.ArchiveTenant(ctx, request)
}

func (p sandboxTenantAdminPort) ArchiveTenant(ctx context.Context, request *identityv1.ArchiveTenantRequest) (*identityv1.ArchiveTenantResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ArchiveTenant")
	return &identityv1.ArchiveTenantResponse{}, nil
}

func newTenantAdminPort(deps platform.Deps) tenantAdminPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxTenantAdminPort{log: deps.Logger}
	}
	return grpcTenantAdminPort{client: identityv1.NewTenantAdminServiceClient(deps.Conn)}
}

// usageRecordCalculationPort is what the subscriptions domain needs of
// acme.subscriptions.v1.UsageRecordCalculationService.
type usageRecordCalculationPort interface {
	EstimateUsageRecord(ctx context.Context, request *subscriptionsv1.EstimateUsageRecordRequest) (*subscriptionsv1.EstimateUsageRecordResponse, error)
}

// grpcUsageRecordCalculationPort reaches acme.subscriptions.v1.UsageRecordCalculationService over gRPC.
type grpcUsageRecordCalculationPort struct {
	client subscriptionsv1.UsageRecordCalculationServiceClient
}

// sandboxUsageRecordCalculationPort answers in the sandbox environment, where
// acme.subscriptions.v1.UsageRecordCalculationService is not deployed.
type sandboxUsageRecordCalculationPort struct{ log *slog.Logger }

func (p grpcUsageRecordCalculationPort) EstimateUsageRecord(ctx context.Context, request *subscriptionsv1.EstimateUsageRecordRequest) (*subscriptionsv1.EstimateUsageRecordResponse, error) {
	return p.client.EstimateUsageRecord(ctx, request)
}

func (p sandboxUsageRecordCalculationPort) EstimateUsageRecord(ctx context.Context, request *subscriptionsv1.EstimateUsageRecordRequest) (*subscriptionsv1.EstimateUsageRecordResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateUsageRecord")
	return &subscriptionsv1.EstimateUsageRecordResponse{}, nil
}

func newUsageRecordCalculationPort(deps platform.Deps) usageRecordCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxUsageRecordCalculationPort{log: deps.Logger}
	}
	return grpcUsageRecordCalculationPort{client: subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn)}
}

// usageRecordValidationPort is what the subscriptions domain needs of
// acme.subscriptions.v1.UsageRecordValidationService.
type usageRecordValidationPort interface {
	ScoreUsageRecord(ctx context.Context, request *subscriptionsv1.ScoreUsageRecordRequest) (*subscriptionsv1.ScoreUsageRecordResponse, error)
}

// grpcUsageRecordValidationPort reaches acme.subscriptions.v1.UsageRecordValidationService over gRPC.
type grpcUsageRecordValidationPort struct {
	client subscriptionsv1.UsageRecordValidationServiceClient
}

// sandboxUsageRecordValidationPort answers in the sandbox environment, where
// acme.subscriptions.v1.UsageRecordValidationService is not deployed.
type sandboxUsageRecordValidationPort struct{ log *slog.Logger }

func (p grpcUsageRecordValidationPort) ScoreUsageRecord(ctx context.Context, request *subscriptionsv1.ScoreUsageRecordRequest) (*subscriptionsv1.ScoreUsageRecordResponse, error) {
	return p.client.ScoreUsageRecord(ctx, request)
}

func (p sandboxUsageRecordValidationPort) ScoreUsageRecord(ctx context.Context, request *subscriptionsv1.ScoreUsageRecordRequest) (*subscriptionsv1.ScoreUsageRecordResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScoreUsageRecord")
	return &subscriptionsv1.ScoreUsageRecordResponse{}, nil
}

func newUsageRecordValidationPort(deps platform.Deps) usageRecordValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxUsageRecordValidationPort{log: deps.Logger}
	}
	return grpcUsageRecordValidationPort{client: subscriptionsv1.NewUsageRecordValidationServiceClient(deps.Conn)}
}
