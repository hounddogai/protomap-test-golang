package customers

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	keymanagementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/keymanagement/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	modelregistryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/modelregistry/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// apiCredentialNotificationPort is what the customers domain needs of
// acme.identity.v1.ApiCredentialNotificationService.
type apiCredentialNotificationPort interface {
	NotifyApiCredentialWatchers(ctx context.Context, request *identityv1.NotifyApiCredentialWatchersRequest) (*identityv1.NotifyApiCredentialWatchersResponse, error)
}

// grpcApiCredentialNotificationPort reaches acme.identity.v1.ApiCredentialNotificationService over gRPC.
type grpcApiCredentialNotificationPort struct {
	client identityv1.ApiCredentialNotificationServiceClient
}

// sandboxApiCredentialNotificationPort answers in the sandbox environment, where
// acme.identity.v1.ApiCredentialNotificationService is not deployed.
type sandboxApiCredentialNotificationPort struct{ log *slog.Logger }

func (p grpcApiCredentialNotificationPort) NotifyApiCredentialWatchers(ctx context.Context, request *identityv1.NotifyApiCredentialWatchersRequest) (*identityv1.NotifyApiCredentialWatchersResponse, error) {
	return p.client.NotifyApiCredentialWatchers(ctx, request)
}

func (p sandboxApiCredentialNotificationPort) NotifyApiCredentialWatchers(ctx context.Context, request *identityv1.NotifyApiCredentialWatchersRequest) (*identityv1.NotifyApiCredentialWatchersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NotifyApiCredentialWatchers")
	return &identityv1.NotifyApiCredentialWatchersResponse{}, nil
}

func newApiCredentialNotificationPort(deps platform.Deps) apiCredentialNotificationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxApiCredentialNotificationPort{log: deps.Logger}
	}
	return grpcApiCredentialNotificationPort{client: identityv1.NewApiCredentialNotificationServiceClient(deps.Conn)}
}

// carrierRateCalculationPort is what the customers domain needs of acme.shipping.v1.CarrierRateCalculationService.
type carrierRateCalculationPort interface {
	SimulateCarrierRate(ctx context.Context, request *shippingv1.SimulateCarrierRateRequest) (*shippingv1.SimulateCarrierRateResponse, error)
}

// grpcCarrierRateCalculationPort reaches acme.shipping.v1.CarrierRateCalculationService over gRPC.
type grpcCarrierRateCalculationPort struct {
	client shippingv1.CarrierRateCalculationServiceClient
}

// sandboxCarrierRateCalculationPort answers in the sandbox environment, where
// acme.shipping.v1.CarrierRateCalculationService is not deployed.
type sandboxCarrierRateCalculationPort struct{ log *slog.Logger }

func (p grpcCarrierRateCalculationPort) SimulateCarrierRate(ctx context.Context, request *shippingv1.SimulateCarrierRateRequest) (*shippingv1.SimulateCarrierRateResponse, error) {
	return p.client.SimulateCarrierRate(ctx, request)
}

func (p sandboxCarrierRateCalculationPort) SimulateCarrierRate(ctx context.Context, request *shippingv1.SimulateCarrierRateRequest) (*shippingv1.SimulateCarrierRateResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SimulateCarrierRate")
	return &shippingv1.SimulateCarrierRateResponse{}, nil
}

func newCarrierRateCalculationPort(deps platform.Deps) carrierRateCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCarrierRateCalculationPort{log: deps.Logger}
	}
	return grpcCarrierRateCalculationPort{client: shippingv1.NewCarrierRateCalculationServiceClient(deps.Conn)}
}

// consentRecordCalculationPort is what the customers domain needs of acme.customers.v1.ConsentRecordCalculationService.
type consentRecordCalculationPort interface {
	CalculateConsentRecord(ctx context.Context, request *customersv1.CalculateConsentRecordRequest) (*customersv1.CalculateConsentRecordResponse, error)
}

// grpcConsentRecordCalculationPort reaches acme.customers.v1.ConsentRecordCalculationService over gRPC.
type grpcConsentRecordCalculationPort struct {
	client customersv1.ConsentRecordCalculationServiceClient
}

// sandboxConsentRecordCalculationPort answers in the sandbox environment, where
// acme.customers.v1.ConsentRecordCalculationService is not deployed.
type sandboxConsentRecordCalculationPort struct{ log *slog.Logger }

func (p grpcConsentRecordCalculationPort) CalculateConsentRecord(ctx context.Context, request *customersv1.CalculateConsentRecordRequest) (*customersv1.CalculateConsentRecordResponse, error) {
	return p.client.CalculateConsentRecord(ctx, request)
}

func (p sandboxConsentRecordCalculationPort) CalculateConsentRecord(ctx context.Context, request *customersv1.CalculateConsentRecordRequest) (*customersv1.CalculateConsentRecordResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculateConsentRecord")
	return &customersv1.CalculateConsentRecordResponse{}, nil
}

func newConsentRecordCalculationPort(deps platform.Deps) consentRecordCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxConsentRecordCalculationPort{log: deps.Logger}
	}
	return grpcConsentRecordCalculationPort{client: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn)}
}

// consentRecordSyncPort is what the customers domain needs of acme.customers.v1.ConsentRecordSyncService.
type consentRecordSyncPort interface {
	PullConsentRecords(ctx context.Context, request *customersv1.PullConsentRecordsRequest) (*customersv1.PullConsentRecordsResponse, error)
}

// grpcConsentRecordSyncPort reaches acme.customers.v1.ConsentRecordSyncService over gRPC.
type grpcConsentRecordSyncPort struct {
	client customersv1.ConsentRecordSyncServiceClient
}

// sandboxConsentRecordSyncPort answers in the sandbox environment, where acme.customers.v1.ConsentRecordSyncService is
// not deployed.
type sandboxConsentRecordSyncPort struct{ log *slog.Logger }

func (p grpcConsentRecordSyncPort) PullConsentRecords(ctx context.Context, request *customersv1.PullConsentRecordsRequest) (*customersv1.PullConsentRecordsResponse, error) {
	return p.client.PullConsentRecords(ctx, request)
}

func (p sandboxConsentRecordSyncPort) PullConsentRecords(ctx context.Context, request *customersv1.PullConsentRecordsRequest) (*customersv1.PullConsentRecordsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PullConsentRecords")
	return &customersv1.PullConsentRecordsResponse{}, nil
}

func newConsentRecordSyncPort(deps platform.Deps) consentRecordSyncPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxConsentRecordSyncPort{log: deps.Logger}
	}
	return grpcConsentRecordSyncPort{client: customersv1.NewConsentRecordSyncServiceClient(deps.Conn)}
}

// cryptoKeyQueryPort is what the customers domain needs of acme.keymanagement.v1.CryptoKeyQueryService.
type cryptoKeyQueryPort interface {
	SuggestCryptoKeys(ctx context.Context, request *keymanagementv1.SuggestCryptoKeysRequest) (*keymanagementv1.SuggestCryptoKeysResponse, error)
}

// grpcCryptoKeyQueryPort reaches acme.keymanagement.v1.CryptoKeyQueryService over gRPC.
type grpcCryptoKeyQueryPort struct {
	client keymanagementv1.CryptoKeyQueryServiceClient
}

// sandboxCryptoKeyQueryPort answers in the sandbox environment, where acme.keymanagement.v1.CryptoKeyQueryService is
// not deployed.
type sandboxCryptoKeyQueryPort struct{ log *slog.Logger }

func (p grpcCryptoKeyQueryPort) SuggestCryptoKeys(ctx context.Context, request *keymanagementv1.SuggestCryptoKeysRequest) (*keymanagementv1.SuggestCryptoKeysResponse, error) {
	return p.client.SuggestCryptoKeys(ctx, request)
}

func (p sandboxCryptoKeyQueryPort) SuggestCryptoKeys(ctx context.Context, request *keymanagementv1.SuggestCryptoKeysRequest) (*keymanagementv1.SuggestCryptoKeysResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SuggestCryptoKeys")
	return &keymanagementv1.SuggestCryptoKeysResponse{}, nil
}

func newCryptoKeyQueryPort(deps platform.Deps) cryptoKeyQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCryptoKeyQueryPort{log: deps.Logger}
	}
	return grpcCryptoKeyQueryPort{client: keymanagementv1.NewCryptoKeyQueryServiceClient(deps.Conn)}
}

// customerCalculationPort is what the customers domain needs of acme.customers.v1.CustomerCalculationService.
type customerCalculationPort interface {
	SimulateCustomer(ctx context.Context, request *customersv1.SimulateCustomerRequest) (*customersv1.SimulateCustomerResponse, error)
}

// grpcCustomerCalculationPort reaches acme.customers.v1.CustomerCalculationService over gRPC.
type grpcCustomerCalculationPort struct {
	client customersv1.CustomerCalculationServiceClient
}

// sandboxCustomerCalculationPort answers in the sandbox environment, where acme.customers.v1.CustomerCalculationService
// is not deployed.
type sandboxCustomerCalculationPort struct{ log *slog.Logger }

func (p grpcCustomerCalculationPort) SimulateCustomer(ctx context.Context, request *customersv1.SimulateCustomerRequest) (*customersv1.SimulateCustomerResponse, error) {
	return p.client.SimulateCustomer(ctx, request)
}

func (p sandboxCustomerCalculationPort) SimulateCustomer(ctx context.Context, request *customersv1.SimulateCustomerRequest) (*customersv1.SimulateCustomerResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SimulateCustomer")
	return &customersv1.SimulateCustomerResponse{}, nil
}

func newCustomerCalculationPort(deps platform.Deps) customerCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerCalculationPort{log: deps.Logger}
	}
	return grpcCustomerCalculationPort{client: customersv1.NewCustomerCalculationServiceClient(deps.Conn)}
}

// customerSegmentImportPort is what the customers domain needs of acme.customers.v1.CustomerSegmentImportService.
type customerSegmentImportPort interface {
	CancelCustomerSegmentImport(ctx context.Context, request *customersv1.CancelCustomerSegmentImportRequest) (*customersv1.CancelCustomerSegmentImportResponse, error)
	RetryCustomerSegmentImport(ctx context.Context, request *customersv1.RetryCustomerSegmentImportRequest) (*customersv1.RetryCustomerSegmentImportResponse, error)
}

// grpcCustomerSegmentImportPort reaches acme.customers.v1.CustomerSegmentImportService over gRPC.
type grpcCustomerSegmentImportPort struct {
	client customersv1.CustomerSegmentImportServiceClient
}

// sandboxCustomerSegmentImportPort answers in the sandbox environment, where
// acme.customers.v1.CustomerSegmentImportService is not deployed.
type sandboxCustomerSegmentImportPort struct{ log *slog.Logger }

func (p grpcCustomerSegmentImportPort) CancelCustomerSegmentImport(ctx context.Context, request *customersv1.CancelCustomerSegmentImportRequest) (*customersv1.CancelCustomerSegmentImportResponse, error) {
	return p.client.CancelCustomerSegmentImport(ctx, request)
}

func (p sandboxCustomerSegmentImportPort) CancelCustomerSegmentImport(ctx context.Context, request *customersv1.CancelCustomerSegmentImportRequest) (*customersv1.CancelCustomerSegmentImportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CancelCustomerSegmentImport")
	return &customersv1.CancelCustomerSegmentImportResponse{}, nil
}

func (p grpcCustomerSegmentImportPort) RetryCustomerSegmentImport(ctx context.Context, request *customersv1.RetryCustomerSegmentImportRequest) (*customersv1.RetryCustomerSegmentImportResponse, error) {
	return p.client.RetryCustomerSegmentImport(ctx, request)
}

func (p sandboxCustomerSegmentImportPort) RetryCustomerSegmentImport(ctx context.Context, request *customersv1.RetryCustomerSegmentImportRequest) (*customersv1.RetryCustomerSegmentImportResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RetryCustomerSegmentImport")
	return &customersv1.RetryCustomerSegmentImportResponse{}, nil
}

func newCustomerSegmentImportPort(deps platform.Deps) customerSegmentImportPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerSegmentImportPort{log: deps.Logger}
	}
	return grpcCustomerSegmentImportPort{client: customersv1.NewCustomerSegmentImportServiceClient(deps.Conn)}
}

// customerValidationPort is what the customers domain needs of acme.customers.v1.CustomerValidationService.
type customerValidationPort interface {
	CheckCustomerEligibility(ctx context.Context, request *customersv1.CheckCustomerEligibilityRequest) (*customersv1.CheckCustomerEligibilityResponse, error)
}

// grpcCustomerValidationPort reaches acme.customers.v1.CustomerValidationService over gRPC.
type grpcCustomerValidationPort struct {
	client customersv1.CustomerValidationServiceClient
}

// sandboxCustomerValidationPort answers in the sandbox environment, where acme.customers.v1.CustomerValidationService
// is not deployed.
type sandboxCustomerValidationPort struct{ log *slog.Logger }

func (p grpcCustomerValidationPort) CheckCustomerEligibility(ctx context.Context, request *customersv1.CheckCustomerEligibilityRequest) (*customersv1.CheckCustomerEligibilityResponse, error) {
	return p.client.CheckCustomerEligibility(ctx, request)
}

func (p sandboxCustomerValidationPort) CheckCustomerEligibility(ctx context.Context, request *customersv1.CheckCustomerEligibilityRequest) (*customersv1.CheckCustomerEligibilityResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CheckCustomerEligibility")
	return &customersv1.CheckCustomerEligibilityResponse{}, nil
}

func newCustomerValidationPort(deps platform.Deps) customerValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerValidationPort{log: deps.Logger}
	}
	return grpcCustomerValidationPort{client: customersv1.NewCustomerValidationServiceClient(deps.Conn)}
}

// householdQueryPort is what the customers domain needs of acme.customers.v1.HouseholdQueryService.
type householdQueryPort interface {
	FindHouseholds(ctx context.Context, request *customersv1.FindHouseholdsRequest) (*customersv1.FindHouseholdsResponse, error)
}

// grpcHouseholdQueryPort reaches acme.customers.v1.HouseholdQueryService over gRPC.
type grpcHouseholdQueryPort struct {
	client customersv1.HouseholdQueryServiceClient
}

// sandboxHouseholdQueryPort answers in the sandbox environment, where acme.customers.v1.HouseholdQueryService is not
// deployed.
type sandboxHouseholdQueryPort struct{ log *slog.Logger }

func (p grpcHouseholdQueryPort) FindHouseholds(ctx context.Context, request *customersv1.FindHouseholdsRequest) (*customersv1.FindHouseholdsResponse, error) {
	return p.client.FindHouseholds(ctx, request)
}

func (p sandboxHouseholdQueryPort) FindHouseholds(ctx context.Context, request *customersv1.FindHouseholdsRequest) (*customersv1.FindHouseholdsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers FindHouseholds")
	return &customersv1.FindHouseholdsResponse{}, nil
}

func newHouseholdQueryPort(deps platform.Deps) householdQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxHouseholdQueryPort{log: deps.Logger}
	}
	return grpcHouseholdQueryPort{client: customersv1.NewHouseholdQueryServiceClient(deps.Conn)}
}

// householdValidationPort is what the customers domain needs of acme.customers.v1.HouseholdValidationService.
type householdValidationPort interface {
	PreviewHousehold(ctx context.Context, request *customersv1.PreviewHouseholdRequest) (*customersv1.PreviewHouseholdResponse, error)
	ValidateHousehold(ctx context.Context, request *customersv1.ValidateHouseholdRequest) (*customersv1.ValidateHouseholdResponse, error)
}

// grpcHouseholdValidationPort reaches acme.customers.v1.HouseholdValidationService over gRPC.
type grpcHouseholdValidationPort struct {
	client customersv1.HouseholdValidationServiceClient
}

// sandboxHouseholdValidationPort answers in the sandbox environment, where acme.customers.v1.HouseholdValidationService
// is not deployed.
type sandboxHouseholdValidationPort struct{ log *slog.Logger }

func (p grpcHouseholdValidationPort) PreviewHousehold(ctx context.Context, request *customersv1.PreviewHouseholdRequest) (*customersv1.PreviewHouseholdResponse, error) {
	return p.client.PreviewHousehold(ctx, request)
}

func (p sandboxHouseholdValidationPort) PreviewHousehold(ctx context.Context, request *customersv1.PreviewHouseholdRequest) (*customersv1.PreviewHouseholdResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers PreviewHousehold")
	return &customersv1.PreviewHouseholdResponse{}, nil
}

func (p grpcHouseholdValidationPort) ValidateHousehold(ctx context.Context, request *customersv1.ValidateHouseholdRequest) (*customersv1.ValidateHouseholdResponse, error) {
	return p.client.ValidateHousehold(ctx, request)
}

func (p sandboxHouseholdValidationPort) ValidateHousehold(ctx context.Context, request *customersv1.ValidateHouseholdRequest) (*customersv1.ValidateHouseholdResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidateHousehold")
	return &customersv1.ValidateHouseholdResponse{}, nil
}

func newHouseholdValidationPort(deps platform.Deps) householdValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxHouseholdValidationPort{log: deps.Logger}
	}
	return grpcHouseholdValidationPort{client: customersv1.NewHouseholdValidationServiceClient(deps.Conn)}
}

// ledgerAccountValidationPort is what the customers domain needs of acme.ledger.v1.LedgerAccountValidationService.
type ledgerAccountValidationPort interface {
	NormalizeLedgerAccount(ctx context.Context, request *ledgerv1.NormalizeLedgerAccountRequest) (*ledgerv1.NormalizeLedgerAccountResponse, error)
}

// grpcLedgerAccountValidationPort reaches acme.ledger.v1.LedgerAccountValidationService over gRPC.
type grpcLedgerAccountValidationPort struct {
	client ledgerv1.LedgerAccountValidationServiceClient
}

// sandboxLedgerAccountValidationPort answers in the sandbox environment, where
// acme.ledger.v1.LedgerAccountValidationService is not deployed.
type sandboxLedgerAccountValidationPort struct{ log *slog.Logger }

func (p grpcLedgerAccountValidationPort) NormalizeLedgerAccount(ctx context.Context, request *ledgerv1.NormalizeLedgerAccountRequest) (*ledgerv1.NormalizeLedgerAccountResponse, error) {
	return p.client.NormalizeLedgerAccount(ctx, request)
}

func (p sandboxLedgerAccountValidationPort) NormalizeLedgerAccount(ctx context.Context, request *ledgerv1.NormalizeLedgerAccountRequest) (*ledgerv1.NormalizeLedgerAccountResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizeLedgerAccount")
	return &ledgerv1.NormalizeLedgerAccountResponse{}, nil
}

func newLedgerAccountValidationPort(deps platform.Deps) ledgerAccountValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxLedgerAccountValidationPort{log: deps.Logger}
	}
	return grpcLedgerAccountValidationPort{client: ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn)}
}

// loyaltyMemberValidationPort is what the customers domain needs of acme.loyalty.v1.LoyaltyMemberValidationService.
type loyaltyMemberValidationPort interface {
	ScoreLoyaltyMember(ctx context.Context, request *loyaltyv1.ScoreLoyaltyMemberRequest) (*loyaltyv1.ScoreLoyaltyMemberResponse, error)
}

// grpcLoyaltyMemberValidationPort reaches acme.loyalty.v1.LoyaltyMemberValidationService over gRPC.
type grpcLoyaltyMemberValidationPort struct {
	client loyaltyv1.LoyaltyMemberValidationServiceClient
}

// sandboxLoyaltyMemberValidationPort answers in the sandbox environment, where
// acme.loyalty.v1.LoyaltyMemberValidationService is not deployed.
type sandboxLoyaltyMemberValidationPort struct{ log *slog.Logger }

func (p grpcLoyaltyMemberValidationPort) ScoreLoyaltyMember(ctx context.Context, request *loyaltyv1.ScoreLoyaltyMemberRequest) (*loyaltyv1.ScoreLoyaltyMemberResponse, error) {
	return p.client.ScoreLoyaltyMember(ctx, request)
}

func (p sandboxLoyaltyMemberValidationPort) ScoreLoyaltyMember(ctx context.Context, request *loyaltyv1.ScoreLoyaltyMemberRequest) (*loyaltyv1.ScoreLoyaltyMemberResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ScoreLoyaltyMember")
	return &loyaltyv1.ScoreLoyaltyMemberResponse{}, nil
}

func newLoyaltyMemberValidationPort(deps platform.Deps) loyaltyMemberValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxLoyaltyMemberValidationPort{log: deps.Logger}
	}
	return grpcLoyaltyMemberValidationPort{client: loyaltyv1.NewLoyaltyMemberValidationServiceClient(deps.Conn)}
}

// notificationQueryPort is what the customers domain needs of acme.notifications.v1.NotificationQueryService.
type notificationQueryPort interface {
	SummarizeNotifications(ctx context.Context, request *notificationsv1.SummarizeNotificationsRequest) (*notificationsv1.SummarizeNotificationsResponse, error)
}

// grpcNotificationQueryPort reaches acme.notifications.v1.NotificationQueryService over gRPC.
type grpcNotificationQueryPort struct {
	client notificationsv1.NotificationQueryServiceClient
}

// sandboxNotificationQueryPort answers in the sandbox environment, where acme.notifications.v1.NotificationQueryService
// is not deployed.
type sandboxNotificationQueryPort struct{ log *slog.Logger }

func (p grpcNotificationQueryPort) SummarizeNotifications(ctx context.Context, request *notificationsv1.SummarizeNotificationsRequest) (*notificationsv1.SummarizeNotificationsResponse, error) {
	return p.client.SummarizeNotifications(ctx, request)
}

func (p sandboxNotificationQueryPort) SummarizeNotifications(ctx context.Context, request *notificationsv1.SummarizeNotificationsRequest) (*notificationsv1.SummarizeNotificationsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SummarizeNotifications")
	return &notificationsv1.SummarizeNotificationsResponse{}, nil
}

func newNotificationQueryPort(deps platform.Deps) notificationQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxNotificationQueryPort{log: deps.Logger}
	}
	return grpcNotificationQueryPort{client: notificationsv1.NewNotificationQueryServiceClient(deps.Conn)}
}

// registeredModelCalculationPort is what the customers domain needs of
// acme.modelregistry.v1.RegisteredModelCalculationService.
type registeredModelCalculationPort interface {
	EstimateRegisteredModel(ctx context.Context, request *modelregistryv1.EstimateRegisteredModelRequest) (*modelregistryv1.EstimateRegisteredModelResponse, error)
}

// grpcRegisteredModelCalculationPort reaches acme.modelregistry.v1.RegisteredModelCalculationService over gRPC.
type grpcRegisteredModelCalculationPort struct {
	client modelregistryv1.RegisteredModelCalculationServiceClient
}

// sandboxRegisteredModelCalculationPort answers in the sandbox environment, where
// acme.modelregistry.v1.RegisteredModelCalculationService is not deployed.
type sandboxRegisteredModelCalculationPort struct{ log *slog.Logger }

func (p grpcRegisteredModelCalculationPort) EstimateRegisteredModel(ctx context.Context, request *modelregistryv1.EstimateRegisteredModelRequest) (*modelregistryv1.EstimateRegisteredModelResponse, error) {
	return p.client.EstimateRegisteredModel(ctx, request)
}

func (p sandboxRegisteredModelCalculationPort) EstimateRegisteredModel(ctx context.Context, request *modelregistryv1.EstimateRegisteredModelRequest) (*modelregistryv1.EstimateRegisteredModelResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateRegisteredModel")
	return &modelregistryv1.EstimateRegisteredModelResponse{}, nil
}

func newRegisteredModelCalculationPort(deps platform.Deps) registeredModelCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRegisteredModelCalculationPort{log: deps.Logger}
	}
	return grpcRegisteredModelCalculationPort{client: modelregistryv1.NewRegisteredModelCalculationServiceClient(deps.Conn)}
}

// roleHistoryPort is what the customers domain needs of acme.identity.v1.RoleHistoryService.
type roleHistoryPort interface {
	RevertRole(ctx context.Context, request *identityv1.RevertRoleRequest) (*identityv1.RevertRoleResponse, error)
}

// grpcRoleHistoryPort reaches acme.identity.v1.RoleHistoryService over gRPC.
type grpcRoleHistoryPort struct {
	client identityv1.RoleHistoryServiceClient
}

// sandboxRoleHistoryPort answers in the sandbox environment, where acme.identity.v1.RoleHistoryService is not deployed.
type sandboxRoleHistoryPort struct{ log *slog.Logger }

func (p grpcRoleHistoryPort) RevertRole(ctx context.Context, request *identityv1.RevertRoleRequest) (*identityv1.RevertRoleResponse, error) {
	return p.client.RevertRole(ctx, request)
}

func (p sandboxRoleHistoryPort) RevertRole(ctx context.Context, request *identityv1.RevertRoleRequest) (*identityv1.RevertRoleResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RevertRole")
	return &identityv1.RevertRoleResponse{}, nil
}

func newRoleHistoryPort(deps platform.Deps) roleHistoryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRoleHistoryPort{log: deps.Logger}
	}
	return grpcRoleHistoryPort{client: identityv1.NewRoleHistoryServiceClient(deps.Conn)}
}

// searchIndexValidationPort is what the customers domain needs of acme.search.v1.SearchIndexValidationService.
type searchIndexValidationPort interface {
	NormalizeSearchIndex(ctx context.Context, request *searchv1.NormalizeSearchIndexRequest) (*searchv1.NormalizeSearchIndexResponse, error)
}

// grpcSearchIndexValidationPort reaches acme.search.v1.SearchIndexValidationService over gRPC.
type grpcSearchIndexValidationPort struct {
	client searchv1.SearchIndexValidationServiceClient
}

// sandboxSearchIndexValidationPort answers in the sandbox environment, where
// acme.search.v1.SearchIndexValidationService is not deployed.
type sandboxSearchIndexValidationPort struct{ log *slog.Logger }

func (p grpcSearchIndexValidationPort) NormalizeSearchIndex(ctx context.Context, request *searchv1.NormalizeSearchIndexRequest) (*searchv1.NormalizeSearchIndexResponse, error) {
	return p.client.NormalizeSearchIndex(ctx, request)
}

func (p sandboxSearchIndexValidationPort) NormalizeSearchIndex(ctx context.Context, request *searchv1.NormalizeSearchIndexRequest) (*searchv1.NormalizeSearchIndexResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers NormalizeSearchIndex")
	return &searchv1.NormalizeSearchIndexResponse{}, nil
}

func newSearchIndexValidationPort(deps platform.Deps) searchIndexValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSearchIndexValidationPort{log: deps.Logger}
	}
	return grpcSearchIndexValidationPort{client: searchv1.NewSearchIndexValidationServiceClient(deps.Conn)}
}

// stockReservationApprovalPort is what the customers domain needs of acme.inventory.v1.StockReservationApprovalService.
type stockReservationApprovalPort interface {
	EscalateStockReservation(ctx context.Context, request *inventoryv1.EscalateStockReservationRequest) (*inventoryv1.EscalateStockReservationResponse, error)
}

// grpcStockReservationApprovalPort reaches acme.inventory.v1.StockReservationApprovalService over gRPC.
type grpcStockReservationApprovalPort struct {
	client inventoryv1.StockReservationApprovalServiceClient
}

// sandboxStockReservationApprovalPort answers in the sandbox environment, where
// acme.inventory.v1.StockReservationApprovalService is not deployed.
type sandboxStockReservationApprovalPort struct{ log *slog.Logger }

func (p grpcStockReservationApprovalPort) EscalateStockReservation(ctx context.Context, request *inventoryv1.EscalateStockReservationRequest) (*inventoryv1.EscalateStockReservationResponse, error) {
	return p.client.EscalateStockReservation(ctx, request)
}

func (p sandboxStockReservationApprovalPort) EscalateStockReservation(ctx context.Context, request *inventoryv1.EscalateStockReservationRequest) (*inventoryv1.EscalateStockReservationResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EscalateStockReservation")
	return &inventoryv1.EscalateStockReservationResponse{}, nil
}

func newStockReservationApprovalPort(deps platform.Deps) stockReservationApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxStockReservationApprovalPort{log: deps.Logger}
	}
	return grpcStockReservationApprovalPort{client: inventoryv1.NewStockReservationApprovalServiceClient(deps.Conn)}
}

// tenantAdminPort is what the customers domain needs of acme.identity.v1.TenantAdminService.
type tenantAdminPort interface {
	InspectTenant(ctx context.Context, request *identityv1.InspectTenantRequest) (*identityv1.InspectTenantResponse, error)
}

// grpcTenantAdminPort reaches acme.identity.v1.TenantAdminService over gRPC.
type grpcTenantAdminPort struct {
	client identityv1.TenantAdminServiceClient
}

// sandboxTenantAdminPort answers in the sandbox environment, where acme.identity.v1.TenantAdminService is not deployed.
type sandboxTenantAdminPort struct{ log *slog.Logger }

func (p grpcTenantAdminPort) InspectTenant(ctx context.Context, request *identityv1.InspectTenantRequest) (*identityv1.InspectTenantResponse, error) {
	return p.client.InspectTenant(ctx, request)
}

func (p sandboxTenantAdminPort) InspectTenant(ctx context.Context, request *identityv1.InspectTenantRequest) (*identityv1.InspectTenantResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers InspectTenant")
	return &identityv1.InspectTenantResponse{}, nil
}

func newTenantAdminPort(deps platform.Deps) tenantAdminPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxTenantAdminPort{log: deps.Logger}
	}
	return grpcTenantAdminPort{client: identityv1.NewTenantAdminServiceClient(deps.Conn)}
}

// userQueryPort is what the customers domain needs of acme.identity.v1.UserQueryService.
type userQueryPort interface {
	ResolveUsers(ctx context.Context, request *identityv1.ResolveUsersRequest) (*identityv1.ResolveUsersResponse, error)
}

// grpcUserQueryPort reaches acme.identity.v1.UserQueryService over gRPC.
type grpcUserQueryPort struct {
	client identityv1.UserQueryServiceClient
}

// sandboxUserQueryPort answers in the sandbox environment, where acme.identity.v1.UserQueryService is not deployed.
type sandboxUserQueryPort struct{ log *slog.Logger }

func (p grpcUserQueryPort) ResolveUsers(ctx context.Context, request *identityv1.ResolveUsersRequest) (*identityv1.ResolveUsersResponse, error) {
	return p.client.ResolveUsers(ctx, request)
}

func (p sandboxUserQueryPort) ResolveUsers(ctx context.Context, request *identityv1.ResolveUsersRequest) (*identityv1.ResolveUsersResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ResolveUsers")
	return &identityv1.ResolveUsersResponse{}, nil
}

func newUserQueryPort(deps platform.Deps) userQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxUserQueryPort{log: deps.Logger}
	}
	return grpcUserQueryPort{client: identityv1.NewUserQueryServiceClient(deps.Conn)}
}

// userValidationPort is what the customers domain needs of acme.identity.v1.UserValidationService.
type userValidationPort interface {
	CheckUserEligibility(ctx context.Context, request *identityv1.CheckUserEligibilityRequest) (*identityv1.CheckUserEligibilityResponse, error)
}

// grpcUserValidationPort reaches acme.identity.v1.UserValidationService over gRPC.
type grpcUserValidationPort struct {
	client identityv1.UserValidationServiceClient
}

// sandboxUserValidationPort answers in the sandbox environment, where acme.identity.v1.UserValidationService is not
// deployed.
type sandboxUserValidationPort struct{ log *slog.Logger }

func (p grpcUserValidationPort) CheckUserEligibility(ctx context.Context, request *identityv1.CheckUserEligibilityRequest) (*identityv1.CheckUserEligibilityResponse, error) {
	return p.client.CheckUserEligibility(ctx, request)
}

func (p sandboxUserValidationPort) CheckUserEligibility(ctx context.Context, request *identityv1.CheckUserEligibilityRequest) (*identityv1.CheckUserEligibilityResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CheckUserEligibility")
	return &identityv1.CheckUserEligibilityResponse{}, nil
}

func newUserValidationPort(deps platform.Deps) userValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxUserValidationPort{log: deps.Logger}
	}
	return grpcUserValidationPort{client: identityv1.NewUserValidationServiceClient(deps.Conn)}
}

// watchlistCalculationPort is what the customers domain needs of acme.fraud.v1.WatchlistCalculationService.
type watchlistCalculationPort interface {
	EstimateWatchlist(ctx context.Context, request *fraudv1.EstimateWatchlistRequest) (*fraudv1.EstimateWatchlistResponse, error)
}

// grpcWatchlistCalculationPort reaches acme.fraud.v1.WatchlistCalculationService over gRPC.
type grpcWatchlistCalculationPort struct {
	client fraudv1.WatchlistCalculationServiceClient
}

// sandboxWatchlistCalculationPort answers in the sandbox environment, where acme.fraud.v1.WatchlistCalculationService
// is not deployed.
type sandboxWatchlistCalculationPort struct{ log *slog.Logger }

func (p grpcWatchlistCalculationPort) EstimateWatchlist(ctx context.Context, request *fraudv1.EstimateWatchlistRequest) (*fraudv1.EstimateWatchlistResponse, error) {
	return p.client.EstimateWatchlist(ctx, request)
}

func (p sandboxWatchlistCalculationPort) EstimateWatchlist(ctx context.Context, request *fraudv1.EstimateWatchlistRequest) (*fraudv1.EstimateWatchlistResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EstimateWatchlist")
	return &fraudv1.EstimateWatchlistResponse{}, nil
}

func newWatchlistCalculationPort(deps platform.Deps) watchlistCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxWatchlistCalculationPort{log: deps.Logger}
	}
	return grpcWatchlistCalculationPort{client: fraudv1.NewWatchlistCalculationServiceClient(deps.Conn)}
}
