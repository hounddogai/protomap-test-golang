package payments

import (
	"context"
	"time"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	recruitingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/recruiting/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
)

// meteredAccessPolicyValidation records how long each call to acme.authorization.v1.AccessPolicyValidationService
// takes.
type meteredAccessPolicyValidation struct {
	authorizationv1.AccessPolicyValidationServiceClient
	recorder *metrics.Recorder
}

// meteredAuditEventQuery records how long each call to acme.audit.v1.AuditEventQueryService takes.
type meteredAuditEventQuery struct {
	auditv1.AuditEventQueryServiceClient
	recorder *metrics.Recorder
}

// meteredCustomerCalculation records how long each call to acme.customers.v1.CustomerCalculationService takes.
type meteredCustomerCalculation struct {
	customersv1.CustomerCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomerCalculation) CalculateCustomer(ctx context.Context, request *customersv1.CalculateCustomerRequest, options ...grpc.CallOption) (*customersv1.CalculateCustomerResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerCalculationService/CalculateCustomer", time.Now())
	return c.CustomerCalculationServiceClient.CalculateCustomer(ctx, request, options...)
}

// meteredCustomerSegmentValidation records how long each call to acme.customers.v1.CustomerSegmentValidationService
// takes.
type meteredCustomerSegmentValidation struct {
	customersv1.CustomerSegmentValidationServiceClient
	recorder *metrics.Recorder
}

// meteredCustomerValidation records how long each call to acme.customers.v1.CustomerValidationService takes.
type meteredCustomerValidation struct {
	customersv1.CustomerValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredCustomerValidation) LintCustomers(ctx context.Context, request *customersv1.LintCustomersRequest, options ...grpc.CallOption) (*customersv1.LintCustomersResponse, error) {
	defer c.recorder.Observe("acme.customers.v1.CustomerValidationService/LintCustomers", time.Now())
	return c.CustomerValidationServiceClient.LintCustomers(ctx, request, options...)
}

// meteredDocumentValidation records how long each call to acme.documents.v1.DocumentValidationService takes.
type meteredDocumentValidation struct {
	documentsv1.DocumentValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredDocumentValidation) LintDocuments(ctx context.Context, request *documentsv1.LintDocumentsRequest, options ...grpc.CallOption) (*documentsv1.LintDocumentsResponse, error) {
	defer c.recorder.Observe("acme.documents.v1.DocumentValidationService/LintDocuments", time.Now())
	return c.DocumentValidationServiceClient.LintDocuments(ctx, request, options...)
}

// meteredFeatureFlag records how long each call to acme.featureflags.v1.FeatureFlagService takes.
type meteredFeatureFlag struct {
	featureflagsv1.FeatureFlagServiceClient
	recorder *metrics.Recorder
}

func (c meteredFeatureFlag) CreateFeatureFlag(ctx context.Context, request *featureflagsv1.CreateFeatureFlagRequest, options ...grpc.CallOption) (*featureflagsv1.CreateFeatureFlagResponse, error) {
	defer c.recorder.Observe("acme.featureflags.v1.FeatureFlagService/CreateFeatureFlag", time.Now())
	return c.FeatureFlagServiceClient.CreateFeatureFlag(ctx, request, options...)
}

// meteredJobOpening records how long each call to acme.recruiting.v1.JobOpeningService takes.
type meteredJobOpening struct {
	recruitingv1.JobOpeningServiceClient
	recorder *metrics.Recorder
}

func (c meteredJobOpening) UpdateJobOpening(ctx context.Context, request *recruitingv1.UpdateJobOpeningRequest, options ...grpc.CallOption) (*recruitingv1.UpdateJobOpeningResponse, error) {
	defer c.recorder.Observe("acme.recruiting.v1.JobOpeningService/UpdateJobOpening", time.Now())
	return c.JobOpeningServiceClient.UpdateJobOpening(ctx, request, options...)
}

// meteredLedgerAccount records how long each call to acme.ledger.v1.LedgerAccountService takes.
type meteredLedgerAccount struct {
	ledgerv1.LedgerAccountServiceClient
	recorder *metrics.Recorder
}

// meteredPaymentInstrumentValidation records how long each call to acme.payments.v1.PaymentInstrumentValidationService
// takes.
type meteredPaymentInstrumentValidation struct {
	paymentsv1.PaymentInstrumentValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPaymentInstrumentValidation) ScorePaymentInstrument(ctx context.Context, request *paymentsv1.ScorePaymentInstrumentRequest, options ...grpc.CallOption) (*paymentsv1.ScorePaymentInstrumentResponse, error) {
	defer c.recorder.Observe("acme.payments.v1.PaymentInstrumentValidationService/ScorePaymentInstrument", time.Now())
	return c.PaymentInstrumentValidationServiceClient.ScorePaymentInstrument(ctx, request, options...)
}

// meteredPaymentLifecycle records how long each call to acme.payments.v1.PaymentLifecycleService takes.
type meteredPaymentLifecycle struct {
	paymentsv1.PaymentLifecycleServiceClient
	recorder *metrics.Recorder
}

// meteredPaymentValidation records how long each call to acme.payments.v1.PaymentValidationService takes.
type meteredPaymentValidation struct {
	paymentsv1.PaymentValidationServiceClient
	recorder *metrics.Recorder
}

func (c meteredPaymentValidation) ScorePayment(ctx context.Context, request *paymentsv1.ScorePaymentRequest, options ...grpc.CallOption) (*paymentsv1.ScorePaymentResponse, error) {
	defer c.recorder.Observe("acme.payments.v1.PaymentValidationService/ScorePayment", time.Now())
	return c.PaymentValidationServiceClient.ScorePayment(ctx, request, options...)
}

// meteredPickListApproval records how long each call to acme.warehousing.v1.PickListApprovalService takes.
type meteredPickListApproval struct {
	warehousingv1.PickListApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredPickListApproval) RecallPickList(ctx context.Context, request *warehousingv1.RecallPickListRequest, options ...grpc.CallOption) (*warehousingv1.RecallPickListResponse, error) {
	defer c.recorder.Observe("acme.warehousing.v1.PickListApprovalService/RecallPickList", time.Now())
	return c.PickListApprovalServiceClient.RecallPickList(ctx, request, options...)
}

// meteredRefundCalculation records how long each call to acme.payments.v1.RefundCalculationService takes.
type meteredRefundCalculation struct {
	paymentsv1.RefundCalculationServiceClient
	recorder *metrics.Recorder
}

func (c meteredRefundCalculation) QuoteRefund(ctx context.Context, request *paymentsv1.QuoteRefundRequest, options ...grpc.CallOption) (*paymentsv1.QuoteRefundResponse, error) {
	defer c.recorder.Observe("acme.payments.v1.RefundCalculationService/QuoteRefund", time.Now())
	return c.RefundCalculationServiceClient.QuoteRefund(ctx, request, options...)
}

// meteredRefundLifecycle records how long each call to acme.payments.v1.RefundLifecycleService takes.
type meteredRefundLifecycle struct {
	paymentsv1.RefundLifecycleServiceClient
	recorder *metrics.Recorder
}

func (c meteredRefundLifecycle) RenewRefund(ctx context.Context, request *paymentsv1.RenewRefundRequest, options ...grpc.CallOption) (*paymentsv1.RenewRefundResponse, error) {
	defer c.recorder.Observe("acme.payments.v1.RefundLifecycleService/RenewRefund", time.Now())
	return c.RefundLifecycleServiceClient.RenewRefund(ctx, request, options...)
}

// meteredRefundReporting records how long each call to acme.payments.v1.RefundReportingService takes.
type meteredRefundReporting struct {
	paymentsv1.RefundReportingServiceClient
	recorder *metrics.Recorder
}

func (c meteredRefundReporting) ScheduleRefundReport(ctx context.Context, request *paymentsv1.ScheduleRefundReportRequest, options ...grpc.CallOption) (*paymentsv1.ScheduleRefundReportResponse, error) {
	defer c.recorder.Observe("acme.payments.v1.RefundReportingService/ScheduleRefundReport", time.Now())
	return c.RefundReportingServiceClient.ScheduleRefundReport(ctx, request, options...)
}

// meteredReturnRequestApproval records how long each call to acme.returns.v1.ReturnRequestApprovalService takes.
type meteredReturnRequestApproval struct {
	returnsv1.ReturnRequestApprovalServiceClient
	recorder *metrics.Recorder
}

func (c meteredReturnRequestApproval) DelegateReturnRequestApproval(ctx context.Context, request *returnsv1.DelegateReturnRequestApprovalRequest, options ...grpc.CallOption) (*returnsv1.DelegateReturnRequestApprovalResponse, error) {
	defer c.recorder.Observe("acme.returns.v1.ReturnRequestApprovalService/DelegateReturnRequestApproval", time.Now())
	return c.ReturnRequestApprovalServiceClient.DelegateReturnRequestApproval(ctx, request, options...)
}

// meteredRoleNotification records how long each call to acme.identity.v1.RoleNotificationService takes.
type meteredRoleNotification struct {
	identityv1.RoleNotificationServiceClient
	recorder *metrics.Recorder
}

// meteredSearchIndexNotification records how long each call to acme.search.v1.SearchIndexNotificationService takes.
type meteredSearchIndexNotification struct {
	searchv1.SearchIndexNotificationServiceClient
	recorder *metrics.Recorder
}
