package payments

import (
	"context"
	"log/slog"

	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sponsorshipsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sponsorships/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
)

// customerCalculationPort is what the payments domain needs of acme.customers.v1.CustomerCalculationService.
type customerCalculationPort interface {
	QuoteCustomer(ctx context.Context, request *customersv1.QuoteCustomerRequest) (*customersv1.QuoteCustomerResponse, error)
}

// grpcCustomerCalculationPort reaches acme.customers.v1.CustomerCalculationService over gRPC.
type grpcCustomerCalculationPort struct {
	client customersv1.CustomerCalculationServiceClient
}

// sandboxCustomerCalculationPort answers in the sandbox environment, where acme.customers.v1.CustomerCalculationService
// is not deployed.
type sandboxCustomerCalculationPort struct{ log *slog.Logger }

func (p grpcCustomerCalculationPort) QuoteCustomer(ctx context.Context, request *customersv1.QuoteCustomerRequest) (*customersv1.QuoteCustomerResponse, error) {
	return p.client.QuoteCustomer(ctx, request)
}

func (p sandboxCustomerCalculationPort) QuoteCustomer(ctx context.Context, request *customersv1.QuoteCustomerRequest) (*customersv1.QuoteCustomerResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers QuoteCustomer")
	return &customersv1.QuoteCustomerResponse{}, nil
}

func newCustomerCalculationPort(deps platform.Deps) customerCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxCustomerCalculationPort{log: deps.Logger}
	}
	return grpcCustomerCalculationPort{client: customersv1.NewCustomerCalculationServiceClient(deps.Conn)}
}

// documentPort is what the payments domain needs of acme.documents.v1.DocumentService.
type documentPort interface {
	ListDocuments(ctx context.Context, request *documentsv1.ListDocumentsRequest) (*documentsv1.ListDocumentsResponse, error)
}

// grpcDocumentPort reaches acme.documents.v1.DocumentService over gRPC.
type grpcDocumentPort struct {
	client documentsv1.DocumentServiceClient
}

// sandboxDocumentPort answers in the sandbox environment, where acme.documents.v1.DocumentService is not deployed.
type sandboxDocumentPort struct{ log *slog.Logger }

func (p grpcDocumentPort) ListDocuments(ctx context.Context, request *documentsv1.ListDocumentsRequest) (*documentsv1.ListDocumentsResponse, error) {
	return p.client.ListDocuments(ctx, request)
}

func (p sandboxDocumentPort) ListDocuments(ctx context.Context, request *documentsv1.ListDocumentsRequest) (*documentsv1.ListDocumentsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListDocuments")
	return &documentsv1.ListDocumentsResponse{}, nil
}

func newDocumentPort(deps platform.Deps) documentPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxDocumentPort{log: deps.Logger}
	}
	return grpcDocumentPort{client: documentsv1.NewDocumentServiceClient(deps.Conn)}
}

// metricSeriesPort is what the payments domain needs of acme.metrics.v1.MetricSeriesService.
type metricSeriesPort interface {
	ListMetricSerieses(ctx context.Context, request *metricsv1.ListMetricSeriesesRequest) (*metricsv1.ListMetricSeriesesResponse, error)
}

// grpcMetricSeriesPort reaches acme.metrics.v1.MetricSeriesService over gRPC.
type grpcMetricSeriesPort struct {
	client metricsv1.MetricSeriesServiceClient
}

// sandboxMetricSeriesPort answers in the sandbox environment, where acme.metrics.v1.MetricSeriesService is not
// deployed.
type sandboxMetricSeriesPort struct{ log *slog.Logger }

func (p grpcMetricSeriesPort) ListMetricSerieses(ctx context.Context, request *metricsv1.ListMetricSeriesesRequest) (*metricsv1.ListMetricSeriesesResponse, error) {
	return p.client.ListMetricSerieses(ctx, request)
}

func (p sandboxMetricSeriesPort) ListMetricSerieses(ctx context.Context, request *metricsv1.ListMetricSeriesesRequest) (*metricsv1.ListMetricSeriesesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListMetricSerieses")
	return &metricsv1.ListMetricSeriesesResponse{}, nil
}

func newMetricSeriesPort(deps platform.Deps) metricSeriesPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxMetricSeriesPort{log: deps.Logger}
	}
	return grpcMetricSeriesPort{client: metricsv1.NewMetricSeriesServiceClient(deps.Conn)}
}

// paymentCalculationPort is what the payments domain needs of acme.payments.v1.PaymentCalculationService.
type paymentCalculationPort interface {
	CalculatePayment(ctx context.Context, request *paymentsv1.CalculatePaymentRequest) (*paymentsv1.CalculatePaymentResponse, error)
	GetPaymentRates(ctx context.Context, request *paymentsv1.GetPaymentRatesRequest) (*paymentsv1.GetPaymentRatesResponse, error)
}

// grpcPaymentCalculationPort reaches acme.payments.v1.PaymentCalculationService over gRPC.
type grpcPaymentCalculationPort struct {
	client paymentsv1.PaymentCalculationServiceClient
}

// sandboxPaymentCalculationPort answers in the sandbox environment, where acme.payments.v1.PaymentCalculationService is
// not deployed.
type sandboxPaymentCalculationPort struct{ log *slog.Logger }

func (p grpcPaymentCalculationPort) CalculatePayment(ctx context.Context, request *paymentsv1.CalculatePaymentRequest) (*paymentsv1.CalculatePaymentResponse, error) {
	return p.client.CalculatePayment(ctx, request)
}

func (p sandboxPaymentCalculationPort) CalculatePayment(ctx context.Context, request *paymentsv1.CalculatePaymentRequest) (*paymentsv1.CalculatePaymentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CalculatePayment")
	return &paymentsv1.CalculatePaymentResponse{}, nil
}

func (p grpcPaymentCalculationPort) GetPaymentRates(ctx context.Context, request *paymentsv1.GetPaymentRatesRequest) (*paymentsv1.GetPaymentRatesResponse, error) {
	return p.client.GetPaymentRates(ctx, request)
}

func (p sandboxPaymentCalculationPort) GetPaymentRates(ctx context.Context, request *paymentsv1.GetPaymentRatesRequest) (*paymentsv1.GetPaymentRatesResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetPaymentRates")
	return &paymentsv1.GetPaymentRatesResponse{}, nil
}

func newPaymentCalculationPort(deps platform.Deps) paymentCalculationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPaymentCalculationPort{log: deps.Logger}
	}
	return grpcPaymentCalculationPort{client: paymentsv1.NewPaymentCalculationServiceClient(deps.Conn)}
}

// paymentInstrumentSyncPort is what the payments domain needs of acme.payments.v1.PaymentInstrumentSyncService.
type paymentInstrumentSyncPort interface {
	ResolvePaymentInstrumentConflict(ctx context.Context, request *paymentsv1.ResolvePaymentInstrumentConflictRequest) (*paymentsv1.ResolvePaymentInstrumentConflictResponse, error)
}

// grpcPaymentInstrumentSyncPort reaches acme.payments.v1.PaymentInstrumentSyncService over gRPC.
type grpcPaymentInstrumentSyncPort struct {
	client paymentsv1.PaymentInstrumentSyncServiceClient
}

// sandboxPaymentInstrumentSyncPort answers in the sandbox environment, where
// acme.payments.v1.PaymentInstrumentSyncService is not deployed.
type sandboxPaymentInstrumentSyncPort struct{ log *slog.Logger }

func (p grpcPaymentInstrumentSyncPort) ResolvePaymentInstrumentConflict(ctx context.Context, request *paymentsv1.ResolvePaymentInstrumentConflictRequest) (*paymentsv1.ResolvePaymentInstrumentConflictResponse, error) {
	return p.client.ResolvePaymentInstrumentConflict(ctx, request)
}

func (p sandboxPaymentInstrumentSyncPort) ResolvePaymentInstrumentConflict(ctx context.Context, request *paymentsv1.ResolvePaymentInstrumentConflictRequest) (*paymentsv1.ResolvePaymentInstrumentConflictResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ResolvePaymentInstrumentConflict")
	return &paymentsv1.ResolvePaymentInstrumentConflictResponse{}, nil
}

func newPaymentInstrumentSyncPort(deps platform.Deps) paymentInstrumentSyncPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPaymentInstrumentSyncPort{log: deps.Logger}
	}
	return grpcPaymentInstrumentSyncPort{client: paymentsv1.NewPaymentInstrumentSyncServiceClient(deps.Conn)}
}

// paymentInstrumentValidationPort is what the payments domain needs of
// acme.payments.v1.PaymentInstrumentValidationService.
type paymentInstrumentValidationPort interface {
	ValidatePaymentInstrument(ctx context.Context, request *paymentsv1.ValidatePaymentInstrumentRequest) (*paymentsv1.ValidatePaymentInstrumentResponse, error)
}

// grpcPaymentInstrumentValidationPort reaches acme.payments.v1.PaymentInstrumentValidationService over gRPC.
type grpcPaymentInstrumentValidationPort struct {
	client paymentsv1.PaymentInstrumentValidationServiceClient
}

// sandboxPaymentInstrumentValidationPort answers in the sandbox environment, where
// acme.payments.v1.PaymentInstrumentValidationService is not deployed.
type sandboxPaymentInstrumentValidationPort struct{ log *slog.Logger }

func (p grpcPaymentInstrumentValidationPort) ValidatePaymentInstrument(ctx context.Context, request *paymentsv1.ValidatePaymentInstrumentRequest) (*paymentsv1.ValidatePaymentInstrumentResponse, error) {
	return p.client.ValidatePaymentInstrument(ctx, request)
}

func (p sandboxPaymentInstrumentValidationPort) ValidatePaymentInstrument(ctx context.Context, request *paymentsv1.ValidatePaymentInstrumentRequest) (*paymentsv1.ValidatePaymentInstrumentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ValidatePaymentInstrument")
	return &paymentsv1.ValidatePaymentInstrumentResponse{}, nil
}

func newPaymentInstrumentValidationPort(deps platform.Deps) paymentInstrumentValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPaymentInstrumentValidationPort{log: deps.Logger}
	}
	return grpcPaymentInstrumentValidationPort{client: paymentsv1.NewPaymentInstrumentValidationServiceClient(deps.Conn)}
}

// paymentLifecyclePort is what the payments domain needs of acme.payments.v1.PaymentLifecycleService.
type paymentLifecyclePort interface {
	SchedulePaymentExpiry(ctx context.Context, request *paymentsv1.SchedulePaymentExpiryRequest) (*paymentsv1.SchedulePaymentExpiryResponse, error)
}

// grpcPaymentLifecyclePort reaches acme.payments.v1.PaymentLifecycleService over gRPC.
type grpcPaymentLifecyclePort struct {
	client paymentsv1.PaymentLifecycleServiceClient
}

// sandboxPaymentLifecyclePort answers in the sandbox environment, where acme.payments.v1.PaymentLifecycleService is not
// deployed.
type sandboxPaymentLifecyclePort struct{ log *slog.Logger }

func (p grpcPaymentLifecyclePort) SchedulePaymentExpiry(ctx context.Context, request *paymentsv1.SchedulePaymentExpiryRequest) (*paymentsv1.SchedulePaymentExpiryResponse, error) {
	return p.client.SchedulePaymentExpiry(ctx, request)
}

func (p sandboxPaymentLifecyclePort) SchedulePaymentExpiry(ctx context.Context, request *paymentsv1.SchedulePaymentExpiryRequest) (*paymentsv1.SchedulePaymentExpiryResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SchedulePaymentExpiry")
	return &paymentsv1.SchedulePaymentExpiryResponse{}, nil
}

func newPaymentLifecyclePort(deps platform.Deps) paymentLifecyclePort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPaymentLifecyclePort{log: deps.Logger}
	}
	return grpcPaymentLifecyclePort{client: paymentsv1.NewPaymentLifecycleServiceClient(deps.Conn)}
}

// paymentPort is what the payments domain needs of acme.payments.v1.PaymentService.
type paymentPort interface {
	GetPayment(ctx context.Context, request *paymentsv1.GetPaymentRequest) (*paymentsv1.GetPaymentResponse, error)
}

// grpcPaymentPort reaches acme.payments.v1.PaymentService over gRPC.
type grpcPaymentPort struct {
	client paymentsv1.PaymentServiceClient
}

// sandboxPaymentPort answers in the sandbox environment, where acme.payments.v1.PaymentService is not deployed.
type sandboxPaymentPort struct{ log *slog.Logger }

func (p grpcPaymentPort) GetPayment(ctx context.Context, request *paymentsv1.GetPaymentRequest) (*paymentsv1.GetPaymentResponse, error) {
	return p.client.GetPayment(ctx, request)
}

func (p sandboxPaymentPort) GetPayment(ctx context.Context, request *paymentsv1.GetPaymentRequest) (*paymentsv1.GetPaymentResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers GetPayment")
	return &paymentsv1.GetPaymentResponse{}, nil
}

func newPaymentPort(deps platform.Deps) paymentPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPaymentPort{log: deps.Logger}
	}
	return grpcPaymentPort{client: paymentsv1.NewPaymentServiceClient(deps.Conn)}
}

// payoutSyncPort is what the payments domain needs of acme.payments.v1.PayoutSyncService.
type payoutSyncPort interface {
	SyncPayouts(ctx context.Context, request *paymentsv1.SyncPayoutsRequest) (*paymentsv1.SyncPayoutsResponse, error)
}

// grpcPayoutSyncPort reaches acme.payments.v1.PayoutSyncService over gRPC.
type grpcPayoutSyncPort struct {
	client paymentsv1.PayoutSyncServiceClient
}

// sandboxPayoutSyncPort answers in the sandbox environment, where acme.payments.v1.PayoutSyncService is not deployed.
type sandboxPayoutSyncPort struct{ log *slog.Logger }

func (p grpcPayoutSyncPort) SyncPayouts(ctx context.Context, request *paymentsv1.SyncPayoutsRequest) (*paymentsv1.SyncPayoutsResponse, error) {
	return p.client.SyncPayouts(ctx, request)
}

func (p sandboxPayoutSyncPort) SyncPayouts(ctx context.Context, request *paymentsv1.SyncPayoutsRequest) (*paymentsv1.SyncPayoutsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SyncPayouts")
	return &paymentsv1.SyncPayoutsResponse{}, nil
}

func newPayoutSyncPort(deps platform.Deps) payoutSyncPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPayoutSyncPort{log: deps.Logger}
	}
	return grpcPayoutSyncPort{client: paymentsv1.NewPayoutSyncServiceClient(deps.Conn)}
}

// payoutValidationPort is what the payments domain needs of acme.payments.v1.PayoutValidationService.
type payoutValidationPort interface {
	CheckPayoutEligibility(ctx context.Context, request *paymentsv1.CheckPayoutEligibilityRequest) (*paymentsv1.CheckPayoutEligibilityResponse, error)
}

// grpcPayoutValidationPort reaches acme.payments.v1.PayoutValidationService over gRPC.
type grpcPayoutValidationPort struct {
	client paymentsv1.PayoutValidationServiceClient
}

// sandboxPayoutValidationPort answers in the sandbox environment, where acme.payments.v1.PayoutValidationService is not
// deployed.
type sandboxPayoutValidationPort struct{ log *slog.Logger }

func (p grpcPayoutValidationPort) CheckPayoutEligibility(ctx context.Context, request *paymentsv1.CheckPayoutEligibilityRequest) (*paymentsv1.CheckPayoutEligibilityResponse, error) {
	return p.client.CheckPayoutEligibility(ctx, request)
}

func (p sandboxPayoutValidationPort) CheckPayoutEligibility(ctx context.Context, request *paymentsv1.CheckPayoutEligibilityRequest) (*paymentsv1.CheckPayoutEligibilityResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CheckPayoutEligibility")
	return &paymentsv1.CheckPayoutEligibilityResponse{}, nil
}

func newPayoutValidationPort(deps platform.Deps) payoutValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPayoutValidationPort{log: deps.Logger}
	}
	return grpcPayoutValidationPort{client: paymentsv1.NewPayoutValidationServiceClient(deps.Conn)}
}

// pointsLedgerValidationPort is what the payments domain needs of acme.loyalty.v1.PointsLedgerValidationService.
type pointsLedgerValidationPort interface {
	CheckPointsLedgerEligibility(ctx context.Context, request *loyaltyv1.CheckPointsLedgerEligibilityRequest) (*loyaltyv1.CheckPointsLedgerEligibilityResponse, error)
}

// grpcPointsLedgerValidationPort reaches acme.loyalty.v1.PointsLedgerValidationService over gRPC.
type grpcPointsLedgerValidationPort struct {
	client loyaltyv1.PointsLedgerValidationServiceClient
}

// sandboxPointsLedgerValidationPort answers in the sandbox environment, where
// acme.loyalty.v1.PointsLedgerValidationService is not deployed.
type sandboxPointsLedgerValidationPort struct{ log *slog.Logger }

func (p grpcPointsLedgerValidationPort) CheckPointsLedgerEligibility(ctx context.Context, request *loyaltyv1.CheckPointsLedgerEligibilityRequest) (*loyaltyv1.CheckPointsLedgerEligibilityResponse, error) {
	return p.client.CheckPointsLedgerEligibility(ctx, request)
}

func (p sandboxPointsLedgerValidationPort) CheckPointsLedgerEligibility(ctx context.Context, request *loyaltyv1.CheckPointsLedgerEligibilityRequest) (*loyaltyv1.CheckPointsLedgerEligibilityResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers CheckPointsLedgerEligibility")
	return &loyaltyv1.CheckPointsLedgerEligibilityResponse{}, nil
}

func newPointsLedgerValidationPort(deps platform.Deps) pointsLedgerValidationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPointsLedgerValidationPort{log: deps.Logger}
	}
	return grpcPointsLedgerValidationPort{client: loyaltyv1.NewPointsLedgerValidationServiceClient(deps.Conn)}
}

// putawayTaskApprovalPort is what the payments domain needs of acme.warehousing.v1.PutawayTaskApprovalService.
type putawayTaskApprovalPort interface {
	RecallPutawayTask(ctx context.Context, request *warehousingv1.RecallPutawayTaskRequest) (*warehousingv1.RecallPutawayTaskResponse, error)
}

// grpcPutawayTaskApprovalPort reaches acme.warehousing.v1.PutawayTaskApprovalService over gRPC.
type grpcPutawayTaskApprovalPort struct {
	client warehousingv1.PutawayTaskApprovalServiceClient
}

// sandboxPutawayTaskApprovalPort answers in the sandbox environment, where
// acme.warehousing.v1.PutawayTaskApprovalService is not deployed.
type sandboxPutawayTaskApprovalPort struct{ log *slog.Logger }

func (p grpcPutawayTaskApprovalPort) RecallPutawayTask(ctx context.Context, request *warehousingv1.RecallPutawayTaskRequest) (*warehousingv1.RecallPutawayTaskResponse, error) {
	return p.client.RecallPutawayTask(ctx, request)
}

func (p sandboxPutawayTaskApprovalPort) RecallPutawayTask(ctx context.Context, request *warehousingv1.RecallPutawayTaskRequest) (*warehousingv1.RecallPutawayTaskResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers RecallPutawayTask")
	return &warehousingv1.RecallPutawayTaskResponse{}, nil
}

func newPutawayTaskApprovalPort(deps platform.Deps) putawayTaskApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxPutawayTaskApprovalPort{log: deps.Logger}
	}
	return grpcPutawayTaskApprovalPort{client: warehousingv1.NewPutawayTaskApprovalServiceClient(deps.Conn)}
}

// returnLabelQueryPort is what the payments domain needs of acme.returns.v1.ReturnLabelQueryService.
type returnLabelQueryPort interface {
	SummarizeReturnLabels(ctx context.Context, request *returnsv1.SummarizeReturnLabelsRequest) (*returnsv1.SummarizeReturnLabelsResponse, error)
}

// grpcReturnLabelQueryPort reaches acme.returns.v1.ReturnLabelQueryService over gRPC.
type grpcReturnLabelQueryPort struct {
	client returnsv1.ReturnLabelQueryServiceClient
}

// sandboxReturnLabelQueryPort answers in the sandbox environment, where acme.returns.v1.ReturnLabelQueryService is not
// deployed.
type sandboxReturnLabelQueryPort struct{ log *slog.Logger }

func (p grpcReturnLabelQueryPort) SummarizeReturnLabels(ctx context.Context, request *returnsv1.SummarizeReturnLabelsRequest) (*returnsv1.SummarizeReturnLabelsResponse, error) {
	return p.client.SummarizeReturnLabels(ctx, request)
}

func (p sandboxReturnLabelQueryPort) SummarizeReturnLabels(ctx context.Context, request *returnsv1.SummarizeReturnLabelsRequest) (*returnsv1.SummarizeReturnLabelsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SummarizeReturnLabels")
	return &returnsv1.SummarizeReturnLabelsResponse{}, nil
}

func newReturnLabelQueryPort(deps platform.Deps) returnLabelQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReturnLabelQueryPort{log: deps.Logger}
	}
	return grpcReturnLabelQueryPort{client: returnsv1.NewReturnLabelQueryServiceClient(deps.Conn)}
}

// reviewVoteApprovalPort is what the payments domain needs of acme.reviews.v1.ReviewVoteApprovalService.
type reviewVoteApprovalPort interface {
	EscalateReviewVote(ctx context.Context, request *reviewsv1.EscalateReviewVoteRequest) (*reviewsv1.EscalateReviewVoteResponse, error)
}

// grpcReviewVoteApprovalPort reaches acme.reviews.v1.ReviewVoteApprovalService over gRPC.
type grpcReviewVoteApprovalPort struct {
	client reviewsv1.ReviewVoteApprovalServiceClient
}

// sandboxReviewVoteApprovalPort answers in the sandbox environment, where acme.reviews.v1.ReviewVoteApprovalService is
// not deployed.
type sandboxReviewVoteApprovalPort struct{ log *slog.Logger }

func (p grpcReviewVoteApprovalPort) EscalateReviewVote(ctx context.Context, request *reviewsv1.EscalateReviewVoteRequest) (*reviewsv1.EscalateReviewVoteResponse, error) {
	return p.client.EscalateReviewVote(ctx, request)
}

func (p sandboxReviewVoteApprovalPort) EscalateReviewVote(ctx context.Context, request *reviewsv1.EscalateReviewVoteRequest) (*reviewsv1.EscalateReviewVoteResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers EscalateReviewVote")
	return &reviewsv1.EscalateReviewVoteResponse{}, nil
}

func newReviewVoteApprovalPort(deps platform.Deps) reviewVoteApprovalPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxReviewVoteApprovalPort{log: deps.Logger}
	}
	return grpcReviewVoteApprovalPort{client: reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn)}
}

// riskAssessmentQueryPort is what the payments domain needs of acme.fraud.v1.RiskAssessmentQueryService.
type riskAssessmentQueryPort interface {
	BrowseRiskAssessments(ctx context.Context, request *fraudv1.BrowseRiskAssessmentsRequest) (*fraudv1.BrowseRiskAssessmentsResponse, error)
}

// grpcRiskAssessmentQueryPort reaches acme.fraud.v1.RiskAssessmentQueryService over gRPC.
type grpcRiskAssessmentQueryPort struct {
	client fraudv1.RiskAssessmentQueryServiceClient
}

// sandboxRiskAssessmentQueryPort answers in the sandbox environment, where acme.fraud.v1.RiskAssessmentQueryService is
// not deployed.
type sandboxRiskAssessmentQueryPort struct{ log *slog.Logger }

func (p grpcRiskAssessmentQueryPort) BrowseRiskAssessments(ctx context.Context, request *fraudv1.BrowseRiskAssessmentsRequest) (*fraudv1.BrowseRiskAssessmentsResponse, error) {
	return p.client.BrowseRiskAssessments(ctx, request)
}

func (p sandboxRiskAssessmentQueryPort) BrowseRiskAssessments(ctx context.Context, request *fraudv1.BrowseRiskAssessmentsRequest) (*fraudv1.BrowseRiskAssessmentsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers BrowseRiskAssessments")
	return &fraudv1.BrowseRiskAssessmentsResponse{}, nil
}

func newRiskAssessmentQueryPort(deps platform.Deps) riskAssessmentQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxRiskAssessmentQueryPort{log: deps.Logger}
	}
	return grpcRiskAssessmentQueryPort{client: fraudv1.NewRiskAssessmentQueryServiceClient(deps.Conn)}
}

// searchIndexNotificationPort is what the payments domain needs of acme.search.v1.SearchIndexNotificationService.
type searchIndexNotificationPort interface {
	SendSearchIndexTestNotice(ctx context.Context, request *searchv1.SendSearchIndexTestNoticeRequest) (*searchv1.SendSearchIndexTestNoticeResponse, error)
}

// grpcSearchIndexNotificationPort reaches acme.search.v1.SearchIndexNotificationService over gRPC.
type grpcSearchIndexNotificationPort struct {
	client searchv1.SearchIndexNotificationServiceClient
}

// sandboxSearchIndexNotificationPort answers in the sandbox environment, where
// acme.search.v1.SearchIndexNotificationService is not deployed.
type sandboxSearchIndexNotificationPort struct{ log *slog.Logger }

func (p grpcSearchIndexNotificationPort) SendSearchIndexTestNotice(ctx context.Context, request *searchv1.SendSearchIndexTestNoticeRequest) (*searchv1.SendSearchIndexTestNoticeResponse, error) {
	return p.client.SendSearchIndexTestNotice(ctx, request)
}

func (p sandboxSearchIndexNotificationPort) SendSearchIndexTestNotice(ctx context.Context, request *searchv1.SendSearchIndexTestNoticeRequest) (*searchv1.SendSearchIndexTestNoticeResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SendSearchIndexTestNotice")
	return &searchv1.SendSearchIndexTestNoticeResponse{}, nil
}

func newSearchIndexNotificationPort(deps platform.Deps) searchIndexNotificationPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSearchIndexNotificationPort{log: deps.Logger}
	}
	return grpcSearchIndexNotificationPort{client: searchv1.NewSearchIndexNotificationServiceClient(deps.Conn)}
}

// sponsorQueryPort is what the payments domain needs of acme.sponsorships.v1.SponsorQueryService.
type sponsorQueryPort interface {
	SummarizeSponsors(ctx context.Context, request *sponsorshipsv1.SummarizeSponsorsRequest) (*sponsorshipsv1.SummarizeSponsorsResponse, error)
}

// grpcSponsorQueryPort reaches acme.sponsorships.v1.SponsorQueryService over gRPC.
type grpcSponsorQueryPort struct {
	client sponsorshipsv1.SponsorQueryServiceClient
}

// sandboxSponsorQueryPort answers in the sandbox environment, where acme.sponsorships.v1.SponsorQueryService is not
// deployed.
type sandboxSponsorQueryPort struct{ log *slog.Logger }

func (p grpcSponsorQueryPort) SummarizeSponsors(ctx context.Context, request *sponsorshipsv1.SummarizeSponsorsRequest) (*sponsorshipsv1.SummarizeSponsorsResponse, error) {
	return p.client.SummarizeSponsors(ctx, request)
}

func (p sandboxSponsorQueryPort) SummarizeSponsors(ctx context.Context, request *sponsorshipsv1.SummarizeSponsorsRequest) (*sponsorshipsv1.SummarizeSponsorsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers SummarizeSponsors")
	return &sponsorshipsv1.SummarizeSponsorsResponse{}, nil
}

func newSponsorQueryPort(deps platform.Deps) sponsorQueryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSponsorQueryPort{log: deps.Logger}
	}
	return grpcSponsorQueryPort{client: sponsorshipsv1.NewSponsorQueryServiceClient(deps.Conn)}
}

// synonymHistoryPort is what the payments domain needs of acme.search.v1.SynonymHistoryService.
type synonymHistoryPort interface {
	ListSynonymVersions(ctx context.Context, request *searchv1.ListSynonymVersionsRequest) (*searchv1.ListSynonymVersionsResponse, error)
}

// grpcSynonymHistoryPort reaches acme.search.v1.SynonymHistoryService over gRPC.
type grpcSynonymHistoryPort struct {
	client searchv1.SynonymHistoryServiceClient
}

// sandboxSynonymHistoryPort answers in the sandbox environment, where acme.search.v1.SynonymHistoryService is not
// deployed.
type sandboxSynonymHistoryPort struct{ log *slog.Logger }

func (p grpcSynonymHistoryPort) ListSynonymVersions(ctx context.Context, request *searchv1.ListSynonymVersionsRequest) (*searchv1.ListSynonymVersionsResponse, error) {
	return p.client.ListSynonymVersions(ctx, request)
}

func (p sandboxSynonymHistoryPort) ListSynonymVersions(ctx context.Context, request *searchv1.ListSynonymVersionsRequest) (*searchv1.ListSynonymVersionsResponse, error) {
	p.log.InfoContext(ctx, "sandbox answers ListSynonymVersions")
	return &searchv1.ListSynonymVersionsResponse{}, nil
}

func newSynonymHistoryPort(deps platform.Deps) synonymHistoryPort {
	if deps.Flags.Enabled("sandbox") {
		return sandboxSynonymHistoryPort{log: deps.Logger}
	}
	return grpcSynonymHistoryPort{client: searchv1.NewSynonymHistoryServiceClient(deps.Conn)}
}
