package pricing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	experimentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/experiments/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	prescriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/prescriptions/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// PriceQuoteApprovalServer implements `acme.pricing.v1.PriceQuoteApprovalService`.
// Routes price quotes through approval, with escalation and delegation.
type PriceQuoteApprovalServer struct {
	pricingv1.UnimplementedPriceQuoteApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	consentRecordValidationClient   customersv1.ConsentRecordValidationServiceClient
	bus                             *eventbus.Bus
	currencyRateReportingOnce       sync.Once
	currencyRateReportingCached     pricingv1.CurrencyRateReportingServiceClient
	priceQuoteValidationClient      pricingv1.PriceQuoteValidationServiceClient
	searchIndexExportClient         searchv1.SearchIndexExportServiceClient
	searchIndexExportClientFallback searchv1.SearchIndexExportServiceClient
	backorderWorkflowOnce           sync.Once
	backorderWorkflowCached         ordersv1.BackorderWorkflowServiceClient
	userClient                      identityv1.UserServiceClient
	paymentInstrumentWorkflowClient paymentsv1.PaymentInstrumentWorkflowServiceClient
	orderWorkflowClient             ordersv1.OrderWorkflowServiceClient
	currencyRateReportingClient     pricingv1.CurrencyRateReportingServiceClient
	currencyRateValidationClient    pricingv1.CurrencyRateValidationServiceClient
	experimentClient                experimentsv1.ExperimentServiceClient
	customerCalculationClient       customersv1.CustomerCalculationServiceClient
	preferenceCalculationClient     customersv1.PreferenceCalculationServiceClient
	refundApprovalClient            paymentsv1.RefundApprovalServiceClient
	meteredHouseholdValidation      meteredHouseholdValidation
	prescriptionQueryClient         prescriptionsv1.PrescriptionQueryServiceClient
}

// NewPriceQuoteApprovalServer wires a PriceQuoteApprovalServer.
func NewPriceQuoteApprovalServer(deps platform.Deps) *PriceQuoteApprovalServer {
	return &PriceQuoteApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.pricing.v1.PriceQuoteApprovalService"),
		conn:                            deps.Conn,
		consentRecordValidationClient:   customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		bus:                             deps.Bus,
		priceQuoteValidationClient:      pricingv1.NewPriceQuoteValidationServiceClient(deps.Conn),
		searchIndexExportClient:         searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		searchIndexExportClientFallback: searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		userClient:                      identityv1.NewUserServiceClient(deps.Conn),
		paymentInstrumentWorkflowClient: paymentsv1.NewPaymentInstrumentWorkflowServiceClient(deps.Conn),
		orderWorkflowClient:             ordersv1.NewOrderWorkflowServiceClient(deps.Conn),
		currencyRateReportingClient:     pricingv1.NewCurrencyRateReportingServiceClient(deps.Conn),
		currencyRateValidationClient:    pricingv1.NewCurrencyRateValidationServiceClient(deps.Conn),
		experimentClient:                experimentsv1.NewExperimentServiceClient(deps.Conn),
		customerCalculationClient:       customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		preferenceCalculationClient:     customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		refundApprovalClient:            paymentsv1.NewRefundApprovalServiceClient(deps.Conn),
		meteredHouseholdValidation:      meteredHouseholdValidation{HouseholdValidationServiceClient: customersv1.NewHouseholdValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		prescriptionQueryClient:         prescriptionsv1.NewPrescriptionQueryServiceClient(deps.Conn),
	}
}

// SubmitPriceQuote submits a price quote.
func (s *PriceQuoteApprovalServer) SubmitPriceQuote(ctx context.Context, req *pricingv1.SubmitPriceQuoteRequest) (*pricingv1.SubmitPriceQuoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "submit price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPriceQuote())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded pricingv1.PriceQuote
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	estimatePreferenceRequest := &customersv1.EstimatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedBy: (&decoded).GetAudit().GetCreatedBy(),
				Etag:      req.GetPriceQuote().GetAudit().GetEtag(),
			},
		},
	}
	estimatePreferenceReply := new(customersv1.EstimatePreferenceResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.PreferenceCalculationService_EstimatePreference_FullMethodName, estimatePreferenceRequest, estimatePreferenceReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/EstimatePreference")
	}
	packed, err := anypb.New(req.GetPriceQuote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreConsentRecordReply, err := s.consentRecordValidationClient.ScoreConsentRecord(ctx, &customersv1.ScoreConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.ScoreConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient:  req.GetPriceQuote().GetBillingAddress().GetRecipient(),
				PostalCode: req.GetPriceQuote().GetBillingAddress().GetPostalCode(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "pricing.submit_price_quote.normalize_price_quote",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"price_quote.audit.etag":          req.GetPriceQuote().GetAudit().GetEtag(),
			"price_quote.bank.account_holder": req.GetPriceQuote().GetBank().GetAccountHolder(),
			"context.tenant_id":               req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	validateConsentRecordRequest := buildValidateConsentRecordRequestForSubmitPriceQuote(req, estimatePreferenceReply, scoreConsentRecordReply)
	validateConsentRecordReply, err := s.consentRecordValidationClient.ValidateConsentRecord(ctx, validateConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ValidateConsentRecord")
	}
	return &pricingv1.SubmitPriceQuoteResponse{
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				Etag:      estimatePreferenceReply.GetPreference().GetAudit().GetEtag(),
				UpdatedBy: estimatePreferenceReply.GetPreference().GetAudit().GetUpdatedBy(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			Counterparty: req.GetPriceQuote().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreConsentRecordReply.GetResult().GetReferenceIds(),
			Message:      validateConsentRecordReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Outcome: &pricingv1.SubmitPriceQuoteResponse_Outcome{
			ReferenceId: validateConsentRecordReply.GetOutcome().GetReferenceId(),
		},
	}, nil
}

// ApprovePriceQuote approves a price quote.
func (s *PriceQuoteApprovalServer) ApprovePriceQuote(ctx context.Context, req *pricingv1.ApprovePriceQuoteRequest) (*pricingv1.ApprovePriceQuoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "approve price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	metadata2 := make(map[string]string, len(req.GetPriceQuote().GetMetadata()))
	for key, value := range req.GetPriceQuote().GetMetadata() {
		metadata2[key] = value
	}
	scheduleCurrencyRateReportRequest := &pricingv1.ScheduleCurrencyRateReportRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
		Reason:      tenancy.Value(ctx, "reason"),
		Details:     &pricingv1.ScheduleCurrencyRateReportRequest_Details{Filters: metadata2},
	}
	scheduleCurrencyRateReportReply, err := s.currencyRateReportingLazy().ScheduleCurrencyRateReport(ctx, scheduleCurrencyRateReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/ScheduleCurrencyRateReport")
	}
	var accountHolder string
	switch choice := req.GetPriceQuote().GetSettlement().(type) {
	case *pricingv1.PriceQuote_Card:
		accountHolder = choice.Card.GetHolderName()
	case *pricingv1.PriceQuote_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *pricingv1.PriceQuote_VoucherCode:
		accountHolder = choice.VoucherCode
	}
	lintPriceQuotesRequest := &pricingv1.LintPriceQuotesRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{
				Region:      req.GetPriceQuote().GetBillingAddress().GetRegion(),
				CountryCode: req.GetPriceQuote().GetBillingAddress().GetCountryCode(),
			},
			Settlement: &pricingv1.PriceQuote_Bank{Bank: &pricingv1.PriceQuoteBankSettlement{AccountHolder: accountHolder}},
		},
	}
	lintPriceQuotesReply, err := pricingv1.PriceQuoteValidationServiceClient.LintPriceQuotes(s.priceQuoteValidationClient, ctx, lintPriceQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/LintPriceQuotes")
	}
	return &pricingv1.ApprovePriceQuoteResponse{
		Outcome: &pricingv1.ApprovePriceQuoteResponse_Outcome{
			Status: scheduleCurrencyRateReportReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message: scheduleCurrencyRateReportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PriceQuote: &pricingv1.PriceQuote{
			Counterparty: &pricingv1.PriceListRef{
				PriceListId: lintPriceQuotesReply.GetPriceQuote().GetCounterparty().GetPriceListId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintPriceQuotesReply.GetPriceQuote().GetAudit().GetCreatedAt(),
			},
			PriceQuoteId:   req.GetPriceQuoteId(),
			BillingAddress: req.GetPriceQuote().GetBillingAddress(),
		},
	}, nil
}

// RejectPriceQuote rejects a price quote.
func (s *PriceQuoteApprovalServer) RejectPriceQuote(ctx context.Context, req *pricingv1.RejectPriceQuoteRequest) (*pricingv1.RejectPriceQuoteResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "reject price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *searchv1.ShareSearchIndexExportRequest, ...grpc.CallOption) (*searchv1.ShareSearchIndexExportResponse, error){
		"primary":  s.searchIndexExportClient.ShareSearchIndexExport,
		"fallback": s.searchIndexExportClientFallback.ShareSearchIndexExport,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	shareSearchIndexExportRequest := &searchv1.ShareSearchIndexExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.ShareSearchIndexExportRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		Reason:      req.GetReason(),
		Annotations: annotations,
	}
	shareSearchIndexExportReply, err := route(ctx, shareSearchIndexExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ShareSearchIndexExport")
	}
	advanceBackorderWorkflowRequest := &ordersv1.AdvanceBackorderWorkflowRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetPriceQuote().GetAudit().GetCreatedAt()},
			Comments: []*ordersv1.BackorderComment{&ordersv1.BackorderComment{
				CreatedAt: shareSearchIndexExportReply.GetSearchIndex().GetAudit().GetCreatedAt(),
			}},
		},
	}
	advanceBackorderWorkflowReply, err := s.backorderWorkflowLazy().AdvanceBackorderWorkflow(ctx, advanceBackorderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderWorkflowService/AdvanceBackorderWorkflow")
	}
	return &pricingv1.RejectPriceQuoteResponse{
		Outcome: &pricingv1.RejectPriceQuoteResponse_Outcome{
			ReferenceId: shareSearchIndexExportReply.GetOutcome().GetReferenceId(),
		},
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				Etag: advanceBackorderWorkflowReply.GetBackorder().GetAudit().GetEtag(),
			},
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: advanceBackorderWorkflowReply.GetBackorder().GetAssignee().GetDisplayName(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			Metadata:     req.GetPriceQuote().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalatePriceQuote escalates a price quote.
func (s *PriceQuoteApprovalServer) EscalatePriceQuote(ctx context.Context, req *pricingv1.EscalatePriceQuoteRequest) (*pricingv1.EscalatePriceQuoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "escalate price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	explainPriceQuoteDecisionRequest := &pricingv1.ExplainPriceQuoteDecisionRequest{
		Context: req.GetContext(),
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{
				Lines:    req.GetPriceQuote().GetBillingAddress().GetLines(),
				Locality: req.GetPriceQuote().GetBillingAddress().GetLocality(),
			},
			Settlement: &pricingv1.PriceQuote_Bank{Bank: &pricingv1.PriceQuoteBankSettlement{Iban: req.GetPriceQuote().GetBank().GetIban()}},
		},
	}
	send := s.explainPriceQuoteDecisionSender(explainPriceQuoteDecisionRequest)
	explainPriceQuoteDecisionReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceQuoteValidationService/ExplainPriceQuoteDecision")
	}
	lintPriceOverridesRequest := &pricingv1.LintPriceOverridesRequest{
		Context: req.GetContext(),
		PriceOverride: &pricingv1.PriceOverride{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainPriceQuoteDecisionReply.GetPriceQuote().GetAudit().GetCreatedBy(),
				Etag:      explainPriceQuoteDecisionReply.GetPriceQuote().GetAudit().GetEtag(),
				UpdatedBy: req.GetPriceQuote().GetAudit().GetUpdatedBy(),
			},
		},
	}
	lintPriceOverridesReply := new(pricingv1.LintPriceOverridesResponse)
	if err := grpcx.Invoke(ctx, s.conn, pricingv1.PriceOverrideValidationService_LintPriceOverrides_FullMethodName, lintPriceOverridesRequest, lintPriceOverridesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideValidationService/LintPriceOverrides")
	}
	searchUsersReply, err := s.userClient.SearchUsers(ctx, &identityv1.SearchUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SearchUsersRequest_Filter{
			Region: explainPriceQuoteDecisionReply.GetPriceQuote().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/SearchUsers")
	}
	startPaymentInstrumentWorkflowRequest := &paymentsv1.StartPaymentInstrumentWorkflowRequest{}
	startPaymentInstrumentWorkflowRequest.Context = req.GetContext()
	startPaymentInstrumentWorkflowRequest.PaymentInstrument = &paymentsv1.PaymentInstrument{
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		Holder: &customersv1.CustomerRef{
			DisplayName: lintPriceOverridesReply.GetPriceOverride().GetOwner().GetDisplayName(),
		},
		Funding: &paymentsv1.PaymentInstrument_Card{Card: &paymentsv1.PaymentInstrumentCardFunding{
			Token: explainPriceQuoteDecisionReply.GetPriceQuote().GetCard().GetToken(),
		}},
		Audit: &commonv1.AuditInfo{
			Etag: lintPriceOverridesReply.GetPriceOverride().GetAudit().GetEtag(),
		},
	}
	startPaymentInstrumentWorkflowReply, err := s.paymentInstrumentWorkflowClient.StartPaymentInstrumentWorkflow(ctx, startPaymentInstrumentWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentWorkflowService/StartPaymentInstrumentWorkflow")
	}
	return &pricingv1.EscalatePriceQuoteResponse{
		PriceQuote: &pricingv1.PriceQuote{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: explainPriceQuoteDecisionReply.GetPriceQuote().GetBillingAddress().GetRecipient(),
			},
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Tax: &pricingv1.PriceQuoteLineTax{
					Amount: lintPriceOverridesReply.GetPriceOverride().GetDiscount().GetAmount(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: startPaymentInstrumentWorkflowReply.GetPaymentInstrument().GetAudit().GetCreatedAt(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
		},
		Result: &commonv1.OperationResult{
			Message: searchUsersReply.GetResult().GetMessage(),
			Status:  startPaymentInstrumentWorkflowReply.GetResult().GetStatus(),
		},
		Outcome: &pricingv1.EscalatePriceQuoteResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ListPendingPriceQuotes lists price quotes a page at a time.
func (s *PriceQuoteApprovalServer) ListPendingPriceQuotes(ctx context.Context, req *pricingv1.ListPendingPriceQuotesRequest) (*pricingv1.ListPendingPriceQuotesResponse, error) {
	s.log.InfoContext(ctx, "list pending price quotes", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetContext().GetActor(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	advanceOrderWorkflowRequest := &ordersv1.AdvanceOrderWorkflowRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Audit:          &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &ordersv1.AdvanceOrderWorkflowRequest_Details{
			Notify:     &commonv1.ContactInfo{Email: holder.Email},
			Parameters: labels,
		},
	}
	advanceOrderWorkflowReply, err := grpcx.Call(ctx, s.orderWorkflowClient.AdvanceOrderWorkflow, advanceOrderWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderWorkflowService/AdvanceOrderWorkflow")
	}
	streamCurrencyRateMetricsStream, err := s.currencyRateReportingClient.StreamCurrencyRateMetrics(ctx, &pricingv1.StreamCurrencyRateMetricsRequest{
		Context: req.GetContext(),
		Filter: &pricingv1.StreamCurrencyRateMetricsRequest_Filter{
			Region:  advanceOrderWorkflowReply.GetOrder().GetBillingAddress().GetRegion(),
			OwnerId: req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/StreamCurrencyRateMetrics")
	}
	for {
		received, err := streamCurrencyRateMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateReportingService/StreamCurrencyRateMetrics")
		}
		_ = received
	}
	return &pricingv1.ListPendingPriceQuotesResponse{
		PriceQuotes: []*pricingv1.PriceQuote{&pricingv1.PriceQuote{
			Metadata: advanceOrderWorkflowReply.GetOrder().GetMetadata(),
		}},
		Result: &commonv1.OperationResult{Status: advanceOrderWorkflowReply.GetResult().GetStatus()},
	}, nil
}

// DelegatePriceQuoteApproval delegates a price quote.
func (s *PriceQuoteApprovalServer) DelegatePriceQuoteApproval(ctx context.Context, req *pricingv1.DelegatePriceQuoteApprovalRequest) (*pricingv1.DelegatePriceQuoteApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "delegate price quote approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetPriceQuote())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateCurrencyRateReply, err := s.currencyRateValidationClient.ValidateCurrencyRate(ctx, &pricingv1.ValidateCurrencyRateRequest{
		Context: req.GetContext(),
		CurrencyRate: &pricingv1.CurrencyRate{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pricing.v1.CurrencyRateValidationService/ValidateCurrencyRate")
	}
	searchExperimentsRequest := newSearchExperimentsRequest(withSearchExperimentsContext(req.GetContext()), withSearchExperimentsFilter(&experimentsv1.SearchExperimentsRequest_Filter{
		Region:       req.GetPriceQuote().GetBillingAddress().GetRegion(),
		CreatedAfter: req.GetPriceQuote().GetAudit().GetUpdatedAt(),
		OwnerId:      req.GetPriceQuote().GetCounterparty().GetPriceListId(),
		Labels:       req.GetPriceQuote().GetMetadata(),
	}))
	searchExperimentsReply, err := s.experimentClient.SearchExperiments(ctx, searchExperimentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.experiments.v1.ExperimentService/SearchExperiments")
	}
	calculateCustomerReply, err := s.customerCalculationClient.CalculateCustomer(ctx, &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	return &pricingv1.DelegatePriceQuoteApprovalResponse{
		PriceQuote: &pricingv1.PriceQuote{
			Lines: []*pricingv1.PriceQuoteLine{&pricingv1.PriceQuoteLine{
				Attributes: validateCurrencyRateReply.GetCurrencyRate().GetAttributes(),
			}},
			PriceQuoteId: req.GetPriceQuoteId(),
			Counterparty: req.GetPriceQuote().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{
			Message: validateCurrencyRateReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &pricingv1.DelegatePriceQuoteApprovalResponse_Outcome{
			Status:  searchExperimentsReply.GetResult().GetStatus(),
			Message: calculateCustomerReply.GetResult().GetMessage(),
		},
	}, nil
}

// RecallPriceQuote recalls a price quote.
func (s *PriceQuoteApprovalServer) RecallPriceQuote(ctx context.Context, req *pricingv1.RecallPriceQuoteRequest) (*pricingv1.RecallPriceQuoteResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "recall price quote", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	for _, line := range req.GetPriceQuote().GetLines() {
		if _, err := s.preferenceCalculationClient.RecalculatePreferences(ctx, &customersv1.RecalculatePreferencesRequest{
			Context: req.GetContext(),
			Preference: &customersv1.Preference{
				Description: line.GetDescription(),
				Action:      &customersv1.Preference_Discount{Discount: &customersv1.PreferenceDiscountAction{Amount: line.GetAmount()}},
				Audit:       &commonv1.AuditInfo{UpdatedBy: req.GetPriceQuote().GetAudit().GetUpdatedBy()},
			},
			Details: &customersv1.RecalculatePreferencesRequest_Details{
				Destination: &commonv1.PostalAddress{
					Recipient: req.GetPriceQuote().GetBillingAddress().GetRecipient(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/RecalculatePreferences")
		}
	}
	ctx = tenancy.With(ctx, "locality", req.GetPriceQuote().GetBillingAddress().GetLocality())
	ctx = tenancy.With(ctx, "display_name", req.GetPriceQuote().GetCounterparty().GetDisplayName())
	submitRefundRequest := &paymentsv1.SubmitRefundRequest{}
	submitRefundRequest.Context = req.GetContext()
	submitRefundRequest.Refund = &paymentsv1.Refund{
		Audit:          &commonv1.AuditInfo{CreatedBy: req.GetPriceQuote().GetAudit().GetCreatedBy()},
		BillingAddress: &commonv1.PostalAddress{Locality: tenancy.Value(ctx, "locality")},
	}
	submitRefundRequest.Details = &paymentsv1.SubmitRefundRequest_Details{
		Approver: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
	}
	submitRefundReply, err := s.refundApprovalClient.SubmitRefund(ctx, submitRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundApprovalService/SubmitRefund")
	}
	priceQuoteCache := cache.New[string, *pricingv1.PriceQuote](1)
	priceQuoteCache.Put(req.GetContext().GetTraceId(), req.GetPriceQuote())
	cachedPriceQuote, _ := priceQuoteCache.Get(req.GetContext().GetTraceId())
	validateHouseholdRequest := &customersv1.ValidateHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Region: submitRefundReply.GetRefund().GetBillingAddress().GetRegion(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: cachedPriceQuote.GetAudit().GetUpdatedBy(),
						CreatedAt: req.GetPriceQuote().GetAudit().GetCreatedAt(),
					},
				}},
			}},
		},
	}
	validateHouseholdReply, err := s.meteredHouseholdValidation.ValidateHousehold(ctx, validateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ValidateHousehold")
	}
	var accountHolder string
	switch choice := req.GetPriceQuote().GetSettlement().(type) {
	case *pricingv1.PriceQuote_Card:
		accountHolder = choice.Card.GetHolderName()
	case *pricingv1.PriceQuote_Bank:
		accountHolder = choice.Bank.GetAccountHolder()
	case *pricingv1.PriceQuote_VoucherCode:
		accountHolder = choice.VoucherCode
	}
	resumeOrderWorkflowRequest := &ordersv1.ResumeOrderWorkflowRequest{
		Context: req.GetContext(),
		Order: &ordersv1.Order{
			Settlement: &ordersv1.Order_Bank{Bank: &ordersv1.OrderBankSettlement{AccountHolder: accountHolder}},
		},
	}
	type resumeOrderWorkflowResult struct {
		reply *ordersv1.ResumeOrderWorkflowResponse
		err   error
	}
	resumeOrderWorkflowResults := make(chan resumeOrderWorkflowResult, 1)
	go func() {
		reply, err := s.orderWorkflowClient.ResumeOrderWorkflow(ctx, resumeOrderWorkflowRequest)
		resumeOrderWorkflowResults <- resumeOrderWorkflowResult{reply, err}
	}()
	var resumeOrderWorkflowReply *ordersv1.ResumeOrderWorkflowResponse
	select {
	case result := <-resumeOrderWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderWorkflowService/ResumeOrderWorkflow")
		}
		resumeOrderWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &pricingv1.RecallPriceQuoteResponse{
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				CreatedBy: submitRefundReply.GetRefund().GetAudit().GetCreatedBy(),
			},
			IssuedAt:     resumeOrderWorkflowReply.GetOrder().GetIssuedAt(),
			PriceQuoteId: req.GetPriceQuoteId(),
		},
		Outcome: &pricingv1.RecallPriceQuoteResponse_Outcome{
			Message:     submitRefundReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: validateHouseholdReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetPriceQuoteApprovalStatus returns a price quote.
func (s *PriceQuoteApprovalServer) GetPriceQuoteApprovalStatus(ctx context.Context, req *pricingv1.GetPriceQuoteApprovalStatusRequest) (*pricingv1.GetPriceQuoteApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "price_quote", req.GetPriceQuoteId(), "get_price_quote_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get price quote approval status", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "locale", req.GetContext().GetLocale())
	normalizeConsentRecordRequest := &customersv1.NormalizeConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizeConsentRecordRequest_Details{
			Locale:  tenancy.Value(ctx, "locale"),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
		ConsentRecord: &customersv1.ConsentRecord{Source: req.GetPriceQuoteId()},
	}
	var normalizeConsentRecordReply *customersv1.NormalizeConsentRecordResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord", func(ctx context.Context) error {
		answer, err := s.consentRecordValidationClient.NormalizeConsentRecord(ctx, normalizeConsentRecordRequest)
		normalizeConsentRecordReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
	}
	exportPrescriptionsStream, err := s.prescriptionQueryClient.ExportPrescriptions(ctx, &prescriptionsv1.ExportPrescriptionsRequest{
		Context: req.GetContext(),
		Filter: &prescriptionsv1.ExportPrescriptionsRequest_Filter{
			CreatedAfter: normalizeConsentRecordReply.GetOutcome().GetEffectiveAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.prescriptions.v1.PrescriptionQueryService/ExportPrescriptions")
	}
	for {
		received, err := exportPrescriptionsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.prescriptions.v1.PrescriptionQueryService/ExportPrescriptions")
		}
		_ = received
	}
	return &pricingv1.GetPriceQuoteApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Details: normalizeConsentRecordReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PriceQuote: &pricingv1.PriceQuote{PriceQuoteId: req.GetPriceQuoteId()},
	}, nil
}

// CommentPriceQuoteApproval comments a price quote.
func (s *PriceQuoteApprovalServer) CommentPriceQuoteApproval(ctx context.Context, req *pricingv1.CommentPriceQuoteApprovalRequest) (*pricingv1.CommentPriceQuoteApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentPriceQuoteApproval", "error", err)
		return nil, err
	}
	if err := s.store.SavePriceQuote(ctx, req.GetContext().GetTenantId(), req.GetPriceQuote()); err != nil {
		return nil, grpcx.Wrap(err, "save price quote")
	}
	s.log.InfoContext(ctx, "comment price quote approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPriceQuote().GetAudit().GetEtag())
	recallRefundRequest := buildRecallRefundRequestForCommentPriceQuoteApproval(req)
	recallRefundReply, err := s.refundApprovalClient.RecallRefund(ctx, recallRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundApprovalService/RecallRefund")
	}
	return &pricingv1.CommentPriceQuoteApprovalResponse{
		PriceQuote: &pricingv1.PriceQuote{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recallRefundReply.GetRefund().GetAudit().GetUpdatedBy(),
			},
			Counterparty: &pricingv1.PriceListRef{
				DisplayName: recallRefundReply.GetRefund().GetCounterparty().GetDisplayName(),
			},
			PriceQuoteId: req.GetPriceQuoteId(),
			DueAt:        req.GetPriceQuote().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// currencyRateReportingLazy dials acme.pricing.v1.CurrencyRateReportingService the first time a handler needs it.
func (s *PriceQuoteApprovalServer) currencyRateReportingLazy() pricingv1.CurrencyRateReportingServiceClient {
	s.currencyRateReportingOnce.Do(func() { s.currencyRateReportingCached = pricingv1.NewCurrencyRateReportingServiceClient(s.conn) })
	return s.currencyRateReportingCached
}

// backorderWorkflowLazy dials acme.orders.v1.BackorderWorkflowService the first time a handler needs it.
func (s *PriceQuoteApprovalServer) backorderWorkflowLazy() ordersv1.BackorderWorkflowServiceClient {
	s.backorderWorkflowOnce.Do(func() { s.backorderWorkflowCached = ordersv1.NewBackorderWorkflowServiceClient(s.conn) })
	return s.backorderWorkflowCached
}

// explainPriceQuoteDecisionSender returns a function that sends one request to
// acme.pricing.v1.PriceQuoteValidationService/ExplainPriceQuoteDecision.
func (s *PriceQuoteApprovalServer) explainPriceQuoteDecisionSender(request *pricingv1.ExplainPriceQuoteDecisionRequest) func(context.Context) (*pricingv1.ExplainPriceQuoteDecisionResponse, error) {
	return func(ctx context.Context) (*pricingv1.ExplainPriceQuoteDecisionResponse, error) {
		return s.priceQuoteValidationClient.ExplainPriceQuoteDecision(ctx, request)
	}
}
