package payments

import (
	"context"
	"log/slog"
	"sync"

	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	routingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/routing/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentInstrumentCalculationServer implements `acme.payments.v1.PaymentInstrumentCalculationService`.
// Computes the figures of a payment instrument, such as totals, taxes, and estimates.
type PaymentInstrumentCalculationServer struct {
	paymentsv1.UnimplementedPaymentInstrumentCalculationServiceServer
	store                         *Store
	log                           *slog.Logger
	apiCredentialValidationClient identityv1.ApiCredentialValidationServiceClient
	conn                          *grpc.ClientConn
	paymentValidationOnce         sync.Once
	paymentValidationCached       paymentsv1.PaymentValidationServiceClient
	clients                       *registry.Registry
	paymentValidationClient       paymentsv1.PaymentValidationServiceClient
	riskAssessmentQueryPort       riskAssessmentQueryPort
	refundCalculationClient       paymentsv1.RefundCalculationServiceClient
	routeCalculationClient        routingv1.RouteCalculationServiceClient
	subscriptionQueryClient       subscriptionsv1.SubscriptionQueryServiceClient
	meteredRefundCalculation      meteredRefundCalculation
	returnLabelQueryPort          returnLabelQueryPort
	searchQueryNotificationClient searchv1.SearchQueryNotificationServiceClient
}

// NewPaymentInstrumentCalculationServer wires a PaymentInstrumentCalculationServer.
func NewPaymentInstrumentCalculationServer(deps platform.Deps) *PaymentInstrumentCalculationServer {
	return &PaymentInstrumentCalculationServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.payments.v1.PaymentInstrumentCalculationService"),
		apiCredentialValidationClient: identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		conn:                          deps.Conn,
		clients:                       deps.Clients,
		paymentValidationClient:       paymentsv1.NewPaymentValidationServiceClient(deps.Conn),
		riskAssessmentQueryPort:       newRiskAssessmentQueryPort(deps),
		refundCalculationClient:       paymentsv1.NewRefundCalculationServiceClient(deps.Conn),
		routeCalculationClient:        routingv1.NewRouteCalculationServiceClient(deps.Conn),
		subscriptionQueryClient:       subscriptionsv1.NewSubscriptionQueryServiceClient(deps.Conn),
		meteredRefundCalculation:      meteredRefundCalculation{RefundCalculationServiceClient: paymentsv1.NewRefundCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		returnLabelQueryPort:          newReturnLabelQueryPort(deps),
		searchQueryNotificationClient: searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
	}
}

// CalculatePaymentInstrument calculates a payment instrument.
func (s *PaymentInstrumentCalculationServer) CalculatePaymentInstrument(ctx context.Context, req *paymentsv1.CalculatePaymentInstrumentRequest) (*paymentsv1.CalculatePaymentInstrumentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "calculate payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetPaymentInstrument().GetContact().GetEmail())
	var iban string
	switch choice := req.GetPaymentInstrument().GetFunding().(type) {
	case *paymentsv1.PaymentInstrument_Card:
		iban = choice.Card.GetHolderName()
	case *paymentsv1.PaymentInstrument_Bank:
		iban = choice.Bank.GetIban()
	case *paymentsv1.PaymentInstrument_ExternalAccountId:
		iban = choice.ExternalAccountId
	}
	normalizeApiCredentialRequest := &identityv1.NormalizeApiCredentialRequest{
		Context: req.GetContext(),
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			Funding: &identityv1.ApiCredential_Bank{Bank: &identityv1.ApiCredentialBankFunding{Iban: iban}},
		},
	}
	normalizeApiCredentialReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*identityv1.NormalizeApiCredentialResponse, error) {
		return s.apiCredentialValidationClient.NormalizeApiCredential(ctx, normalizeApiCredentialRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/NormalizeApiCredential")
	}
	explainPaymentDecisionRequest := &paymentsv1.ExplainPaymentDecisionRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizeApiCredentialReply.GetApiCredential().GetAudit().GetCreatedBy(),
			},
		},
	}
	explainPaymentDecisionReply, err := s.paymentValidationLazy().ExplainPaymentDecision(ctx, explainPaymentDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/ExplainPaymentDecision")
	}
	paymentValidation, ok := s.clients.Get("acme.payments.v1.PaymentValidationService").(paymentsv1.PaymentValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.payments.v1.PaymentValidationService")
	}
	lintPaymentsRequest := &paymentsv1.LintPaymentsRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{
				Last4: normalizeApiCredentialReply.GetApiCredential().GetCard().GetLast4(),
			}},
		},
		Details: &paymentsv1.LintPaymentsRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetPaymentInstrument().GetContact().GetName()},
		},
	}
	lintPaymentsReply, err := paymentValidation.LintPayments(ctx, lintPaymentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/LintPayments")
	}
	simulateRefundRequest := &paymentsv1.SimulateRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{Etag: req.GetPaymentInstrument().GetAudit().GetEtag()},
		},
	}
	simulateRefundReply, err := simulateRefundCall(ctx, simulateRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/SimulateRefund")
	}
	_ = simulateRefundReply
	return &paymentsv1.CalculatePaymentInstrumentResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				CreatedAt: normalizeApiCredentialReply.GetApiCredential().GetAudit().GetCreatedAt(),
				Etag:      explainPaymentDecisionReply.GetPayment().GetAudit().GetEtag(),
			},
			Holder: &customersv1.CustomerRef{
				DisplayName: explainPaymentDecisionReply.GetPayment().GetCounterparty().GetDisplayName(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Available:           req.GetPaymentInstrument().GetAvailable(),
		},
		Result: &commonv1.OperationResult{
			Status:       lintPaymentsReply.GetOutcome().GetStatus(),
			ReferenceIds: lintPaymentsReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// EstimatePaymentInstrument estimates a payment instrument.
func (s *PaymentInstrumentCalculationServer) EstimatePaymentInstrument(ctx context.Context, req *paymentsv1.EstimatePaymentInstrumentRequest) (*paymentsv1.EstimatePaymentInstrumentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EstimatePaymentInstrument", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "estimate payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	previewPaymentRequest := &paymentsv1.PreviewPaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetPaymentInstrument().GetAudit().GetUpdatedAt(),
			},
		},
	}
	previewPaymentReply, err := grpcx.Call(ctx, s.paymentValidationClient.PreviewPayment, previewPaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/PreviewPayment")
	}
	browseRiskAssessmentsRequest := &fraudv1.BrowseRiskAssessmentsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.BrowseRiskAssessmentsRequest_Filter{
			Labels:       req.GetPaymentInstrument().GetLabels(),
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: previewPaymentReply.GetPayment().GetAudit().GetCreatedAt(),
		},
	}
	browseRiskAssessmentsReply, err := s.riskAssessmentQueryPort.BrowseRiskAssessments(ctx, browseRiskAssessmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentQueryService/BrowseRiskAssessments")
	}
	explainRefundCalculationRequest := &paymentsv1.ExplainRefundCalculationRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Settlement: &paymentsv1.Refund_Bank{Bank: &paymentsv1.RefundBankSettlement{Bic: previewPaymentReply.GetPayment().GetBank().GetBic()}},
		},
	}
	send := s.explainRefundCalculationSender(explainRefundCalculationRequest)
	explainRefundCalculationReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/ExplainRefundCalculation")
	}
	labels := make(map[string]string, len(req.GetPaymentInstrument().GetLabels()))
	for key, value := range req.GetPaymentInstrument().GetLabels() {
		labels[key] = value
	}
	countReturnLabelsRequest := &returnsv1.CountReturnLabelsRequest{
		Context: req.GetContext(),
		Filter: &returnsv1.CountReturnLabelsRequest_Filter{
			Region: explainRefundCalculationReply.GetRefund().GetBillingAddress().GetRegion(),
			Labels: labels,
		},
	}
	countReturnLabelsReply, err := grpcx.NewClient(s.conn, returnsv1.NewReturnLabelQueryServiceClient).CountReturnLabels(ctx, countReturnLabelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelQueryService/CountReturnLabels")
	}
	return &paymentsv1.EstimatePaymentInstrumentResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit:               &commonv1.AuditInfo{Etag: previewPaymentReply.GetPayment().GetAudit().GetEtag()},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Holder:              req.GetPaymentInstrument().GetHolder(),
		},
		Result: &commonv1.OperationResult{
			Status:  browseRiskAssessmentsReply.GetResult().GetStatus(),
			Message: browseRiskAssessmentsReply.GetResult().GetMessage(),
			Details: countReturnLabelsReply.GetResult().GetDetails(),
		},
	}, nil
}

// QuotePaymentInstrument quotes a payment instrument.
func (s *PaymentInstrumentCalculationServer) QuotePaymentInstrument(ctx context.Context, req *paymentsv1.QuotePaymentInstrumentRequest) (*paymentsv1.QuotePaymentInstrumentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "quote payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	step := &paymentInstrumentCalculationServerQuotePaymentInstrumentCalculateRouteStep{request: &routingv1.CalculateRouteRequest{
		Context: req.GetContext(),
		Route: &routingv1.Route{
			Settlement:     &routingv1.Route_Card{Card: &routingv1.RouteCardSettlement{Last4: req.GetPaymentInstrument().GetCard().GetLast4()}},
			BillingAddress: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
	}}
	if err := s.runQuotePaymentInstrumentCalculateRoute(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.routing.v1.RouteCalculationService/CalculateRoute")
	}
	calculateRouteReply := step.reply
	jobs := make(chan *paymentsv1.PaymentInstrumentLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit := range jobs {
				if _, err := s.refundCalculationClient.QuoteRefund(workCtx, &paymentsv1.QuoteRefundRequest{
					Context: req.GetContext(),
					Refund: &paymentsv1.Refund{
						Lines:          []*paymentsv1.RefundLine{&paymentsv1.RefundLine{Tax: &paymentsv1.RefundLineTax{Amount: limit.GetAmount()}}},
						BillingAddress: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
						Audit: &commonv1.AuditInfo{
							UpdatedAt: calculateRouteReply.GetRoute().GetAudit().GetUpdatedAt(),
						},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPaymentInstrument().GetLimits() {
		if workCtx.Err() != nil {
			break
		}
		select {
		case jobs <- queued:
		case <-workCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	stopWork()
	close(failures)
	if err := <-failures; err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/QuoteRefund")
	}
	return &paymentsv1.QuotePaymentInstrumentResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: &commonv1.ContactInfo{
				Email: calculateRouteReply.GetRoute().GetCounterparty().GetEmail(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Balance:             req.GetPaymentInstrument().GetBalance(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ComparePaymentInstrumentOptions compares a payment instrument.
func (s *PaymentInstrumentCalculationServer) ComparePaymentInstrumentOptions(ctx context.Context, req *paymentsv1.ComparePaymentInstrumentOptionsRequest) (*paymentsv1.ComparePaymentInstrumentOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ComparePaymentInstrumentOptions", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "compare payment instrument options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	findSubscriptionsRequest := &subscriptionsv1.FindSubscriptionsRequest{
		Context: req.GetContext(),
		Filter:  &subscriptionsv1.FindSubscriptionsRequest_Filter{Region: tenancy.Value(ctx, "region")},
	}
	var findSubscriptionsReply *subscriptionsv1.FindSubscriptionsResponse
	if err := pipeline.Run(ctx, "acme.subscriptions.v1.SubscriptionQueryService/FindSubscriptions", func(ctx context.Context) error {
		answer, err := s.subscriptionQueryClient.FindSubscriptions(ctx, findSubscriptionsRequest)
		findSubscriptionsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionQueryService/FindSubscriptions")
	}
	quoteRefundRequest := &paymentsv1.QuoteRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{
				Email:      req.GetPaymentInstrument().GetHolder().GetEmail(),
				CustomerId: req.GetPaymentInstrument().GetHolder().GetCustomerId(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Recipient:  req.GetDetails().GetDestination().GetRecipient(),
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	}
	quoteRefundReply, err := s.meteredRefundCalculation.QuoteRefund(ctx, quoteRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/QuoteRefund")
	}
	return &paymentsv1.ComparePaymentInstrumentOptionsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: findSubscriptionsReply.GetResult().GetReferenceIds(),
			Message:      findSubscriptionsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				CreatedBy: quoteRefundReply.GetRefund().GetAudit().GetCreatedBy(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Available:           req.GetPaymentInstrument().GetAvailable(),
		},
	}, nil
}

// ExplainPaymentInstrumentCalculation explains a payment instrument.
func (s *PaymentInstrumentCalculationServer) ExplainPaymentInstrumentCalculation(ctx context.Context, req *paymentsv1.ExplainPaymentInstrumentCalculationRequest) (*paymentsv1.ExplainPaymentInstrumentCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "explain payment instrument calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	labels := make(map[string]string, len(req.GetPaymentInstrument().GetLabels()))
	for key, value := range req.GetPaymentInstrument().GetLabels() {
		labels[key] = value
	}
	summarizeReturnLabelsRequest := &returnsv1.SummarizeReturnLabelsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &returnsv1.SummarizeReturnLabelsRequest_Details{Filters: labels},
	}
	summarizeReturnLabelsReply, err := s.returnLabelQueryPort.SummarizeReturnLabels(ctx, summarizeReturnLabelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelQueryService/SummarizeReturnLabels")
	}
	quoteRefundRequest := &paymentsv1.QuoteRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{Etag: req.GetPaymentInstrument().GetAudit().GetEtag()},
		},
	}
	quoteRefundReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*paymentsv1.QuoteRefundResponse, error) {
		return s.refundCalculationClient.QuoteRefund(ctx, quoteRefundRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/QuoteRefund")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*paymentsv1.RecalculateRefundsResponse, len(req.GetPaymentInstrument().GetLimits()))
	for i, limit := range req.GetPaymentInstrument().GetLimits() {
		group.Go(func() error {
			reply, err := s.refundCalculationClient.RecalculateRefunds(groupCtx, &paymentsv1.RecalculateRefundsRequest{
				Context: req.GetContext(),
				Refund: &paymentsv1.Refund{
					Lines: []*paymentsv1.RefundLine{&paymentsv1.RefundLine{Tax: &paymentsv1.RefundLineTax{Amount: limit.GetAmount()}}},
					Settlement: &paymentsv1.Refund_Card{Card: &paymentsv1.RefundCardSettlement{
						HolderName: quoteRefundReply.GetRefund().GetCard().GetHolderName(),
					}},
					BillingAddress: &commonv1.PostalAddress{
						Region: quoteRefundReply.GetRefund().GetBillingAddress().GetRegion(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedAt: req.GetPaymentInstrument().GetAudit().GetCreatedAt(),
					},
				},
			})
			if err != nil {
				return err
			}
			replies[i] = reply
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/RecalculateRefunds")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	simulateRefundRequest := &paymentsv1.SimulateRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: quoteRefundReply.GetRefund().GetCounterparty().GetDisplayName(),
			},
		},
	}
	simulateRefundReply, err := simulateRefundCall(ctx, simulateRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundCalculationService/SimulateRefund")
	}
	return &paymentsv1.ExplainPaymentInstrumentCalculationResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeReturnLabelsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  summarizeReturnLabelsReply.GetResult().GetReferenceIds(),
			Details:       quoteRefundReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				Email: simulateRefundReply.GetRefund().GetCounterparty().GetEmail(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Status:              req.GetPaymentInstrument().GetStatus(),
		},
	}, nil
}

// GetPaymentInstrumentRates returns a payment instrument.
func (s *PaymentInstrumentCalculationServer) GetPaymentInstrumentRates(ctx context.Context, req *paymentsv1.GetPaymentInstrumentRatesRequest) (*paymentsv1.GetPaymentInstrumentRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "get_payment_instrument_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment instrument rates", "tenant", req.GetContext().GetTenantId())
	carePlanCalculation, ok := s.clients.Get("acme.careplans.v1.CarePlanCalculationService").(careplansv1.CarePlanCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.careplans.v1.CarePlanCalculationService")
	}
	explainCarePlanCalculationRequest := &careplansv1.ExplainCarePlanCalculationRequest{
		Context: req.GetContext(),
		Details: &careplansv1.ExplainCarePlanCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
	}
	explainCarePlanCalculationReply, err := carePlanCalculation.ExplainCarePlanCalculation(ctx, explainCarePlanCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanCalculationService/ExplainCarePlanCalculation")
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	step := &paymentInstrumentCalculationServerGetPaymentInstrumentRatesSubscribeSearchQueryUpdatesStep{request: &searchv1.SubscribeSearchQueryUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.SubscribeSearchQueryUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Payload:   packed,
		},
	}}
	if err := s.runGetPaymentInstrumentRatesSubscribeSearchQueryUpdates(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/SubscribeSearchQueryUpdates")
	}
	subscribeSearchQueryUpdatesReply := step.reply
	return &paymentsv1.GetPaymentInstrumentRatesResponse{
		Result: &commonv1.OperationResult{
			Message: explainCarePlanCalculationReply.GetResult().GetMessage(),
			Details: subscribeSearchQueryUpdatesReply.GetResult().GetDetails(),
			Status:  subscribeSearchQueryUpdatesReply.GetOutcome().GetStatus(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{PaymentInstrumentId: req.GetPaymentInstrumentId()},
	}, nil
}

// paymentValidationLazy dials acme.payments.v1.PaymentValidationService the first time a handler needs it.
func (s *PaymentInstrumentCalculationServer) paymentValidationLazy() paymentsv1.PaymentValidationServiceClient {
	s.paymentValidationOnce.Do(func() { s.paymentValidationCached = paymentsv1.NewPaymentValidationServiceClient(s.conn) })
	return s.paymentValidationCached
}

// explainRefundCalculationSender returns a function that sends one request to
// acme.payments.v1.RefundCalculationService/ExplainRefundCalculation.
func (s *PaymentInstrumentCalculationServer) explainRefundCalculationSender(request *paymentsv1.ExplainRefundCalculationRequest) func(context.Context) (*paymentsv1.ExplainRefundCalculationResponse, error) {
	return func(ctx context.Context) (*paymentsv1.ExplainRefundCalculationResponse, error) {
		return s.refundCalculationClient.ExplainRefundCalculation(ctx, request)
	}
}

// paymentInstrumentCalculationServerQuotePaymentInstrumentCalculateRouteStep carries the request of one step of
// QuotePaymentInstrument and, once it ran, its reply.
type paymentInstrumentCalculationServerQuotePaymentInstrumentCalculateRouteStep struct {
	request *routingv1.CalculateRouteRequest
	reply   *routingv1.CalculateRouteResponse
}

func (s *PaymentInstrumentCalculationServer) runQuotePaymentInstrumentCalculateRoute(ctx context.Context, step *paymentInstrumentCalculationServerQuotePaymentInstrumentCalculateRouteStep) error {
	reply, err := s.routeCalculationClient.CalculateRoute(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// paymentInstrumentCalculationServerGetPaymentInstrumentRatesSubscribeSearchQueryUpdatesStep carries the request of one
// step of GetPaymentInstrumentRates and, once it ran, its reply.
type paymentInstrumentCalculationServerGetPaymentInstrumentRatesSubscribeSearchQueryUpdatesStep struct {
	request *searchv1.SubscribeSearchQueryUpdatesRequest
	reply   *searchv1.SubscribeSearchQueryUpdatesResponse
}

func (s *PaymentInstrumentCalculationServer) runGetPaymentInstrumentRatesSubscribeSearchQueryUpdates(ctx context.Context, step *paymentInstrumentCalculationServerGetPaymentInstrumentRatesSubscribeSearchQueryUpdatesStep) error {
	reply, err := s.searchQueryNotificationClient.SubscribeSearchQueryUpdates(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
