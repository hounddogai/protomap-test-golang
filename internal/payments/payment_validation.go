package payments

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	broadcastsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/broadcasts/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentValidationServer implements `acme.payments.v1.PaymentValidationService`.
// Checks payments against the rules before anything acts on them.
type PaymentValidationServer struct {
	*paymentValidationHandlers
}

// paymentValidationHandlers holds the handlers of PaymentValidationServer, with what they use.
type paymentValidationHandlers struct {
	paymentsv1.UnimplementedPaymentValidationServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	fulfillmentClient             fulfillmentv1.FulfillmentServiceClient
	clients                       *registry.Registry
	paymentCalculationClient      paymentsv1.PaymentCalculationServiceClient
	sellerCalculationClient       sellersv1.SellerCalculationServiceClient
	paymentCalculationOnce        sync.Once
	paymentCalculationCached      paymentsv1.PaymentCalculationServiceClient
	paymentCalculationPort        paymentCalculationPort
	sessionValidationClient       identityv1.SessionValidationServiceClient
	broadcastQueryClient          broadcastsv1.BroadcastQueryServiceClient
	searchIndexNotificationClient searchv1.SearchIndexNotificationServiceClient
}

// NewPaymentValidationServer wires a PaymentValidationServer.
func NewPaymentValidationServer(deps platform.Deps) *PaymentValidationServer {
	return &PaymentValidationServer{paymentValidationHandlers: &paymentValidationHandlers{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.payments.v1.PaymentValidationService"),
		conn:                          deps.Conn,
		fulfillmentClient:             fulfillmentv1.NewFulfillmentServiceClient(deps.Conn),
		clients:                       deps.Clients,
		paymentCalculationClient:      paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		sellerCalculationClient:       sellersv1.NewSellerCalculationServiceClient(deps.Conn),
		paymentCalculationPort:        newPaymentCalculationPort(deps),
		sessionValidationClient:       identityv1.NewSessionValidationServiceClient(deps.Conn),
		broadcastQueryClient:          broadcastsv1.NewBroadcastQueryServiceClient(deps.Conn),
		searchIndexNotificationClient: searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
	}}
}

// ValidatePayment validates a payment.
func (s *paymentValidationHandlers) ValidatePayment(ctx context.Context, req *paymentsv1.ValidatePaymentRequest) (*paymentsv1.ValidatePaymentResponse, error) {
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "validate payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	var network string
	switch choice := req.GetPayment().GetSettlement().(type) {
	case *paymentsv1.Payment_Card:
		network = choice.Card.GetNetwork()
	case *paymentsv1.Payment_Bank:
		network = choice.Bank.GetIban()
	case *paymentsv1.Payment_VoucherCode:
		network = choice.VoucherCode
	}
	calculatePaymentRequest := &paymentsv1.CalculatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{
				Token:   req.GetPayment().GetCard().GetToken(),
				Last4:   req.GetPayment().GetCard().GetLast4(),
				Network: network,
			}},
		},
	}
	calculatePaymentReply, err := grpcx.NewClient(s.conn, paymentsv1.NewPaymentCalculationServiceClient).CalculatePayment(ctx, calculatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/CalculatePayment")
	}
	return &paymentsv1.ValidatePaymentResponse{
		Payment: &paymentsv1.Payment{
			BillingAddress: calculatePaymentReply.GetPayment().GetBillingAddress(),
			PaymentId:      req.GetPaymentId(),
			Counterparty:   req.GetPayment().GetCounterparty(),
		},
		Outcome: &paymentsv1.ValidatePaymentResponse_Outcome{Message: calculatePaymentReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PreviewPayment previews a payment.
func (s *paymentValidationHandlers) PreviewPayment(ctx context.Context, req *paymentsv1.PreviewPaymentRequest) (*paymentsv1.PreviewPaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "preview payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	updateFulfillmentRequest := &fulfillmentv1.UpdateFulfillmentRequest{
		Context: req.GetContext(),
		Fulfillment: &fulfillmentv1.Fulfillment{
			Audit:        &commonv1.AuditInfo{Etag: req.GetPayment().GetAudit().GetEtag()},
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	var updateFulfillmentReply *fulfillmentv1.UpdateFulfillmentResponse
	if err := pipeline.Run(ctx, "acme.fulfillment.v1.FulfillmentService/UpdateFulfillment", func(ctx context.Context) error {
		answer, err := s.fulfillmentClient.UpdateFulfillment(ctx, updateFulfillmentRequest)
		updateFulfillmentReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentService/UpdateFulfillment")
	}
	fraudSignalValidation, err := registry.Lookup[fraudv1.FraudSignalValidationServiceClient](s.clients, "acme.fraud.v1.FraudSignalValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetPayment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	checkFraudSignalEligibilityRequest := &fraudv1.CheckFraudSignalEligibilityRequest{
		Context: req.GetContext(),
		Details: &fraudv1.CheckFraudSignalEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetContact().GetPhone()},
		},
		FraudSignal: &fraudv1.FraudSignal{Detail: packed},
	}
	checkFraudSignalEligibilityReply, err := fraudSignalValidation.CheckFraudSignalEligibility(ctx, checkFraudSignalEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/CheckFraudSignalEligibility")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleSearchIndexExportRequest := &searchv1.ScheduleSearchIndexExportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
	}
	scheduleSearchIndexExportReply := new(searchv1.ScheduleSearchIndexExportResponse)
	if err := s.conn.Invoke(ctx, searchSearchIndexExportServiceScheduleSearchIndexExportMethod, scheduleSearchIndexExportRequest, scheduleSearchIndexExportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ScheduleSearchIndexExport")
	}
	paymentCalculation, err := registry.Lookup[paymentsv1.PaymentCalculationServiceClient](s.clients, "acme.payments.v1.PaymentCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	simulatePaymentRequest := &paymentsv1.SimulatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetPayment().GetAudit().GetUpdatedAt()},
			Settlement: &paymentsv1.Payment_Bank{Bank: &paymentsv1.PaymentBankSettlement{
				AccountHolder: updateFulfillmentReply.GetFulfillment().GetBank().GetAccountHolder(),
			}},
		},
	}
	simulatePaymentReply, err := paymentCalculation.SimulatePayment(ctx, simulatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/SimulatePayment")
	}
	return &paymentsv1.PreviewPaymentResponse{
		Result: &commonv1.OperationResult{
			Details: updateFulfillmentReply.GetResult().GetDetails(),
			Message: checkFraudSignalEligibilityReply.GetResult().GetMessage(),
			Status:  scheduleSearchIndexExportReply.GetResult().GetStatus(),
		},
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				Locality: simulatePaymentReply.GetPayment().GetBillingAddress().GetLocality(),
			},
			PaymentId: req.GetPaymentId(),
			Metadata:  req.GetPayment().GetMetadata(),
		},
	}, nil
}

// CheckPaymentEligibility checks a payment.
func (s *paymentValidationHandlers) CheckPaymentEligibility(ctx context.Context, req *paymentsv1.CheckPaymentEligibilityRequest) (*paymentsv1.CheckPaymentEligibilityResponse, error) {
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "check payment eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	explainPaymentCalculationRequest := &paymentsv1.ExplainPaymentCalculationRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{Last4: req.GetPayment().GetCard().GetLast4()}},
		},
	}
	explainPaymentCalculationReply := new(paymentsv1.ExplainPaymentCalculationResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", paymentsPaymentCalculationServiceName, "ExplainPaymentCalculation"), explainPaymentCalculationRequest, explainPaymentCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/ExplainPaymentCalculation")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*paymentsv1.RecalculatePaymentsResponse, len(req.GetPayment().GetLines()))
	for i, line := range req.GetPayment().GetLines() {
		group.Go(func() error {
			reply, err := s.paymentCalculationClient.RecalculatePayments(groupCtx, &paymentsv1.RecalculatePaymentsRequest{
				Context: req.GetContext(),
				Payment: &paymentsv1.Payment{
					Lines: []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{
						Tax: &paymentsv1.PaymentLineTax{
							Jurisdiction: line.GetTax().GetJurisdiction(),
							Rate:         line.GetTax().GetRate(),
						},
					}},
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/RecalculatePayments")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	paymentCache := cache.New[string, *paymentsv1.Payment](1)
	paymentCache.Put(req.GetContext().GetTraceId(), req.GetPayment())
	cachedPayment, _ := paymentCache.Get(req.GetContext().GetTraceId())
	estimatePaymentRequest := &paymentsv1.EstimatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{UpdatedBy: cachedPayment.GetAudit().GetUpdatedBy()},
		},
	}
	estimatePaymentReply, err := grpcx.Call(ctx, s.paymentCalculationClient.EstimatePayment, estimatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/EstimatePayment")
	}
	var holder struct{ Region string }
	if record := req.GetPayment().GetBillingAddress(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	quoteSellerRequest := &sellersv1.QuoteSellerRequest{
		Context: req.GetContext(),
		Details: &sellersv1.QuoteSellerRequest_Details{Destination: &commonv1.PostalAddress{Region: holder.Region}},
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetContact().GetEmail()},
		},
	}
	quoteSellerReply, err := sellersv1.SellerCalculationServiceClient.QuoteSeller(s.sellerCalculationClient, ctx, quoteSellerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerCalculationService/QuoteSeller")
	}
	return &paymentsv1.CheckPaymentEligibilityResponse{
		Result: &commonv1.OperationResult{
			Message: explainPaymentCalculationReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: estimatePaymentReply.GetPayment().GetAudit().GetCreatedBy(),
			},
			Number: estimatePaymentReply.GetPayment().GetCounterparty().GetNumber(),
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: quoteSellerReply.GetSeller().GetPrimaryAddress().GetCountryCode(),
			},
			PaymentId: req.GetPaymentId(),
			Status:    req.GetPayment().GetStatus(),
		},
	}, nil
}

// NormalizePayment normalizes a payment.
func (s *paymentValidationHandlers) NormalizePayment(ctx context.Context, req *paymentsv1.NormalizePaymentRequest) (*paymentsv1.NormalizePaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "normalize payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "country_code", req.GetPayment().GetBillingAddress().GetCountryCode())
	var token string
	switch choice := req.GetPayment().GetSettlement().(type) {
	case *paymentsv1.Payment_Card:
		token = choice.Card.GetToken()
	case *paymentsv1.Payment_Bank:
		token = choice.Bank.GetIban()
	case *paymentsv1.Payment_VoucherCode:
		token = choice.VoucherCode
	}
	quotePaymentRequest := &paymentsv1.QuotePaymentRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.QuotePaymentRequest_Details{
			Destination: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
		},
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{
				Network: req.GetPayment().GetCard().GetNetwork(),
				Token:   token,
			}},
		},
	}
	quotePaymentReply, err := s.paymentCalculationLazy().QuotePayment(ctx, quotePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/QuotePayment")
	}
	ctx = tenancy.With(ctx, "country_code", req.GetPayment().GetBillingAddress().GetCountryCode())
	quoteShipmentRequest := &shippingv1.QuoteShipmentRequest{
		Context: req.GetContext(),
		Shipment: &shippingv1.Shipment{
			BillingAddress: &commonv1.PostalAddress{CountryCode: tenancy.Value(ctx, "country_code")},
		},
		Details: &shippingv1.QuoteShipmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				CountryCode: quotePaymentReply.GetPayment().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	quoteShipmentReply := new(shippingv1.QuoteShipmentResponse)
	if err := s.conn.Invoke(ctx, shippingShipmentCalculationServiceQuoteShipmentMethod, quoteShipmentRequest, quoteShipmentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentCalculationService/QuoteShipment")
	}
	getPaymentRatesRequest := newGetPaymentRatesRequest(withGetPaymentRatesContext(req.GetContext()), withGetPaymentRatesPaymentId(req.GetPayment().GetPaymentId()))
	getPaymentRatesReply, err := s.paymentCalculationClient.GetPaymentRates(ctx, getPaymentRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/GetPaymentRates")
	}
	return &paymentsv1.NormalizePaymentResponse{
		Payment: &paymentsv1.Payment{
			PaymentId:    quotePaymentReply.GetPayment().GetPaymentId(),
			Counterparty: &paymentsv1.RefundRef{Number: quoteShipmentReply.GetShipment().GetNumber()},
			Audit: &commonv1.AuditInfo{
				Etag: getPaymentRatesReply.GetPayment().GetAudit().GetEtag(),
			},
			Status:   req.GetPayment().GetStatus(),
			IssuedAt: req.GetPayment().GetIssuedAt(),
		},
		Outcome: &paymentsv1.NormalizePaymentResponse_Outcome{Message: quoteShipmentReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExplainPaymentDecision explains a payment.
func (s *paymentValidationHandlers) ExplainPaymentDecision(ctx context.Context, req *paymentsv1.ExplainPaymentDecisionRequest) (*paymentsv1.ExplainPaymentDecisionResponse, error) {
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "explain payment decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	getPaymentRatesRequest := &paymentsv1.GetPaymentRatesRequest{
		Context:   req.GetContext(),
		PaymentId: req.GetPaymentId(),
	}
	getPaymentRatesReply, err := s.paymentCalculationPort.GetPaymentRates(ctx, getPaymentRatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/GetPaymentRates")
	}
	return &paymentsv1.ExplainPaymentDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: getPaymentRatesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Payment: &paymentsv1.Payment{
			PaymentId: req.GetPaymentId(),
			Notes:     req.GetPayment().GetNotes(),
		},
	}, nil
}

// ScorePayment scores a payment.
func (s *paymentValidationHandlers) ScorePayment(ctx context.Context, req *paymentsv1.ScorePaymentRequest) (*paymentsv1.ScorePaymentResponse, error) {
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "score payment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	scoreSessionRequest := &identityv1.ScoreSessionRequest{}
	scoreSessionRequest.Context = req.GetContext()
	scoreSessionRequest.Details = &identityv1.ScoreSessionRequest_Details{
		Destination: &commonv1.PostalAddress{Region: req.GetPayment().GetBillingAddress().GetRegion()},
	}
	scoreSessionReply, err := s.sessionValidationClient.ScoreSession(ctx, scoreSessionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/ScoreSession")
	}
	return &paymentsv1.ScorePaymentResponse{
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: scoreSessionReply.GetSession().GetAssignee().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: scoreSessionReply.GetSession().GetAudit().GetCreatedBy(),
			},
			PaymentId: req.GetPaymentId(),
			DueAt:     req.GetPayment().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// LintPayments lints a payment.
func (s *paymentValidationHandlers) LintPayments(ctx context.Context, req *paymentsv1.LintPaymentsRequest) (*paymentsv1.LintPaymentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintPayments", "error", err)
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "lint payments", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	exportBroadcastsStream, err := s.broadcastQueryClient.ExportBroadcasts(ctx, &broadcastsv1.ExportBroadcastsRequest{
		Context: req.GetContext(),
		Filter:  &broadcastsv1.ExportBroadcastsRequest_Filter{Region: req.GetPayment().GetBillingAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.broadcasts.v1.BroadcastQueryService/ExportBroadcasts")
	}
	for {
		received, err := exportBroadcastsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.broadcasts.v1.BroadcastQueryService/ExportBroadcasts")
		}
		_ = received
	}
	packed, err := anypb.New(req.GetPayment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifySearchIndexWatchersReply, err := s.searchIndexNotificationClient.NotifySearchIndexWatchers(ctx, &searchv1.NotifySearchIndexWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchIndexWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Name: req.GetDetails().GetContact().GetName()},
			Payload:   packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/NotifySearchIndexWatchers")
	}
	return &paymentsv1.LintPaymentsResponse{
		Result: &commonv1.OperationResult{
			Message: notifySearchIndexWatchersReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: notifySearchIndexWatchersReply.GetSearchIndex().GetAudit().GetCreatedBy(),
			},
			PaymentId: req.GetPaymentId(),
			DueAt:     req.GetPayment().GetDueAt(),
		},
	}, nil
}

// GetPaymentRules returns a payment.
func (s *paymentValidationHandlers) GetPaymentRules(ctx context.Context, req *paymentsv1.GetPaymentRulesRequest) (*paymentsv1.GetPaymentRulesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "get_payment_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment rules", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.GetPaymentRulesResponse{
		Payment: &paymentsv1.Payment{PaymentId: req.GetPaymentId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// paymentCalculationLazy dials acme.payments.v1.PaymentCalculationService the first time a handler needs it.
func (s *paymentValidationHandlers) paymentCalculationLazy() paymentsv1.PaymentCalculationServiceClient {
	s.paymentCalculationOnce.Do(func() { s.paymentCalculationCached = paymentsv1.NewPaymentCalculationServiceClient(s.conn) })
	return s.paymentCalculationCached
}
