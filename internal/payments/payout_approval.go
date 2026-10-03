package payments

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// PayoutApprovalServer implements `acme.payments.v1.PayoutApprovalService`.
// Routes payouts through approval, with escalation and delegation.
type PayoutApprovalServer struct {
	paymentsv1.UnimplementedPayoutApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	customerClient                  customersv1.CustomerServiceClient
	paymentInstrumentApprovalClient paymentsv1.PaymentInstrumentApprovalServiceClient
	preferenceValidationClient      customersv1.PreferenceValidationServiceClient
	customerCalculationClient       customersv1.CustomerCalculationServiceClient
	returnLabelWorkflowClient       returnsv1.ReturnLabelWorkflowServiceClient
	paymentApprovalClient           paymentsv1.PaymentApprovalServiceClient
	conn                            *grpc.ClientConn
	householdQueryClient            customersv1.HouseholdQueryServiceClient
	collectionWorkflowClient        merchandisingv1.CollectionWorkflowServiceClient
	scheduleDisputeReportFn         func(context.Context, *paymentsv1.ScheduleDisputeReportRequest, ...grpc.CallOption) (*paymentsv1.ScheduleDisputeReportResponse, error)
	clients                         *registry.Registry
}

// NewPayoutApprovalServer wires a PayoutApprovalServer.
func NewPayoutApprovalServer(deps platform.Deps) *PayoutApprovalServer {
	disputeReporting := paymentsv1.NewDisputeReportingServiceClient(deps.Conn)
	return &PayoutApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.payments.v1.PayoutApprovalService"),
		customerClient:                  customersv1.NewCustomerServiceClient(deps.Conn),
		paymentInstrumentApprovalClient: paymentsv1.NewPaymentInstrumentApprovalServiceClient(deps.Conn),
		preferenceValidationClient:      customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		customerCalculationClient:       customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		returnLabelWorkflowClient:       returnsv1.NewReturnLabelWorkflowServiceClient(deps.Conn),
		paymentApprovalClient:           paymentsv1.NewPaymentApprovalServiceClient(deps.Conn),
		conn:                            deps.Conn,
		householdQueryClient:            customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		collectionWorkflowClient:        merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		scheduleDisputeReportFn:         disputeReporting.ScheduleDisputeReport,
		clients:                         deps.Clients,
	}
}

// SubmitPayout submits a payout.
func (s *PayoutApprovalServer) SubmitPayout(ctx context.Context, req *paymentsv1.SubmitPayoutRequest) (*paymentsv1.SubmitPayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitPayout", "error", err)
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "submit payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	for _, line := range req.GetPayout().GetLines() {
		if _, err := s.customerClient.CloneCustomer(ctx, &customersv1.CloneCustomerRequest{
			Context: req.GetContext(),
			Customer: &customersv1.Customer{
				Attributes: line.GetAttributes(),
				PrimaryAddress: &commonv1.PostalAddress{
					CountryCode: req.GetPayout().GetBillingAddress().GetCountryCode(),
				},
				RelationshipManager: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CloneCustomer")
		}
	}
	submitPaymentInstrumentRequest := buildSubmitPaymentInstrumentRequestForSubmitPayout(req)
	submitPaymentInstrumentReply, err := s.paymentInstrumentApprovalClient.SubmitPaymentInstrument(ctx, submitPaymentInstrumentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentApprovalService/SubmitPaymentInstrument")
	}
	checkPreferenceEligibilityRequest := buildCheckPreferenceEligibilityRequestForSubmitPayout(req, submitPaymentInstrumentReply)
	checkPreferenceEligibilityReply, err := s.preferenceValidationClient.CheckPreferenceEligibility(ctx, checkPreferenceEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/CheckPreferenceEligibility")
	}
	return &paymentsv1.SubmitPayoutResponse{
		Result: &commonv1.OperationResult{
			Message: submitPaymentInstrumentReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &paymentsv1.SubmitPayoutResponse_Outcome{
			Message:   submitPaymentInstrumentReply.GetResult().GetMessage(),
			NextSteps: checkPreferenceEligibilityReply.GetOutcome().GetNextSteps(),
		},
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: checkPreferenceEligibilityReply.GetPreference().GetAudit().GetUpdatedAt(),
			},
			PayoutId:     req.GetPayoutId(),
			Counterparty: req.GetPayout().GetCounterparty(),
		},
	}, nil
}

// ApprovePayout approves a payout.
func (s *PayoutApprovalServer) ApprovePayout(ctx context.Context, req *paymentsv1.ApprovePayoutRequest) (*paymentsv1.ApprovePayoutResponse, error) {
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "approve payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	recalculateCustomersRequest := &customersv1.RecalculateCustomersRequest{}
	recalculateCustomersRequest.Context = req.GetContext()
	recalculateCustomersRequest.Customer = &customersv1.Customer{
		Contact:          &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{Number: req.GetPayout().GetCounterparty().GetNumber()}},
	}
	recalculateCustomersReply, err := s.customerCalculationClient.RecalculateCustomers(ctx, recalculateCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/RecalculateCustomers")
	}
	for _, line := range req.GetPayout().GetLines() {
		if _, err := s.returnLabelWorkflowClient.CancelReturnLabelWorkflow(ctx, &returnsv1.CancelReturnLabelWorkflowRequest{
			Context: req.GetContext(),
			ReturnLabel: &returnsv1.ReturnLabel{
				Lines: []*returnsv1.ReturnLabelLine{&returnsv1.ReturnLabelLine{
					Tax: &returnsv1.ReturnLabelLineTax{
						Exemptions:   line.GetTax().GetExemptions(),
						Amount:       line.GetAmount(),
						Jurisdiction: line.GetTax().GetJurisdiction(),
					},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelWorkflowService/CancelReturnLabelWorkflow")
		}
	}
	return &paymentsv1.ApprovePayoutResponse{
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				CreatedBy: recalculateCustomersReply.GetCustomer().GetAudit().GetCreatedBy(),
				Etag:      recalculateCustomersReply.GetCustomer().GetAudit().GetEtag(),
			},
			PayoutId: req.GetPayoutId(),
			Metadata: req.GetPayout().GetMetadata(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectPayout rejects a payout.
func (s *PayoutApprovalServer) RejectPayout(ctx context.Context, req *paymentsv1.RejectPayoutRequest) (*paymentsv1.RejectPayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "reject payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	var iban string
	switch choice := req.GetPayout().GetSettlement().(type) {
	case *paymentsv1.Payout_Card:
		iban = choice.Card.GetHolderName()
	case *paymentsv1.Payout_Bank:
		iban = choice.Bank.GetIban()
	case *paymentsv1.Payout_VoucherCode:
		iban = choice.VoucherCode
	}
	submitPaymentRequest := &paymentsv1.SubmitPaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Bank{Bank: &paymentsv1.PaymentBankSettlement{Iban: iban}},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetPayout().GetBillingAddress().GetPostalCode(),
			},
			Counterparty: &paymentsv1.RefundRef{Number: req.GetPayout().GetCounterparty().GetNumber()},
		},
	}
	submitPaymentReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*paymentsv1.SubmitPaymentResponse, error) {
		return s.paymentApprovalClient.SubmitPayment(ctx, submitPaymentRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentApprovalService/SubmitPayment")
	}
	encoded, err := protojson.Marshal(req.GetPayout())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Payout
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	countDisputeTotalsRequest := &paymentsv1.CountDisputeTotalsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.CountDisputeTotalsRequest_Filter{
			Region:       (&decoded).GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetPayout().GetAudit().GetCreatedAt(),
			OwnerId:      submitPaymentReply.GetPayment().GetPaymentId(),
		},
	}
	countDisputeTotalsReply := new(paymentsv1.CountDisputeTotalsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", paymentsDisputeReportingServiceName, "CountDisputeTotals"), countDisputeTotalsRequest, countDisputeTotalsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeReportingService/CountDisputeTotals")
	}
	return &paymentsv1.RejectPayoutResponse{
		Result: &commonv1.OperationResult{
			Details:      submitPaymentReply.GetResult().GetDetails(),
			ReferenceIds: countDisputeTotalsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Payout: &paymentsv1.Payout{
			PayoutId:     req.GetPayoutId(),
			Counterparty: req.GetPayout().GetCounterparty(),
		},
	}, nil
}

// EscalatePayout escalates a payout.
func (s *PayoutApprovalServer) EscalatePayout(ctx context.Context, req *paymentsv1.EscalatePayoutRequest) (*paymentsv1.EscalatePayoutResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalatePayout", "error", err)
		return nil, err
	}
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "escalate payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	exportHouseholdsStream, err := s.householdQueryClient.ExportHouseholds(ctx, &customersv1.ExportHouseholdsRequest{
		Context: req.GetContext(),
		Filter:  &customersv1.ExportHouseholdsRequest_Filter{Region: req.GetPayout().GetBillingAddress().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
	}
	for {
		received, err := exportHouseholdsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
		}
		_ = received
	}
	return &paymentsv1.EscalatePayoutResponse{
		Payout: &paymentsv1.Payout{
			PayoutId:       req.GetPayoutId(),
			BillingAddress: req.GetPayout().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingPayouts lists payouts a page at a time.
func (s *PayoutApprovalServer) ListPendingPayouts(ctx context.Context, req *paymentsv1.ListPendingPayoutsRequest) (*paymentsv1.ListPendingPayoutsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingPayouts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending payouts", "tenant", req.GetContext().GetTenantId())
	signalCollectionWorkflowRequest := &merchandisingv1.SignalCollectionWorkflowRequest{}
	signalCollectionWorkflowRequest.Context = req.GetContext()
	signalCollectionWorkflowRequest.Collection = &merchandisingv1.Collection{
		Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			}},
		}},
	}
	signalCollectionWorkflowReply, err := s.collectionWorkflowClient.SignalCollectionWorkflow(ctx, signalCollectionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/SignalCollectionWorkflow")
	}
	return &paymentsv1.ListPendingPayoutsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: signalCollectionWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Payouts: []*paymentsv1.Payout{&paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: signalCollectionWorkflowReply.GetCollection().GetManager().GetDisplayName(),
			},
		}},
	}, nil
}

// RecallPayout recalls a payout.
func (s *PayoutApprovalServer) RecallPayout(ctx context.Context, req *paymentsv1.RecallPayoutRequest) (*paymentsv1.RecallPayoutResponse, error) {
	if err := s.store.SavePayout(ctx, req.GetContext().GetTenantId(), req.GetPayout()); err != nil {
		return nil, grpcx.Wrap(err, "save payout")
	}
	s.log.InfoContext(ctx, "recall payout", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayout().GetAudit().GetEtag())
	scheduleDisputeReportRequest := &paymentsv1.ScheduleDisputeReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
	}
	scheduleDisputeReportReply, err := s.scheduleDisputeReportFn(ctx, scheduleDisputeReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeReportingService/ScheduleDisputeReport")
	}
	escalatePaymentRequest := &paymentsv1.EscalatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{Locality: req.GetPayout().GetBillingAddress().GetLocality()},
		},
	}
	send := s.escalatePaymentSender(escalatePaymentRequest)
	escalatePaymentReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentApprovalService/EscalatePayment")
	}
	manifestLifecycle, err := registry.Lookup[shippingv1.ManifestLifecycleServiceClient](s.clients, "acme.shipping.v1.ManifestLifecycleService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	scheduleManifestExpiryRequest := &shippingv1.ScheduleManifestExpiryRequest{
		Context: req.GetContext(),
		Manifest: &shippingv1.Manifest{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetPayout().GetBillingAddress().GetRegion()},
			Counterparty:   &customersv1.CustomerRef{Email: req.GetDetails().GetApprover().GetEmail()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: escalatePaymentReply.GetPayment().GetAudit().GetUpdatedAt(),
			},
		},
	}
	scheduleManifestExpiryReply, err := manifestLifecycle.ScheduleManifestExpiry(ctx, scheduleManifestExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestLifecycleService/ScheduleManifestExpiry")
	}
	return &paymentsv1.RecallPayoutResponse{
		Outcome: &paymentsv1.RecallPayoutResponse_Outcome{
			Message: scheduleDisputeReportReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: escalatePaymentReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Payout: &paymentsv1.Payout{
			Audit: &commonv1.AuditInfo{
				CreatedBy: scheduleManifestExpiryReply.GetManifest().GetAudit().GetCreatedBy(),
			},
			PayoutId: req.GetPayoutId(),
			IssuedAt: req.GetPayout().GetIssuedAt(),
		},
	}, nil
}

// GetPayoutApprovalStatus returns a payout.
func (s *PayoutApprovalServer) GetPayoutApprovalStatus(ctx context.Context, req *paymentsv1.GetPayoutApprovalStatusRequest) (*paymentsv1.GetPayoutApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payout", req.GetPayoutId(), "get_payout_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payout approval status", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.GetPayoutApprovalStatusResponse{
		Payout: &paymentsv1.Payout{PayoutId: req.GetPayoutId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// escalatePaymentSender returns a function that sends one request to
// acme.payments.v1.PaymentApprovalService/EscalatePayment.
func (s *PayoutApprovalServer) escalatePaymentSender(request *paymentsv1.EscalatePaymentRequest) func(context.Context) (*paymentsv1.EscalatePaymentResponse, error) {
	return func(ctx context.Context) (*paymentsv1.EscalatePaymentResponse, error) {
		return s.paymentApprovalClient.EscalatePayment(ctx, request)
	}
}
