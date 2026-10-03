package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// PaymentInstrumentWorkflowServer implements `acme.payments.v1.PaymentInstrumentWorkflowService`.
// Runs the long-lived workflow that takes a payment instrument from start to finish.
type PaymentInstrumentWorkflowServer struct {
	*paymentInstrumentWorkflowHandlers
}

// paymentInstrumentWorkflowHandlers holds the handlers of PaymentInstrumentWorkflowServer, with what they use.
type paymentInstrumentWorkflowHandlers struct {
	paymentsv1.UnimplementedPaymentInstrumentWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	conn                            *grpc.ClientConn
	payoutReportingClient           paymentsv1.PayoutReportingServiceClient
	disputeImportClient             paymentsv1.DisputeImportServiceClient
	stockReservationWorkflowClient  inventoryv1.StockReservationWorkflowServiceClient
	customerValidationClient        customersv1.CustomerValidationServiceClient
	returnRequestApprovalClient     returnsv1.ReturnRequestApprovalServiceClient
	meteredReturnRequestApproval    meteredReturnRequestApproval
	paymentClient                   paymentsv1.PaymentServiceClient
	parcelWorkflowClient            shippingv1.ParcelWorkflowServiceClient
	payoutValidationClient          paymentsv1.PayoutValidationServiceClient
	merchandisingRuleWorkflowClient merchandisingv1.MerchandisingRuleWorkflowServiceClient
	meteredCustomerCalculation      meteredCustomerCalculation
}

// NewPaymentInstrumentWorkflowServer wires a PaymentInstrumentWorkflowServer.
func NewPaymentInstrumentWorkflowServer(deps platform.Deps) *PaymentInstrumentWorkflowServer {
	return &PaymentInstrumentWorkflowServer{paymentInstrumentWorkflowHandlers: &paymentInstrumentWorkflowHandlers{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.payments.v1.PaymentInstrumentWorkflowService"),
		conn:                            deps.Conn,
		payoutReportingClient:           paymentsv1.NewPayoutReportingServiceClient(deps.Conn),
		disputeImportClient:             paymentsv1.NewDisputeImportServiceClient(deps.Conn),
		stockReservationWorkflowClient:  inventoryv1.NewStockReservationWorkflowServiceClient(deps.Conn),
		customerValidationClient:        customersv1.NewCustomerValidationServiceClient(deps.Conn),
		returnRequestApprovalClient:     returnsv1.NewReturnRequestApprovalServiceClient(deps.Conn),
		meteredReturnRequestApproval:    meteredReturnRequestApproval{ReturnRequestApprovalServiceClient: returnsv1.NewReturnRequestApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		paymentClient:                   paymentsv1.NewPaymentServiceClient(deps.Conn),
		parcelWorkflowClient:            shippingv1.NewParcelWorkflowServiceClient(deps.Conn),
		payoutValidationClient:          paymentsv1.NewPayoutValidationServiceClient(deps.Conn),
		merchandisingRuleWorkflowClient: merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn),
		meteredCustomerCalculation:      meteredCustomerCalculation{CustomerCalculationServiceClient: customersv1.NewCustomerCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}}
}

// StartPaymentInstrumentWorkflow starts a payment instrument.
func (s *paymentInstrumentWorkflowHandlers) StartPaymentInstrumentWorkflow(ctx context.Context, req *paymentsv1.StartPaymentInstrumentWorkflowRequest) (*paymentsv1.StartPaymentInstrumentWorkflowResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "start payment instrument workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	paymentInstrumentCache := cache.New[string, *paymentsv1.PaymentInstrument](1)
	paymentInstrumentCache.Put(req.GetContext().GetTraceId(), req.GetPaymentInstrument())
	cachedPaymentInstrument, _ := paymentInstrumentCache.Get(req.GetContext().GetTraceId())
	explainFeatureFlagDecisionRequest := &featureflagsv1.ExplainFeatureFlagDecisionRequest{
		Context: req.GetContext(),
		Details: &featureflagsv1.ExplainFeatureFlagDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{
				Name:             req.GetPaymentInstrument().GetContact().GetName(),
				PreferredChannel: req.GetPaymentInstrument().GetContact().GetPreferredChannel(),
			},
		},
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Owner: &identityv1.UserRef{
				DisplayName: cachedPaymentInstrument.GetHolder().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPaymentInstrument().GetAudit().GetCreatedBy(),
			},
		},
	}
	explainFeatureFlagDecisionReply := new(featureflagsv1.ExplainFeatureFlagDecisionResponse)
	if err := grpcx.Invoke(ctx, s.conn, featureflagsv1.FeatureFlagValidationService_ExplainFeatureFlagDecision_FullMethodName, explainFeatureFlagDecisionRequest, explainFeatureFlagDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	parameters := make(map[string]string, len(req.GetDetails().GetParameters()))
	for key2, value2 := range req.GetDetails().GetParameters() {
		parameters[key2] = value2
	}
	generatePayoutReportRequest := &paymentsv1.GeneratePayoutReportRequest{
		Context:     req.GetContext(),
		Reason:      explainFeatureFlagDecisionReply.GetFeatureFlag().GetBlock().GetReason(),
		Annotations: annotations,
		Details: &paymentsv1.GeneratePayoutReportRequest_Details{
			Period:  explainFeatureFlagDecisionReply.GetFeatureFlag().GetEffective(),
			Filters: parameters,
		},
	}
	generatePayoutReportReply, err := grpcx.Call(ctx, s.payoutReportingClient.GeneratePayoutReport, generatePayoutReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutReportingService/GeneratePayoutReport")
	}
	return &paymentsv1.StartPaymentInstrumentWorkflowResponse{
		Outcome: &paymentsv1.StartPaymentInstrumentWorkflowResponse_Outcome{
			Status: explainFeatureFlagDecisionReply.GetOutcome().GetStatus(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainFeatureFlagDecisionReply.GetFeatureFlag().GetAudit().GetUpdatedBy(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			CurrencyCode:        req.GetPaymentInstrument().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: generatePayoutReportReply.GetResult().GetStatus()},
	}, nil
}

// AdvancePaymentInstrumentWorkflow advances a payment instrument.
func (s *paymentInstrumentWorkflowHandlers) AdvancePaymentInstrumentWorkflow(ctx context.Context, req *paymentsv1.AdvancePaymentInstrumentWorkflowRequest) (*paymentsv1.AdvancePaymentInstrumentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvancePaymentInstrumentWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "advance payment instrument workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	mapDisputeImportColumnsRequest := buildMapDisputeImportColumnsRequestForAdvancePaymentInstrumentWorkflow(req)
	mapDisputeImportColumnsReply, err := s.disputeImportClient.MapDisputeImportColumns(ctx, mapDisputeImportColumnsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeImportService/MapDisputeImportColumns")
	}
	retryStockReservationWorkflowRequest := &inventoryv1.RetryStockReservationWorkflowRequest{
		Context: req.GetContext(),
		StockReservation: &inventoryv1.StockReservation{
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: mapDisputeImportColumnsReply.GetDispute().GetRequester().GetPreferredChannel(),
				},
			}},
		},
		Details: &inventoryv1.RetryStockReservationWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	retryStockReservationWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*inventoryv1.RetryStockReservationWorkflowResponse, error) {
		return s.stockReservationWorkflowClient.RetryStockReservationWorkflow(ctx, retryStockReservationWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationWorkflowService/RetryStockReservationWorkflow")
	}
	return &paymentsv1.AdvancePaymentInstrumentWorkflowResponse{
		Outcome: &paymentsv1.AdvancePaymentInstrumentWorkflowResponse_Outcome{
			Status: mapDisputeImportColumnsReply.GetOutcome().GetStatus(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: retryStockReservationWorkflowReply.GetStockReservation().GetAudit().GetUpdatedBy(),
			},
			Holder: &customersv1.CustomerRef{
				DisplayName: retryStockReservationWorkflowReply.GetStockReservation().GetOrganizer().GetDisplayName(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			OpenedAt:            req.GetPaymentInstrument().GetOpenedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelPaymentInstrumentWorkflow cancels a payment instrument.
func (s *paymentInstrumentWorkflowHandlers) CancelPaymentInstrumentWorkflow(ctx context.Context, req *paymentsv1.CancelPaymentInstrumentWorkflowRequest) (*paymentsv1.CancelPaymentInstrumentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "cancel payment instrument workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	checkCustomerEligibilityRequest := buildCheckCustomerEligibilityRequestForCancelPaymentInstrumentWorkflow(req)
	checkCustomerEligibilityReply, err := s.customerValidationClient.CheckCustomerEligibility(ctx, checkCustomerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/CheckCustomerEligibility")
	}
	ctx = tenancy.With(ctx, "email", req.GetPaymentInstrument().GetHolder().GetEmail())
	snapshot, err := protojson.Marshal(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	repairSearchIndexRequest := &searchv1.RepairSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetPaymentInstrument().GetHolder().GetDisplayName(),
				UserId:      checkCustomerEligibilityReply.GetCustomer().GetRelationshipManager().GetUserId(),
				Email:       tenancy.Value(ctx, "email"),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &searchv1.RepairSearchIndexRequest_Details{Payload: snapshot},
	}
	repairSearchIndexReply, err := grpcx.NewClient(s.conn, searchv1.NewSearchIndexAdminServiceClient).RepairSearchIndex(ctx, repairSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexAdminService/RepairSearchIndex")
	}
	for limit := range iterx.Each(req.GetPaymentInstrument().GetLimits(), func(candidate *paymentsv1.PaymentInstrumentLimit) bool { return candidate != nil }) {
		if _, err := s.returnRequestApprovalClient.RecallReturnRequest(ctx, &returnsv1.RecallReturnRequestRequest{
			Context: req.GetContext(),
			ReturnRequest: &returnsv1.ReturnRequest{
				Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
					Tax: &returnsv1.ReturnRequestLineTax{Amount: limit.GetAmount()},
				}},
				Settlement: &returnsv1.ReturnRequest_Bank{Bank: &returnsv1.ReturnRequestBankSettlement{
					AccountHolder: req.GetPaymentInstrument().GetBank().GetAccountHolder(),
				}},
				BillingAddress: &commonv1.PostalAddress{
					PostalCode: repairSearchIndexReply.GetSearchIndex().GetAddress().GetPostalCode(),
				},
			},
			Details: &returnsv1.RecallReturnRequestRequest_Details{
				Approver: &identityv1.UserRef{
					DisplayName: repairSearchIndexReply.GetSearchIndex().GetOwner().GetDisplayName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestApprovalService/RecallReturnRequest")
		}
	}
	return &paymentsv1.CancelPaymentInstrumentWorkflowResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Contact: checkCustomerEligibilityReply.GetCustomer().GetContact(),
			Holder: &customersv1.CustomerRef{
				CustomerId: checkCustomerEligibilityReply.GetCustomer().GetCustomerId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: repairSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedAt(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			CurrencyCode:        req.GetPaymentInstrument().GetCurrencyCode(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryPaymentInstrumentWorkflow retrys a payment instrument.
func (s *paymentInstrumentWorkflowHandlers) RetryPaymentInstrumentWorkflow(ctx context.Context, req *paymentsv1.RetryPaymentInstrumentWorkflowRequest) (*paymentsv1.RetryPaymentInstrumentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryPaymentInstrumentWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "retry payment instrument workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	var last4 string
	switch choice := req.GetPaymentInstrument().GetFunding().(type) {
	case *paymentsv1.PaymentInstrument_Card:
		last4 = choice.Card.GetLast4()
	case *paymentsv1.PaymentInstrument_Bank:
		last4 = choice.Bank.GetIban()
	case *paymentsv1.PaymentInstrument_ExternalAccountId:
		last4 = choice.ExternalAccountId
	}
	delegateReturnRequestApprovalRequest := &returnsv1.DelegateReturnRequestApprovalRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				CustomerId: req.GetPaymentInstrument().GetHolder().GetCustomerId(),
			},
			Settlement: &returnsv1.ReturnRequest_Card{Card: &returnsv1.ReturnRequestCardSettlement{Last4: last4}},
		},
	}
	delegateReturnRequestApprovalReply, err := s.meteredReturnRequestApproval.DelegateReturnRequestApproval(ctx, delegateReturnRequestApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestApprovalService/DelegateReturnRequestApproval")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*paymentsv1.ClonePaymentResponse, len(req.GetPaymentInstrument().GetLimits()))
	for i, limit := range req.GetPaymentInstrument().GetLimits() {
		group.Go(func() error {
			reply, err := s.paymentClient.ClonePayment(groupCtx, &paymentsv1.ClonePaymentRequest{
				Context: req.GetContext(),
				Payment: &paymentsv1.Payment{
					Lines:      []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{Tax: &paymentsv1.PaymentLineTax{Amount: limit.GetAmount()}}},
					Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{Last4: req.GetPaymentInstrument().GetCard().GetLast4()}},
					BillingAddress: &commonv1.PostalAddress{
						Region: delegateReturnRequestApprovalReply.GetReturnRequest().GetBillingAddress().GetRegion(),
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentService/ClonePayment")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	encoded, err := protojson.Marshal(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.PaymentInstrument
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	streamPayoutMetricsStream, err := s.payoutReportingClient.StreamPayoutMetrics(ctx, &paymentsv1.StreamPayoutMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone:            (&decoded).GetContact().GetPhone(),
			PreferredChannel: req.GetPaymentInstrument().GetContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutReportingService/StreamPayoutMetrics")
	}
	for {
		received, err := streamPayoutMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutReportingService/StreamPayoutMetrics")
		}
		_ = received
	}
	return &paymentsv1.RetryPaymentInstrumentWorkflowResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				DisplayName: delegateReturnRequestApprovalReply.GetReturnRequest().GetCounterparty().GetDisplayName(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			OpenedAt:            req.GetPaymentInstrument().GetOpenedAt(),
		},
		Outcome: &paymentsv1.RetryPaymentInstrumentWorkflowResponse_Outcome{
			Message: delegateReturnRequestApprovalReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PausePaymentInstrumentWorkflow pauses a payment instrument.
func (s *paymentInstrumentWorkflowHandlers) PausePaymentInstrumentWorkflow(ctx context.Context, req *paymentsv1.PausePaymentInstrumentWorkflowRequest) (*paymentsv1.PausePaymentInstrumentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "pause payment instrument workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	pauseParcelWorkflowRequest := &shippingv1.PauseParcelWorkflowRequest{
		Context: req.GetContext(),
		Details: &shippingv1.PauseParcelWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: req.GetDetails().GetNotify().GetEmail(),
				Phone: req.GetPaymentInstrument().GetContact().GetPhone(),
			},
		},
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
			},
		},
	}
	pauseParcelWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*shippingv1.PauseParcelWorkflowResponse, error) {
		return s.parcelWorkflowClient.PauseParcelWorkflow(ctx, pauseParcelWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/PauseParcelWorkflow")
	}
	return &paymentsv1.PausePaymentInstrumentWorkflowResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				CreatedBy: pauseParcelWorkflowReply.GetParcel().GetAudit().GetCreatedBy(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Labels:              req.GetPaymentInstrument().GetLabels(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: pauseParcelWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ResumePaymentInstrumentWorkflow resumes a payment instrument.
func (s *paymentInstrumentWorkflowHandlers) ResumePaymentInstrumentWorkflow(ctx context.Context, req *paymentsv1.ResumePaymentInstrumentWorkflowRequest) (*paymentsv1.ResumePaymentInstrumentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "resume payment instrument workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	validatePayoutRequest := &paymentsv1.ValidatePayoutRequest{}
	validatePayoutRequest.Context = req.GetContext()
	validatePayoutRequest.Payout = &paymentsv1.Payout{
		Settlement: &paymentsv1.Payout_Card{Card: &paymentsv1.PayoutCardSettlement{Token: req.GetPaymentInstrument().GetCard().GetToken()}},
	}
	validatePayoutRequest.Details = &paymentsv1.ValidatePayoutRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetPaymentInstrument().GetHolder().GetEmail()},
	}
	validatePayoutReply, err := s.payoutValidationClient.ValidatePayout(ctx, validatePayoutRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutValidationService/ValidatePayout")
	}
	for limit := range iterx.Each(req.GetPaymentInstrument().GetLimits(), func(candidate *paymentsv1.PaymentInstrumentLimit) bool { return candidate != nil }) {
		if _, err := s.merchandisingRuleWorkflowClient.RetryMerchandisingRuleWorkflow(ctx, &merchandisingv1.RetryMerchandisingRuleWorkflowRequest{
			Context: req.GetContext(),
			MerchandisingRule: &merchandisingv1.MerchandisingRule{
				Action: &merchandisingv1.MerchandisingRule_Discount{Discount: &merchandisingv1.MerchandisingRuleDiscountAction{Amount: limit.GetAmount()}},
				Name:   limit.GetName(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/RetryMerchandisingRuleWorkflow")
		}
	}
	return &paymentsv1.ResumePaymentInstrumentWorkflowResponse{
		Outcome: &paymentsv1.ResumePaymentInstrumentWorkflowResponse_Outcome{Status: validatePayoutReply.GetOutcome().GetStatus()},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Labels:              req.GetPaymentInstrument().GetLabels(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectPaymentInstrumentWorkflow returns a payment instrument.
func (s *paymentInstrumentWorkflowHandlers) InspectPaymentInstrumentWorkflow(ctx context.Context, req *paymentsv1.InspectPaymentInstrumentWorkflowRequest) (*paymentsv1.InspectPaymentInstrumentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "inspect_payment_instrument_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect payment instrument workflow", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.InspectPaymentInstrumentWorkflowResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{PaymentInstrumentId: req.GetPaymentInstrumentId()},
		Result:            &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPaymentInstrumentWorkflows lists payment instruments a page at a time.
func (s *paymentInstrumentWorkflowHandlers) ListPaymentInstrumentWorkflows(ctx context.Context, req *paymentsv1.ListPaymentInstrumentWorkflowsRequest) (*paymentsv1.ListPaymentInstrumentWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list payment instrument workflows", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.ListPaymentInstrumentWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalPaymentInstrumentWorkflow signals a payment instrument.
func (s *paymentInstrumentWorkflowHandlers) SignalPaymentInstrumentWorkflow(ctx context.Context, req *paymentsv1.SignalPaymentInstrumentWorkflowRequest) (*paymentsv1.SignalPaymentInstrumentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "signal payment instrument workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	calculateCustomerRequest := &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{Etag: req.GetPaymentInstrument().GetAudit().GetEtag()},
		},
	}
	calculateCustomerReply, err := s.meteredCustomerCalculation.CalculateCustomer(ctx, calculateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	jobs := make(chan *paymentsv1.PaymentInstrumentLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit := range jobs {
				if _, err := s.parcelWorkflowClient.ResumeParcelWorkflow(workCtx, &shippingv1.ResumeParcelWorkflowRequest{
					Context: req.GetContext(),
					Parcel: &shippingv1.Parcel{
						Name: limit.GetName(),
						Locator: &shippingv1.Parcel_Address{Address: &commonv1.PostalAddress{
							Recipient:  calculateCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
							PostalCode: calculateCustomerReply.GetCustomer().GetPrimaryAddress().GetPostalCode(),
						}},
						Owner: &identityv1.UserRef{Email: req.GetPaymentInstrument().GetContact().GetEmail()},
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
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelWorkflowService/ResumeParcelWorkflow")
	}
	return &paymentsv1.SignalPaymentInstrumentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: calculateCustomerReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Contact:             req.GetPaymentInstrument().GetContact(),
		},
	}, nil
}
