package payments

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	prescriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/prescriptions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentInstrumentApprovalServer implements `acme.payments.v1.PaymentInstrumentApprovalService`.
// Routes payment instruments through approval, with escalation and delegation.
type PaymentInstrumentApprovalServer struct {
	*paymentInstrumentApprovalHandlers
}

// paymentInstrumentApprovalHandlers holds the handlers of PaymentInstrumentApprovalServer, with what they use.
type paymentInstrumentApprovalHandlers struct {
	paymentsv1.UnimplementedPaymentInstrumentApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	notificationQueryClient         notificationsv1.NotificationQueryServiceClient
	conn                            *grpc.ClientConn
	consentRecordValidationOnce     sync.Once
	consentRecordValidationCached   customersv1.ConsentRecordValidationServiceClient
	prescriptionClient              prescriptionsv1.PrescriptionServiceClient
	replenishmentPlanApprovalClient inventoryv1.ReplenishmentPlanApprovalServiceClient
	translationKeyClient            localizationv1.TranslationKeyServiceClient
	rewardApprovalClient            loyaltyv1.RewardApprovalServiceClient
	meteredRefundReporting          meteredRefundReporting
	paymentLifecyclePort            paymentLifecyclePort
	customerClient                  customersv1.CustomerServiceClient
	consentRecordValidationClient   customersv1.ConsentRecordValidationServiceClient
	stockMovementWorkflowClient     inventoryv1.StockMovementWorkflowServiceClient
	householdValidationClient       customersv1.HouseholdValidationServiceClient
}

// NewPaymentInstrumentApprovalServer wires a PaymentInstrumentApprovalServer.
func NewPaymentInstrumentApprovalServer(deps platform.Deps) *PaymentInstrumentApprovalServer {
	return &PaymentInstrumentApprovalServer{paymentInstrumentApprovalHandlers: &paymentInstrumentApprovalHandlers{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.payments.v1.PaymentInstrumentApprovalService"),
		notificationQueryClient:         notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		conn:                            deps.Conn,
		prescriptionClient:              prescriptionsv1.NewPrescriptionServiceClient(deps.Conn),
		replenishmentPlanApprovalClient: inventoryv1.NewReplenishmentPlanApprovalServiceClient(deps.Conn),
		translationKeyClient:            localizationv1.NewTranslationKeyServiceClient(deps.Conn),
		rewardApprovalClient:            loyaltyv1.NewRewardApprovalServiceClient(deps.Conn),
		meteredRefundReporting:          meteredRefundReporting{RefundReportingServiceClient: paymentsv1.NewRefundReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		paymentLifecyclePort:            newPaymentLifecyclePort(deps),
		customerClient:                  customersv1.NewCustomerServiceClient(deps.Conn),
		consentRecordValidationClient:   customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		stockMovementWorkflowClient:     inventoryv1.NewStockMovementWorkflowServiceClient(deps.Conn),
		householdValidationClient:       customersv1.NewHouseholdValidationServiceClient(deps.Conn),
	}}
}

// SubmitPaymentInstrument submits a payment instrument.
func (s *paymentInstrumentApprovalHandlers) SubmitPaymentInstrument(ctx context.Context, req *paymentsv1.SubmitPaymentInstrumentRequest) (*paymentsv1.SubmitPaymentInstrumentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "submit payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	resolveNotificationsRequest := buildResolveNotificationsRequestForSubmitPaymentInstrument(req)
	resolveNotificationsReply, err := s.notificationQueryClient.ResolveNotifications(ctx, resolveNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/ResolveNotifications")
	}
	normalizeConsentRecordRequest := &customersv1.NormalizeConsentRecordRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizeConsentRecordRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetApprover().GetEmail(),
				PreferredChannel: req.GetPaymentInstrument().GetContact().GetPreferredChannel(),
			},
		},
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			CorrelationId: resolveNotificationsReply.GetResult().GetCorrelationId(),
		},
	}
	normalizeConsentRecordReply, err := s.consentRecordValidationLazy().NormalizeConsentRecord(ctx, normalizeConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord")
	}
	return &paymentsv1.SubmitPaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolveNotificationsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &paymentsv1.SubmitPaymentInstrumentResponse_Outcome{
			Message: normalizeConsentRecordReply.GetResult().GetMessage(),
			Status:  normalizeConsentRecordReply.GetResult().GetStatus(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			OpenedAt:            req.GetPaymentInstrument().GetOpenedAt(),
		},
	}, nil
}

// ApprovePaymentInstrument approves a payment instrument.
func (s *paymentInstrumentApprovalHandlers) ApprovePaymentInstrument(ctx context.Context, req *paymentsv1.ApprovePaymentInstrumentRequest) (*paymentsv1.ApprovePaymentInstrumentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "approve payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	listPrescriptionsRequest := buildListPrescriptionsRequestForApprovePaymentInstrument(req)
	listPrescriptionsReply, err := s.prescriptionClient.ListPrescriptions(ctx, listPrescriptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.prescriptions.v1.PrescriptionService/ListPrescriptions")
	}
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetApprover().GetUserId())
	listPendingReplenishmentPlansRequest := &inventoryv1.ListPendingReplenishmentPlansRequest{
		Context: req.GetContext(),
		Filter: &inventoryv1.ListPendingReplenishmentPlansRequest_Filter{
			Labels:       req.GetPaymentInstrument().GetLabels(),
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      tenancy.Value(ctx, "user_id"),
		},
	}
	listPendingReplenishmentPlansReply, err := grpcx.Call(ctx, s.replenishmentPlanApprovalClient.ListPendingReplenishmentPlans, listPendingReplenishmentPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanApprovalService/ListPendingReplenishmentPlans")
	}
	_ = listPendingReplenishmentPlansReply
	return &paymentsv1.ApprovePaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  listPrescriptionsReply.GetResult().GetReferenceIds(),
			CorrelationId: listPrescriptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Balance:             req.GetPaymentInstrument().GetBalance(),
		},
	}, nil
}

// RejectPaymentInstrument rejects a payment instrument.
func (s *paymentInstrumentApprovalHandlers) RejectPaymentInstrument(ctx context.Context, req *paymentsv1.RejectPaymentInstrumentRequest) (*paymentsv1.RejectPaymentInstrumentResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "reject payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	for _, limit := range req.GetPaymentInstrument().GetLimits() {
		if _, err := s.translationKeyClient.CreateTranslationKey(ctx, &localizationv1.CreateTranslationKeyRequest{
			Context:        req.GetContext(),
			TranslationKey: &localizationv1.TranslationKey{Name: limit.GetName()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyService/CreateTranslationKey")
		}
	}
	delegateRewardApprovalReply, err := s.rewardApprovalClient.DelegateRewardApproval(ctx, &loyaltyv1.DelegateRewardApprovalRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.DelegateRewardApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetPaymentInstrument().GetContact().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/DelegateRewardApproval")
	}
	scheduleDisputeExpiryRequest := &paymentsv1.ScheduleDisputeExpiryRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Origin: &paymentsv1.Dispute_Customer{Customer: &paymentsv1.PaymentRef{
				DisplayName: delegateRewardApprovalReply.GetReward().GetOwner().GetDisplayName(),
			}},
		},
		Details: &paymentsv1.ScheduleDisputeExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetPaymentInstrument().GetContact().GetName()},
		},
	}
	scheduleDisputeExpiryReply := new(paymentsv1.ScheduleDisputeExpiryResponse)
	if err := grpcx.Invoke(ctx, s.conn, paymentsv1.DisputeLifecycleService_ScheduleDisputeExpiry_FullMethodName, scheduleDisputeExpiryRequest, scheduleDisputeExpiryReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeLifecycleService/ScheduleDisputeExpiry")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleRefundReportRequest := &paymentsv1.ScheduleRefundReportRequest{
		Context:     req.GetContext(),
		Reason:      delegateRewardApprovalReply.GetReward().GetBlock().GetReason(),
		Annotations: annotations,
		Details: &paymentsv1.ScheduleRefundReportRequest_Details{
			Period:  delegateRewardApprovalReply.GetReward().GetEffective(),
			Filters: delegateRewardApprovalReply.GetResult().GetDetails(),
		},
	}
	scheduleRefundReportReply, err := s.meteredRefundReporting.ScheduleRefundReport(ctx, scheduleRefundReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/ScheduleRefundReport")
	}
	return &paymentsv1.RejectPaymentInstrumentResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Limits: []*paymentsv1.PaymentInstrumentLimit{&paymentsv1.PaymentInstrumentLimit{
				Amount: delegateRewardApprovalReply.GetReward().GetDiscount().GetAmount(),
			}},
			Audit: &commonv1.AuditInfo{
				Etag: delegateRewardApprovalReply.GetReward().GetAudit().GetEtag(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			OpenedAt:            req.GetPaymentInstrument().GetOpenedAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: scheduleDisputeExpiryReply.GetResult().GetCorrelationId(),
			Status:        scheduleRefundReportReply.GetResult().GetStatus(),
			Message:       scheduleRefundReportReply.GetResult().GetMessage(),
		},
	}, nil
}

// EscalatePaymentInstrument escalates a payment instrument.
func (s *paymentInstrumentApprovalHandlers) EscalatePaymentInstrument(ctx context.Context, req *paymentsv1.EscalatePaymentInstrumentRequest) (*paymentsv1.EscalatePaymentInstrumentResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "escalate payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "preferred_channel", req.GetPaymentInstrument().GetContact().GetPreferredChannel())
	schedulePaymentExpiryRequest := &paymentsv1.SchedulePaymentExpiryRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.SchedulePaymentExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
		},
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{Last4: req.GetPaymentInstrument().GetCard().GetLast4()}},
		},
	}
	schedulePaymentExpiryReply, err := s.paymentLifecyclePort.SchedulePaymentExpiry(ctx, schedulePaymentExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentLifecycleService/SchedulePaymentExpiry")
	}
	encoded, err := protojson.Marshal(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.PaymentInstrument
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	encoded2, err := protojson.Marshal(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 paymentsv1.PaymentInstrument
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	paymentInstrumentCache := cache.New[string, *paymentsv1.PaymentInstrument](1)
	paymentInstrumentCache.Put(req.GetContext().GetTraceId(), req.GetPaymentInstrument())
	cachedPaymentInstrument, _ := paymentInstrumentCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareSynonymExportRequest := &searchv1.ShareSynonymExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.ShareSynonymExportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: (&decoded).GetContact().GetPreferredChannel(),
				Email:            (&decoded2).GetHolder().GetEmail(),
				Phone:            cachedPaymentInstrument.GetContact().GetPhone(),
			},
			Payload: packed,
		},
	}
	shareSynonymExportReply := new(searchv1.ShareSynonymExportResponse)
	if err := grpcx.Invoke(ctx, s.conn, searchv1.SynonymExportService_ShareSynonymExport_FullMethodName, shareSynonymExportRequest, shareSynonymExportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/ShareSynonymExport")
	}
	updateCustomerRequest := &customersv1.UpdateCustomerRequest{}
	updateCustomerRequest.Context = req.GetContext()
	updateCustomerRequest.Customer = &customersv1.Customer{
		Contact: &commonv1.ContactInfo{Email: req.GetPaymentInstrument().GetContact().GetEmail()},
		IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{
			Number: schedulePaymentExpiryReply.GetPayment().GetCounterparty().GetNumber(),
		}},
		Audit: &commonv1.AuditInfo{
			UpdatedBy: schedulePaymentExpiryReply.GetPayment().GetAudit().GetUpdatedBy(),
		},
	}
	updateCustomerReply, err := s.customerClient.UpdateCustomer(ctx, updateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/UpdateCustomer")
	}
	snapshot, err := protojson.Marshal(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolvePayoutConflictRequest := &paymentsv1.ResolvePayoutConflictRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: shareSynonymExportReply.GetSynonym().GetOwner().GetDisplayName(),
				Number:      schedulePaymentExpiryReply.GetPayment().GetCounterparty().GetNumber(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetPaymentInstrument().GetAudit().GetCreatedBy(),
			},
		},
		Details: &paymentsv1.ResolvePayoutConflictRequest_Details{Payload: snapshot},
	}
	resolvePayoutConflictReply, err := resolvePayoutConflictCall(ctx, resolvePayoutConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/ResolvePayoutConflict")
	}
	return &paymentsv1.EscalatePaymentInstrumentResponse{
		Result: &commonv1.OperationResult{
			Status:  schedulePaymentExpiryReply.GetOutcome().GetStatus(),
			Message: shareSynonymExportReply.GetResult().GetMessage(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				CreatedAt: updateCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
		},
		Outcome: &paymentsv1.EscalatePaymentInstrumentResponse_Outcome{
			Message:     resolvePayoutConflictReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// ListPendingPaymentInstruments lists payment instruments a page at a time.
func (s *paymentInstrumentApprovalHandlers) ListPendingPaymentInstruments(ctx context.Context, req *paymentsv1.ListPendingPaymentInstrumentsRequest) (*paymentsv1.ListPendingPaymentInstrumentsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending payment instruments", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scheduleFraudSignalExpiryRequest := &fraudv1.ScheduleFraudSignalExpiryRequest{
		Context: req.GetContext(),
		Details: &fraudv1.ScheduleFraudSignalExpiryRequest_Details{
			Notify:     &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Parameters: labels,
		},
		FraudSignal: &fraudv1.FraudSignal{
			Measurements: []*fraudv1.FraudSignalMeasurement{&fraudv1.FraudSignalMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
			Detail:       packed,
		},
	}
	scheduleFraudSignalExpiryReply, err := grpcx.NewClient(s.conn, fraudv1.NewFraudSignalLifecycleServiceClient).ScheduleFraudSignalExpiry(ctx, scheduleFraudSignalExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalLifecycleService/ScheduleFraudSignalExpiry")
	}
	return &paymentsv1.ListPendingPaymentInstrumentsResponse{
		Result: &commonv1.OperationResult{
			Message:       scheduleFraudSignalExpiryReply.GetOutcome().GetMessage(),
			CorrelationId: scheduleFraudSignalExpiryReply.GetFraudSignal().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// DelegatePaymentInstrumentApproval delegates a payment instrument.
func (s *paymentInstrumentApprovalHandlers) DelegatePaymentInstrumentApproval(ctx context.Context, req *paymentsv1.DelegatePaymentInstrumentApprovalRequest) (*paymentsv1.DelegatePaymentInstrumentApprovalResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "delegate payment instrument approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	checkPreferenceEligibilityRequest := &customersv1.CheckPreferenceEligibilityRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetPaymentInstrument().GetAudit().GetUpdatedAt(),
			},
		},
	}
	checkPreferenceEligibilityReply := new(customersv1.CheckPreferenceEligibilityResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.PreferenceValidationService_CheckPreferenceEligibility_FullMethodName, checkPreferenceEligibilityRequest, checkPreferenceEligibilityReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/CheckPreferenceEligibility")
	}
	return &paymentsv1.DelegatePaymentInstrumentApprovalResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Limits: []*paymentsv1.PaymentInstrumentLimit{&paymentsv1.PaymentInstrumentLimit{
				Name: checkPreferenceEligibilityReply.GetPreference().GetName(),
			}},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Status:              req.GetPaymentInstrument().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallPaymentInstrument recalls a payment instrument.
func (s *paymentInstrumentApprovalHandlers) RecallPaymentInstrument(ctx context.Context, req *paymentsv1.RecallPaymentInstrumentRequest) (*paymentsv1.RecallPaymentInstrumentResponse, error) {
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "recall payment instrument", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	scoreConsentRecordRequest := buildScoreConsentRecordRequestForRecallPaymentInstrument(req)
	scoreConsentRecordReply, err := s.consentRecordValidationClient.ScoreConsentRecord(ctx, scoreConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord")
	}
	packed, err := anypb.New(req.GetPaymentInstrument())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	retryStockMovementWorkflowRequest := &inventoryv1.RetryStockMovementWorkflowRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				CustomerId: req.GetPaymentInstrument().GetHolder().GetCustomerId(),
				Email:      req.GetDetails().GetApprover().GetEmail(),
			},
			Detail: packed,
		},
	}
	retryStockMovementWorkflowReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*inventoryv1.RetryStockMovementWorkflowResponse, error) {
		return s.stockMovementWorkflowClient.RetryStockMovementWorkflow(ctx, retryStockMovementWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementWorkflowService/RetryStockMovementWorkflow")
	}
	return &paymentsv1.RecallPaymentInstrumentResponse{
		Outcome: &paymentsv1.RecallPaymentInstrumentResponse_Outcome{
			Status:  scoreConsentRecordReply.GetResult().GetStatus(),
			Message: retryStockMovementWorkflowReply.GetResult().GetMessage(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			OpenedAt:            req.GetPaymentInstrument().GetOpenedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetPaymentInstrumentApprovalStatus returns a payment instrument.
func (s *paymentInstrumentApprovalHandlers) GetPaymentInstrumentApprovalStatus(ctx context.Context, req *paymentsv1.GetPaymentInstrumentApprovalStatusRequest) (*paymentsv1.GetPaymentInstrumentApprovalStatusResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment_instrument", req.GetPaymentInstrumentId(), "get_payment_instrument_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get payment instrument approval status", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.GetPaymentInstrumentApprovalStatusResponse{
		PaymentInstrument: &paymentsv1.PaymentInstrument{PaymentInstrumentId: req.GetPaymentInstrumentId()},
		Result:            &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentPaymentInstrumentApproval comments a payment instrument.
func (s *paymentInstrumentApprovalHandlers) CommentPaymentInstrumentApproval(ctx context.Context, req *paymentsv1.CommentPaymentInstrumentApprovalRequest) (*paymentsv1.CommentPaymentInstrumentApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePaymentInstrument(ctx, req.GetContext().GetTenantId(), req.GetPaymentInstrument()); err != nil {
		return nil, grpcx.Wrap(err, "save payment instrument")
	}
	s.log.InfoContext(ctx, "comment payment instrument approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPaymentInstrument().GetAudit().GetEtag())
	var iban string
	switch choice := req.GetPaymentInstrument().GetFunding().(type) {
	case *paymentsv1.PaymentInstrument_Card:
		iban = choice.Card.GetHolderName()
	case *paymentsv1.PaymentInstrument_Bank:
		iban = choice.Bank.GetIban()
	case *paymentsv1.PaymentInstrument_ExternalAccountId:
		iban = choice.ExternalAccountId
	}
	recallReplenishmentPlanReply, err := s.replenishmentPlanApprovalClient.RecallReplenishmentPlan(ctx, &inventoryv1.RecallReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Settlement: &inventoryv1.ReplenishmentPlan_Bank{Bank: &inventoryv1.ReplenishmentPlanBankSettlement{Iban: iban}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanApprovalService/RecallReplenishmentPlan")
	}
	paymentInstrumentCache := cache.New[string, *paymentsv1.PaymentInstrument](1)
	paymentInstrumentCache.Put(req.GetContext().GetTraceId(), req.GetPaymentInstrument())
	cachedPaymentInstrument, _ := paymentInstrumentCache.Get(req.GetContext().GetTraceId())
	cloned := proto.Clone(req.GetPaymentInstrument()).(*paymentsv1.PaymentInstrument)
	explainHouseholdDecisionRequest := &customersv1.ExplainHouseholdDecisionRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						UpdatedAt: req.GetPaymentInstrument().GetAudit().GetUpdatedAt(),
						CreatedBy: cachedPaymentInstrument.GetAudit().GetCreatedBy(),
						UpdatedBy: recallReplenishmentPlanReply.GetReplenishmentPlan().GetAudit().GetUpdatedBy(),
					},
					Manager: &identityv1.UserRef{DisplayName: cloned.GetHolder().GetDisplayName()},
				}},
			}},
		},
	}
	send := s.explainHouseholdDecisionSender(explainHouseholdDecisionRequest)
	explainHouseholdDecisionReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision")
	}
	return &paymentsv1.CommentPaymentInstrumentApprovalResponse{
		Outcome: &paymentsv1.CommentPaymentInstrumentApprovalResponse_Outcome{
			Status:      recallReplenishmentPlanReply.GetResult().GetStatus(),
			ReferenceId: recallReplenishmentPlanReply.GetOutcome().GetReferenceId(),
		},
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder: &customersv1.CustomerRef{
				DisplayName: explainHouseholdDecisionReply.GetHousehold().GetManager().GetDisplayName(),
			},
			PaymentInstrumentId: req.GetPaymentInstrumentId(),
			Status:              req.GetPaymentInstrument().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// consentRecordValidationLazy dials acme.customers.v1.ConsentRecordValidationService the first time a handler needs it.
func (s *paymentInstrumentApprovalHandlers) consentRecordValidationLazy() customersv1.ConsentRecordValidationServiceClient {
	s.consentRecordValidationOnce.Do(func() { s.consentRecordValidationCached = customersv1.NewConsentRecordValidationServiceClient(s.conn) })
	return s.consentRecordValidationCached
}

// explainHouseholdDecisionSender returns a function that sends one request to
// acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision.
func (s *paymentInstrumentApprovalHandlers) explainHouseholdDecisionSender(request *customersv1.ExplainHouseholdDecisionRequest) func(context.Context) (*customersv1.ExplainHouseholdDecisionResponse, error) {
	return func(ctx context.Context) (*customersv1.ExplainHouseholdDecisionResponse, error) {
		return s.householdValidationClient.ExplainHouseholdDecision(ctx, request)
	}
}
