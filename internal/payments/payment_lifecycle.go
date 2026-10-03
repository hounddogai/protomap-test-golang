package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	performancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PaymentLifecycleServer implements `acme.payments.v1.PaymentLifecycleService`.
// Moves a payment between the states of its lifecycle.
type PaymentLifecycleServer struct {
	paymentsv1.UnimplementedPaymentLifecycleServiceServer
	store                              *Store
	log                                *slog.Logger
	tenantExportClient                 identityv1.TenantExportServiceClient
	paymentPort                        paymentPort
	meteredRoleNotification            meteredRoleNotification
	searchQueryHistoryClient           searchv1.SearchQueryHistoryServiceClient
	reviewClient                       performancev1.ReviewServiceClient
	validateRefundFn                   func(context.Context, *paymentsv1.ValidateRefundRequest, ...grpc.CallOption) (*paymentsv1.ValidateRefundResponse, error)
	paymentInstrumentCalculationClient paymentsv1.PaymentInstrumentCalculationServiceClient
	giftCardBatchApprovalClient        giftcardsv1.GiftCardBatchApprovalServiceClient
	fraudRuleWorkflowClient            fraudv1.FraudRuleWorkflowServiceClient
	conn                               *grpc.ClientConn
	householdCalculationClient         customersv1.HouseholdCalculationServiceClient
	paymentClient                      paymentsv1.PaymentServiceClient
	preferenceValidationClient         customersv1.PreferenceValidationServiceClient
}

// NewPaymentLifecycleServer wires a PaymentLifecycleServer.
func NewPaymentLifecycleServer(deps platform.Deps) *PaymentLifecycleServer {
	refundValidation := paymentsv1.NewRefundValidationServiceClient(deps.Conn)
	return &PaymentLifecycleServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.payments.v1.PaymentLifecycleService"),
		tenantExportClient:                 identityv1.NewTenantExportServiceClient(deps.Conn),
		paymentPort:                        newPaymentPort(deps),
		meteredRoleNotification:            meteredRoleNotification{RoleNotificationServiceClient: identityv1.NewRoleNotificationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchQueryHistoryClient:           searchv1.NewSearchQueryHistoryServiceClient(deps.Conn),
		reviewClient:                       performancev1.NewReviewServiceClient(deps.Conn),
		validateRefundFn:                   refundValidation.ValidateRefund,
		paymentInstrumentCalculationClient: paymentsv1.NewPaymentInstrumentCalculationServiceClient(deps.Conn),
		giftCardBatchApprovalClient:        giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		fraudRuleWorkflowClient:            fraudv1.NewFraudRuleWorkflowServiceClient(deps.Conn),
		conn:                               deps.Conn,
		householdCalculationClient:         customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		paymentClient:                      paymentsv1.NewPaymentServiceClient(deps.Conn),
		preferenceValidationClient:         customersv1.NewPreferenceValidationServiceClient(deps.Conn),
	}
}

// ActivatePayment activates a payment.
func (s *PaymentLifecycleServer) ActivatePayment(ctx context.Context, req *paymentsv1.ActivatePaymentRequest) (*paymentsv1.ActivatePaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ActivatePayment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "activate_payment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate payment", "tenant", req.GetContext().GetTenantId())
	streamTenantExportStream, err := s.tenantExportClient.StreamTenantExport(ctx, &identityv1.StreamTenantExportRequest{
		Context: req.GetContext(),
		Filter: &identityv1.StreamTenantExportRequest_Filter{
			Labels:  req.GetAnnotations(),
			OwnerId: req.GetDetails().GetNewOwner().GetUserId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
	}
	for {
		received, err := streamTenantExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/StreamTenantExport")
		}
		_ = received
	}
	getPaymentRequest := &paymentsv1.GetPaymentRequest{
		Context:   req.GetContext(),
		PaymentId: req.GetPaymentId(),
	}
	getPaymentReply, err := s.paymentPort.GetPayment(ctx, getPaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentService/GetPayment")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	detailsCache := cache.New[string, *paymentsv1.ActivatePaymentRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sendRoleTestNoticeRequest := &identityv1.SendRoleTestNoticeRequest{
		Context: req.GetContext(),
		Details: &identityv1.SendRoleTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Email:            tenancy.Value(ctx, "email"),
				Phone:            cachedDetails.GetNotifyContact().GetPhone(),
			},
			Payload: packed,
		},
	}
	sendRoleTestNoticeReply, err := s.meteredRoleNotification.SendRoleTestNotice(ctx, sendRoleTestNoticeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/SendRoleTestNotice")
	}
	return &paymentsv1.ActivatePaymentResponse{
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				Locality: getPaymentReply.GetPayment().GetBillingAddress().GetLocality(),
			},
			DueAt:     getPaymentReply.GetPayment().GetDueAt(),
			PaymentId: req.GetPaymentId(),
		},
		Outcome: &paymentsv1.ActivatePaymentResponse_Outcome{
			Status:      sendRoleTestNoticeReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: sendRoleTestNoticeReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// SuspendPayment suspends a payment.
func (s *PaymentLifecycleServer) SuspendPayment(ctx context.Context, req *paymentsv1.SuspendPaymentRequest) (*paymentsv1.SuspendPaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "suspend_payment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend payment", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*paymentsv1.SuspendPaymentRequest_Details)
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertSearchQueryRequest := &searchv1.RevertSearchQueryRequest{
		Context: req.GetContext(),
		SearchQuery: &searchv1.SearchQuery{
			Subject: &searchv1.SearchIndexRef{DisplayName: cloned.GetNewOwner().GetDisplayName()},
			Detail:  packed,
		},
		Details: &searchv1.RevertSearchQueryRequest_Details{
			AuthorEmail: req.GetDetails().GetNewOwner().GetEmail(),
			Snapshot:    snapshot,
		},
	}
	revertSearchQueryReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*searchv1.RevertSearchQueryResponse, error) {
		return s.searchQueryHistoryClient.RevertSearchQuery(ctx, revertSearchQueryRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryHistoryService/RevertSearchQuery")
	}
	return &paymentsv1.SuspendPaymentResponse{
		Outcome: &paymentsv1.SuspendPaymentResponse_Outcome{
			Status:      revertSearchQueryReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Payment: &paymentsv1.Payment{PaymentId: req.GetPaymentId()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// TerminatePayment terminates a payment.
func (s *PaymentLifecycleServer) TerminatePayment(ctx context.Context, req *paymentsv1.TerminatePaymentRequest) (*paymentsv1.TerminatePaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected TerminatePayment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "terminate_payment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate payment", "tenant", req.GetContext().GetTenantId())
	watchReviewsStream, err := s.reviewClient.WatchReviews(ctx, &performancev1.WatchReviewsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Name:  req.GetDetails().GetNotifyContact().GetName(),
			Email: req.GetDetails().GetNotifyContact().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.performance.v1.ReviewService/WatchReviews")
	}
	for {
		received, err := watchReviewsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.performance.v1.ReviewService/WatchReviews")
		}
		_ = received
	}
	validateRefundRequest := &paymentsv1.ValidateRefundRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	validateRefundReply, err := s.validateRefundFn(ctx, validateRefundRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundValidationService/ValidateRefund")
	}
	comparePaymentInstrumentOptionsReply, err := s.paymentInstrumentCalculationClient.ComparePaymentInstrumentOptions(ctx, &paymentsv1.ComparePaymentInstrumentOptionsRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Holder:  &customersv1.CustomerRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
			Funding: &paymentsv1.PaymentInstrument_Bank{Bank: &paymentsv1.PaymentInstrumentBankFunding{Bic: validateRefundReply.GetRefund().GetBank().GetBic()}},
		},
		Details: &paymentsv1.ComparePaymentInstrumentOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: validateRefundReply.GetRefund().GetBillingAddress().GetPostalCode(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentCalculationService/ComparePaymentInstrumentOptions")
	}
	explainPaymentInstrumentCalculationRequest := &paymentsv1.ExplainPaymentInstrumentCalculationRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Funding: &paymentsv1.PaymentInstrument_Card{Card: &paymentsv1.PaymentInstrumentCardFunding{
				Token: comparePaymentInstrumentOptionsReply.GetPaymentInstrument().GetCard().GetToken(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateRefundReply.GetRefund().GetAudit().GetCreatedBy(),
			},
			Contact: &commonv1.ContactInfo{
				Name: comparePaymentInstrumentOptionsReply.GetPaymentInstrument().GetContact().GetName(),
			},
		},
	}
	explainPaymentInstrumentCalculationReply, err := grpcx.Call(ctx, s.paymentInstrumentCalculationClient.ExplainPaymentInstrumentCalculation, explainPaymentInstrumentCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentCalculationService/ExplainPaymentInstrumentCalculation")
	}
	return &paymentsv1.TerminatePaymentResponse{
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				Lines: validateRefundReply.GetRefund().GetBillingAddress().GetLines(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: comparePaymentInstrumentOptionsReply.GetPaymentInstrument().GetAudit().GetUpdatedBy(),
			},
			PaymentId: req.GetPaymentId(),
		},
		Outcome: &paymentsv1.TerminatePaymentResponse_Outcome{
			Status:      explainPaymentInstrumentCalculationReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RenewPayment renews a payment.
func (s *PaymentLifecycleServer) RenewPayment(ctx context.Context, req *paymentsv1.RenewPaymentRequest) (*paymentsv1.RenewPaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "renew_payment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew payment", "tenant", req.GetContext().GetTenantId())
	recallGiftCardBatchRequest := &giftcardsv1.RecallGiftCardBatchRequest{}
	recallGiftCardBatchRequest.Context = req.GetContext()
	recallGiftCardBatchRequest.GiftCardBatch = &giftcardsv1.GiftCardBatch{
		Counterparty: &giftcardsv1.GiftCardRef{
			DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
		},
	}
	recallGiftCardBatchReply, err := s.giftCardBatchApprovalClient.RecallGiftCardBatch(ctx, recallGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/RecallGiftCardBatch")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := paymentLifecycleServerAdvanceFraudRuleWorkflowSteps[mode]
	advanceFraudRuleWorkflowRequest := &fraudv1.AdvanceFraudRuleWorkflowRequest{
		Context: req.GetContext(),
		FraudRule: &fraudv1.FraudRule{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetNewOwner().GetEmail()},
		},
	}
	advanceFraudRuleWorkflowReply, err := run(s, ctx, advanceFraudRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleWorkflowService/AdvanceFraudRuleWorkflow")
	}
	return &paymentsv1.RenewPaymentResponse{
		Outcome: &paymentsv1.RenewPaymentResponse_Outcome{
			Status:      recallGiftCardBatchReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: recallGiftCardBatchReply.GetResult().GetStatus()},
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: advanceFraudRuleWorkflowReply.GetFraudRule().GetAudit().GetUpdatedAt(),
			},
			PaymentId: req.GetPaymentId(),
		},
	}, nil
}

// ExpirePayment expires a payment.
func (s *PaymentLifecycleServer) ExpirePayment(ctx context.Context, req *paymentsv1.ExpirePaymentRequest) (*paymentsv1.ExpirePaymentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpirePayment", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "payment", req.GetPaymentId(), "expire_payment"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire payment", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *paymentsv1.ExpirePaymentRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	calculateDisputeRequest := &paymentsv1.CalculateDisputeRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Origin: &paymentsv1.Dispute_Customer{Customer: &paymentsv1.PaymentRef{DisplayName: cachedDetails.GetNewOwner().GetDisplayName()}},
		},
	}
	calculateDisputeReply := new(paymentsv1.CalculateDisputeResponse)
	if err := s.conn.Invoke(ctx, paymentsDisputeCalculationServiceCalculateDisputeMethod, calculateDisputeRequest, calculateDisputeReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeCalculationService/CalculateDispute")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	recalculateHouseholdsRequest := &customersv1.RecalculateHouseholdsRequest{}
	recalculateHouseholdsRequest.Context = req.GetContext()
	recalculateHouseholdsRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Audit: &commonv1.AuditInfo{
					CreatedBy: calculateDisputeReply.GetDispute().GetAudit().GetCreatedBy(),
					CreatedAt: calculateDisputeReply.GetDispute().GetAudit().GetCreatedAt(),
				},
				Manager: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
			}},
		}},
	}
	recalculateHouseholdsReply, err := s.householdCalculationClient.RecalculateHouseholds(ctx, recalculateHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds")
	}
	return &paymentsv1.ExpirePaymentResponse{
		Payment: &paymentsv1.Payment{
			PaymentId: calculateDisputeReply.GetDispute().GetCustomer().GetPaymentId(),
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Result: &commonv1.OperationResult{
			Status:  recalculateHouseholdsReply.GetResult().GetStatus(),
			Message: recalculateHouseholdsReply.GetResult().GetMessage(),
		},
		Outcome: &paymentsv1.ExpirePaymentResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SchedulePaymentExpiry schedules a payment.
func (s *PaymentLifecycleServer) SchedulePaymentExpiry(ctx context.Context, req *paymentsv1.SchedulePaymentExpiryRequest) (*paymentsv1.SchedulePaymentExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePayment(ctx, req.GetContext().GetTenantId(), req.GetPayment()); err != nil {
		return nil, grpcx.Wrap(err, "save payment")
	}
	s.log.InfoContext(ctx, "schedule payment expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPayment().GetAudit().GetEtag())
	jobs := make(chan *paymentsv1.PaymentAttachment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attachment := range jobs {
				if _, err := s.paymentClient.CreatePayment(workCtx, &paymentsv1.CreatePaymentRequest{
					Context: req.GetContext(),
					Payment: &paymentsv1.Payment{
						Attachments: []*paymentsv1.PaymentAttachment{&paymentsv1.PaymentAttachment{
							FileName:    attachment.GetFileName(),
							ContentType: attachment.GetContentType(),
							Checksum:    attachment.GetChecksum(),
							SizeBytes:   attachment.GetSizeBytes(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPayment().GetAttachments() {
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
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentService/CreatePayment")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.SchedulePaymentExpiryRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	normalizePreferenceReply, err := s.preferenceValidationClient.NormalizePreference(ctx, &customersv1.NormalizePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizePreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: (&decoded).GetNotify().GetPreferredChannel(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/NormalizePreference")
	}
	return &paymentsv1.SchedulePaymentExpiryResponse{
		Payment: &paymentsv1.Payment{
			Lines: []*paymentsv1.PaymentLine{&paymentsv1.PaymentLine{
				Amount: normalizePreferenceReply.GetPreference().GetDiscount().GetAmount(),
			}},
			PaymentId: req.GetPaymentId(),
			Notes:     req.GetPayment().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// paymentLifecycleServerAdvanceFraudRuleWorkflowSteps are the ways to run
// acme.fraud.v1.FraudRuleWorkflowService/AdvanceFraudRuleWorkflow, by name.
var paymentLifecycleServerAdvanceFraudRuleWorkflowSteps = map[string]func(*PaymentLifecycleServer, context.Context, *fraudv1.AdvanceFraudRuleWorkflowRequest) (*fraudv1.AdvanceFraudRuleWorkflowResponse, error){
	"default": (*PaymentLifecycleServer).advanceFraudRuleWorkflowStep,
	"audited": (*PaymentLifecycleServer).advanceFraudRuleWorkflowAuditedStep,
}

func (s *PaymentLifecycleServer) advanceFraudRuleWorkflowStep(ctx context.Context, request *fraudv1.AdvanceFraudRuleWorkflowRequest) (*fraudv1.AdvanceFraudRuleWorkflowResponse, error) {
	return s.fraudRuleWorkflowClient.AdvanceFraudRuleWorkflow(ctx, request)
}

func (s *PaymentLifecycleServer) advanceFraudRuleWorkflowAuditedStep(ctx context.Context, request *fraudv1.AdvanceFraudRuleWorkflowRequest) (*fraudv1.AdvanceFraudRuleWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.fraud.v1.FraudRuleWorkflowService/AdvanceFraudRuleWorkflow")
	return s.fraudRuleWorkflowClient.AdvanceFraudRuleWorkflow(ctx, request)
}
