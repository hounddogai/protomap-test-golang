package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// RefundLifecycleServer implements `acme.payments.v1.RefundLifecycleService`.
// Moves a refund between the states of its lifecycle.
type RefundLifecycleServer struct {
	*refundLifecycleHandlers
}

// refundLifecycleHandlers holds the handlers of RefundLifecycleServer, with what they use.
type refundLifecycleHandlers struct {
	paymentsv1.UnimplementedRefundLifecycleServiceServer
	store                            *Store
	log                              *slog.Logger
	pickListApprovalClient           warehousingv1.PickListApprovalServiceClient
	ledgerAccountClient              ledgerv1.LedgerAccountServiceClient
	customerCalculationPort          customerCalculationPort
	paymentClient                    paymentsv1.PaymentServiceClient
	conn                             *grpc.ClientConn
	paymentInstrumentReportingOnce   sync.Once
	paymentInstrumentReportingCached paymentsv1.PaymentInstrumentReportingServiceClient
	paymentInstrumentSyncPort        paymentInstrumentSyncPort
	auditEventClient                 auditv1.AuditEventServiceClient
	customerSegmentValidationClient  customersv1.CustomerSegmentValidationServiceClient
	reviewVoteWorkflowClient         reviewsv1.ReviewVoteWorkflowServiceClient
	refundReportingClient            paymentsv1.RefundReportingServiceClient
	paymentInstrumentReportingClient paymentsv1.PaymentInstrumentReportingServiceClient
	sellerListingReportingClient     sellersv1.SellerListingReportingServiceClient
	disputeValidationClient          paymentsv1.DisputeValidationServiceClient
}

// NewRefundLifecycleServer wires a RefundLifecycleServer.
func NewRefundLifecycleServer(deps platform.Deps) *RefundLifecycleServer {
	return &RefundLifecycleServer{refundLifecycleHandlers: &refundLifecycleHandlers{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.payments.v1.RefundLifecycleService"),
		pickListApprovalClient:           warehousingv1.NewPickListApprovalServiceClient(deps.Conn),
		ledgerAccountClient:              ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		customerCalculationPort:          newCustomerCalculationPort(deps),
		paymentClient:                    paymentsv1.NewPaymentServiceClient(deps.Conn),
		conn:                             deps.Conn,
		paymentInstrumentSyncPort:        newPaymentInstrumentSyncPort(deps),
		auditEventClient:                 auditv1.NewAuditEventServiceClient(deps.Conn),
		customerSegmentValidationClient:  customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		reviewVoteWorkflowClient:         reviewsv1.NewReviewVoteWorkflowServiceClient(deps.Conn),
		refundReportingClient:            paymentsv1.NewRefundReportingServiceClient(deps.Conn),
		paymentInstrumentReportingClient: paymentsv1.NewPaymentInstrumentReportingServiceClient(deps.Conn),
		sellerListingReportingClient:     sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		disputeValidationClient:          paymentsv1.NewDisputeValidationServiceClient(deps.Conn),
	}}
}

// ActivateRefund activates a refund.
func (s *refundLifecycleHandlers) ActivateRefund(ctx context.Context, req *paymentsv1.ActivateRefundRequest) (*paymentsv1.ActivateRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ActivateRefund", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "activate_refund"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate refund", "tenant", req.GetContext().GetTenantId())
	recallPickListRequest := buildRecallPickListRequestForActivateRefund(req)
	recallPickListReply, err := s.pickListApprovalClient.RecallPickList(ctx, recallPickListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListApprovalService/RecallPickList")
	}
	searchLedgerAccountsRequest := &ledgerv1.SearchLedgerAccountsRequest{}
	searchLedgerAccountsRequest.Context = req.GetContext()
	searchLedgerAccountsRequest.Filter = &ledgerv1.SearchLedgerAccountsRequest_Filter{
		Region:       recallPickListReply.GetPickList().GetBillingAddress().GetRegion(),
		CreatedAfter: req.GetDetails().GetEffectiveAt(),
	}
	searchLedgerAccountsReply, err := s.ledgerAccountClient.SearchLedgerAccounts(ctx, searchLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/SearchLedgerAccounts")
	}
	return &paymentsv1.ActivateRefundResponse{
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: recallPickListReply.GetPickList().GetBillingAddress().GetPostalCode(),
				Region:     recallPickListReply.GetPickList().GetBillingAddress().GetRegion(),
			},
			RefundId: req.GetRefundId(),
		},
		Result: &commonv1.OperationResult{Status: searchLedgerAccountsReply.GetResult().GetStatus()},
		Outcome: &paymentsv1.ActivateRefundResponse_Outcome{
			Status:      searchLedgerAccountsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// SuspendRefund suspends a refund.
func (s *refundLifecycleHandlers) SuspendRefund(ctx context.Context, req *paymentsv1.SuspendRefundRequest) (*paymentsv1.SuspendRefundResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "suspend_refund"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend refund", "tenant", req.GetContext().GetTenantId())
	quoteCustomerRequest := &customersv1.QuoteCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
			Contact: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetNotifyContact().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
			Name: req.GetDetails().GetNotifyContact().GetName(),
		},
	}
	quoteCustomerReply, err := s.customerCalculationPort.QuoteCustomer(ctx, quoteCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/QuoteCustomer")
	}
	createPaymentRequest := &paymentsv1.CreatePaymentRequest{}
	createPaymentRequest.Context = req.GetContext()
	createPaymentRequest.Payment = &paymentsv1.Payment{
		Audit: &commonv1.AuditInfo{
			UpdatedAt: quoteCustomerReply.GetCustomer().GetAudit().GetUpdatedAt(),
		},
		Counterparty: &paymentsv1.RefundRef{
			Number: quoteCustomerReply.GetCustomer().GetPassport().GetNumber(),
		},
	}
	createPaymentReply, err := s.paymentClient.CreatePayment(ctx, createPaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentService/CreatePayment")
	}
	return &paymentsv1.SuspendRefundResponse{
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: quoteCustomerReply.GetCustomer().GetPrimaryAddress().GetRecipient(),
			},
			Counterparty: &customersv1.CustomerRef{
				Email: quoteCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: createPaymentReply.GetPayment().GetAudit().GetUpdatedAt(),
			},
			Total:    createPaymentReply.GetPayment().GetTotal(),
			RefundId: req.GetRefundId(),
		},
		Outcome: &paymentsv1.SuspendRefundResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReinstateRefund reinstates a refund.
func (s *refundLifecycleHandlers) ReinstateRefund(ctx context.Context, req *paymentsv1.ReinstateRefundRequest) (*paymentsv1.ReinstateRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReinstateRefund", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "reinstate_refund"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate refund", "tenant", req.GetContext().GetTenantId())
	countPaymentInstrumentTotalsRequest := &paymentsv1.CountPaymentInstrumentTotalsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.CountPaymentInstrumentTotalsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}
	countPaymentInstrumentTotalsReply, err := s.paymentInstrumentReportingLazy().CountPaymentInstrumentTotals(ctx, countPaymentInstrumentTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentReportingService/CountPaymentInstrumentTotals")
	}
	detailsCache := cache.New[string, *paymentsv1.ReinstateRefundRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotifyContact().GetEmail())
	delegatePickListApprovalRequest := &warehousingv1.DelegatePickListApprovalRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				Email:       tenancy.Value(ctx, "email"),
			},
		},
		Details: &warehousingv1.DelegatePickListApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  cachedDetails.GetNewOwner().GetEmail(),
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
			},
		},
	}
	send := s.delegatePickListApprovalSender(delegatePickListApprovalRequest)
	delegatePickListApprovalReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListApprovalService/DelegatePickListApproval")
	}
	return &paymentsv1.ReinstateRefundResponse{
		Result: &commonv1.OperationResult{
			Details: countPaymentInstrumentTotalsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				CreatedBy: delegatePickListApprovalReply.GetPickList().GetAudit().GetCreatedBy(),
			},
			RefundId:     req.GetRefundId(),
			Counterparty: &customersv1.CustomerRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
		Outcome: &paymentsv1.ReinstateRefundResponse_Outcome{
			EffectiveAt: delegatePickListApprovalReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// TerminateRefund terminates a refund.
func (s *refundLifecycleHandlers) TerminateRefund(ctx context.Context, req *paymentsv1.TerminateRefundRequest) (*paymentsv1.TerminateRefundResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "terminate_refund"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate refund", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullPayoutsRequest := &paymentsv1.PullPayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &paymentsv1.PullPayoutsRequest_Details{Payload: snapshot},
	}
	pullPayoutsReply := new(paymentsv1.PullPayoutsResponse)
	if err := grpc.Invoke(ctx, "/acme.payments.v1.PayoutSyncService/PullPayouts", pullPayoutsRequest, pullPayoutsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/PullPayouts")
	}
	snapshot2, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolvePaymentInstrumentConflictRequest := &paymentsv1.ResolvePaymentInstrumentConflictRequest{
		Context: req.GetContext(),
		PaymentInstrument: &paymentsv1.PaymentInstrument{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pullPayoutsReply.GetPayout().GetAudit().GetUpdatedBy(),
			},
			Funding: &paymentsv1.PaymentInstrument_Bank{Bank: &paymentsv1.PaymentInstrumentBankFunding{
				AccountHolder: pullPayoutsReply.GetPayout().GetBank().GetAccountHolder(),
			}},
			Holder: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &paymentsv1.ResolvePaymentInstrumentConflictRequest_Details{Payload: snapshot2},
	}
	resolvePaymentInstrumentConflictReply, err := s.paymentInstrumentSyncPort.ResolvePaymentInstrumentConflict(ctx, resolvePaymentInstrumentConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentSyncService/ResolvePaymentInstrumentConflict")
	}
	return &paymentsv1.TerminateRefundResponse{
		Outcome: &paymentsv1.TerminateRefundResponse_Outcome{
			Message:     pullPayoutsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{
				Email: resolvePaymentInstrumentConflictReply.GetPaymentInstrument().GetContact().GetEmail(),
			},
			RefundId: req.GetRefundId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RenewRefund renews a refund.
func (s *refundLifecycleHandlers) RenewRefund(ctx context.Context, req *paymentsv1.RenewRefundRequest) (*paymentsv1.RenewRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "renew_refund"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew refund", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.RenewRefundRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	createAuditEventRequest := &auditv1.CreateAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject:    &auditv1.AuditTrailRef{DisplayName: (&decoded).GetNewOwner().GetDisplayName()},
			OccurredAt: req.GetDetails().GetEffectiveAt(),
		},
	}
	createAuditEventReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*auditv1.CreateAuditEventResponse, error) {
		return s.auditEventClient.CreateAuditEvent(ctx, createAuditEventRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventService/CreateAuditEvent")
	}
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
			},
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetReason()}},
		},
		Details: &customersv1.LintCustomerSegmentsRequest_Details{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
	}
	lintCustomerSegmentsReply, err := grpcx.Call(ctx, s.customerSegmentValidationClient.LintCustomerSegments, lintCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &paymentsv1.RenewRefundResponse{
		Refund: &paymentsv1.Refund{
			Lines: []*paymentsv1.RefundLine{&paymentsv1.RefundLine{
				Attributes: createAuditEventReply.GetAuditEvent().GetAttributes(),
			}},
			RefundId: req.GetRefundId(),
		},
		Outcome: &paymentsv1.RenewRefundResponse_Outcome{
			Message:     lintCustomerSegmentsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExpireRefund expires a refund.
func (s *refundLifecycleHandlers) ExpireRefund(ctx context.Context, req *paymentsv1.ExpireRefundRequest) (*paymentsv1.ExpireRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "expire_refund"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire refund", "tenant", req.GetContext().GetTenantId())
	advanceReviewVoteWorkflowRequest := &reviewsv1.AdvanceReviewVoteWorkflowRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
		Details: &reviewsv1.AdvanceReviewVoteWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotifyContact().GetPhone()},
		},
	}
	advanceReviewVoteWorkflowReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*reviewsv1.AdvanceReviewVoteWorkflowResponse, error) {
		return s.reviewVoteWorkflowClient.AdvanceReviewVoteWorkflow(ctx, advanceReviewVoteWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.reviews.v1.ReviewVoteWorkflowService/AdvanceReviewVoteWorkflow")
	}
	streamRefundMetricsStream, err := s.refundReportingClient.StreamRefundMetrics(ctx, &paymentsv1.StreamRefundMetricsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetDetails().GetNewOwner().GetEmail(),
			Name:  req.GetDetails().GetNotifyContact().GetName(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/StreamRefundMetrics")
	}
	for {
		received, err := streamRefundMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.RefundReportingService/StreamRefundMetrics")
		}
		_ = received
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	sharePaymentInstrumentReportRequest := &paymentsv1.SharePaymentInstrumentReportRequest{}
	sharePaymentInstrumentReportRequest.Context = req.GetContext()
	sharePaymentInstrumentReportRequest.Details = &paymentsv1.SharePaymentInstrumentReportRequest_Details{
		Recipient: &commonv1.ContactInfo{
			Email: advanceReviewVoteWorkflowReply.GetReviewVote().GetSubject().GetEmail(),
			Name:  req.GetDetails().GetNotifyContact().GetName(),
		},
		Payload: packed,
	}
	sharePaymentInstrumentReportReply, err := s.paymentInstrumentReportingClient.SharePaymentInstrumentReport(ctx, sharePaymentInstrumentReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentReportingService/SharePaymentInstrumentReport")
	}
	return &paymentsv1.ExpireRefundResponse{
		Outcome: &paymentsv1.ExpireRefundResponse_Outcome{
			Message:     advanceReviewVoteWorkflowReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Status: sharePaymentInstrumentReportReply.GetResult().GetStatus(),
		},
		Refund: &paymentsv1.Refund{RefundId: req.GetRefundId()},
	}, nil
}

// GetRefundLifecycle returns a refund.
func (s *refundLifecycleHandlers) GetRefundLifecycle(ctx context.Context, req *paymentsv1.GetRefundLifecycleRequest) (*paymentsv1.GetRefundLifecycleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "get_refund_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get refund lifecycle", "tenant", req.GetContext().GetTenantId())
	countSellerListingTotalsRequest := buildCountSellerListingTotalsRequestForGetRefundLifecycle(req)
	countSellerListingTotalsReply, err := s.sellerListingReportingClient.CountSellerListingTotals(ctx, countSellerListingTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/CountSellerListingTotals")
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	normalizeDisputeReply, err := s.disputeValidationClient.NormalizeDispute(ctx, &paymentsv1.NormalizeDisputeRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Requester: &commonv1.ContactInfo{Email: (&decoded).GetActor().GetEmail()},
			Assignee:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeValidationService/NormalizeDispute")
	}
	return &paymentsv1.GetRefundLifecycleResponse{
		Result: &commonv1.OperationResult{
			Details:       countSellerListingTotalsReply.GetResult().GetDetails(),
			CorrelationId: countSellerListingTotalsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  normalizeDisputeReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: normalizeDisputeReply.GetDispute().GetAudit().GetUpdatedAt(),
			},
			RefundId: req.GetRefundId(),
		},
	}, nil
}

// paymentInstrumentReportingLazy dials acme.payments.v1.PaymentInstrumentReportingService the first time a handler
// needs it.
func (s *refundLifecycleHandlers) paymentInstrumentReportingLazy() paymentsv1.PaymentInstrumentReportingServiceClient {
	s.paymentInstrumentReportingOnce.Do(func() {
		s.paymentInstrumentReportingCached = paymentsv1.NewPaymentInstrumentReportingServiceClient(s.conn)
	})
	return s.paymentInstrumentReportingCached
}

// delegatePickListApprovalSender returns a function that sends one request to
// acme.warehousing.v1.PickListApprovalService/DelegatePickListApproval.
func (s *refundLifecycleHandlers) delegatePickListApprovalSender(request *warehousingv1.DelegatePickListApprovalRequest) func(context.Context) (*warehousingv1.DelegatePickListApprovalResponse, error) {
	return func(ctx context.Context) (*warehousingv1.DelegatePickListApprovalResponse, error) {
		return s.pickListApprovalClient.DelegatePickListApproval(ctx, request)
	}
}
