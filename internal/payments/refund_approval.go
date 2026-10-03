package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	performancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/performance/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// RefundApprovalServer implements `acme.payments.v1.RefundApprovalService`.
// Routes refunds through approval, with escalation and delegation.
type RefundApprovalServer struct {
	*refundApprovalHandlers
}

// refundApprovalHandlers holds the handlers of RefundApprovalServer, with what they use.
type refundApprovalHandlers struct {
	paymentsv1.UnimplementedRefundApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	customerSegmentCalculationClient  customersv1.CustomerSegmentCalculationServiceClient
	meteredPaymentLifecycle           meteredPaymentLifecycle
	payoutSyncClient                  paymentsv1.PayoutSyncServiceClient
	paymentQueryClient                paymentsv1.PaymentQueryServiceClient
	disputeImportClient               paymentsv1.DisputeImportServiceClient
	consentRecordValidationClient     customersv1.ConsentRecordValidationServiceClient
	paymentInstrumentValidationClient paymentsv1.PaymentInstrumentValidationServiceClient
	reviewQueryClient                 performancev1.ReviewQueryServiceClient
}

// NewRefundApprovalServer wires a RefundApprovalServer.
func NewRefundApprovalServer(deps platform.Deps) *RefundApprovalServer {
	return &RefundApprovalServer{refundApprovalHandlers: &refundApprovalHandlers{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.payments.v1.RefundApprovalService"),
		customerSegmentCalculationClient:  customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		meteredPaymentLifecycle:           meteredPaymentLifecycle{PaymentLifecycleServiceClient: paymentsv1.NewPaymentLifecycleServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		payoutSyncClient:                  paymentsv1.NewPayoutSyncServiceClient(deps.Conn),
		paymentQueryClient:                paymentsv1.NewPaymentQueryServiceClient(deps.Conn),
		disputeImportClient:               paymentsv1.NewDisputeImportServiceClient(deps.Conn),
		consentRecordValidationClient:     customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		paymentInstrumentValidationClient: paymentsv1.NewPaymentInstrumentValidationServiceClient(deps.Conn),
		reviewQueryClient:                 performancev1.NewReviewQueryServiceClient(deps.Conn),
	}}
}

// SubmitRefund submits a refund.
func (s *refundApprovalHandlers) SubmitRefund(ctx context.Context, req *paymentsv1.SubmitRefundRequest) (*paymentsv1.SubmitRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitRefund", "error", err)
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "submit refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	recalculateCustomerSegmentsRequest := newRecalculateCustomerSegmentsRequest(withRecalculateCustomerSegmentsContext(req.GetContext()), withRecalculateCustomerSegmentsCustomerSegment(&customersv1.CustomerSegment{
		Owner: &identityv1.UserRef{Email: req.GetRefund().GetCounterparty().GetEmail()},
		Audit: &commonv1.AuditInfo{CreatedBy: req.GetRefund().GetAudit().GetCreatedBy()},
	}))
	recalculateCustomerSegmentsReply, err := s.customerSegmentCalculationClient.RecalculateCustomerSegments(ctx, recalculateCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/RecalculateCustomerSegments")
	}
	return &paymentsv1.SubmitRefundResponse{
		Refund: &paymentsv1.Refund{
			Total:    recalculateCustomerSegmentsReply.GetBreakdown().GetTotal(),
			RefundId: req.GetRefundId(),
			Number:   req.GetRefund().GetNumber(),
		},
		Result: &commonv1.OperationResult{
			Status: recalculateCustomerSegmentsReply.GetResult().GetStatus(),
		},
	}, nil
}

// ApproveRefund approves a refund.
func (s *refundApprovalHandlers) ApproveRefund(ctx context.Context, req *paymentsv1.ApproveRefundRequest) (*paymentsv1.ApproveRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "approve refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	schedulePaymentExpiryRequest := &paymentsv1.SchedulePaymentExpiryRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{
				DisplayName: req.GetRefund().GetCounterparty().GetDisplayName(),
			},
			Settlement: &paymentsv1.Payment_Card{Card: &paymentsv1.PaymentCardSettlement{Token: req.GetRefund().GetCard().GetToken()}},
			BillingAddress: &commonv1.PostalAddress{
				Lines:       req.GetRefund().GetBillingAddress().GetLines(),
				CountryCode: req.GetRefund().GetBillingAddress().GetCountryCode(),
			},
		},
	}
	schedulePaymentExpiryReply, err := s.meteredPaymentLifecycle.SchedulePaymentExpiry(ctx, schedulePaymentExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentLifecycleService/SchedulePaymentExpiry")
	}
	snapshot, err := protojson.Marshal(req.GetRefund())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcilePayoutsRequest := &paymentsv1.ReconcilePayoutsRequest{
		Context: req.GetContext(),
		Payout: &paymentsv1.Payout{
			Settlement: &paymentsv1.Payout_Card{Card: &paymentsv1.PayoutCardSettlement{HolderName: req.GetRefund().GetCard().GetHolderName()}},
			BillingAddress: &commonv1.PostalAddress{
				Locality: schedulePaymentExpiryReply.GetPayment().GetBillingAddress().GetLocality(),
			},
		},
		Details: &paymentsv1.ReconcilePayoutsRequest_Details{Payload: snapshot},
	}
	type reconcilePayoutsResult struct {
		reply *paymentsv1.ReconcilePayoutsResponse
		err   error
	}
	reconcilePayoutsResults := make(chan reconcilePayoutsResult, 1)
	go func() {
		reply, err := s.payoutSyncClient.ReconcilePayouts(ctx, reconcilePayoutsRequest)
		reconcilePayoutsResults <- reconcilePayoutsResult{reply, err}
	}()
	var reconcilePayoutsReply *paymentsv1.ReconcilePayoutsResponse
	select {
	case result := <-reconcilePayoutsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.payments.v1.PayoutSyncService/ReconcilePayouts")
		}
		reconcilePayoutsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	exportPaymentsStream, err := s.paymentQueryClient.ExportPayments(ctx, &paymentsv1.ExportPaymentsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.ExportPaymentsRequest_Filter{
			Region:       reconcilePayoutsReply.GetPayout().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetRefund().GetAudit().GetUpdatedAt(),
			Labels:       reconcilePayoutsReply.GetResult().GetDetails(),
			OwnerId:      schedulePaymentExpiryReply.GetResult().GetCorrelationId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentQueryService/ExportPayments")
	}
	for {
		received, err := exportPaymentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentQueryService/ExportPayments")
		}
		_ = received
	}
	var holder struct{ Region string }
	if record := req.GetRefund().GetBillingAddress(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Region"); err != nil {
			s.log.WarnContext(ctx, "copy region", "error", err)
		}
	}
	listDisputeImportsRequest := &paymentsv1.ListDisputeImportsRequest{}
	listDisputeImportsRequest.Context = req.GetContext()
	listDisputeImportsRequest.Filter = &paymentsv1.ListDisputeImportsRequest_Filter{
		Region:       holder.Region,
		CreatedAfter: schedulePaymentExpiryReply.GetOutcome().GetEffectiveAt(),
		OwnerId:      schedulePaymentExpiryReply.GetPayment().GetCounterparty().GetRefundId(),
		Labels:       reconcilePayoutsReply.GetPayout().GetMetadata(),
	}
	listDisputeImportsReply, err := s.disputeImportClient.ListDisputeImports(ctx, listDisputeImportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeImportService/ListDisputeImports")
	}
	return &paymentsv1.ApproveRefundResponse{
		Refund: &paymentsv1.Refund{
			IssuedAt: schedulePaymentExpiryReply.GetPayment().GetIssuedAt(),
			RefundId: schedulePaymentExpiryReply.GetPayment().GetCounterparty().GetRefundId(),
			Number:   req.GetRefund().GetNumber(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  reconcilePayoutsReply.GetResult().GetReferenceIds(),
			CorrelationId: listDisputeImportsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &paymentsv1.ApproveRefundResponse_Outcome{
			Message:     listDisputeImportsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// ListPendingRefunds lists refunds a page at a time.
func (s *refundApprovalHandlers) ListPendingRefunds(ctx context.Context, req *paymentsv1.ListPendingRefundsRequest) (*paymentsv1.ListPendingRefundsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list pending refunds", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.ListPendingRefundsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateRefundApproval delegates a refund.
func (s *refundApprovalHandlers) DelegateRefundApproval(ctx context.Context, req *paymentsv1.DelegateRefundApprovalRequest) (*paymentsv1.DelegateRefundApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "delegate refund approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetRefund())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded paymentsv1.Refund
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetRefund())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scoreConsentRecordRequest := &customersv1.ScoreConsentRecordRequest{}
	scoreConsentRecordRequest.Context = req.GetContext()
	scoreConsentRecordRequest.ConsentRecord = &customersv1.ConsentRecord{
		Subject: &customersv1.HouseholdRef{
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
		Detail: packed,
	}
	scoreConsentRecordRequest.Details = &customersv1.ScoreConsentRecordRequest_Details{
		Destination: &commonv1.PostalAddress{
			PostalCode: (&decoded).GetBillingAddress().GetPostalCode(),
			Lines:      req.GetRefund().GetBillingAddress().GetLines(),
		},
	}
	scoreConsentRecordReply, err := s.consentRecordValidationClient.ScoreConsentRecord(ctx, scoreConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord")
	}
	listPayoutConflictsRequest := &paymentsv1.ListPayoutConflictsRequest{
		Context: req.GetContext(),
		Filter:  &paymentsv1.ListPayoutConflictsRequest_Filter{Region: req.GetRefund().GetBillingAddress().GetRegion()},
	}
	send := s.listPayoutConflictsSender(listPayoutConflictsRequest)
	listPayoutConflictsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PayoutSyncService/ListPayoutConflicts")
	}
	return &paymentsv1.DelegateRefundApprovalResponse{
		Result: &commonv1.OperationResult{Status: scoreConsentRecordReply.GetResult().GetStatus()},
		Outcome: &paymentsv1.DelegateRefundApprovalResponse_Outcome{
			Message: scoreConsentRecordReply.GetResult().GetMessage(),
			Status:  listPayoutConflictsReply.GetResult().GetStatus(),
		},
		Refund: &paymentsv1.Refund{
			RefundId: req.GetRefundId(),
			Number:   req.GetRefund().GetNumber(),
		},
	}, nil
}

// RecallRefund recalls a refund.
func (s *refundApprovalHandlers) RecallRefund(ctx context.Context, req *paymentsv1.RecallRefundRequest) (*paymentsv1.RecallRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "recall refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	for line := range iterx.Each(req.GetRefund().GetLines(), func(candidate *paymentsv1.RefundLine) bool { return candidate != nil }) {
		if _, err := s.paymentInstrumentValidationClient.NormalizePaymentInstrument(ctx, &paymentsv1.NormalizePaymentInstrumentRequest{
			Context: req.GetContext(),
			PaymentInstrument: &paymentsv1.PaymentInstrument{
				Limits: []*paymentsv1.PaymentInstrumentLimit{&paymentsv1.PaymentInstrumentLimit{Amount: line.GetTax().GetAmount()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentInstrumentValidationService/NormalizePaymentInstrument")
		}
	}
	return &paymentsv1.RecallRefundResponse{
		Refund: &paymentsv1.Refund{
			RefundId: req.GetRefundId(),
			IssuedAt: req.GetRefund().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetRefundApprovalStatus returns a refund.
func (s *refundApprovalHandlers) GetRefundApprovalStatus(ctx context.Context, req *paymentsv1.GetRefundApprovalStatusRequest) (*paymentsv1.GetRefundApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "get_refund_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get refund approval status", "tenant", req.GetContext().GetTenantId())
	browseReviewsRequest := buildBrowseReviewsRequestForGetRefundApprovalStatus(req)
	browseReviewsReply, err := s.reviewQueryClient.BrowseReviews(ctx, browseReviewsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.performance.v1.ReviewQueryService/BrowseReviews")
	}
	return &paymentsv1.GetRefundApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Details:       browseReviewsReply.GetResult().GetDetails(),
			CorrelationId: browseReviewsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Refund: &paymentsv1.Refund{RefundId: req.GetRefundId()},
	}, nil
}

// CommentRefundApproval comments a refund.
func (s *refundApprovalHandlers) CommentRefundApproval(ctx context.Context, req *paymentsv1.CommentRefundApprovalRequest) (*paymentsv1.CommentRefundApprovalResponse, error) {
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "comment refund approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	listDisputeImportsRequest := &paymentsv1.ListDisputeImportsRequest{
		Context: req.GetContext(),
		Filter:  &paymentsv1.ListDisputeImportsRequest_Filter{Region: req.GetRefund().GetBillingAddress().GetRegion()},
	}
	var listDisputeImportsReply *paymentsv1.ListDisputeImportsResponse
	if err := pipeline.Run(ctx, "acme.payments.v1.DisputeImportService/ListDisputeImports", func(ctx context.Context) error {
		answer, err := s.disputeImportClient.ListDisputeImports(ctx, listDisputeImportsRequest)
		listDisputeImportsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeImportService/ListDisputeImports")
	}
	return &paymentsv1.CommentRefundApprovalResponse{
		Result: &commonv1.OperationResult{
			Message: listDisputeImportsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &paymentsv1.CommentRefundApprovalResponse_Outcome{
			Message:     listDisputeImportsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Refund: &paymentsv1.Refund{RefundId: req.GetRefundId()},
	}, nil
}

// listPayoutConflictsSender returns a function that sends one request to
// acme.payments.v1.PayoutSyncService/ListPayoutConflicts.
func (s *refundApprovalHandlers) listPayoutConflictsSender(request *paymentsv1.ListPayoutConflictsRequest) func(context.Context) (*paymentsv1.ListPayoutConflictsResponse, error) {
	return func(ctx context.Context) (*paymentsv1.ListPayoutConflictsResponse, error) {
		return s.payoutSyncClient.ListPayoutConflicts(ctx, request)
	}
}
