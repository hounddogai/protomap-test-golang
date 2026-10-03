package payments

import (
	"context"
	"errors"
	"io"
	"log/slog"

	budgetingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// RefundQueryServer implements `acme.payments.v1.RefundQueryService`.
// Answers read-only questions about refunds, for screens and reports.
type RefundQueryServer struct {
	paymentsv1.UnimplementedRefundQueryServiceServer
	store                            *Store
	log                              *slog.Logger
	paymentValidationClient          paymentsv1.PaymentValidationServiceClient
	paymentQueryClient               paymentsv1.PaymentQueryServiceClient
	conn                             *grpc.ClientConn
	loyaltyMemberQueryClient         loyaltyv1.LoyaltyMemberQueryServiceClient
	loyaltyMemberQueryClientFallback loyaltyv1.LoyaltyMemberQueryServiceClient
	sellerValidationClient           sellersv1.SellerValidationServiceClient
}

// NewRefundQueryServer wires a RefundQueryServer.
func NewRefundQueryServer(deps platform.Deps) *RefundQueryServer {
	return &RefundQueryServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.payments.v1.RefundQueryService"),
		paymentValidationClient:          paymentsv1.NewPaymentValidationServiceClient(deps.Conn),
		paymentQueryClient:               paymentsv1.NewPaymentQueryServiceClient(deps.Conn),
		conn:                             deps.Conn,
		loyaltyMemberQueryClient:         loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn),
		loyaltyMemberQueryClientFallback: loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn),
		sellerValidationClient:           sellersv1.NewSellerValidationServiceClient(deps.Conn),
	}
}

// LookupRefund returns a refund.
func (s *RefundQueryServer) LookupRefund(ctx context.Context, req *paymentsv1.LookupRefundRequest) (*paymentsv1.LookupRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "lookup_refund"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup refund", "tenant", req.GetContext().GetTenantId())
	normalizePaymentRequest := &paymentsv1.NormalizePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Counterparty: &paymentsv1.RefundRef{RefundId: req.GetRefundId()},
		},
		Details: &paymentsv1.NormalizePaymentRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	normalizePaymentReply, err := paymentsv1.PaymentValidationServiceClient.NormalizePayment(s.paymentValidationClient, ctx, normalizePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentValidationService/NormalizePayment")
	}
	exportPaymentsStream, err := s.paymentQueryClient.ExportPayments(ctx, &paymentsv1.ExportPaymentsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.ExportPaymentsRequest_Filter{
			Region:       normalizePaymentReply.GetPayment().GetBillingAddress().GetRegion(),
			CreatedAfter: normalizePaymentReply.GetPayment().GetAudit().GetCreatedAt(),
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
	return &paymentsv1.LookupRefundResponse{
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				Lines: normalizePaymentReply.GetPayment().GetBillingAddress().GetLines(),
			},
			RefundId: req.GetRefundId(),
		},
		Result: &commonv1.OperationResult{
			Message: normalizePaymentReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// BrowseRefunds lists refunds a page at a time.
func (s *RefundQueryServer) BrowseRefunds(ctx context.Context, req *paymentsv1.BrowseRefundsRequest) (*paymentsv1.BrowseRefundsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "browse refunds", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.BrowseRefundsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindRefunds lists refunds a page at a time.
func (s *RefundQueryServer) FindRefunds(ctx context.Context, req *paymentsv1.FindRefundsRequest) (*paymentsv1.FindRefundsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "find refunds", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.FindRefundsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// ResolveRefunds returns several refunds at once.
func (s *RefundQueryServer) ResolveRefunds(ctx context.Context, req *paymentsv1.ResolveRefundsRequest) (*paymentsv1.ResolveRefundsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve refunds", "tenant", req.GetContext().GetTenantId())
	suggestBudgetsRequest := &budgetingv1.SuggestBudgetsRequest{
		Context: req.GetContext(),
		Filter:  &budgetingv1.SuggestBudgetsRequest_Filter{CreatedAfter: req.GetContext().GetDeadline()},
	}
	suggestBudgetsReply := new(budgetingv1.SuggestBudgetsResponse)
	if err := s.conn.Invoke(ctx, budgetingBudgetQueryServiceSuggestBudgetsMethod, suggestBudgetsRequest, suggestBudgetsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.budgeting.v1.BudgetQueryService/SuggestBudgets")
	}
	return &paymentsv1.ResolveRefundsResponse{
		Result: &commonv1.OperationResult{
			Status:  suggestBudgetsReply.GetResult().GetStatus(),
			Message: suggestBudgetsReply.GetResult().GetMessage(),
		},
	}, nil
}

// ExportRefunds streams every matching refund.
func (s *RefundQueryServer) ExportRefunds(req *paymentsv1.ExportRefundsRequest, stream grpc.ServerStreamingServer[paymentsv1.ExportRefundsResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export refunds", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "query", req.GetFilter().GetQuery())
	exportPaymentsStream, err := s.paymentQueryClient.ExportPayments(ctx, &paymentsv1.ExportPaymentsRequest{
		Context: req.GetContext(),
		Filter: &paymentsv1.ExportPaymentsRequest_Filter{
			Query:        tenancy.Value(ctx, "query"),
			Region:       req.GetFilter().GetRegion(),
			CreatedAfter: req.GetFilter().GetCreatedAfter(),
			OwnerId:      req.GetFilter().GetOwnerId(),
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.payments.v1.PaymentQueryService/ExportPayments")
	}
	for {
		received, err := exportPaymentsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return grpcx.Wrap(err, "acme.payments.v1.PaymentQueryService/ExportPayments")
		}
		_ = received
	}
	if err := stream.Send(&paymentsv1.ExportRefundsResponse{
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeRefunds summarizes a refund.
func (s *RefundQueryServer) SummarizeRefunds(ctx context.Context, req *paymentsv1.SummarizeRefundsRequest) (*paymentsv1.SummarizeRefundsResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "summarize_refunds"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize refunds", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *loyaltyv1.ResolveLoyaltyMembersRequest, ...grpc.CallOption) (*loyaltyv1.ResolveLoyaltyMembersResponse, error){
		"primary":  s.loyaltyMemberQueryClient.ResolveLoyaltyMembers,
		"fallback": s.loyaltyMemberQueryClientFallback.ResolveLoyaltyMembers,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	resolveLoyaltyMembersRequest := &loyaltyv1.ResolveLoyaltyMembersRequest{Context: req.GetContext()}
	resolveLoyaltyMembersReply, err := route(ctx, resolveLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/ResolveLoyaltyMembers")
	}
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	compareDisputeOptionsRequest := &paymentsv1.CompareDisputeOptionsRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
		Dispute: &paymentsv1.Dispute{
			Requester: &commonv1.ContactInfo{Email: (&decoded).GetActor().GetEmail()},
		},
	}
	compareDisputeOptionsReply := new(paymentsv1.CompareDisputeOptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, paymentsv1.DisputeCalculationService_CompareDisputeOptions_FullMethodName, compareDisputeOptionsRequest, compareDisputeOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeCalculationService/CompareDisputeOptions")
	}
	return &paymentsv1.SummarizeRefundsResponse{
		Result: &commonv1.OperationResult{
			Details: resolveLoyaltyMembersReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestRefunds lists refunds a page at a time.
func (s *RefundQueryServer) SuggestRefunds(ctx context.Context, req *paymentsv1.SuggestRefundsRequest) (*paymentsv1.SuggestRefundsResponse, error) {
	s.log.InfoContext(ctx, "suggest refunds", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	summarizePaymentsRequest := &paymentsv1.SummarizePaymentsRequest{
		Context:     req.GetContext(),
		Annotations: labels,
		Details:     &paymentsv1.SummarizePaymentsRequest_Details{Filters: req.GetContext().GetBaggage()},
	}
	send := s.summarizePaymentsSender(summarizePaymentsRequest)
	summarizePaymentsReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentQueryService/SummarizePayments")
	}
	return &paymentsv1.SuggestRefundsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizePaymentsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// PeekRefund returns a refund.
func (s *RefundQueryServer) PeekRefund(ctx context.Context, req *paymentsv1.PeekRefundRequest) (*paymentsv1.PeekRefundResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "peek_refund"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek refund", "tenant", req.GetContext().GetTenantId())
	checkSellerEligibilityRequest := buildCheckSellerEligibilityRequestForPeekRefund(req)
	checkSellerEligibilityReply, err := s.sellerValidationClient.CheckSellerEligibility(ctx, checkSellerEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerValidationService/CheckSellerEligibility")
	}
	normalizeRewardRequest := &loyaltyv1.NormalizeRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkSellerEligibilityReply.GetSeller().GetAudit().GetCreatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: checkSellerEligibilityReply.GetSeller().GetRelationshipManager().GetDisplayName(),
			},
		},
		Details: &loyaltyv1.NormalizeRewardRequest_Details{
			Contact: &commonv1.ContactInfo{
				Phone: checkSellerEligibilityReply.GetSeller().GetContact().GetPhone(),
				Email: checkSellerEligibilityReply.GetSeller().GetRelationshipManager().GetEmail(),
			},
		},
	}
	normalizeRewardReply := new(loyaltyv1.NormalizeRewardResponse)
	if err := grpcx.Invoke(ctx, s.conn, loyaltyv1.RewardValidationService_NormalizeReward_FullMethodName, normalizeRewardRequest, normalizeRewardReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/NormalizeReward")
	}
	return &paymentsv1.PeekRefundResponse{
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				CreatedBy: checkSellerEligibilityReply.GetSeller().GetAudit().GetCreatedBy(),
			},
			RefundId: req.GetRefundId(),
		},
		Result: &commonv1.OperationResult{Status: normalizeRewardReply.GetResult().GetStatus()},
	}, nil
}

// summarizePaymentsSender returns a function that sends one request to
// acme.payments.v1.PaymentQueryService/SummarizePayments.
func (s *RefundQueryServer) summarizePaymentsSender(request *paymentsv1.SummarizePaymentsRequest) func(context.Context) (*paymentsv1.SummarizePaymentsResponse, error) {
	return func(ctx context.Context) (*paymentsv1.SummarizePaymentsResponse, error) {
		return s.paymentQueryClient.SummarizePayments(ctx, request)
	}
}
