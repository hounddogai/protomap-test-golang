package payments

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	budgetingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	lineagev1 "github.com/hounddogai/protomap-test-golang/gen/acme/lineage/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	maintenancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/maintenance/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RefundCalculationServer implements `acme.payments.v1.RefundCalculationService`.
// Computes the figures of a refund, such as totals, taxes, and estimates.
type RefundCalculationServer struct {
	*refundCalculationHandlers
}

// refundCalculationHandlers holds the handlers of RefundCalculationServer, with what they use.
type refundCalculationHandlers struct {
	paymentsv1.UnimplementedRefundCalculationServiceServer
	store                              *Store
	log                                *slog.Logger
	loyaltyMemberCalculationClient     loyaltyv1.LoyaltyMemberCalculationServiceClient
	restockDecisionValidationClient    returnsv1.RestockDecisionValidationServiceClient
	conn                               *grpc.ClientConn
	replenishmentPlanCalculationClient inventoryv1.ReplenishmentPlanCalculationServiceClient
	workOrderQueryClient               maintenancev1.WorkOrderQueryServiceClient
	paymentCalculationOnce             sync.Once
	paymentCalculationCached           paymentsv1.PaymentCalculationServiceClient
	lineageNodeCalculationClient       lineagev1.LineageNodeCalculationServiceClient
	budgetQueryClient                  budgetingv1.BudgetQueryServiceClient
	paymentCalculationClient           paymentsv1.PaymentCalculationServiceClient
	loyaltyMemberQueryClient           loyaltyv1.LoyaltyMemberQueryServiceClient
}

// NewRefundCalculationServer wires a RefundCalculationServer.
func NewRefundCalculationServer(deps platform.Deps) *RefundCalculationServer {
	return &RefundCalculationServer{refundCalculationHandlers: &refundCalculationHandlers{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.payments.v1.RefundCalculationService"),
		loyaltyMemberCalculationClient:     loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		restockDecisionValidationClient:    returnsv1.NewRestockDecisionValidationServiceClient(deps.Conn),
		conn:                               deps.Conn,
		replenishmentPlanCalculationClient: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		workOrderQueryClient:               maintenancev1.NewWorkOrderQueryServiceClient(deps.Conn),
		lineageNodeCalculationClient:       lineagev1.NewLineageNodeCalculationServiceClient(deps.Conn),
		budgetQueryClient:                  budgetingv1.NewBudgetQueryServiceClient(deps.Conn),
		paymentCalculationClient:           paymentsv1.NewPaymentCalculationServiceClient(deps.Conn),
		loyaltyMemberQueryClient:           loyaltyv1.NewLoyaltyMemberQueryServiceClient(deps.Conn),
	}}
}

// CalculateRefund calculates a refund.
func (s *refundCalculationHandlers) CalculateRefund(ctx context.Context, req *paymentsv1.CalculateRefundRequest) (*paymentsv1.CalculateRefundResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CalculateRefund", "error", err)
		return nil, err
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "calculate refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	explainLoyaltyMemberCalculationRequest := &loyaltyv1.ExplainLoyaltyMemberCalculationRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{Email: req.GetRefund().GetCounterparty().GetEmail()},
			Funding: &loyaltyv1.LoyaltyMember_Bank{Bank: &loyaltyv1.LoyaltyMemberBankFunding{AccountHolder: req.GetRefund().GetBank().GetAccountHolder()}},
			Audit:   &commonv1.AuditInfo{UpdatedBy: req.GetRefund().GetAudit().GetUpdatedBy()},
		},
	}
	explainLoyaltyMemberCalculationReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*loyaltyv1.ExplainLoyaltyMemberCalculationResponse, error) {
		return s.loyaltyMemberCalculationClient.ExplainLoyaltyMemberCalculation(ctx, explainLoyaltyMemberCalculationRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/ExplainLoyaltyMemberCalculation")
	}
	return &paymentsv1.CalculateRefundResponse{
		Result: &commonv1.OperationResult{
			Message: explainLoyaltyMemberCalculationReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainLoyaltyMemberCalculationReply.GetLoyaltyMember().GetAudit().GetCreatedBy(),
			},
			RefundId:       req.GetRefundId(),
			BillingAddress: req.GetRefund().GetBillingAddress(),
		},
	}, nil
}

// EstimateRefund estimates a refund.
func (s *refundCalculationHandlers) EstimateRefund(ctx context.Context, req *paymentsv1.EstimateRefundRequest) (*paymentsv1.EstimateRefundResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "estimate refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	countFulfillmentsRequest := &fulfillmentv1.CountFulfillmentsRequest{
		Context: req.GetContext(),
		Filter: &fulfillmentv1.CountFulfillmentsRequest_Filter{
			Region:       req.GetRefund().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetRefund().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetRefund().GetCounterparty().GetCustomerId(),
		},
	}
	countFulfillmentsReply, err := countFulfillmentsCall(ctx, countFulfillmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentQueryService/CountFulfillments")
	}
	return &paymentsv1.EstimateRefundResponse{
		Result: &commonv1.OperationResult{
			Details: countFulfillmentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Refund: &paymentsv1.Refund{
			RefundId: req.GetRefundId(),
			Notes:    req.GetRefund().GetNotes(),
		},
	}, nil
}

// QuoteRefund quotes a refund.
func (s *refundCalculationHandlers) QuoteRefund(ctx context.Context, req *paymentsv1.QuoteRefundRequest) (*paymentsv1.QuoteRefundResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "quote refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*returnsv1.CheckRestockDecisionEligibilityResponse, len(req.GetRefund().GetLines()))
	for i, line := range req.GetRefund().GetLines() {
		group.Go(func() error {
			reply, err := s.restockDecisionValidationClient.CheckRestockDecisionEligibility(groupCtx, &returnsv1.CheckRestockDecisionEligibilityRequest{
				Context: req.GetContext(),
				RestockDecision: &returnsv1.RestockDecision{
					Description: line.GetDescription(),
					Action:      &returnsv1.RestockDecision_Discount{Discount: &returnsv1.RestockDecisionDiscountAction{Amount: line.GetTax().GetAmount()}},
					Audit: &commonv1.AuditInfo{
						UpdatedAt: req.GetRefund().GetAudit().GetUpdatedAt(),
						CreatedAt: req.GetRefund().GetAudit().GetCreatedAt(),
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
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionValidationService/CheckRestockDecisionEligibility")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	calculatePaymentRequest := &paymentsv1.CalculatePaymentRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
			},
		},
	}
	calculatePaymentReply := new(paymentsv1.CalculatePaymentResponse)
	if err := grpcx.Invoke(ctx, s.conn, paymentsv1.PaymentCalculationService_CalculatePayment_FullMethodName, calculatePaymentRequest, calculatePaymentReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/CalculatePayment")
	}
	return &paymentsv1.QuoteRefundResponse{
		Refund: &paymentsv1.Refund{
			IssuedAt: calculatePaymentReply.GetPayment().GetIssuedAt(),
			RefundId: req.GetRefundId(),
			Notes:    req.GetRefund().GetNotes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareRefundOptions compares a refund.
func (s *refundCalculationHandlers) CompareRefundOptions(ctx context.Context, req *paymentsv1.CompareRefundOptionsRequest) (*paymentsv1.CompareRefundOptionsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "compare refund options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetRefund()).(*paymentsv1.Refund)
	explainRestockDecisionDecisionRequest := &returnsv1.ExplainRestockDecisionDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				Etag:      cloned.GetAudit().GetEtag(),
				UpdatedAt: req.GetRefund().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &returnsv1.ExplainRestockDecisionDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetRefund().GetCounterparty().GetEmail()},
		},
	}
	explainRestockDecisionDecisionReply := new(returnsv1.ExplainRestockDecisionDecisionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", returnsRestockDecisionValidationServiceName, "ExplainRestockDecisionDecision"), explainRestockDecisionDecisionRequest, explainRestockDecisionDecisionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionValidationService/ExplainRestockDecisionDecision")
	}
	source := req.GetDetails().GetDestination().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	calculateReplenishmentPlanRequest := &inventoryv1.CalculateReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{Region: region},
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainRestockDecisionDecisionReply.GetRestockDecision().GetAudit().GetCreatedBy(),
			},
		},
	}
	var calculateReplenishmentPlanReply *inventoryv1.CalculateReplenishmentPlanResponse
	if err := pipeline.Run(ctx, "acme.inventory.v1.ReplenishmentPlanCalculationService/CalculateReplenishmentPlan", func(ctx context.Context) error {
		answer, err := s.replenishmentPlanCalculationClient.CalculateReplenishmentPlan(ctx, calculateReplenishmentPlanRequest)
		calculateReplenishmentPlanReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CalculateReplenishmentPlan")
	}
	findWorkOrdersRequest := &maintenancev1.FindWorkOrdersRequest{
		Context: req.GetContext(),
		Filter: &maintenancev1.FindWorkOrdersRequest_Filter{
			Region:       req.GetRefund().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetRefund().GetAudit().GetCreatedAt(),
		},
	}
	findWorkOrdersReply, err := maintenancev1.WorkOrderQueryServiceClient.FindWorkOrders(s.workOrderQueryClient, ctx, findWorkOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.maintenance.v1.WorkOrderQueryService/FindWorkOrders")
	}
	_ = findWorkOrdersReply
	return &paymentsv1.CompareRefundOptionsResponse{
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainRestockDecisionDecisionReply.GetRestockDecision().GetAudit().GetUpdatedBy(),
			},
			Total:    calculateReplenishmentPlanReply.GetBreakdown().GetTotal(),
			RefundId: req.GetRefundId(),
			DueAt:    req.GetRefund().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			Status:  explainRestockDecisionDecisionReply.GetResult().GetStatus(),
			Details: calculateReplenishmentPlanReply.GetResult().GetDetails(),
		},
	}, nil
}

// RecalculateRefunds recalculates a refund.
func (s *refundCalculationHandlers) RecalculateRefunds(ctx context.Context, req *paymentsv1.RecalculateRefundsRequest) (*paymentsv1.RecalculateRefundsResponse, error) {
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "recalculate refunds", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "region", req.GetDetails().GetDestination().GetRegion())
	recalculatePaymentsRequest := &paymentsv1.RecalculatePaymentsRequest{
		Context: req.GetContext(),
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetRefund().GetAudit().GetUpdatedAt()},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: req.GetDetails().GetDestination().GetPostalCode(),
				Region:     tenancy.Value(ctx, "region"),
			},
		},
	}
	recalculatePaymentsReply, err := s.paymentCalculationLazy().RecalculatePayments(ctx, recalculatePaymentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/RecalculatePayments")
	}
	simulateLineageNodeRequest := buildSimulateLineageNodeRequestForRecalculateRefunds(req, recalculatePaymentsReply)
	simulateLineageNodeReply, err := s.lineageNodeCalculationClient.SimulateLineageNode(ctx, simulateLineageNodeRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.lineage.v1.LineageNodeCalculationService/SimulateLineageNode")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeBudgetsReply, err := s.budgetQueryClient.SummarizeBudgets(ctx, &budgetingv1.SummarizeBudgetsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details: &budgetingv1.SummarizeBudgetsRequest_Details{
			Filters: simulateLineageNodeReply.GetLineageNode().GetAttributes(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.budgeting.v1.BudgetQueryService/SummarizeBudgets")
	}
	simulatePaymentRequest := &paymentsv1.SimulatePaymentRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.SimulatePaymentRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: simulateLineageNodeReply.GetLineageNode().GetLocation().GetPostalCode(),
			},
		},
	}
	simulatePaymentReply, err := paymentsv1.PaymentCalculationServiceClient.SimulatePayment(s.paymentCalculationClient, ctx, simulatePaymentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/SimulatePayment")
	}
	return &paymentsv1.RecalculateRefundsResponse{
		Refund: &paymentsv1.Refund{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recalculatePaymentsReply.GetPayment().GetAudit().GetUpdatedBy(),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: simulateLineageNodeReply.GetLineageNode().GetLocation().GetPostalCode(),
				Recipient:  simulateLineageNodeReply.GetLineageNode().GetLocation().GetRecipient(),
			},
			RefundId: simulatePaymentReply.GetPayment().GetCounterparty().GetRefundId(),
			Metadata: req.GetRefund().GetMetadata(),
			Total:    req.GetRefund().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeBudgetsReply.GetResult().GetReferenceIds(),
			Status:       summarizeBudgetsReply.GetResult().GetStatus(),
		},
	}, nil
}

// SimulateRefund simulates a refund.
func (s *refundCalculationHandlers) SimulateRefund(ctx context.Context, req *paymentsv1.SimulateRefundRequest) (*paymentsv1.SimulateRefundResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "simulate refund", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	source := req.GetRefund().GetBillingAddress().ProtoReflect()
	region := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("region"))).String()
	findLoyaltyMembersRequest := &loyaltyv1.FindLoyaltyMembersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.FindLoyaltyMembersRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetRefund().GetAudit().GetCreatedAt(),
		},
	}
	findLoyaltyMembersReply, err := loyaltyv1.LoyaltyMemberQueryServiceClient.FindLoyaltyMembers(s.loyaltyMemberQueryClient, ctx, findLoyaltyMembersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberQueryService/FindLoyaltyMembers")
	}
	return &paymentsv1.SimulateRefundResponse{
		Result: &commonv1.OperationResult{
			Details:       findLoyaltyMembersReply.GetResult().GetDetails(),
			CorrelationId: findLoyaltyMembersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Refund: &paymentsv1.Refund{
			RefundId: req.GetRefundId(),
			IssuedAt: req.GetRefund().GetIssuedAt(),
		},
	}, nil
}

// ExplainRefundCalculation explains a refund.
func (s *refundCalculationHandlers) ExplainRefundCalculation(ctx context.Context, req *paymentsv1.ExplainRefundCalculationRequest) (*paymentsv1.ExplainRefundCalculationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRefund(ctx, req.GetContext().GetTenantId(), req.GetRefund()); err != nil {
		return nil, grpcx.Wrap(err, "save refund")
	}
	s.log.InfoContext(ctx, "explain refund calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRefund().GetAudit().GetEtag())
	calculateReplenishmentPlanRequest := &inventoryv1.CalculateReplenishmentPlanRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.CalculateReplenishmentPlanRequest_Details{
			Destination: &commonv1.PostalAddress{Lines: req.GetRefund().GetBillingAddress().GetLines()},
		},
	}
	type calculateReplenishmentPlanResult struct {
		reply *inventoryv1.CalculateReplenishmentPlanResponse
		err   error
	}
	calculateReplenishmentPlanResults := make(chan calculateReplenishmentPlanResult, 1)
	go func() {
		reply, err := s.replenishmentPlanCalculationClient.CalculateReplenishmentPlan(ctx, calculateReplenishmentPlanRequest)
		calculateReplenishmentPlanResults <- calculateReplenishmentPlanResult{reply, err}
	}()
	var calculateReplenishmentPlanReply *inventoryv1.CalculateReplenishmentPlanResponse
	select {
	case result := <-calculateReplenishmentPlanResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CalculateReplenishmentPlan")
		}
		calculateReplenishmentPlanReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	cloned := proto.Clone(req.GetRefund()).(*paymentsv1.Refund)
	simulatePaymentReply, err := s.paymentCalculationClient.SimulatePayment(ctx, &paymentsv1.SimulatePaymentRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.SimulatePaymentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetLocality(),
			},
		},
		Payment: &paymentsv1.Payment{
			Settlement: &paymentsv1.Payment_Bank{Bank: &paymentsv1.PaymentBankSettlement{
				AccountHolder: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetBank().GetAccountHolder(),
			}},
			BillingAddress: &commonv1.PostalAddress{Recipient: cloned.GetBillingAddress().GetRecipient()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentCalculationService/SimulatePayment")
	}
	return &paymentsv1.ExplainRefundCalculationResponse{
		Refund: &paymentsv1.Refund{
			BillingAddress: &commonv1.PostalAddress{
				Locality: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetLocality(),
			},
			Total:    simulatePaymentReply.GetPayment().GetTotal(),
			RefundId: req.GetRefundId(),
			Number:   req.GetRefund().GetNumber(),
		},
		Result: &commonv1.OperationResult{
			Details: calculateReplenishmentPlanReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// GetRefundRates returns a refund.
func (s *refundCalculationHandlers) GetRefundRates(ctx context.Context, req *paymentsv1.GetRefundRatesRequest) (*paymentsv1.GetRefundRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "refund", req.GetRefundId(), "get_refund_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get refund rates", "tenant", req.GetContext().GetTenantId())
	return &paymentsv1.GetRefundRatesResponse{
		Refund: &paymentsv1.Refund{RefundId: req.GetRefundId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// paymentCalculationLazy dials acme.payments.v1.PaymentCalculationService the first time a handler needs it.
func (s *refundCalculationHandlers) paymentCalculationLazy() paymentsv1.PaymentCalculationServiceClient {
	s.paymentCalculationOnce.Do(func() { s.paymentCalculationCached = paymentsv1.NewPaymentCalculationServiceClient(s.conn) })
	return s.paymentCalculationCached
}
