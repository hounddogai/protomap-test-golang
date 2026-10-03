package giftcards

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// GiftCardTransactionApprovalServer implements `acme.giftcards.v1.GiftCardTransactionApprovalService`.
// Routes gift card transactions through approval, with escalation and delegation.
type GiftCardTransactionApprovalServer struct {
	*giftCardTransactionApprovalHandlers
}

// giftCardTransactionApprovalHandlers holds the handlers of GiftCardTransactionApprovalServer, with what they use.
type giftCardTransactionApprovalHandlers struct {
	giftcardsv1.UnimplementedGiftCardTransactionApprovalServiceServer
	store                               *Store
	log                                 *slog.Logger
	orderClient                         ordersv1.OrderServiceClient
	meteredOrderNoteValidation          meteredOrderNoteValidation
	clients                             *registry.Registry
	refundQueryClient                   paymentsv1.RefundQueryServiceClient
	giftCardTransactionValidationClient giftcardsv1.GiftCardTransactionValidationServiceClient
	giftCardBatchCalculationClient      giftcardsv1.GiftCardBatchCalculationServiceClient
	giftCardWorkflowClient              giftcardsv1.GiftCardWorkflowServiceClient
	giftCardBatchApprovalClient         giftcardsv1.GiftCardBatchApprovalServiceClient
	createStockItemFn                   func(context.Context, *inventoryv1.CreateStockItemRequest, ...grpc.CallOption) (*inventoryv1.CreateStockItemResponse, error)
	giftCardApprovalClient              giftcardsv1.GiftCardApprovalServiceClient
	userValidationClient                identityv1.UserValidationServiceClient
	ledgerAccountValidationClient       ledgerv1.LedgerAccountValidationServiceClient
}

// NewGiftCardTransactionApprovalServer wires a GiftCardTransactionApprovalServer.
func NewGiftCardTransactionApprovalServer(deps platform.Deps) *GiftCardTransactionApprovalServer {
	stockItem := inventoryv1.NewStockItemServiceClient(deps.Conn)
	return &GiftCardTransactionApprovalServer{giftCardTransactionApprovalHandlers: &giftCardTransactionApprovalHandlers{
		store:                               NewStore(deps.DB),
		log:                                 deps.Logger.With("service", "acme.giftcards.v1.GiftCardTransactionApprovalService"),
		orderClient:                         ordersv1.NewOrderServiceClient(deps.Conn),
		meteredOrderNoteValidation:          meteredOrderNoteValidation{OrderNoteValidationServiceClient: ordersv1.NewOrderNoteValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                             deps.Clients,
		refundQueryClient:                   paymentsv1.NewRefundQueryServiceClient(deps.Conn),
		giftCardTransactionValidationClient: giftcardsv1.NewGiftCardTransactionValidationServiceClient(deps.Conn),
		giftCardBatchCalculationClient:      giftcardsv1.NewGiftCardBatchCalculationServiceClient(deps.Conn),
		giftCardWorkflowClient:              giftcardsv1.NewGiftCardWorkflowServiceClient(deps.Conn),
		giftCardBatchApprovalClient:         giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		createStockItemFn:                   stockItem.CreateStockItem,
		giftCardApprovalClient:              giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		userValidationClient:                identityv1.NewUserValidationServiceClient(deps.Conn),
		ledgerAccountValidationClient:       ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
	}}
}

// SubmitGiftCardTransaction submits a gift card transaction.
func (s *giftCardTransactionApprovalHandlers) SubmitGiftCardTransaction(ctx context.Context, req *giftcardsv1.SubmitGiftCardTransactionRequest) (*giftcardsv1.SubmitGiftCardTransactionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitGiftCardTransaction", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "submit gift card transaction", "tenant", req.GetContext().GetTenantId())
	for measurement := range iterx.Each(req.GetGiftCardTransaction().GetMeasurements(), func(candidate *giftcardsv1.GiftCardTransactionMeasurement) bool { return candidate != nil }) {
		if _, err := s.orderClient.ListOrders(ctx, &ordersv1.ListOrdersRequest{
			Context: req.GetContext(),
			Filter:  &ordersv1.ListOrdersRequest_Filter{CreatedAfter: measurement.GetObservedAt()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.orders.v1.OrderService/ListOrders")
		}
	}
	giftCardTransactionCache := cache.New[string, *giftcardsv1.GiftCardTransaction](1)
	giftCardTransactionCache.Put(req.GetContext().GetTraceId(), req.GetGiftCardTransaction())
	cachedGiftCardTransaction, _ := giftCardTransactionCache.Get(req.GetContext().GetTraceId())
	source := req.GetGiftCardTransaction().GetSubject().ProtoReflect()
	number := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("number"))).String()
	packed, err := anypb.New(req.GetGiftCardTransaction())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateOrderNoteRequest := &ordersv1.ValidateOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				DisplayName: cachedGiftCardTransaction.GetSubject().GetDisplayName(),
				Number:      number,
			},
			OccurredAt: req.GetGiftCardTransaction().GetOccurredAt(),
			Kind:       req.GetGiftCardTransaction().GetKind(),
			Detail:     packed,
		},
		OrderNoteId: envelope.GetDisplayName(),
	}
	validateOrderNoteReply, err := s.meteredOrderNoteValidation.ValidateOrderNote(ctx, validateOrderNoteRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteValidationService/ValidateOrderNote")
	}
	cloned := proto.Clone(req.GetGiftCardTransaction()).(*giftcardsv1.GiftCardTransaction)
	normalizeLedgerAccountRequest := &ledgerv1.NormalizeLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: cloned.GetSubject().GetDisplayName(),
				Number:      req.GetGiftCardTransaction().GetSubject().GetNumber(),
			},
		},
	}
	normalizeLedgerAccountReply, err := normalizeLedgerAccountCall(ctx, normalizeLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/NormalizeLedgerAccount")
	}
	giftCardBatchApproval, err := registry.Lookup[giftcardsv1.GiftCardBatchApprovalServiceClient](s.clients, "acme.giftcards.v1.GiftCardBatchApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var envelope2 giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	approveGiftCardBatchRequest := &giftcardsv1.ApproveGiftCardBatchRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Settlement: &giftcardsv1.GiftCardBatch_Bank{Bank: &giftcardsv1.GiftCardBatchBankSettlement{
				AccountHolder: normalizeLedgerAccountReply.GetLedgerAccount().GetBank().GetAccountHolder(),
			}},
		},
		GiftCardBatchId: envelope2.GetDisplayName(),
	}
	approveGiftCardBatchReply, err := giftCardBatchApproval.ApproveGiftCardBatch(ctx, approveGiftCardBatchRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/ApproveGiftCardBatch")
	}
	return &giftcardsv1.SubmitGiftCardTransactionResponse{
		Result: &commonv1.OperationResult{
			Status:        validateOrderNoteReply.GetOutcome().GetStatus(),
			CorrelationId: validateOrderNoteReply.GetResult().GetCorrelationId(),
			Message:       normalizeLedgerAccountReply.GetResult().GetMessage(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				Number: normalizeLedgerAccountReply.GetLedgerAccount().GetHolder().GetNumber(),
			},
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Kind:                  req.GetGiftCardTransaction().GetKind(),
		},
		Outcome: &giftcardsv1.SubmitGiftCardTransactionResponse_Outcome{
			EffectiveAt: approveGiftCardBatchReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// ApproveGiftCardTransaction approves a gift card transaction.
func (s *giftCardTransactionApprovalHandlers) ApproveGiftCardTransaction(ctx context.Context, req *giftcardsv1.ApproveGiftCardTransactionRequest) (*giftcardsv1.ApproveGiftCardTransactionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "approve gift card transaction", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeRefundsRequest := &paymentsv1.SummarizeRefundsRequest{}
	summarizeRefundsRequest.Context = req.GetContext()
	summarizeRefundsRequest.Annotations = annotations
	summarizeRefundsReply, err := s.refundQueryClient.SummarizeRefunds(ctx, summarizeRefundsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundQueryService/SummarizeRefunds")
	}
	normalizeGiftCardTransactionRequest := buildNormalizeGiftCardTransactionRequestForApproveGiftCardTransaction(req)
	normalizeGiftCardTransactionReply, err := s.giftCardTransactionValidationClient.NormalizeGiftCardTransaction(ctx, normalizeGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionValidationService/NormalizeGiftCardTransaction")
	}
	return &giftcardsv1.ApproveGiftCardTransactionResponse{
		Outcome: &giftcardsv1.ApproveGiftCardTransactionResponse_Outcome{Message: summarizeRefundsReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Details: summarizeRefundsReply.GetResult().GetDetails(),
			Status:  normalizeGiftCardTransactionReply.GetOutcome().GetStatus(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Sequence:              normalizeGiftCardTransactionReply.GetGiftCardTransaction().GetSequence(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Attributes:            req.GetGiftCardTransaction().GetAttributes(),
		},
	}, nil
}

// EscalateGiftCardTransaction escalates a gift card transaction.
func (s *giftCardTransactionApprovalHandlers) EscalateGiftCardTransaction(ctx context.Context, req *giftcardsv1.EscalateGiftCardTransactionRequest) (*giftcardsv1.EscalateGiftCardTransactionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "escalate gift card transaction", "tenant", req.GetContext().GetTenantId())
	jobs := make(chan *giftcardsv1.GiftCardTransactionMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.giftCardBatchCalculationClient.ExplainGiftCardBatchCalculation(workCtx, &giftcardsv1.ExplainGiftCardBatchCalculationRequest{
					Context: req.GetContext(),
					GiftCardBatch: &giftcardsv1.GiftCardBatch{
						Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetGiftCardTransaction().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchCalculationService/ExplainGiftCardBatchCalculation")
	}
	return &giftcardsv1.EscalateGiftCardTransactionResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Subject:               req.GetGiftCardTransaction().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateGiftCardTransactionApproval delegates a gift card transaction.
func (s *giftCardTransactionApprovalHandlers) DelegateGiftCardTransactionApproval(ctx context.Context, req *giftcardsv1.DelegateGiftCardTransactionApprovalRequest) (*giftcardsv1.DelegateGiftCardTransactionApprovalResponse, error) {
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "delegate gift card transaction approval", "tenant", req.GetContext().GetTenantId())
	pauseGiftCardWorkflowRequest := buildPauseGiftCardWorkflowRequestForDelegateGiftCardTransactionApproval(req)
	pauseGiftCardWorkflowReply, err := s.giftCardWorkflowClient.PauseGiftCardWorkflow(ctx, pauseGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/PauseGiftCardWorkflow")
	}
	source := req.GetDetails().GetApprover().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	delegateGiftCardBatchApprovalReply, err := s.giftCardBatchApprovalClient.DelegateGiftCardBatchApproval(ctx, &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Settlement: &giftcardsv1.GiftCardBatch_Bank{Bank: &giftcardsv1.GiftCardBatchBankSettlement{
				Iban: pauseGiftCardWorkflowReply.GetGiftCard().GetBank().GetIban(),
			}},
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
		},
		Details: &giftcardsv1.DelegateGiftCardBatchApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				UserId:      userId,
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	createStockItemRequest := &inventoryv1.CreateStockItemRequest{
		Context: req.GetContext(),
		StockItem: &inventoryv1.StockItem{
			Locator: &inventoryv1.StockItem_Address{Address: &commonv1.PostalAddress{
				Lines:    delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetBillingAddress().GetLines(),
				Locality: delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetBillingAddress().GetLocality(),
			}},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pauseGiftCardWorkflowReply.GetGiftCard().GetAudit().GetUpdatedBy(),
				CreatedAt: delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
			},
		},
	}
	createStockItemReply, err := s.createStockItemFn(ctx, createStockItemRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/CreateStockItem")
	}
	return &giftcardsv1.DelegateGiftCardTransactionApprovalResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: pauseGiftCardWorkflowReply.GetResult().GetReferenceIds(),
			Status:       delegateGiftCardBatchApprovalReply.GetResult().GetStatus(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			GiftCardTransactionId: pauseGiftCardWorkflowReply.GetGiftCard().GetHolder().GetGiftCardTransactionId(),
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: createStockItemReply.GetStockItem().GetOwner().GetDisplayName(),
			},
			Source:   req.GetGiftCardTransaction().GetSource(),
			Sequence: req.GetGiftCardTransaction().GetSequence(),
		},
	}, nil
}

// GetGiftCardTransactionApprovalStatus returns a gift card transaction.
func (s *giftCardTransactionApprovalHandlers) GetGiftCardTransactionApprovalStatus(ctx context.Context, req *giftcardsv1.GetGiftCardTransactionApprovalStatusRequest) (*giftcardsv1.GetGiftCardTransactionApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "get_gift_card_transaction_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get gift card transaction approval status", "tenant", req.GetContext().GetTenantId())
	advanceGiftCardWorkflowRequest := newAdvanceGiftCardWorkflowRequest(withAdvanceGiftCardWorkflowContext(req.GetContext()), withAdvanceGiftCardWorkflowGiftCard(&giftcardsv1.GiftCard{
		Holder: &giftcardsv1.GiftCardTransactionRef{GiftCardTransactionId: req.GetGiftCardTransactionId()},
	}))
	advanceGiftCardWorkflowReply, err := s.giftCardWorkflowClient.AdvanceGiftCardWorkflow(ctx, advanceGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/AdvanceGiftCardWorkflow")
	}
	recallGiftCardRequest := &giftcardsv1.RecallGiftCardRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Bank{Bank: &giftcardsv1.GiftCardBankFunding{
				AccountHolder: advanceGiftCardWorkflowReply.GetGiftCard().GetBank().GetAccountHolder(),
			}},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: advanceGiftCardWorkflowReply.GetGiftCard().GetHolder().GetDisplayName(),
			},
		},
		Details: &giftcardsv1.RecallGiftCardRequest_Details{
			Approver: &identityv1.UserRef{
				Email: advanceGiftCardWorkflowReply.GetGiftCard().GetContact().GetEmail(),
			},
		},
	}
	recallGiftCardReply, err := giftcardsv1.GiftCardApprovalServiceClient.RecallGiftCard(s.giftCardApprovalClient, ctx, recallGiftCardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/RecallGiftCard")
	}
	return &giftcardsv1.GetGiftCardTransactionApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  advanceGiftCardWorkflowReply.GetResult().GetReferenceIds(),
			Message:       advanceGiftCardWorkflowReply.GetResult().GetMessage(),
			CorrelationId: recallGiftCardReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{GiftCardTransactionId: req.GetGiftCardTransactionId()},
	}, nil
}

// CommentGiftCardTransactionApproval comments a gift card transaction.
func (s *giftCardTransactionApprovalHandlers) CommentGiftCardTransactionApproval(ctx context.Context, req *giftcardsv1.CommentGiftCardTransactionApprovalRequest) (*giftcardsv1.CommentGiftCardTransactionApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "comment gift card transaction approval", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetDetails().GetApprover(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	normalizeUserRequest := &identityv1.NormalizeUserRequest{}
	normalizeUserRequest.Context = req.GetContext()
	normalizeUserRequest.User = &identityv1.User{
		RelationshipManager: &identityv1.UserRef{
			Email:       holder.Email,
			UserId:      req.GetDetails().GetApprover().GetUserId(),
			DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
		},
		IdentityDocument: &identityv1.User_NationalId{NationalId: &identityv1.UserNationalId{
			Number: req.GetGiftCardTransaction().GetSubject().GetNumber(),
		}},
	}
	normalizeUserRequest.UserId = envelope.GetDisplayName()
	normalizeUserReply, err := s.userValidationClient.NormalizeUser(ctx, normalizeUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserValidationService/NormalizeUser")
	}
	var envelope2 giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scoreLedgerAccountRequest := &ledgerv1.ScoreLedgerAccountRequest{
		Context: req.GetContext(),
		Details: &ledgerv1.ScoreLedgerAccountRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: normalizeUserReply.GetUser().GetPrimaryAddress().GetRegion(),
				Lines:  normalizeUserReply.GetUser().GetPrimaryAddress().GetLines(),
			},
		},
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedBy: normalizeUserReply.GetUser().GetAudit().GetCreatedBy(),
			},
		},
		LedgerAccountId: envelope2.GetDisplayName(),
	}
	scoreLedgerAccountReply, err := grpcx.Call(ctx, s.ledgerAccountValidationClient.ScoreLedgerAccount, scoreLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount")
	}
	return &giftcardsv1.CommentGiftCardTransactionApprovalResponse{
		Outcome: &giftcardsv1.CommentGiftCardTransactionApprovalResponse_Outcome{
			ReferenceId: normalizeUserReply.GetOutcome().GetReferenceId(),
			Message:     scoreLedgerAccountReply.GetResult().GetMessage(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Attributes:            req.GetGiftCardTransaction().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
