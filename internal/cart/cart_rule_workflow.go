package cart

import (
	"context"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CartRuleWorkflowServer implements `acme.cart.v1.CartRuleWorkflowService`.
// Runs the long-lived workflow that takes a cart rule from start to finish.
type CartRuleWorkflowServer struct {
	cartv1.UnimplementedCartRuleWorkflowServiceServer
	store                              *Store
	log                                *slog.Logger
	giftCardApprovalClient             giftcardsv1.GiftCardApprovalServiceClient
	notificationPort                   notificationPort
	cartValidationPort                 cartValidationPort
	savedListApprovalClient            cartv1.SavedListApprovalServiceClient
	bus                                *eventbus.Bus
	clients                            *registry.Registry
	documentQueryClient                documentsv1.DocumentQueryServiceClient
	commentReplenishmentPlanApprovalFn func(context.Context, *inventoryv1.CommentReplenishmentPlanApprovalRequest, ...grpc.CallOption) (*inventoryv1.CommentReplenishmentPlanApprovalResponse, error)
	parcelApprovalClient               shippingv1.ParcelApprovalServiceClient
	invoiceBatchQueryClient            invoicingv1.InvoiceBatchQueryServiceClient
	conn                               *grpc.ClientConn
}

// NewCartRuleWorkflowServer wires a CartRuleWorkflowServer.
func NewCartRuleWorkflowServer(deps platform.Deps) *CartRuleWorkflowServer {
	replenishmentPlanApproval := inventoryv1.NewReplenishmentPlanApprovalServiceClient(deps.Conn)
	return &CartRuleWorkflowServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.cart.v1.CartRuleWorkflowService"),
		giftCardApprovalClient:             giftcardsv1.NewGiftCardApprovalServiceClient(deps.Conn),
		notificationPort:                   newNotificationPort(deps),
		cartValidationPort:                 newCartValidationPort(deps),
		savedListApprovalClient:            cartv1.NewSavedListApprovalServiceClient(deps.Conn),
		bus:                                deps.Bus,
		clients:                            deps.Clients,
		documentQueryClient:                documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		commentReplenishmentPlanApprovalFn: replenishmentPlanApproval.CommentReplenishmentPlanApproval,
		parcelApprovalClient:               shippingv1.NewParcelApprovalServiceClient(deps.Conn),
		invoiceBatchQueryClient:            invoicingv1.NewInvoiceBatchQueryServiceClient(deps.Conn),
		conn:                               deps.Conn,
	}
}

// StartCartRuleWorkflow starts a cart rule.
func (s *CartRuleWorkflowServer) StartCartRuleWorkflow(ctx context.Context, req *cartv1.StartCartRuleWorkflowRequest) (*cartv1.StartCartRuleWorkflowResponse, error) {
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "start cart rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	delegateGiftCardApprovalReply, err := s.giftCardApprovalClient.DelegateGiftCardApproval(ctx, &giftcardsv1.DelegateGiftCardApprovalRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{Etag: req.GetCartRule().GetAudit().GetEtag()},
		},
		Details: &giftcardsv1.DelegateGiftCardApprovalRequest_Details{
			Approver: &identityv1.UserRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardApprovalService/DelegateGiftCardApproval")
	}
	return &cartv1.StartCartRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: delegateGiftCardApprovalReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Parameters: req.GetCartRule().GetParameters(),
		},
	}, nil
}

// AdvanceCartRuleWorkflow advances a cart rule.
func (s *CartRuleWorkflowServer) AdvanceCartRuleWorkflow(ctx context.Context, req *cartv1.AdvanceCartRuleWorkflowRequest) (*cartv1.AdvanceCartRuleWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "advance cart rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	batchGetNotificationsRequest := &notificationsv1.BatchGetNotificationsRequest{Context: req.GetContext()}
	batchGetNotificationsReply, err := s.notificationPort.BatchGetNotifications(ctx, batchGetNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/BatchGetNotifications")
	}
	return &cartv1.AdvanceCartRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: batchGetNotificationsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Parameters: req.GetDetails().GetParameters(),
		},
	}, nil
}

// CancelCartRuleWorkflow cancels a cart rule.
func (s *CartRuleWorkflowServer) CancelCartRuleWorkflow(ctx context.Context, req *cartv1.CancelCartRuleWorkflowRequest) (*cartv1.CancelCartRuleWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "cancel cart rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetCartRule().GetOwner().GetEmail())
	normalizeCartRequest := &cartv1.NormalizeCartRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetCartRule().GetAudit().GetUpdatedBy(),
				CreatedBy: req.GetCartRule().GetAudit().GetCreatedBy(),
			},
			Lines: []*cartv1.CartLine{&cartv1.CartLine{
				Tax: &cartv1.CartLineTax{Amount: req.GetCartRule().GetDiscount().GetAmount()},
			}},
		},
		Details: &cartv1.NormalizeCartRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	}
	normalizeCartReply, err := s.cartValidationPort.NormalizeCart(ctx, normalizeCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartValidationService/NormalizeCart")
	}
	step := &cartRuleWorkflowServerCancelCartRuleWorkflowRecallSavedListStep{request: &cartv1.RecallSavedListRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: normalizeCartReply.GetCart().GetBillingAddress().GetPostalCode(),
			},
		},
	}}
	if err := s.runCancelCartRuleWorkflowRecallSavedList(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListApprovalService/RecallSavedList")
	}
	recallSavedListReply := step.reply
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "cart.cancel_cart_rule_workflow.retry_cart_workflow",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.notify.phone": req.GetDetails().GetNotify().GetPhone(),
			"context.tenant_id":    req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	cartRuleValidation, err := registry.Lookup[cartv1.CartRuleValidationServiceClient](s.clients, "acme.cart.v1.CartRuleValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	explainCartRuleDecisionRequest := &cartv1.ExplainCartRuleDecisionRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetCartRule().GetAudit().GetUpdatedAt(),
				CreatedAt: req.GetCartRule().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: normalizeCartReply.GetCart().GetCounterparty().GetDisplayName(),
			},
		},
	}
	explainCartRuleDecisionReply, err := cartRuleValidation.ExplainCartRuleDecision(ctx, explainCartRuleDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/ExplainCartRuleDecision")
	}
	return &cartv1.CancelCartRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: normalizeCartReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &cartv1.CancelCartRuleWorkflowResponse_Outcome{
			EffectiveAt: normalizeCartReply.GetOutcome().GetEffectiveAt(),
			NextSteps:   recallSavedListReply.GetOutcome().GetNextSteps(),
			Message:     explainCartRuleDecisionReply.GetResult().GetMessage(),
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Name:       req.GetCartRule().GetName(),
		},
	}, nil
}

// RetryCartRuleWorkflow retrys a cart rule.
func (s *CartRuleWorkflowServer) RetryCartRuleWorkflow(ctx context.Context, req *cartv1.RetryCartRuleWorkflowRequest) (*cartv1.RetryCartRuleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "retry cart rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	recallSavedListRequest := &cartv1.RecallSavedListRequest{
		Context: req.GetContext(),
		Details: &cartv1.RecallSavedListRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		SavedList: &cartv1.SavedList{
			Lines:        []*cartv1.SavedListLine{&cartv1.SavedListLine{Amount: req.GetCartRule().GetDiscount().GetAmount()}},
			Counterparty: &cartv1.CartRuleRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()},
		},
	}
	recallSavedListReply, err := grpcx.Call(ctx, s.savedListApprovalClient.RecallSavedList, recallSavedListRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListApprovalService/RecallSavedList")
	}
	suggestDocumentsRequest := &documentsv1.SuggestDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.SuggestDocumentsRequest_Filter{
			Region:       recallSavedListReply.GetSavedList().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetCartRule().GetAudit().GetCreatedAt(),
			OwnerId:      recallSavedListReply.GetSavedList().GetCounterparty().GetCartRuleId(),
			Labels:       recallSavedListReply.GetSavedList().GetMetadata(),
		},
	}
	suggestDocumentsReply, err := grpcx.Call(ctx, s.documentQueryClient.SuggestDocuments, suggestDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/SuggestDocuments")
	}
	return &cartv1.RetryCartRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: recallSavedListReply.GetOutcome().GetMessage(),
			Details: suggestDocumentsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				Etag: recallSavedListReply.GetSavedList().GetAudit().GetEtag(),
			},
			CartRuleId: req.GetCartRuleId(),
			Version:    req.GetCartRule().GetVersion(),
		},
	}, nil
}

// PauseCartRuleWorkflow pauses a cart rule.
func (s *CartRuleWorkflowServer) PauseCartRuleWorkflow(ctx context.Context, req *cartv1.PauseCartRuleWorkflowRequest) (*cartv1.PauseCartRuleWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "pause cart rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	commentReplenishmentPlanApprovalRequest := &inventoryv1.CommentReplenishmentPlanApprovalRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.CommentReplenishmentPlanApprovalRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetCartRule().GetOwner().GetUserId()},
		},
	}
	commentReplenishmentPlanApprovalReply, err := s.commentReplenishmentPlanApprovalFn(ctx, commentReplenishmentPlanApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanApprovalService/CommentReplenishmentPlanApproval")
	}
	commentSavedListApprovalRequest := &cartv1.CommentSavedListApprovalRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Audit: &commonv1.AuditInfo{
				Etag: commentReplenishmentPlanApprovalReply.GetReplenishmentPlan().GetAudit().GetEtag(),
			},
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: commentReplenishmentPlanApprovalReply.GetReplenishmentPlan().GetBillingAddress().GetPostalCode(),
			},
			Lines: []*cartv1.SavedListLine{&cartv1.SavedListLine{
				Tax: &cartv1.SavedListLineTax{Amount: req.GetCartRule().GetDiscount().GetAmount()},
			}},
			Settlement: &cartv1.SavedList_Card{Card: &cartv1.SavedListCardSettlement{
				HolderName: commentReplenishmentPlanApprovalReply.GetReplenishmentPlan().GetCard().GetHolderName(),
			}},
		},
	}
	var commentSavedListApprovalReply *cartv1.CommentSavedListApprovalResponse
	if err := pipeline.Run(ctx, "acme.cart.v1.SavedListApprovalService/CommentSavedListApproval", func(ctx context.Context) error {
		answer, err := s.savedListApprovalClient.CommentSavedListApproval(ctx, commentSavedListApprovalRequest)
		commentSavedListApprovalReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListApprovalService/CommentSavedListApproval")
	}
	return &cartv1.PauseCartRuleWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: commentReplenishmentPlanApprovalReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &cartv1.PauseCartRuleWorkflowResponse_Outcome{
			Status: commentSavedListApprovalReply.GetOutcome().GetStatus(),
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Name:       req.GetCartRule().GetName(),
		},
	}, nil
}

// SignalCartRuleWorkflow signals a cart rule.
func (s *CartRuleWorkflowServer) SignalCartRuleWorkflow(ctx context.Context, req *cartv1.SignalCartRuleWorkflowRequest) (*cartv1.SignalCartRuleWorkflowResponse, error) {
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "signal cart rule workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "updated_by", req.GetCartRule().GetAudit().GetUpdatedBy())
	cartRuleCache := cache.New[string, *cartv1.CartRule](1)
	cartRuleCache.Put(req.GetContext().GetTraceId(), req.GetCartRule())
	cachedCartRule, _ := cartRuleCache.Get(req.GetContext().GetTraceId())
	recallParcelRequest := &shippingv1.RecallParcelRequest{}
	recallParcelRequest.Context = req.GetContext()
	recallParcelRequest.Parcel = &shippingv1.Parcel{
		Audit: &commonv1.AuditInfo{
			UpdatedBy: tenancy.Value(ctx, "updated_by"),
			Etag:      cachedCartRule.GetAudit().GetEtag(),
		},
	}
	recallParcelReply, err := s.parcelApprovalClient.RecallParcel(ctx, recallParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelApprovalService/RecallParcel")
	}
	resolveInvoiceBatchesRequest := &invoicingv1.ResolveInvoiceBatchesRequest{}
	resolveInvoiceBatchesRequest.Context = req.GetContext()
	resolveInvoiceBatchesReply, err := s.invoiceBatchQueryClient.ResolveInvoiceBatches(ctx, resolveInvoiceBatchesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.invoicing.v1.InvoiceBatchQueryService/ResolveInvoiceBatches")
	}
	lintCartRulesRequest := &cartv1.LintCartRulesRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				UserId:      recallParcelReply.GetParcel().GetOwner().GetUserId(),
				DisplayName: recallParcelReply.GetParcel().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedBy: recallParcelReply.GetParcel().GetAudit().GetUpdatedBy(),
			},
		},
		Details: &cartv1.LintCartRulesRequest_Details{
			Contact: &commonv1.ContactInfo{Email: recallParcelReply.GetParcel().GetOwner().GetEmail()},
		},
	}
	lintCartRulesReply := new(cartv1.LintCartRulesResponse)
	if err := grpc.Invoke(ctx, "/acme.cart.v1.CartRuleValidationService/LintCartRules", lintCartRulesRequest, lintCartRulesReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/LintCartRules")
	}
	return &cartv1.SignalCartRuleWorkflowResponse{
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				UserId: recallParcelReply.GetParcel().GetOwner().GetUserId(),
				Email:  lintCartRulesReply.GetCartRule().GetOwner().GetEmail(),
			},
			CartRuleId: req.GetCartRuleId(),
			Name:       req.GetCartRule().GetName(),
		},
		Result: &commonv1.OperationResult{
			Details: resolveInvoiceBatchesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// cartRuleWorkflowServerCancelCartRuleWorkflowRecallSavedListStep carries the request of one step of
// CancelCartRuleWorkflow and, once it ran, its reply.
type cartRuleWorkflowServerCancelCartRuleWorkflowRecallSavedListStep struct {
	request *cartv1.RecallSavedListRequest
	reply   *cartv1.RecallSavedListResponse
}

func (s *CartRuleWorkflowServer) runCancelCartRuleWorkflowRecallSavedList(ctx context.Context, step *cartRuleWorkflowServerCancelCartRuleWorkflowRecallSavedListStep) error {
	reply, err := s.savedListApprovalClient.RecallSavedList(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
