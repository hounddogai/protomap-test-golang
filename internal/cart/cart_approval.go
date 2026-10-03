package cart

import (
	"context"
	"log/slog"
	"sync"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	contentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/content/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// CartApprovalServer implements `acme.cart.v1.CartApprovalService`.
// Routes carts through approval, with escalation and delegation.
type CartApprovalServer struct {
	cartv1.UnimplementedCartApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	cartClient                         cartv1.CartServiceClient
	savedListQueryClient               cartv1.SavedListQueryServiceClient
	meteredCartCalculation             meteredCartCalculation
	cartCalculationClient              cartv1.CartCalculationServiceClient
	cartQueryClient                    cartv1.CartQueryServiceClient
	savedListValidationClient          cartv1.SavedListValidationServiceClient
	searchIndexExportClient            searchv1.SearchIndexExportServiceClient
	orderNoteSyncClient                ordersv1.OrderNoteSyncServiceClient
	bus                                *eventbus.Bus
	paymentIntentWorkflowClient        checkoutv1.PaymentIntentWorkflowServiceClient
	sellerPayoutScheduleWorkflowClient sellersv1.SellerPayoutScheduleWorkflowServiceClient
	articleClient                      contentv1.ArticleServiceClient
	cartRuleCalculationClient          cartv1.CartRuleCalculationServiceClient
}

// NewCartApprovalServer wires a CartApprovalServer.
func NewCartApprovalServer(deps platform.Deps) *CartApprovalServer {
	return &CartApprovalServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.cart.v1.CartApprovalService"),
		cartClient:                         cartv1.NewCartServiceClient(deps.Conn),
		savedListQueryClient:               cartv1.NewSavedListQueryServiceClient(deps.Conn),
		meteredCartCalculation:             meteredCartCalculation{CartCalculationServiceClient: cartv1.NewCartCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		cartCalculationClient:              cartv1.NewCartCalculationServiceClient(deps.Conn),
		cartQueryClient:                    cartv1.NewCartQueryServiceClient(deps.Conn),
		savedListValidationClient:          cartv1.NewSavedListValidationServiceClient(deps.Conn),
		searchIndexExportClient:            searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		orderNoteSyncClient:                ordersv1.NewOrderNoteSyncServiceClient(deps.Conn),
		bus:                                deps.Bus,
		paymentIntentWorkflowClient:        checkoutv1.NewPaymentIntentWorkflowServiceClient(deps.Conn),
		sellerPayoutScheduleWorkflowClient: sellersv1.NewSellerPayoutScheduleWorkflowServiceClient(deps.Conn),
		articleClient:                      contentv1.NewArticleServiceClient(deps.Conn),
		cartRuleCalculationClient:          cartv1.NewCartRuleCalculationServiceClient(deps.Conn),
	}
}

// SubmitCart submits a cart.
func (s *CartApprovalServer) SubmitCart(ctx context.Context, req *cartv1.SubmitCartRequest) (*cartv1.SubmitCartResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "submit cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	cloneCartRequest := newCloneCartRequest(withCloneCartContext(req.GetContext()), withCloneCartCart(&cartv1.Cart{
		Counterparty: &cartv1.SavedListRef{
			SavedListId: req.GetCart().GetCounterparty().GetSavedListId(),
		},
	}))
	cloneCartReply, err := s.cartClient.CloneCart(ctx, cloneCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartService/CloneCart")
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeSavedListsReply, err := s.savedListQueryClient.SummarizeSavedLists(ctx, &cartv1.SummarizeSavedListsRequest{
		Context:     req.GetContext(),
		SavedListId: req.GetCart().GetCounterparty().GetSavedListId(),
		Reason:      tenancy.Value(ctx, "reason"),
		Annotations: annotations,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListQueryService/SummarizeSavedLists")
	}
	return &cartv1.SubmitCartResponse{
		Cart: &cartv1.Cart{
			BillingAddress: &commonv1.PostalAddress{
				Region: cloneCartReply.GetCart().GetBillingAddress().GetRegion(),
			},
			CartId: req.GetCartId(),
			DueAt:  req.GetCart().GetDueAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: summarizeSavedListsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ApproveCart approves a cart.
func (s *CartApprovalServer) ApproveCart(ctx context.Context, req *cartv1.ApproveCartRequest) (*cartv1.ApproveCartResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "approve cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	var last4 string
	switch choice := req.GetCart().GetSettlement().(type) {
	case *cartv1.Cart_Card:
		last4 = choice.Card.GetLast4()
	case *cartv1.Cart_Bank:
		last4 = choice.Bank.GetIban()
	case *cartv1.Cart_VoucherCode:
		last4 = choice.VoucherCode
	}
	recalculateCartsRequest := &cartv1.RecalculateCartsRequest{
		Context: req.GetContext(),
		Details: &cartv1.RecalculateCartsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetCart().GetBillingAddress().GetRecipient(),
				Locality:  req.GetCart().GetBillingAddress().GetLocality(),
			},
		},
		Cart: &cartv1.Cart{
			Settlement: &cartv1.Cart_Card{Card: &cartv1.CartCardSettlement{Last4: last4}},
		},
	}
	recalculateCartsReply, err := s.meteredCartCalculation.RecalculateCarts(ctx, recalculateCartsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/RecalculateCarts")
	}
	jobs := make(chan *cartv1.CartAttachment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attachment := range jobs {
				if _, err := s.cartCalculationClient.ExplainCartCalculation(workCtx, &cartv1.ExplainCartCalculationRequest{
					Context: req.GetContext(),
					Cart: &cartv1.Cart{
						Attachments: []*cartv1.CartAttachment{&cartv1.CartAttachment{
							FileName:    attachment.GetFileName(),
							SizeBytes:   attachment.GetSizeBytes(),
							ContentType: attachment.GetContentType(),
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
	for _, queued := range req.GetCart().GetAttachments() {
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/ExplainCartCalculation")
	}
	resolveCartsRequest := &cartv1.ResolveCartsRequest{Context: req.GetContext()}
	resolveCartsReply, err := grpcx.Call(ctx, s.cartQueryClient.ResolveCarts, resolveCartsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartQueryService/ResolveCarts")
	}
	for attachment2 := range iterx.Each(req.GetCart().GetAttachments(), func(candidate *cartv1.CartAttachment) bool { return candidate != nil }) {
		if _, err := s.savedListValidationClient.LintSavedLists(ctx, &cartv1.LintSavedListsRequest{
			Context: req.GetContext(),
			SavedList: &cartv1.SavedList{
				Attachments: []*cartv1.SavedListAttachment{&cartv1.SavedListAttachment{
					FileName: attachment2.GetFileName(),
					Uri:      attachment2.GetUri(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/LintSavedLists")
		}
	}
	return &cartv1.ApproveCartResponse{
		Result: &commonv1.OperationResult{
			Message: recalculateCartsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &cartv1.ApproveCartResponse_Outcome{Message: resolveCartsReply.GetResult().GetMessage()},
		Cart: &cartv1.Cart{
			CartId:         req.GetCartId(),
			BillingAddress: req.GetCart().GetBillingAddress(),
		},
	}, nil
}

// RejectCart rejects a cart.
func (s *CartApprovalServer) RejectCart(ctx context.Context, req *cartv1.RejectCartRequest) (*cartv1.RejectCartResponse, error) {
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "reject cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	metadata2 := make(map[string]string, len(req.GetCart().GetMetadata()))
	for key, value := range req.GetCart().GetMetadata() {
		metadata2[key] = value
	}
	scheduleSearchIndexExportRequest := &searchv1.ScheduleSearchIndexExportRequest{}
	scheduleSearchIndexExportRequest.Context = req.GetContext()
	scheduleSearchIndexExportRequest.Reason = req.GetReason()
	scheduleSearchIndexExportRequest.Annotations = req.GetAnnotations()
	scheduleSearchIndexExportRequest.Details = &searchv1.ScheduleSearchIndexExportRequest_Details{Filters: metadata2}
	scheduleSearchIndexExportReply, err := s.searchIndexExportClient.ScheduleSearchIndexExport(ctx, scheduleSearchIndexExportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ScheduleSearchIndexExport")
	}
	step := &cartApprovalServerRejectCartGetCartStep{request: &cartv1.GetCartRequest{
		Context: req.GetContext(),
		CartId:  req.GetCartId(),
	}}
	if err := s.runRejectCartGetCart(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartService/GetCart")
	}
	getCartReply := step.reply
	packed, err := anypb.New(req.GetCart())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	pushOrderNoteRequest := &ordersv1.PushOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject: &ordersv1.OrderRef{
				Number:      req.GetCart().GetCounterparty().GetNumber(),
				DisplayName: req.GetCart().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	send := s.pushOrderNoteSender(pushOrderNoteRequest)
	pushOrderNoteReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteSyncService/PushOrderNote")
	}
	return &cartv1.RejectCartResponse{
		Result: &commonv1.OperationResult{
			Message: scheduleSearchIndexExportReply.GetResult().GetMessage(),
			Details: pushOrderNoteReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &cartv1.RejectCartResponse_Outcome{Message: getCartReply.GetResult().GetMessage()},
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{
				DisplayName: pushOrderNoteReply.GetOrderNote().GetSubject().GetDisplayName(),
			},
			CartId:         req.GetCartId(),
			BillingAddress: req.GetCart().GetBillingAddress(),
		},
	}, nil
}

// EscalateCart escalates a cart.
func (s *CartApprovalServer) EscalateCart(ctx context.Context, req *cartv1.EscalateCartRequest) (*cartv1.EscalateCartResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateCart", "error", err)
		return nil, err
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "escalate cart", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	batchGetCartsRequest := &cartv1.BatchGetCartsRequest{}
	batchGetCartsRequest.Context = req.GetContext()
	batchGetCartsReply, err := s.cartClient.BatchGetCarts(ctx, batchGetCartsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartService/BatchGetCarts")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "cart.escalate_cart.summarize_saved_lists",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"saved_list_id":     req.GetCart().GetCounterparty().GetSavedListId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &cartv1.EscalateCartResponse{
		Result: &commonv1.OperationResult{Status: batchGetCartsReply.GetResult().GetStatus()},
		Cart: &cartv1.Cart{
			CartId: req.GetCartId(),
			DueAt:  req.GetCart().GetDueAt(),
		},
	}, nil
}

// ListPendingCarts lists carts a page at a time.
func (s *CartApprovalServer) ListPendingCarts(ctx context.Context, req *cartv1.ListPendingCartsRequest) (*cartv1.ListPendingCartsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingCarts", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending carts", "tenant", req.GetContext().GetTenantId())
	return &cartv1.ListPendingCartsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateCartApproval delegates a cart.
func (s *CartApprovalServer) DelegateCartApproval(ctx context.Context, req *cartv1.DelegateCartApprovalRequest) (*cartv1.DelegateCartApprovalResponse, error) {
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "delegate cart approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*cartv1.DelegateCartApprovalRequest_Details)
	cancelPaymentIntentWorkflowRequest := &checkoutv1.CancelPaymentIntentWorkflowRequest{
		Context: req.GetContext(),
		PaymentIntent: &checkoutv1.PaymentIntent{
			Assignee: &identityv1.UserRef{UserId: cloned.GetApprover().GetUserId()},
		},
	}
	type cancelPaymentIntentWorkflowResult struct {
		reply *checkoutv1.CancelPaymentIntentWorkflowResponse
		err   error
	}
	cancelPaymentIntentWorkflowResults := make(chan cancelPaymentIntentWorkflowResult, 1)
	go func() {
		reply, err := s.paymentIntentWorkflowClient.CancelPaymentIntentWorkflow(ctx, cancelPaymentIntentWorkflowRequest)
		cancelPaymentIntentWorkflowResults <- cancelPaymentIntentWorkflowResult{reply, err}
	}()
	var cancelPaymentIntentWorkflowReply *checkoutv1.CancelPaymentIntentWorkflowResponse
	select {
	case result := <-cancelPaymentIntentWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.checkout.v1.PaymentIntentWorkflowService/CancelPaymentIntentWorkflow")
		}
		cancelPaymentIntentWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	advanceSellerPayoutScheduleWorkflowReply, err := s.sellerPayoutScheduleWorkflowClient.AdvanceSellerPayoutScheduleWorkflow(ctx, &sellersv1.AdvanceSellerPayoutScheduleWorkflowRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleWorkflowService/AdvanceSellerPayoutScheduleWorkflow")
	}
	cloneCartRequest := buildCloneCartRequestForDelegateCartApproval(req, advanceSellerPayoutScheduleWorkflowReply)
	cloneCartReply, err := s.cartClient.CloneCart(ctx, cloneCartRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartService/CloneCart")
	}
	ctx = tenancy.With(ctx, "etag", req.GetCart().GetAudit().GetEtag())
	recallParcelRequest := &shippingv1.RecallParcelRequest{
		Context: req.GetContext(),
		Parcel: &shippingv1.Parcel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cloneCartReply.GetCart().GetAudit().GetUpdatedAt(),
				Etag:      tenancy.Value(ctx, "etag"),
			},
		},
		Details: &shippingv1.RecallParcelRequest_Details{
			Approver: &identityv1.UserRef{
				Email: advanceSellerPayoutScheduleWorkflowReply.GetSellerPayoutSchedule().GetOrganizer().GetEmail(),
			},
		},
	}
	recallParcelReply, err := recallParcelCall(ctx, recallParcelRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelApprovalService/RecallParcel")
	}
	return &cartv1.DelegateCartApprovalResponse{
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cancelPaymentIntentWorkflowReply.GetPaymentIntent().GetAudit().GetUpdatedAt(),
				CreatedAt: recallParcelReply.GetParcel().GetAudit().GetCreatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Lines:  cloneCartReply.GetCart().GetBillingAddress().GetLines(),
				Region: recallParcelReply.GetParcel().GetAddress().GetRegion(),
			},
			CartId:   req.GetCartId(),
			IssuedAt: req.GetCart().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			Status:  advanceSellerPayoutScheduleWorkflowReply.GetResult().GetStatus(),
			Details: advanceSellerPayoutScheduleWorkflowReply.GetResult().GetDetails(),
		},
		Outcome: &cartv1.DelegateCartApprovalResponse_Outcome{Message: cloneCartReply.GetResult().GetMessage()},
	}, nil
}

// GetCartApprovalStatus returns a cart.
func (s *CartApprovalServer) GetCartApprovalStatus(ctx context.Context, req *cartv1.GetCartApprovalStatusRequest) (*cartv1.GetCartApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "cart", req.GetCartId(), "get_cart_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get cart approval status", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	createArticleRequest := &contentv1.CreateArticleRequest{
		Context: req.GetContext(),
		Article: &contentv1.Article{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
			Lines: []*contentv1.ArticleLine{&contentv1.ArticleLine{Attributes: baggage}},
		},
	}
	type createArticleResult struct {
		reply *contentv1.CreateArticleResponse
		err   error
	}
	createArticleResults := make(chan createArticleResult, 1)
	go func() {
		reply, err := s.articleClient.CreateArticle(ctx, createArticleRequest)
		createArticleResults <- createArticleResult{reply, err}
	}()
	var createArticleReply *contentv1.CreateArticleResponse
	select {
	case result := <-createArticleResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.content.v1.ArticleService/CreateArticle")
		}
		createArticleReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &cartv1.GetCartApprovalStatusResponse{
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{Number: createArticleReply.GetArticle().GetNumber()},
			CartId:       req.GetCartId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentCartApproval comments a cart.
func (s *CartApprovalServer) CommentCartApproval(ctx context.Context, req *cartv1.CommentCartApprovalRequest) (*cartv1.CommentCartApprovalResponse, error) {
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "comment cart approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	step := &cartApprovalServerCommentCartApprovalCalculateCartRuleStep{request: &cartv1.CalculateCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{Etag: req.GetCart().GetAudit().GetEtag()},
		},
	}}
	if err := s.runCommentCartApprovalCalculateCartRule(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/CalculateCartRule")
	}
	calculateCartRuleReply := step.reply
	jobs := make(chan *cartv1.CartLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.cartClient.CreateCart(workCtx, &cartv1.CreateCartRequest{
					Context: req.GetContext(),
					Cart: &cartv1.Cart{
						Lines: []*cartv1.CartLine{&cartv1.CartLine{
							Tax: &cartv1.CartLineTax{
								Exemptions: line.GetTax().GetExemptions(),
								Amount:     line.GetAmount(),
							},
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
	for _, queued := range req.GetCart().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartService/CreateCart")
	}
	return &cartv1.CommentCartApprovalResponse{
		Result: &commonv1.OperationResult{
			Status:       calculateCartRuleReply.GetResult().GetStatus(),
			ReferenceIds: calculateCartRuleReply.GetResult().GetReferenceIds(),
		},
		Cart: &cartv1.Cart{
			CartId:       req.GetCartId(),
			Counterparty: req.GetCart().GetCounterparty(),
		},
	}, nil
}

// cartApprovalServerRejectCartGetCartStep carries the request of one step of RejectCart and, once it ran, its reply.
type cartApprovalServerRejectCartGetCartStep struct {
	request *cartv1.GetCartRequest
	reply   *cartv1.GetCartResponse
}

func (s *CartApprovalServer) runRejectCartGetCart(ctx context.Context, step *cartApprovalServerRejectCartGetCartStep) error {
	reply, err := s.cartClient.GetCart(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// pushOrderNoteSender returns a function that sends one request to acme.orders.v1.OrderNoteSyncService/PushOrderNote.
func (s *CartApprovalServer) pushOrderNoteSender(request *ordersv1.PushOrderNoteRequest) func(context.Context) (*ordersv1.PushOrderNoteResponse, error) {
	return func(ctx context.Context) (*ordersv1.PushOrderNoteResponse, error) {
		return s.orderNoteSyncClient.PushOrderNote(ctx, request)
	}
}

// cartApprovalServerCommentCartApprovalCalculateCartRuleStep carries the request of one step of CommentCartApproval
// and, once it ran, its reply.
type cartApprovalServerCommentCartApprovalCalculateCartRuleStep struct {
	request *cartv1.CalculateCartRuleRequest
	reply   *cartv1.CalculateCartRuleResponse
}

func (s *CartApprovalServer) runCommentCartApprovalCalculateCartRule(ctx context.Context, step *cartApprovalServerCommentCartApprovalCalculateCartRuleStep) error {
	reply, err := s.cartRuleCalculationClient.CalculateCartRule(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
