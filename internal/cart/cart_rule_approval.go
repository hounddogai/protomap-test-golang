package cart

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CartRuleApprovalServer implements `acme.cart.v1.CartRuleApprovalService`.
// Routes cart rules through approval, with escalation and delegation.
type CartRuleApprovalServer struct {
	cartv1.UnimplementedCartRuleApprovalServiceServer
	store                     *Store
	log                       *slog.Logger
	sessionQueryClient        identityv1.SessionQueryServiceClient
	disputeImportPort         disputeImportPort
	disputeSyncClient         paymentsv1.DisputeSyncServiceClient
	cartRuleCalculationClient cartv1.CartRuleCalculationServiceClient
	savedListApprovalClient   cartv1.SavedListApprovalServiceClient
	auditEventQueryClient     auditv1.AuditEventQueryServiceClient
	cartWorkflowClient        cartv1.CartWorkflowServiceClient
	cartRuleValidationClient  cartv1.CartRuleValidationServiceClient
	cartClient                cartv1.CartServiceClient
	renewCartRuleFn           func(context.Context, *cartv1.RenewCartRuleRequest, ...grpc.CallOption) (*cartv1.RenewCartRuleResponse, error)
	clients                   *registry.Registry
	synonymExportClient       searchv1.SynonymExportServiceClient
}

// NewCartRuleApprovalServer wires a CartRuleApprovalServer.
func NewCartRuleApprovalServer(deps platform.Deps) *CartRuleApprovalServer {
	cartRuleLifecycle := cartv1.NewCartRuleLifecycleServiceClient(deps.Conn)
	return &CartRuleApprovalServer{
		store:                     NewStore(deps.DB),
		log:                       deps.Logger.With("service", "acme.cart.v1.CartRuleApprovalService"),
		sessionQueryClient:        identityv1.NewSessionQueryServiceClient(deps.Conn),
		disputeImportPort:         newDisputeImportPort(deps),
		disputeSyncClient:         paymentsv1.NewDisputeSyncServiceClient(deps.Conn),
		cartRuleCalculationClient: cartv1.NewCartRuleCalculationServiceClient(deps.Conn),
		savedListApprovalClient:   cartv1.NewSavedListApprovalServiceClient(deps.Conn),
		auditEventQueryClient:     auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		cartWorkflowClient:        cartv1.NewCartWorkflowServiceClient(deps.Conn),
		cartRuleValidationClient:  cartv1.NewCartRuleValidationServiceClient(deps.Conn),
		cartClient:                cartv1.NewCartServiceClient(deps.Conn),
		renewCartRuleFn:           cartRuleLifecycle.RenewCartRule,
		clients:                   deps.Clients,
		synonymExportClient:       searchv1.NewSynonymExportServiceClient(deps.Conn),
	}
}

// SubmitCartRule submits a cart rule.
func (s *CartRuleApprovalServer) SubmitCartRule(ctx context.Context, req *cartv1.SubmitCartRuleRequest) (*cartv1.SubmitCartRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitCartRule", "error", err)
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "submit cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	browseSessionsRequest := buildBrowseSessionsRequestForSubmitCartRule(req)
	browseSessionsReply, err := s.sessionQueryClient.BrowseSessions(ctx, browseSessionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/BrowseSessions")
	}
	encoded, err := protojson.Marshal(req.GetCartRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded cartv1.CartRule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cartRuleCache := cache.New[string, *cartv1.CartRule](1)
	cartRuleCache.Put(req.GetContext().GetTraceId(), req.GetCartRule())
	cachedCartRule, _ := cartRuleCache.Get(req.GetContext().GetTraceId())
	mapDisputeImportColumnsRequest := &paymentsv1.MapDisputeImportColumnsRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Requester: &commonv1.ContactInfo{Email: req.GetCartRule().GetOwner().GetEmail()},
			Assignee:  &identityv1.UserRef{DisplayName: (&decoded).GetOwner().GetDisplayName()},
			Audit:     &commonv1.AuditInfo{UpdatedAt: cachedCartRule.GetAudit().GetUpdatedAt()},
		},
	}
	mapDisputeImportColumnsReply, err := s.disputeImportPort.MapDisputeImportColumns(ctx, mapDisputeImportColumnsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeImportService/MapDisputeImportColumns")
	}
	return &cartv1.SubmitCartRuleResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: browseSessionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &cartv1.SubmitCartRuleResponse_Outcome{
			Status:      browseSessionsReply.GetResult().GetStatus(),
			NextSteps:   mapDisputeImportColumnsReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		CartRule: &cartv1.CartRule{CartRuleId: req.GetCartRuleId()},
	}, nil
}

// ApproveCartRule approves a cart rule.
func (s *CartRuleApprovalServer) ApproveCartRule(ctx context.Context, req *cartv1.ApproveCartRuleRequest) (*cartv1.ApproveCartRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "approve cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	source := req.GetCartRule().GetOwner().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	explainFeatureFlagDecisionRequest := &featureflagsv1.ExplainFeatureFlagDecisionRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Audit:  &commonv1.AuditInfo{UpdatedAt: req.GetCartRule().GetAudit().GetUpdatedAt()},
			Action: &featureflagsv1.FeatureFlag_Route{Route: &featureflagsv1.FeatureFlagRouteAction{Target: req.GetCartRule().GetRoute().GetTarget()}},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				Email:       email,
			},
		},
	}
	explainFeatureFlagDecisionReply, err := explainFeatureFlagDecisionCall(ctx, explainFeatureFlagDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ExplainFeatureFlagDecision")
	}
	return &cartv1.ApproveCartRuleResponse{
		Outcome: &cartv1.ApproveCartRuleResponse_Outcome{
			EffectiveAt: explainFeatureFlagDecisionReply.GetOutcome().GetEffectiveAt(),
		},
		CartRule: &cartv1.CartRule{
			CartRuleId: req.GetCartRuleId(),
			Priority:   req.GetCartRule().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectCartRule rejects a cart rule.
func (s *CartRuleApprovalServer) RejectCartRule(ctx context.Context, req *cartv1.RejectCartRuleRequest) (*cartv1.RejectCartRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectCartRule", "error", err)
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "reject cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*cartv1.RejectCartRuleRequest_Details)
	snapshot, err := protojson.Marshal(req.GetCartRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	syncDisputesReply, err := s.disputeSyncClient.SyncDisputes(ctx, &paymentsv1.SyncDisputesRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Origin:    &paymentsv1.Dispute_Customer{Customer: &paymentsv1.PaymentRef{DisplayName: cloned.GetApprover().GetDisplayName()}},
			Requester: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
			Assignee:  &identityv1.UserRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()},
			Audit:     &commonv1.AuditInfo{CreatedAt: req.GetCartRule().GetAudit().GetCreatedAt()},
		},
		Details: &paymentsv1.SyncDisputesRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/SyncDisputes")
	}
	return &cartv1.RejectCartRuleResponse{
		CartRule: &cartv1.CartRule{
			Priority:   syncDisputesReply.GetDispute().GetPriority(),
			CartRuleId: req.GetCartRuleId(),
			Parameters: req.GetCartRule().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateCartRule escalates a cart rule.
func (s *CartRuleApprovalServer) EscalateCartRule(ctx context.Context, req *cartv1.EscalateCartRuleRequest) (*cartv1.EscalateCartRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "escalate cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCartRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded cartv1.CartRule
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	pullDisputesReply, err := s.disputeSyncClient.PullDisputes(ctx, &paymentsv1.PullDisputesRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetCartRule().GetAudit().GetUpdatedBy(),
				CreatedBy: (&decoded).GetAudit().GetCreatedBy(),
			},
			Origin: &paymentsv1.Dispute_Customer{Customer: &paymentsv1.PaymentRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/PullDisputes")
	}
	jobs := make(chan *cartv1.CartRuleCondition)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for condition := range jobs {
				if _, err := s.cartRuleCalculationClient.CompareCartRuleOptions(workCtx, &cartv1.CompareCartRuleOptionsRequest{
					Context: req.GetContext(),
					CartRule: &cartv1.CartRule{
						Conditions: []*cartv1.CartRuleCondition{&cartv1.CartRuleCondition{
							Value:  condition.GetValue(),
							Negate: condition.GetNegate(),
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
	for _, queued := range req.GetCartRule().GetConditions() {
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/CompareCartRuleOptions")
	}
	commentSavedListApprovalReply, err := s.savedListApprovalClient.CommentSavedListApproval(ctx, &cartv1.CommentSavedListApprovalRequest{
		Context: req.GetContext(),
		SavedList: &cartv1.SavedList{
			Lines: []*cartv1.SavedListLine{&cartv1.SavedListLine{
				Tax: &cartv1.SavedListLineTax{Amount: req.GetCartRule().GetDiscount().GetAmount()},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListApprovalService/CommentSavedListApproval")
	}
	return &cartv1.EscalateCartRuleResponse{
		Outcome: &cartv1.EscalateCartRuleResponse_Outcome{NextSteps: pullDisputesReply.GetOutcome().GetNextSteps()},
		Result: &commonv1.OperationResult{
			Status:       pullDisputesReply.GetResult().GetStatus(),
			ReferenceIds: commentSavedListApprovalReply.GetResult().GetReferenceIds(),
		},
		CartRule: &cartv1.CartRule{
			CartRuleId:  req.GetCartRuleId(),
			Description: req.GetCartRule().GetDescription(),
		},
	}, nil
}

// ListPendingCartRules lists cart rules a page at a time.
func (s *CartRuleApprovalServer) ListPendingCartRules(ctx context.Context, req *cartv1.ListPendingCartRulesRequest) (*cartv1.ListPendingCartRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingCartRules", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending cart rules", "tenant", req.GetContext().GetTenantId())
	resolveAuditEventsRequest := &auditv1.ResolveAuditEventsRequest{Context: req.GetContext()}
	type resolveAuditEventsResult struct {
		reply *auditv1.ResolveAuditEventsResponse
		err   error
	}
	resolveAuditEventsResults := make(chan resolveAuditEventsResult, 1)
	go func() {
		reply, err := s.auditEventQueryClient.ResolveAuditEvents(ctx, resolveAuditEventsRequest)
		resolveAuditEventsResults <- resolveAuditEventsResult{reply, err}
	}()
	var resolveAuditEventsReply *auditv1.ResolveAuditEventsResponse
	select {
	case result := <-resolveAuditEventsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.audit.v1.AuditEventQueryService/ResolveAuditEvents")
		}
		resolveAuditEventsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &cartv1.ListPendingCartRulesResponse{
		Result: &commonv1.OperationResult{
			Details: resolveAuditEventsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PageInfo: &commonv1.PageInfo{
			TotalSize: resolveAuditEventsReply.GetPageInfo().GetTotalSize(),
		},
	}, nil
}

// DelegateCartRuleApproval delegates a cart rule.
func (s *CartRuleApprovalServer) DelegateCartRuleApproval(ctx context.Context, req *cartv1.DelegateCartRuleApprovalRequest) (*cartv1.DelegateCartRuleApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "delegate cart rule approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	signalCartWorkflowRequest := &cartv1.SignalCartWorkflowRequest{
		Context: req.GetContext(),
		Cart: &cartv1.Cart{
			Counterparty: &cartv1.SavedListRef{DisplayName: req.GetCartRule().GetOwner().GetDisplayName()},
		},
		Details: &cartv1.SignalCartWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetCartRule().GetOwner().GetEmail()},
		},
	}
	type signalCartWorkflowResult struct {
		reply *cartv1.SignalCartWorkflowResponse
		err   error
	}
	signalCartWorkflowResults := make(chan signalCartWorkflowResult, 1)
	go func() {
		reply, err := s.cartWorkflowClient.SignalCartWorkflow(ctx, signalCartWorkflowRequest)
		signalCartWorkflowResults <- signalCartWorkflowResult{reply, err}
	}()
	var signalCartWorkflowReply *cartv1.SignalCartWorkflowResponse
	select {
	case result := <-signalCartWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.cart.v1.CartWorkflowService/SignalCartWorkflow")
		}
		signalCartWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	normalizeCartRuleReply, err := s.cartRuleValidationClient.NormalizeCartRule(ctx, &cartv1.NormalizeCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Audit:  &commonv1.AuditInfo{UpdatedBy: req.GetCartRule().GetAudit().GetUpdatedBy()},
			Action: &cartv1.CartRule_Route{Route: &cartv1.CartRuleRouteAction{Weight: req.GetCartRule().GetRoute().GetWeight()}},
		},
		Details: &cartv1.NormalizeCartRuleRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/NormalizeCartRule")
	}
	return &cartv1.DelegateCartRuleApprovalResponse{
		Result: &commonv1.OperationResult{Status: signalCartWorkflowReply.GetResult().GetStatus()},
		CartRule: &cartv1.CartRule{
			Owner:      normalizeCartRuleReply.GetCartRule().GetOwner(),
			CartRuleId: req.GetCartRuleId(),
			Name:       req.GetCartRule().GetName(),
		},
	}, nil
}

// RecallCartRule recalls a cart rule.
func (s *CartRuleApprovalServer) RecallCartRule(ctx context.Context, req *cartv1.RecallCartRuleRequest) (*cartv1.RecallCartRuleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "recall cart rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*cartv1.RecallCartRuleRequest_Details)
	watchCartsStream, err := s.cartClient.WatchCarts(ctx, &cartv1.WatchCartsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: cloned.GetApprover().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartService/WatchCarts")
	}
	for {
		received, err := watchCartsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartService/WatchCarts")
		}
		_ = received
	}
	return &cartv1.RecallCartRuleResponse{
		CartRule: &cartv1.CartRule{
			CartRuleId:  req.GetCartRuleId(),
			Description: req.GetCartRule().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentCartRuleApproval comments a cart rule.
func (s *CartRuleApprovalServer) CommentCartRuleApproval(ctx context.Context, req *cartv1.CommentCartRuleApprovalRequest) (*cartv1.CommentCartRuleApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentCartRuleApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveCartRule(ctx, req.GetContext().GetTenantId(), req.GetCartRule()); err != nil {
		return nil, grpcx.Wrap(err, "save cart rule")
	}
	s.log.InfoContext(ctx, "comment cart rule approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCartRule().GetAudit().GetEtag())
	renewCartRuleRequest := &cartv1.RenewCartRuleRequest{
		Context:    req.GetContext(),
		CartRuleId: req.GetCartRuleId(),
		Details: &cartv1.RenewCartRuleRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: req.GetCartRule().GetOwner().GetEmail()},
			NewOwner: &identityv1.UserRef{
				UserId: req.GetCartRule().GetOwner().GetUserId(),
				Email:  req.GetDetails().GetApprover().GetEmail(),
			},
		},
	}
	renewCartRuleReply, err := s.renewCartRuleFn(ctx, renewCartRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleLifecycleService/RenewCartRule")
	}
	disputeSync, ok := s.clients.Get("acme.payments.v1.DisputeSyncService").(paymentsv1.DisputeSyncServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.payments.v1.DisputeSyncService")
	}
	snapshot, err := protojson.Marshal(req.GetCartRule())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveDisputeConflictRequest := &paymentsv1.ResolveDisputeConflictRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Assignee: &identityv1.UserRef{
				DisplayName: renewCartRuleReply.GetCartRule().GetOwner().GetDisplayName(),
			},
		},
		Details: &paymentsv1.ResolveDisputeConflictRequest_Details{Payload: snapshot},
	}
	resolveDisputeConflictReply, err := disputeSync.ResolveDisputeConflict(ctx, resolveDisputeConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.DisputeSyncService/ResolveDisputeConflict")
	}
	previewCartRuleRequest := &cartv1.PreviewCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Action: &cartv1.CartRule_Discount{Discount: &cartv1.CartRuleDiscountAction{
				Percent: renewCartRuleReply.GetCartRule().GetDiscount().GetPercent(),
			}},
		},
	}
	previewCartRuleReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*cartv1.PreviewCartRuleResponse, error) {
		return s.cartRuleValidationClient.PreviewCartRule(ctx, previewCartRuleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleValidationService/PreviewCartRule")
	}
	streamSynonymExportStream, err := s.synonymExportClient.StreamSynonymExport(ctx, &searchv1.StreamSynonymExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSynonymExportRequest_Filter{
			CreatedAfter: resolveDisputeConflictReply.GetDispute().GetAudit().GetUpdatedAt(),
			OwnerId:      previewCartRuleReply.GetCartRule().GetOwner().GetUserId(),
			Labels:       renewCartRuleReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
	}
	for {
		received, err := streamSynonymExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SynonymExportService/StreamSynonymExport")
		}
		_ = received
	}
	return &cartv1.CommentCartRuleApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: renewCartRuleReply.GetResult().GetDetails(),
			Message: resolveDisputeConflictReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		CartRule: &cartv1.CartRule{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: resolveDisputeConflictReply.GetDispute().GetAudit().GetUpdatedAt(),
			},
			Version:    previewCartRuleReply.GetCartRule().GetVersion(),
			CartRuleId: req.GetCartRuleId(),
			Owner:      req.GetCartRule().GetOwner(),
		},
	}, nil
}
