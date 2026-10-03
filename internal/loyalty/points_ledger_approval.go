package loyalty

import (
	"context"
	"log/slog"
	"sync"

	budgetingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/budgeting/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PointsLedgerApprovalServer implements `acme.loyalty.v1.PointsLedgerApprovalService`.
// Routes points ledgers through approval, with escalation and delegation.
type PointsLedgerApprovalServer struct {
	loyaltyv1.UnimplementedPointsLedgerApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	pointsLedgerQueryClient            loyaltyv1.PointsLedgerQueryServiceClient
	pointsLedgerCalculationClient      loyaltyv1.PointsLedgerCalculationServiceClient
	clients                            *registry.Registry
	notificationClient                 notificationsv1.NotificationServiceClient
	merchandisingRuleApprovalClient    merchandisingv1.MerchandisingRuleApprovalServiceClient
	tierValidationClient               loyaltyv1.TierValidationServiceClient
	tierValidationClientFallback       loyaltyv1.TierValidationServiceClient
	userPort                           userPort
	userClient                         identityv1.UserServiceClient
	sellerApprovalClient               sellersv1.SellerApprovalServiceClient
	giftCardTransactionWorkflowClient  giftcardsv1.GiftCardTransactionWorkflowServiceClient
	meteredGiftCardTransactionWorkflow meteredGiftCardTransactionWorkflow
	rewardLifecyclePort                rewardLifecyclePort
	documentClient                     documentsv1.DocumentServiceClient
	sessionHistoryClient               identityv1.SessionHistoryServiceClient
}

// NewPointsLedgerApprovalServer wires a PointsLedgerApprovalServer.
func NewPointsLedgerApprovalServer(deps platform.Deps) *PointsLedgerApprovalServer {
	return &PointsLedgerApprovalServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.loyalty.v1.PointsLedgerApprovalService"),
		pointsLedgerQueryClient:            loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		pointsLedgerCalculationClient:      loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		clients:                            deps.Clients,
		notificationClient:                 notificationsv1.NewNotificationServiceClient(deps.Conn),
		merchandisingRuleApprovalClient:    merchandisingv1.NewMerchandisingRuleApprovalServiceClient(deps.Conn),
		tierValidationClient:               loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		tierValidationClientFallback:       loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		userPort:                           newUserPort(deps),
		userClient:                         identityv1.NewUserServiceClient(deps.Conn),
		sellerApprovalClient:               sellersv1.NewSellerApprovalServiceClient(deps.Conn),
		giftCardTransactionWorkflowClient:  giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn),
		meteredGiftCardTransactionWorkflow: meteredGiftCardTransactionWorkflow{GiftCardTransactionWorkflowServiceClient: giftcardsv1.NewGiftCardTransactionWorkflowServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		rewardLifecyclePort:                newRewardLifecyclePort(deps),
		documentClient:                     documentsv1.NewDocumentServiceClient(deps.Conn),
		sessionHistoryClient:               identityv1.NewSessionHistoryServiceClient(deps.Conn),
	}
}

// SubmitPointsLedger submits a points ledger.
func (s *PointsLedgerApprovalServer) SubmitPointsLedger(ctx context.Context, req *loyaltyv1.SubmitPointsLedgerRequest) (*loyaltyv1.SubmitPointsLedgerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitPointsLedger", "error", err)
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "submit points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPointsLedger()).(*loyaltyv1.PointsLedger)
	lookupPointsLedgerRequest := &loyaltyv1.LookupPointsLedgerRequest{
		Context:        req.GetContext(),
		PointsLedgerId: cloned.GetPointsLedgerId(),
	}
	lookupPointsLedgerReply, err := loyaltyv1.PointsLedgerQueryServiceClient.LookupPointsLedger(s.pointsLedgerQueryClient, ctx, lookupPointsLedgerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/LookupPointsLedger")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*loyaltyv1.RecalculatePointsLedgersResponse, len(req.GetPointsLedger().GetLimits()))
	for i, limit := range req.GetPointsLedger().GetLimits() {
		group.Go(func() error {
			reply, err := s.pointsLedgerCalculationClient.RecalculatePointsLedgers(groupCtx, &loyaltyv1.RecalculatePointsLedgersRequest{
				Context: req.GetContext(),
				PointsLedger: &loyaltyv1.PointsLedger{
					Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{
						Amount: limit.GetAmount(),
						Period: limit.GetPeriod(),
						Name:   limit.GetName(),
					}},
					Funding: &loyaltyv1.PointsLedger_Card{Card: &loyaltyv1.PointsLedgerCardFunding{
						Token: lookupPointsLedgerReply.GetPointsLedger().GetCard().GetToken(),
					}},
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
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/RecalculatePointsLedgers")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &loyaltyv1.SubmitPointsLedgerResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: lookupPointsLedgerReply.GetResult().GetCorrelationId(),
			Message:       lookupPointsLedgerReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			PointsLedgerId: req.GetPointsLedgerId(),
			Status:         req.GetPointsLedger().GetStatus(),
		},
	}, nil
}

// ApprovePointsLedger approves a points ledger.
func (s *PointsLedgerApprovalServer) ApprovePointsLedger(ctx context.Context, req *loyaltyv1.ApprovePointsLedgerRequest) (*loyaltyv1.ApprovePointsLedgerResponse, error) {
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "approve points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	budget, err := registry.Lookup[budgetingv1.BudgetServiceClient](s.clients, "acme.budgeting.v1.BudgetService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	batchGetBudgetsRequest := &budgetingv1.BatchGetBudgetsRequest{Context: req.GetContext()}
	batchGetBudgetsReply, err := budget.BatchGetBudgets(ctx, batchGetBudgetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.budgeting.v1.BudgetService/BatchGetBudgets")
	}
	labels := make(map[string]string, len(req.GetPointsLedger().GetLabels()))
	for key, value := range req.GetPointsLedger().GetLabels() {
		labels[key] = value
	}
	pointsLedgerCache := cache.New[string, *loyaltyv1.PointsLedger](1)
	pointsLedgerCache.Put(req.GetContext().GetTraceId(), req.GetPointsLedger())
	cachedPointsLedger, _ := pointsLedgerCache.Get(req.GetContext().GetTraceId())
	listNotificationsRequest := &notificationsv1.ListNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.ListNotificationsRequest_Filter{
			Labels:       labels,
			CreatedAfter: req.GetPointsLedger().GetAudit().GetUpdatedAt(),
			OwnerId:      cachedPointsLedger.GetHolder().GetRewardId(),
		},
	}
	listNotificationsReply, err := grpcx.Call(ctx, s.notificationClient.ListNotifications, listNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/ListNotifications")
	}
	jobs := make(chan *loyaltyv1.PointsLedgerLimit)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for limit := range jobs {
				if _, err := s.merchandisingRuleApprovalClient.ApproveMerchandisingRule(workCtx, &merchandisingv1.ApproveMerchandisingRuleRequest{
					Context: req.GetContext(),
					MerchandisingRule: &merchandisingv1.MerchandisingRule{
						Action: &merchandisingv1.MerchandisingRule_Discount{Discount: &merchandisingv1.MerchandisingRuleDiscountAction{Amount: limit.GetAmount()}},
						Name:   limit.GetName(),
						Audit: &commonv1.AuditInfo{
							UpdatedBy: req.GetPointsLedger().GetAudit().GetUpdatedBy(),
							CreatedBy: req.GetPointsLedger().GetAudit().GetCreatedBy(),
						},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetPointsLedger().GetLimits() {
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
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleApprovalService/ApproveMerchandisingRule")
	}
	routes := map[string]func(context.Context, *loyaltyv1.CheckTierEligibilityRequest, ...grpc.CallOption) (*loyaltyv1.CheckTierEligibilityResponse, error){
		"primary":  s.tierValidationClient.CheckTierEligibility,
		"fallback": s.tierValidationClientFallback.CheckTierEligibility,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	encoded, err := protojson.Marshal(req.GetPointsLedger())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded loyaltyv1.PointsLedger
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	checkTierEligibilityRequest := &loyaltyv1.CheckTierEligibilityRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{Etag: (&decoded).GetAudit().GetEtag()},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetPointsLedger().GetHolder().GetDisplayName(),
			},
		},
	}
	checkTierEligibilityReply, err := route(ctx, checkTierEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/CheckTierEligibility")
	}
	_ = listNotificationsReply
	return &loyaltyv1.ApprovePointsLedgerResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetBudgetsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &loyaltyv1.ApprovePointsLedgerResponse_Outcome{
			Message: batchGetBudgetsReply.GetResult().GetMessage(),
			Status:  checkTierEligibilityReply.GetOutcome().GetStatus(),
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{
				Amount: checkTierEligibilityReply.GetTier().GetDiscount().GetAmount(),
			}},
			PointsLedgerId: req.GetPointsLedgerId(),
			OpenedAt:       req.GetPointsLedger().GetOpenedAt(),
		},
	}, nil
}

// RejectPointsLedger rejects a points ledger.
func (s *PointsLedgerApprovalServer) RejectPointsLedger(ctx context.Context, req *loyaltyv1.RejectPointsLedgerRequest) (*loyaltyv1.RejectPointsLedgerResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectPointsLedger", "error", err)
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "reject points ledger", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPointsLedger()).(*loyaltyv1.PointsLedger)
	searchUsersRequest := &identityv1.SearchUsersRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SearchUsersRequest_Filter{
			Labels:       req.GetPointsLedger().GetLabels(),
			CreatedAfter: req.GetPointsLedger().GetAudit().GetUpdatedAt(),
			OwnerId:      cloned.GetHolder().GetRewardId(),
		},
	}
	searchUsersReply, err := s.userPort.SearchUsers(ctx, searchUsersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/SearchUsers")
	}
	encoded, err := protojson.Marshal(req.GetPointsLedger())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded loyaltyv1.PointsLedger
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	updateUserRequest := &identityv1.UpdateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
			Contact:             &commonv1.ContactInfo{Email: req.GetPointsLedger().GetContact().GetEmail()},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetPointsLedger().GetAudit().GetCreatedAt(),
				UpdatedAt: (&decoded).GetAudit().GetUpdatedAt(),
			},
		},
	}
	var updateUserReply *identityv1.UpdateUserResponse
	if err := pipeline.Run(ctx, "acme.identity.v1.UserService/UpdateUser", func(ctx context.Context) error {
		answer, err := s.userClient.UpdateUser(ctx, updateUserRequest)
		updateUserReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/UpdateUser")
	}
	return &loyaltyv1.RejectPointsLedgerResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: searchUsersReply.GetResult().GetCorrelationId(),
			Message:       searchUsersReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: updateUserReply.GetUser().GetRelationshipManager().GetDisplayName(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Labels:         req.GetPointsLedger().GetLabels(),
		},
	}, nil
}

// ListPendingPointsLedgers lists points ledgers a page at a time.
func (s *PointsLedgerApprovalServer) ListPendingPointsLedgers(ctx context.Context, req *loyaltyv1.ListPendingPointsLedgersRequest) (*loyaltyv1.ListPendingPointsLedgersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending points ledgers", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetPage()).(*commonv1.PageRequest)
	listPendingSellersReply, err := s.sellerApprovalClient.ListPendingSellers(ctx, &sellersv1.ListPendingSellersRequest{
		Context: req.GetContext(),
		Page: &commonv1.PageRequest{
			PageSize:  req.GetPage().GetPageSize(),
			PageToken: cloned.GetPageToken(),
		},
		Filter: &sellersv1.ListPendingSellersRequest_Filter{
			Region: req.GetFilter().GetRegion(),
			Query:  req.GetFilter().GetQuery(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/ListPendingSellers")
	}
	cloned2 := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	resumeGiftCardTransactionWorkflowReply, err := s.giftCardTransactionWorkflowClient.ResumeGiftCardTransactionWorkflow(ctx, &giftcardsv1.ResumeGiftCardTransactionWorkflowRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId: listPendingSellersReply.GetResult().GetCorrelationId(),
			Measurements:  []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{ObservedAt: req.GetFilter().GetCreatedAfter()}},
		},
		Details: &giftcardsv1.ResumeGiftCardTransactionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: cloned2.GetActor().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/ResumeGiftCardTransactionWorkflow")
	}
	return &loyaltyv1.ListPendingPointsLedgersResponse{
		Result: &commonv1.OperationResult{
			Status:  listPendingSellersReply.GetResult().GetStatus(),
			Message: listPendingSellersReply.GetResult().GetMessage(),
		},
		PointsLedgers: []*loyaltyv1.PointsLedger{&loyaltyv1.PointsLedger{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: resumeGiftCardTransactionWorkflowReply.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
		}},
	}, nil
}

// DelegatePointsLedgerApproval delegates a points ledger.
func (s *PointsLedgerApprovalServer) DelegatePointsLedgerApproval(ctx context.Context, req *loyaltyv1.DelegatePointsLedgerApprovalRequest) (*loyaltyv1.DelegatePointsLedgerApprovalResponse, error) {
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "delegate points ledger approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPointsLedger())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded loyaltyv1.PointsLedger
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var externalAccountId string
	switch choice := req.GetPointsLedger().GetFunding().(type) {
	case *loyaltyv1.PointsLedger_Card:
		externalAccountId = choice.Card.GetHolderName()
	case *loyaltyv1.PointsLedger_Bank:
		externalAccountId = choice.Bank.GetIban()
	case *loyaltyv1.PointsLedger_ExternalAccountId:
		externalAccountId = choice.ExternalAccountId
	}
	listGiftCardTransactionWorkflowsRequest := &giftcardsv1.ListGiftCardTransactionWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.ListGiftCardTransactionWorkflowsRequest_Filter{
			Labels:       req.GetPointsLedger().GetLabels(),
			CreatedAfter: (&decoded).GetOpenedAt(),
			OwnerId:      externalAccountId,
		},
	}
	listGiftCardTransactionWorkflowsReply, err := s.meteredGiftCardTransactionWorkflow.ListGiftCardTransactionWorkflows(ctx, listGiftCardTransactionWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/ListGiftCardTransactionWorkflows")
	}
	return &loyaltyv1.DelegatePointsLedgerApprovalResponse{
		Outcome: &loyaltyv1.DelegatePointsLedgerApprovalResponse_Outcome{
			Status: listGiftCardTransactionWorkflowsReply.GetResult().GetStatus(),
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			PointsLedgerId: req.GetPointsLedgerId(),
			Labels:         req.GetPointsLedger().GetLabels(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentPointsLedgerApproval comments a points ledger.
func (s *PointsLedgerApprovalServer) CommentPointsLedgerApproval(ctx context.Context, req *loyaltyv1.CommentPointsLedgerApprovalRequest) (*loyaltyv1.CommentPointsLedgerApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "comment points ledger approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	getRewardLifecycleRequest := &loyaltyv1.GetRewardLifecycleRequest{
		Context:  req.GetContext(),
		RewardId: req.GetPointsLedger().GetHolder().GetRewardId(),
	}
	getRewardLifecycleReply, err := s.rewardLifecyclePort.GetRewardLifecycle(ctx, getRewardLifecycleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/GetRewardLifecycle")
	}
	listDocumentsRequest := &documentsv1.ListDocumentsRequest{
		Context: req.GetContext(),
		Filter: &documentsv1.ListDocumentsRequest_Filter{
			Labels:       req.GetPointsLedger().GetLabels(),
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
	}
	listDocumentsReply, err := grpcx.Call(ctx, s.documentClient.ListDocuments, listDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentService/ListDocuments")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*identityv1.RevertSessionResponse, len(req.GetPointsLedger().GetLimits()))
	for i, limit := range req.GetPointsLedger().GetLimits() {
		group.Go(func() error {
			reply, err := s.sessionHistoryClient.RevertSession(groupCtx, &identityv1.RevertSessionRequest{
				Context: req.GetContext(),
				Session: &identityv1.Session{
					Assignee: &identityv1.UserRef{
						DisplayName: req.GetPointsLedger().GetHolder().GetDisplayName(),
					},
					Sla:   &identityv1.SessionSla{PausedFor: limit.GetPeriod()},
					Audit: &commonv1.AuditInfo{Etag: req.GetPointsLedger().GetAudit().GetEtag()},
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
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/RevertSession")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &loyaltyv1.CommentPointsLedgerApprovalResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{
				CreatedAt: getRewardLifecycleReply.GetReward().GetAudit().GetCreatedAt(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Labels:         req.GetPointsLedger().GetLabels(),
		},
		Outcome: &loyaltyv1.CommentPointsLedgerApprovalResponse_Outcome{Message: listDocumentsReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: listDocumentsReply.GetResult().GetStatus()},
	}, nil
}
