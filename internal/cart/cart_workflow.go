package cart

import (
	"context"
	"log/slog"
	"sync"

	billingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/billing/v1"
	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// CartWorkflowServer implements `acme.cart.v1.CartWorkflowService`.
// Runs the long-lived workflow that takes a cart from start to finish.
type CartWorkflowServer struct {
	cartv1.UnimplementedCartWorkflowServiceServer
	store                     *Store
	log                       *slog.Logger
	clients                   *registry.Registry
	billingAccountClient      billingv1.BillingAccountServiceClient
	putawayTaskApprovalClient warehousingv1.PutawayTaskApprovalServiceClient
	conn                      *grpc.ClientConn
	cartCalculationClient     cartv1.CartCalculationServiceClient
	savedListQueryClient      cartv1.SavedListQueryServiceClient
	rewardWorkflowClient      loyaltyv1.RewardWorkflowServiceClient
	sessionNotificationClient identityv1.SessionNotificationServiceClient
	watchlistReportingClient  fraudv1.WatchlistReportingServiceClient
	savedListValidationClient cartv1.SavedListValidationServiceClient
}

// NewCartWorkflowServer wires a CartWorkflowServer.
func NewCartWorkflowServer(deps platform.Deps) *CartWorkflowServer {
	return &CartWorkflowServer{
		store:                     NewStore(deps.DB),
		log:                       deps.Logger.With("service", "acme.cart.v1.CartWorkflowService"),
		clients:                   deps.Clients,
		billingAccountClient:      billingv1.NewBillingAccountServiceClient(deps.Conn),
		putawayTaskApprovalClient: warehousingv1.NewPutawayTaskApprovalServiceClient(deps.Conn),
		conn:                      deps.Conn,
		cartCalculationClient:     cartv1.NewCartCalculationServiceClient(deps.Conn),
		savedListQueryClient:      cartv1.NewSavedListQueryServiceClient(deps.Conn),
		rewardWorkflowClient:      loyaltyv1.NewRewardWorkflowServiceClient(deps.Conn),
		sessionNotificationClient: identityv1.NewSessionNotificationServiceClient(deps.Conn),
		watchlistReportingClient:  fraudv1.NewWatchlistReportingServiceClient(deps.Conn),
		savedListValidationClient: cartv1.NewSavedListValidationServiceClient(deps.Conn),
	}
}

// StartCartWorkflow starts a cart.
func (s *CartWorkflowServer) StartCartWorkflow(ctx context.Context, req *cartv1.StartCartWorkflowRequest) (*cartv1.StartCartWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "start cart workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	sellerScorecardReporting, err := registry.Lookup[sellersv1.SellerScorecardReportingServiceClient](s.clients, "acme.sellers.v1.SellerScorecardReportingService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	scheduleSellerScorecardReportRequest := &sellersv1.ScheduleSellerScorecardReportRequest{
		Context:     req.GetContext(),
		Annotations: req.GetAnnotations(),
	}
	scheduleSellerScorecardReportReply, err := sellerScorecardReporting.ScheduleSellerScorecardReport(ctx, scheduleSellerScorecardReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/ScheduleSellerScorecardReport")
	}
	var holderName string
	switch choice := req.GetCart().GetSettlement().(type) {
	case *cartv1.Cart_Card:
		holderName = choice.Card.GetHolderName()
	case *cartv1.Cart_Bank:
		holderName = choice.Bank.GetIban()
	case *cartv1.Cart_VoucherCode:
		holderName = choice.VoucherCode
	}
	var holder struct{ PreferredChannel string }
	if record := req.GetDetails().GetNotify(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PreferredChannel"); err != nil {
			s.log.WarnContext(ctx, "copy preferred_channel", "error", err)
		}
	}
	updateBillingAccountRequest := &billingv1.UpdateBillingAccountRequest{}
	updateBillingAccountRequest.Context = req.GetContext()
	updateBillingAccountRequest.BillingAccount = &billingv1.BillingAccount{
		Funding: &billingv1.BillingAccount_Card{Card: &billingv1.BillingAccountCardFunding{HolderName: holderName}},
		Contact: &commonv1.ContactInfo{
			PreferredChannel: holder.PreferredChannel,
			Name:             req.GetDetails().GetNotify().GetName(),
			Email:            req.GetDetails().GetNotify().GetEmail(),
		},
	}
	updateBillingAccountReply, err := s.billingAccountClient.UpdateBillingAccount(ctx, updateBillingAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.billing.v1.BillingAccountService/UpdateBillingAccount")
	}
	return &cartv1.StartCartWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: scheduleSellerScorecardReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Cart: &cartv1.Cart{
			Audit: &commonv1.AuditInfo{
				CreatedAt: updateBillingAccountReply.GetBillingAccount().GetAudit().GetCreatedAt(),
			},
			Counterparty: &cartv1.SavedListRef{
				Number: updateBillingAccountReply.GetBillingAccount().GetHolder().GetNumber(),
			},
			CartId:         req.GetCartId(),
			BillingAddress: req.GetCart().GetBillingAddress(),
		},
	}, nil
}

// AdvanceCartWorkflow advances a cart.
func (s *CartWorkflowServer) AdvanceCartWorkflow(ctx context.Context, req *cartv1.AdvanceCartWorkflowRequest) (*cartv1.AdvanceCartWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "advance cart workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	recallPutawayTaskRequest := &warehousingv1.RecallPutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetCart().GetAudit().GetCreatedBy()},
		},
	}
	recallPutawayTaskReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*warehousingv1.RecallPutawayTaskResponse, error) {
		return s.putawayTaskApprovalClient.RecallPutawayTask(ctx, recallPutawayTaskRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskApprovalService/RecallPutawayTask")
	}
	countSessionsRequest := &identityv1.CountSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.CountSessionsRequest_Filter{
			Region:       req.GetCart().GetBillingAddress().GetRegion(),
			CreatedAfter: recallPutawayTaskReply.GetPutawayTask().GetAudit().GetUpdatedAt(),
		},
	}
	countSessionsReply := new(identityv1.CountSessionsResponse)
	if err := s.conn.Invoke(ctx, identitySessionQueryServiceCountSessionsMethod, countSessionsRequest, countSessionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/CountSessions")
	}
	return &cartv1.AdvanceCartWorkflowResponse{
		Cart: &cartv1.Cart{
			DueAt:    recallPutawayTaskReply.GetPutawayTask().GetSla().GetDueAt(),
			CartId:   req.GetCartId(),
			IssuedAt: req.GetCart().GetIssuedAt(),
		},
		Outcome: &cartv1.AdvanceCartWorkflowResponse_Outcome{Message: recallPutawayTaskReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryCartWorkflow retrys a cart.
func (s *CartWorkflowServer) RetryCartWorkflow(ctx context.Context, req *cartv1.RetryCartWorkflowRequest) (*cartv1.RetryCartWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "retry cart workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	for attachment := range iterx.Each(req.GetCart().GetAttachments(), func(candidate *cartv1.CartAttachment) bool { return candidate != nil }) {
		if _, err := s.cartCalculationClient.SimulateCart(ctx, &cartv1.SimulateCartRequest{
			Context: req.GetContext(),
			Cart: &cartv1.Cart{
				Attachments: []*cartv1.CartAttachment{&cartv1.CartAttachment{Uri: attachment.GetUri()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/SimulateCart")
		}
	}
	return &cartv1.RetryCartWorkflowResponse{
		Cart: &cartv1.Cart{
			CartId: req.GetCartId(),
			Number: req.GetCart().GetNumber(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseCartWorkflow pauses a cart.
func (s *CartWorkflowServer) PauseCartWorkflow(ctx context.Context, req *cartv1.PauseCartWorkflowRequest) (*cartv1.PauseCartWorkflowResponse, error) {
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "pause cart workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetCart()).(*cartv1.Cart)
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeSavedListsReply, err := s.savedListQueryClient.SummarizeSavedLists(ctx, &cartv1.SummarizeSavedListsRequest{
		Context:     req.GetContext(),
		SavedListId: cloned.GetCounterparty().GetSavedListId(),
		Annotations: annotations,
		Reason:      req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListQueryService/SummarizeSavedLists")
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
				if _, err := s.cartCalculationClient.RecalculateCarts(workCtx, &cartv1.RecalculateCartsRequest{
					Context: req.GetContext(),
					Cart: &cartv1.Cart{
						Attachments: []*cartv1.CartAttachment{&cartv1.CartAttachment{
							FileName:  attachment.GetFileName(),
							Uri:       attachment.GetUri(),
							Checksum:  attachment.GetChecksum(),
							SizeBytes: attachment.GetSizeBytes(),
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartCalculationService/RecalculateCarts")
	}
	return &cartv1.PauseCartWorkflowResponse{
		Outcome: &cartv1.PauseCartWorkflowResponse_Outcome{Status: summarizeSavedListsReply.GetResult().GetStatus()},
		Cart: &cartv1.Cart{
			CartId: req.GetCartId(),
			DueAt:  req.GetCart().GetDueAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeCartWorkflow resumes a cart.
func (s *CartWorkflowServer) ResumeCartWorkflow(ctx context.Context, req *cartv1.ResumeCartWorkflowRequest) (*cartv1.ResumeCartWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "resume cart workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	listApiCredentialSubscriptionsRequest := &identityv1.ListApiCredentialSubscriptionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListApiCredentialSubscriptionsRequest_Filter{
			Region:       req.GetCart().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetCart().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetCart().GetCartId(),
			Labels:       req.GetCart().GetMetadata(),
		},
	}
	listApiCredentialSubscriptionsReply := new(identityv1.ListApiCredentialSubscriptionsResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.ApiCredentialNotificationService_ListApiCredentialSubscriptions_FullMethodName, listApiCredentialSubscriptionsRequest, listApiCredentialSubscriptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/ListApiCredentialSubscriptions")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	resumeRewardWorkflowRequest := &loyaltyv1.ResumeRewardWorkflowRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetCart().GetAudit().GetUpdatedBy(),
				CreatedAt: req.GetCart().GetAudit().GetCreatedAt(),
			},
		},
		Details: &loyaltyv1.ResumeRewardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
	}
	send := s.resumeRewardWorkflowSender(resumeRewardWorkflowRequest)
	resumeRewardWorkflowReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardWorkflowService/ResumeRewardWorkflow")
	}
	return &cartv1.ResumeCartWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:       listApiCredentialSubscriptionsReply.GetResult().GetStatus(),
			ReferenceIds: listApiCredentialSubscriptionsReply.GetResult().GetReferenceIds(),
		},
		Outcome: &cartv1.ResumeCartWorkflowResponse_Outcome{Status: resumeRewardWorkflowReply.GetOutcome().GetStatus()},
		Cart: &cartv1.Cart{
			CartId:       req.GetCartId(),
			Counterparty: &cartv1.SavedListRef{Number: req.GetCart().GetNumber()},
		},
	}, nil
}

// ListCartWorkflows lists carts a page at a time.
func (s *CartWorkflowServer) ListCartWorkflows(ctx context.Context, req *cartv1.ListCartWorkflowsRequest) (*cartv1.ListCartWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list cart workflows", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeSessionUpdatesReply, err := s.sessionNotificationClient.SubscribeSessionUpdates(ctx, &identityv1.SubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Payload:   packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates")
	}
	return &cartv1.ListCartWorkflowsResponse{
		Carts: []*cartv1.Cart{&cartv1.Cart{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: subscribeSessionUpdatesReply.GetSession().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalCartWorkflow signals a cart.
func (s *CartWorkflowServer) SignalCartWorkflow(ctx context.Context, req *cartv1.SignalCartWorkflowRequest) (*cartv1.SignalCartWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalCartWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCart(ctx, req.GetContext().GetTenantId(), req.GetCart()); err != nil {
		return nil, grpcx.Wrap(err, "save cart")
	}
	s.log.InfoContext(ctx, "signal cart workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCart().GetAudit().GetEtag())
	countWatchlistTotalsRequest := &fraudv1.CountWatchlistTotalsRequest{
		Context: req.GetContext(),
		Filter:  &fraudv1.CountWatchlistTotalsRequest_Filter{Region: req.GetCart().GetBillingAddress().GetRegion()},
	}
	countWatchlistTotalsReply, err := fraudv1.WatchlistReportingServiceClient.CountWatchlistTotals(s.watchlistReportingClient, ctx, countWatchlistTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistReportingService/CountWatchlistTotals")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*cartv1.PreviewSavedListResponse, len(req.GetCart().GetLines()))
	for i, line := range req.GetCart().GetLines() {
		group.Go(func() error {
			reply, err := s.savedListValidationClient.PreviewSavedList(groupCtx, &cartv1.PreviewSavedListRequest{
				Context: req.GetContext(),
				SavedList: &cartv1.SavedList{
					Lines: []*cartv1.SavedListLine{&cartv1.SavedListLine{
						Tax: &cartv1.SavedListLineTax{
							Amount:       line.GetTax().GetAmount(),
							Jurisdiction: line.GetTax().GetJurisdiction(),
						},
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
		return nil, grpcx.Wrap(err, "acme.cart.v1.SavedListValidationService/PreviewSavedList")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &cartv1.SignalCartWorkflowResponse{
		Result: &commonv1.OperationResult{Status: countWatchlistTotalsReply.GetResult().GetStatus()},
		Cart: &cartv1.Cart{
			CartId:       req.GetCartId(),
			Counterparty: &cartv1.SavedListRef{Number: req.GetCart().GetNumber()},
		},
	}, nil
}

// resumeRewardWorkflowSender returns a function that sends one request to
// acme.loyalty.v1.RewardWorkflowService/ResumeRewardWorkflow.
func (s *CartWorkflowServer) resumeRewardWorkflowSender(request *loyaltyv1.ResumeRewardWorkflowRequest) func(context.Context) (*loyaltyv1.ResumeRewardWorkflowResponse, error) {
	return func(ctx context.Context) (*loyaltyv1.ResumeRewardWorkflowResponse, error) {
		return s.rewardWorkflowClient.ResumeRewardWorkflow(ctx, request)
	}
}
