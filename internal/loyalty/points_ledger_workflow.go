package loyalty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	reviewsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/reviews/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// PointsLedgerWorkflowServer implements `acme.loyalty.v1.PointsLedgerWorkflowService`.
// Runs the long-lived workflow that takes a points ledger from start to finish.
type PointsLedgerWorkflowServer struct {
	*pointsLedgerWorkflowHandlers
}

// pointsLedgerWorkflowHandlers holds the handlers of PointsLedgerWorkflowServer, with what they use.
type pointsLedgerWorkflowHandlers struct {
	loyaltyv1.UnimplementedPointsLedgerWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	accessPolicyValidationClient   authorizationv1.AccessPolicyValidationServiceClient
	rewardLifecycleClient          loyaltyv1.RewardLifecycleServiceClient
	rewardValidationClient         loyaltyv1.RewardValidationServiceClient
	loyaltyMemberApprovalClient    loyaltyv1.LoyaltyMemberApprovalServiceClient
	pointsLedgerApprovalClient     loyaltyv1.PointsLedgerApprovalServiceClient
	rewardApprovalClient           loyaltyv1.RewardApprovalServiceClient
	conn                           *grpc.ClientConn
	searchIndexOnce                sync.Once
	searchIndexCached              searchv1.SearchIndexServiceClient
	clients                        *registry.Registry
	tierValidationClient           loyaltyv1.TierValidationServiceClient
	reviewVoteApprovalClient       reviewsv1.ReviewVoteApprovalServiceClient
	loyaltyMemberWorkflowClient    loyaltyv1.LoyaltyMemberWorkflowServiceClient
	loyaltyMemberCalculationClient loyaltyv1.LoyaltyMemberCalculationServiceClient
	meteredPointsLedgerApproval    meteredPointsLedgerApproval
	roleNotificationClient         identityv1.RoleNotificationServiceClient
	tierReportingClient            loyaltyv1.TierReportingServiceClient
}

// NewPointsLedgerWorkflowServer wires a PointsLedgerWorkflowServer.
func NewPointsLedgerWorkflowServer(deps platform.Deps) *PointsLedgerWorkflowServer {
	return &PointsLedgerWorkflowServer{pointsLedgerWorkflowHandlers: &pointsLedgerWorkflowHandlers{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.loyalty.v1.PointsLedgerWorkflowService"),
		accessPolicyValidationClient:   authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		rewardLifecycleClient:          loyaltyv1.NewRewardLifecycleServiceClient(deps.Conn),
		rewardValidationClient:         loyaltyv1.NewRewardValidationServiceClient(deps.Conn),
		loyaltyMemberApprovalClient:    loyaltyv1.NewLoyaltyMemberApprovalServiceClient(deps.Conn),
		pointsLedgerApprovalClient:     loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn),
		rewardApprovalClient:           loyaltyv1.NewRewardApprovalServiceClient(deps.Conn),
		conn:                           deps.Conn,
		clients:                        deps.Clients,
		tierValidationClient:           loyaltyv1.NewTierValidationServiceClient(deps.Conn),
		reviewVoteApprovalClient:       reviewsv1.NewReviewVoteApprovalServiceClient(deps.Conn),
		loyaltyMemberWorkflowClient:    loyaltyv1.NewLoyaltyMemberWorkflowServiceClient(deps.Conn),
		loyaltyMemberCalculationClient: loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		meteredPointsLedgerApproval:    meteredPointsLedgerApproval{PointsLedgerApprovalServiceClient: loyaltyv1.NewPointsLedgerApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		roleNotificationClient:         identityv1.NewRoleNotificationServiceClient(deps.Conn),
		tierReportingClient:            loyaltyv1.NewTierReportingServiceClient(deps.Conn),
	}}
}

// StartPointsLedgerWorkflow starts a points ledger.
func (s *pointsLedgerWorkflowHandlers) StartPointsLedgerWorkflow(ctx context.Context, req *loyaltyv1.StartPointsLedgerWorkflowRequest) (*loyaltyv1.StartPointsLedgerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "start points ledger workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	explainAccessPolicyDecisionRequest := buildExplainAccessPolicyDecisionRequestForStartPointsLedgerWorkflow(req)
	explainAccessPolicyDecisionReply, err := s.accessPolicyValidationClient.ExplainAccessPolicyDecision(ctx, explainAccessPolicyDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/ExplainAccessPolicyDecision")
	}
	getRewardLifecycleReply, err := s.rewardLifecycleClient.GetRewardLifecycle(ctx, &loyaltyv1.GetRewardLifecycleRequest{
		Context:  req.GetContext(),
		RewardId: req.GetPointsLedger().GetHolder().GetRewardId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/GetRewardLifecycle")
	}
	getRewardRulesRequest := &loyaltyv1.GetRewardRulesRequest{
		Context:  req.GetContext(),
		RewardId: req.GetPointsLedger().GetHolder().GetRewardId(),
	}
	getRewardRulesReply, err := grpcx.Call(ctx, s.rewardValidationClient.GetRewardRules, getRewardRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/GetRewardRules")
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
				if _, err := s.loyaltyMemberApprovalClient.EscalateLoyaltyMember(workCtx, &loyaltyv1.EscalateLoyaltyMemberRequest{
					Context: req.GetContext(),
					LoyaltyMember: &loyaltyv1.LoyaltyMember{
						Limits: []*loyaltyv1.LoyaltyMemberLimit{&loyaltyv1.LoyaltyMemberLimit{
							Name:   limit.GetName(),
							Period: limit.GetPeriod(),
							Amount: limit.GetAmount(),
						}},
						Contact: &commonv1.ContactInfo{Name: req.GetPointsLedger().GetContact().GetName()},
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
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberApprovalService/EscalateLoyaltyMember")
	}
	return &loyaltyv1.StartPointsLedgerWorkflowResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			Holder: &loyaltyv1.RewardRef{
				DisplayName: explainAccessPolicyDecisionReply.GetAccessPolicy().GetOwner().GetDisplayName(),
			},
			Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{Name: getRewardRulesReply.GetReward().GetName()}},
			Contact: &commonv1.ContactInfo{
				Email: getRewardRulesReply.GetReward().GetOwner().GetEmail(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Status:         req.GetPointsLedger().GetStatus(),
		},
		Outcome: &loyaltyv1.StartPointsLedgerWorkflowResponse_Outcome{Message: getRewardLifecycleReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvancePointsLedgerWorkflow advances a points ledger.
func (s *pointsLedgerWorkflowHandlers) AdvancePointsLedgerWorkflow(ctx context.Context, req *loyaltyv1.AdvancePointsLedgerWorkflowRequest) (*loyaltyv1.AdvancePointsLedgerWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "advance points ledger workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetPointsLedger()).(*loyaltyv1.PointsLedger)
	rejectPointsLedgerRequest := &loyaltyv1.RejectPointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Holder: &loyaltyv1.RewardRef{Version: req.GetPointsLedger().GetHolder().GetVersion()},
		},
		Details: &loyaltyv1.RejectPointsLedgerRequest_Details{
			Approver: &identityv1.UserRef{DisplayName: cloned.GetHolder().GetDisplayName()},
		},
	}
	rejectPointsLedgerReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*loyaltyv1.RejectPointsLedgerResponse, error) {
		return s.pointsLedgerApprovalClient.RejectPointsLedger(ctx, rejectPointsLedgerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/RejectPointsLedger")
	}
	delegateRewardApprovalRequest := &loyaltyv1.DelegateRewardApprovalRequest{}
	delegateRewardApprovalRequest.Context = req.GetContext()
	delegateRewardApprovalRequest.Details = &loyaltyv1.DelegateRewardApprovalRequest_Details{
		Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
	}
	delegateRewardApprovalReply, err := s.rewardApprovalClient.DelegateRewardApproval(ctx, delegateRewardApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardApprovalService/DelegateRewardApproval")
	}
	createSearchIndexRequest := &searchv1.CreateSearchIndexRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Audit: &commonv1.AuditInfo{
				Etag: rejectPointsLedgerReply.GetPointsLedger().GetAudit().GetEtag(),
			},
		},
	}
	createSearchIndexReply, err := s.searchIndexLazy().CreateSearchIndex(ctx, createSearchIndexRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/CreateSearchIndex")
	}
	return &loyaltyv1.AdvancePointsLedgerWorkflowResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: rejectPointsLedgerReply.GetPointsLedger().GetContact(),
			Audit: &commonv1.AuditInfo{
				CreatedAt: createSearchIndexReply.GetSearchIndex().GetAudit().GetCreatedAt(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Holder:         req.GetPointsLedger().GetHolder(),
		},
		Outcome: &loyaltyv1.AdvancePointsLedgerWorkflowResponse_Outcome{
			ReferenceId: delegateRewardApprovalReply.GetOutcome().GetReferenceId(),
			Message:     delegateRewardApprovalReply.GetOutcome().GetMessage(),
			Status:      createSearchIndexReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelPointsLedgerWorkflow cancels a points ledger.
func (s *pointsLedgerWorkflowHandlers) CancelPointsLedgerWorkflow(ctx context.Context, req *loyaltyv1.CancelPointsLedgerWorkflowRequest) (*loyaltyv1.CancelPointsLedgerWorkflowResponse, error) {
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "cancel points ledger workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	rewardLifecycle, ok := s.clients.Get("acme.loyalty.v1.RewardLifecycleService").(loyaltyv1.RewardLifecycleServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.loyalty.v1.RewardLifecycleService")
	}
	pointsLedgerCache := cache.New[string, *loyaltyv1.PointsLedger](1)
	pointsLedgerCache.Put(req.GetContext().GetTraceId(), req.GetPointsLedger())
	cachedPointsLedger, _ := pointsLedgerCache.Get(req.GetContext().GetTraceId())
	reinstateRewardRequest := &loyaltyv1.ReinstateRewardRequest{
		Context:  req.GetContext(),
		RewardId: req.GetPointsLedger().GetHolder().GetRewardId(),
		Details: &loyaltyv1.ReinstateRewardRequest_Details{
			NotifyContact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Name:             req.GetPointsLedger().GetContact().GetName(),
				Email:            req.GetPointsLedger().GetContact().GetEmail(),
			},
			NewOwner: &identityv1.UserRef{
				DisplayName: cachedPointsLedger.GetHolder().GetDisplayName(),
			},
		},
	}
	reinstateRewardReply, err := rewardLifecycle.ReinstateReward(ctx, reinstateRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/ReinstateReward")
	}
	validateNotificationRequest := &notificationsv1.ValidateNotificationRequest{
		Context: req.GetContext(),
		Notification: &notificationsv1.Notification{
			Comments: []*notificationsv1.NotificationComment{&notificationsv1.NotificationComment{
				CreatedAt: reinstateRewardReply.GetReward().GetAudit().GetCreatedAt(),
			}},
		},
		Details: &notificationsv1.ValidateNotificationRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	validateNotificationReply := new(notificationsv1.ValidateNotificationResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", notificationsNotificationValidationServiceName, "ValidateNotification"), validateNotificationRequest, validateNotificationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationValidationService/ValidateNotification")
	}
	checkTierEligibilityReply, err := s.tierValidationClient.CheckTierEligibility(ctx, &loyaltyv1.CheckTierEligibilityRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: validateNotificationReply.GetNotification().GetAudit().GetUpdatedAt(),
			},
			Action: &loyaltyv1.Tier_Route{Route: &loyaltyv1.TierRouteAction{
				Weight: reinstateRewardReply.GetReward().GetRoute().GetWeight(),
			}},
		},
		Details: &loyaltyv1.CheckTierEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetPointsLedger().GetContact().GetPreferredChannel(),
				Email:            reinstateRewardReply.GetReward().GetOwner().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierValidationService/CheckTierEligibility")
	}
	return &loyaltyv1.CancelPointsLedgerWorkflowResponse{
		PointsLedger: &loyaltyv1.PointsLedger{
			Holder: &loyaltyv1.RewardRef{RewardId: reinstateRewardReply.GetReward().GetRewardId()},
			Contact: &commonv1.ContactInfo{
				Name: validateNotificationReply.GetNotification().GetRequester().GetName(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateNotificationReply.GetNotification().GetAudit().GetCreatedAt(),
				UpdatedBy: checkTierEligibilityReply.GetTier().GetAudit().GetUpdatedBy(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Available:      req.GetPointsLedger().GetAvailable(),
		},
		Outcome: &loyaltyv1.CancelPointsLedgerWorkflowResponse_Outcome{Message: reinstateRewardReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: checkTierEligibilityReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// RetryPointsLedgerWorkflow retrys a points ledger.
func (s *pointsLedgerWorkflowHandlers) RetryPointsLedgerWorkflow(ctx context.Context, req *loyaltyv1.RetryPointsLedgerWorkflowRequest) (*loyaltyv1.RetryPointsLedgerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryPointsLedgerWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "retry points ledger workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	source := req.GetDetails().GetNotify().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	packed, err := anypb.New(req.GetPointsLedger())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	approveReviewVoteRequest := &reviewsv1.ApproveReviewVoteRequest{
		Context: req.GetContext(),
		ReviewVote: &reviewsv1.ReviewVote{
			Subject: &customersv1.CustomerRef{Email: email},
			Detail:  packed,
		},
		Details: &reviewsv1.ApproveReviewVoteRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetPointsLedger().GetHolder().GetDisplayName(),
				Email:       req.GetPointsLedger().GetContact().GetEmail(),
			},
		},
		Annotations: req.GetAnnotations(),
	}
	type approveReviewVoteResult struct {
		reply *reviewsv1.ApproveReviewVoteResponse
		err   error
	}
	approveReviewVoteResults := make(chan approveReviewVoteResult, 1)
	go func() {
		reply, err := s.reviewVoteApprovalClient.ApproveReviewVote(ctx, approveReviewVoteRequest)
		approveReviewVoteResults <- approveReviewVoteResult{reply, err}
	}()
	var approveReviewVoteReply *reviewsv1.ApproveReviewVoteResponse
	select {
	case result := <-approveReviewVoteResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.reviews.v1.ReviewVoteApprovalService/ApproveReviewVote")
		}
		approveReviewVoteReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &loyaltyv1.RetryPointsLedgerWorkflowResponse{
		Outcome: &loyaltyv1.RetryPointsLedgerWorkflowResponse_Outcome{Status: approveReviewVoteReply.GetOutcome().GetStatus()},
		Result: &commonv1.OperationResult{
			Message: approveReviewVoteReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			PointsLedgerId: req.GetPointsLedgerId(),
			Status:         req.GetPointsLedger().GetStatus(),
		},
	}, nil
}

// PausePointsLedgerWorkflow pauses a points ledger.
func (s *pointsLedgerWorkflowHandlers) PausePointsLedgerWorkflow(ctx context.Context, req *loyaltyv1.PausePointsLedgerWorkflowRequest) (*loyaltyv1.PausePointsLedgerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "pause points ledger workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	for limit := range iterx.Each(req.GetPointsLedger().GetLimits(), func(candidate *loyaltyv1.PointsLedgerLimit) bool { return candidate != nil }) {
		if _, err := s.pointsLedgerApprovalClient.DelegatePointsLedgerApproval(ctx, &loyaltyv1.DelegatePointsLedgerApprovalRequest{
			Context: req.GetContext(),
			PointsLedger: &loyaltyv1.PointsLedger{
				Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{
					Period: limit.GetPeriod(),
					Name:   limit.GetName(),
					Amount: limit.GetAmount(),
				}},
				Audit: &commonv1.AuditInfo{UpdatedBy: req.GetPointsLedger().GetAudit().GetUpdatedBy()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/DelegatePointsLedgerApproval")
		}
	}
	cancelLoyaltyMemberWorkflowRequest := &loyaltyv1.CancelLoyaltyMemberWorkflowRequest{}
	cancelLoyaltyMemberWorkflowRequest.Context = req.GetContext()
	cancelLoyaltyMemberWorkflowRequest.LoyaltyMember = &loyaltyv1.LoyaltyMember{
		Funding: &loyaltyv1.LoyaltyMember_Card{Card: &loyaltyv1.LoyaltyMemberCardFunding{Last4: req.GetPointsLedger().GetCard().GetLast4()}},
		Audit:   &commonv1.AuditInfo{CreatedAt: req.GetPointsLedger().GetAudit().GetCreatedAt()},
	}
	cancelLoyaltyMemberWorkflowRequest.Details = &loyaltyv1.CancelLoyaltyMemberWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{Name: req.GetPointsLedger().GetContact().GetName()},
	}
	cancelLoyaltyMemberWorkflowReply, err := s.loyaltyMemberWorkflowClient.CancelLoyaltyMemberWorkflow(ctx, cancelLoyaltyMemberWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberWorkflowService/CancelLoyaltyMemberWorkflow")
	}
	return &loyaltyv1.PausePointsLedgerWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: cancelLoyaltyMemberWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Holder: &loyaltyv1.RewardRef{
				Version: cancelLoyaltyMemberWorkflowReply.GetLoyaltyMember().GetHolder().GetVersion(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Available:      req.GetPointsLedger().GetAvailable(),
		},
	}, nil
}

// InspectPointsLedgerWorkflow returns a points ledger.
func (s *pointsLedgerWorkflowHandlers) InspectPointsLedgerWorkflow(ctx context.Context, req *loyaltyv1.InspectPointsLedgerWorkflowRequest) (*loyaltyv1.InspectPointsLedgerWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "points_ledger", req.GetPointsLedgerId(), "inspect_points_ledger_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect points ledger workflow", "tenant", req.GetContext().GetTenantId())
	commentPointsLedgerApprovalRequest := &loyaltyv1.CommentPointsLedgerApprovalRequest{
		Context:      req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{PointsLedgerId: req.GetPointsLedgerId()},
	}
	commentPointsLedgerApprovalReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*loyaltyv1.CommentPointsLedgerApprovalResponse, error) {
		return s.pointsLedgerApprovalClient.CommentPointsLedgerApproval(ctx, commentPointsLedgerApprovalRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/CommentPointsLedgerApproval")
	}
	return &loyaltyv1.InspectPointsLedgerWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: commentPointsLedgerApprovalReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		PointsLedger: &loyaltyv1.PointsLedger{PointsLedgerId: req.GetPointsLedgerId()},
	}, nil
}

// ListPointsLedgerWorkflows lists points ledgers a page at a time.
func (s *pointsLedgerWorkflowHandlers) ListPointsLedgerWorkflows(ctx context.Context, req *loyaltyv1.ListPointsLedgerWorkflowsRequest) (*loyaltyv1.ListPointsLedgerWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list points ledger workflows", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	checkAccessPolicyEligibilityRequest := &authorizationv1.CheckAccessPolicyEligibilityRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &authorizationv1.CheckAccessPolicyEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			Locale:  req.GetContext().GetLocale(),
		},
	}
	checkAccessPolicyEligibilityReply, err := authorizationv1.AccessPolicyValidationServiceClient.CheckAccessPolicyEligibility(s.accessPolicyValidationClient, ctx, checkAccessPolicyEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/CheckAccessPolicyEligibility")
	}
	return &loyaltyv1.ListPointsLedgerWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Status:  checkAccessPolicyEligibilityReply.GetResult().GetStatus(),
			Message: checkAccessPolicyEligibilityReply.GetOutcome().GetMessage(),
		},
	}, nil
}

// SignalPointsLedgerWorkflow signals a points ledger.
func (s *pointsLedgerWorkflowHandlers) SignalPointsLedgerWorkflow(ctx context.Context, req *loyaltyv1.SignalPointsLedgerWorkflowRequest) (*loyaltyv1.SignalPointsLedgerWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePointsLedger(ctx, req.GetContext().GetTenantId(), req.GetPointsLedger()); err != nil {
		return nil, grpcx.Wrap(err, "save points ledger")
	}
	s.log.InfoContext(ctx, "signal points ledger workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPointsLedger().GetAudit().GetEtag())
	simulateLoyaltyMemberRequest := &loyaltyv1.SimulateLoyaltyMemberRequest{
		Context: req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotify().GetPhone(),
				Name:  req.GetPointsLedger().GetContact().GetName(),
			},
		},
	}
	simulateLoyaltyMemberReply, err := loyaltyv1.LoyaltyMemberCalculationServiceClient.SimulateLoyaltyMember(s.loyaltyMemberCalculationClient, ctx, simulateLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/SimulateLoyaltyMember")
	}
	labels := make(map[string]string, len(req.GetPointsLedger().GetLabels()))
	for key, value := range req.GetPointsLedger().GetLabels() {
		labels[key] = value
	}
	listPendingPointsLedgersRequest := &loyaltyv1.ListPendingPointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.ListPendingPointsLedgersRequest_Filter{
			Labels:       labels,
			CreatedAfter: req.GetPointsLedger().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetPointsLedger().GetHolder().GetRewardId(),
		},
	}
	listPendingPointsLedgersReply, err := s.meteredPointsLedgerApproval.ListPendingPointsLedgers(ctx, listPendingPointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/ListPendingPointsLedgers")
	}
	streamRoleNoticesStream, err := s.roleNotificationClient.StreamRoleNotices(ctx, &identityv1.StreamRoleNoticesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: simulateLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/StreamRoleNotices")
	}
	for {
		received, err := streamRoleNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/StreamRoleNotices")
		}
		_ = received
	}
	shareTierReportReply, err := s.tierReportingClient.ShareTierReport(ctx, &loyaltyv1.ShareTierReportRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ShareTierReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone: simulateLoyaltyMemberReply.GetLoyaltyMember().GetContact().GetPhone(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/ShareTierReport")
	}
	return &loyaltyv1.SignalPointsLedgerWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: simulateLoyaltyMemberReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Balance: simulateLoyaltyMemberReply.GetLoyaltyMember().GetBalance(),
			Audit: &commonv1.AuditInfo{
				UpdatedBy: shareTierReportReply.GetTier().GetAudit().GetUpdatedBy(),
				UpdatedAt: shareTierReportReply.GetTier().GetAudit().GetUpdatedAt(),
			},
			PointsLedgerId: req.GetPointsLedgerId(),
			Labels:         req.GetPointsLedger().GetLabels(),
		},
		Outcome: &loyaltyv1.SignalPointsLedgerWorkflowResponse_Outcome{
			Message: listPendingPointsLedgersReply.GetResult().GetMessage(),
			Status:  listPendingPointsLedgersReply.GetResult().GetStatus(),
		},
	}, nil
}

// searchIndexLazy dials acme.search.v1.SearchIndexService the first time a handler needs it.
func (s *pointsLedgerWorkflowHandlers) searchIndexLazy() searchv1.SearchIndexServiceClient {
	s.searchIndexOnce.Do(func() { s.searchIndexCached = searchv1.NewSearchIndexServiceClient(s.conn) })
	return s.searchIndexCached
}
