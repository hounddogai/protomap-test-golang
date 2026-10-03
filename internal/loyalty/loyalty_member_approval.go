package loyalty

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// LoyaltyMemberApprovalServer implements `acme.loyalty.v1.LoyaltyMemberApprovalService`.
// Routes loyalty members through approval, with escalation and delegation.
type LoyaltyMemberApprovalServer struct {
	loyaltyv1.UnsafeLoyaltyMemberApprovalServiceServer
	store                          *Store
	log                            *slog.Logger
	rewardLifecycleClient          loyaltyv1.RewardLifecycleServiceClient
	conn                           *grpc.ClientConn
	rewardValidationOnce           sync.Once
	rewardValidationCached         loyaltyv1.RewardValidationServiceClient
	tierReportingClient            loyaltyv1.TierReportingServiceClient
	bus                            *eventbus.Bus
	sessionValidationClient        identityv1.SessionValidationServiceClient
	pointsLedgerCalculationClient  loyaltyv1.PointsLedgerCalculationServiceClient
	tierCalculationClient          loyaltyv1.TierCalculationServiceClient
	apiCredentialValidationClient  identityv1.ApiCredentialValidationServiceClient
	loyaltyMemberCalculationClient loyaltyv1.LoyaltyMemberCalculationServiceClient
	rewardValidationClient         loyaltyv1.RewardValidationServiceClient
	meteredVenueValidation         meteredVenueValidation
	clients                        *registry.Registry
	collectionWorkflowClient       merchandisingv1.CollectionWorkflowServiceClient
	accessPolicyClient             authorizationv1.AccessPolicyServiceClient
}

// NewLoyaltyMemberApprovalServer wires a LoyaltyMemberApprovalServer.
func NewLoyaltyMemberApprovalServer(deps platform.Deps) *LoyaltyMemberApprovalServer {
	return &LoyaltyMemberApprovalServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.loyalty.v1.LoyaltyMemberApprovalService"),
		rewardLifecycleClient:          loyaltyv1.NewRewardLifecycleServiceClient(deps.Conn),
		conn:                           deps.Conn,
		tierReportingClient:            loyaltyv1.NewTierReportingServiceClient(deps.Conn),
		bus:                            deps.Bus,
		sessionValidationClient:        identityv1.NewSessionValidationServiceClient(deps.Conn),
		pointsLedgerCalculationClient:  loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		tierCalculationClient:          loyaltyv1.NewTierCalculationServiceClient(deps.Conn),
		apiCredentialValidationClient:  identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		loyaltyMemberCalculationClient: loyaltyv1.NewLoyaltyMemberCalculationServiceClient(deps.Conn),
		rewardValidationClient:         loyaltyv1.NewRewardValidationServiceClient(deps.Conn),
		meteredVenueValidation:         meteredVenueValidation{VenueValidationServiceClient: venuesv1.NewVenueValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		clients:                        deps.Clients,
		collectionWorkflowClient:       merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		accessPolicyClient:             authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
	}
}

// SubmitLoyaltyMember submits a loyalty member.
func (s *LoyaltyMemberApprovalServer) SubmitLoyaltyMember(ctx context.Context, req *loyaltyv1.SubmitLoyaltyMemberRequest) (*loyaltyv1.SubmitLoyaltyMemberResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "submit loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	reinstateRewardRequest := &loyaltyv1.ReinstateRewardRequest{
		Context:  req.GetContext(),
		RewardId: req.GetLoyaltyMember().GetHolder().GetRewardId(),
		Details: &loyaltyv1.ReinstateRewardRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetLoyaltyMember().GetHolder().GetDisplayName(),
			},
		},
	}
	reinstateRewardReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*loyaltyv1.ReinstateRewardResponse, error) {
		return s.rewardLifecycleClient.ReinstateReward(ctx, reinstateRewardRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/ReinstateReward")
	}
	explainRewardDecisionRequest := &loyaltyv1.ExplainRewardDecisionRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: reinstateRewardReply.GetReward().GetAudit().GetUpdatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: reinstateRewardReply.GetReward().GetOwner().GetDisplayName(),
			},
		},
	}
	explainRewardDecisionReply, err := s.rewardValidationLazy().ExplainRewardDecision(ctx, explainRewardDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/ExplainRewardDecision")
	}
	scheduleTierReportRequest := &loyaltyv1.ScheduleTierReportRequest{
		Context:     req.GetContext(),
		Reason:      reinstateRewardReply.GetReward().GetBlock().GetReason(),
		Annotations: req.GetAnnotations(),
	}
	scheduleTierReportReply, err := grpcx.Call(ctx, s.tierReportingClient.ScheduleTierReport, scheduleTierReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/ScheduleTierReport")
	}
	shareTierReportReply, err := s.tierReportingClient.ShareTierReport(ctx, &loyaltyv1.ShareTierReportRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ShareTierReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Email: reinstateRewardReply.GetReward().GetOwner().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/ShareTierReport")
	}
	return &loyaltyv1.SubmitLoyaltyMemberResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				CreatedBy: reinstateRewardReply.GetReward().GetAudit().GetCreatedBy(),
			},
			Holder: &loyaltyv1.RewardRef{
				DisplayName: explainRewardDecisionReply.GetReward().GetOwner().GetDisplayName(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			OpenedAt:        req.GetLoyaltyMember().GetOpenedAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  explainRewardDecisionReply.GetResult().GetReferenceIds(),
			CorrelationId: scheduleTierReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &loyaltyv1.SubmitLoyaltyMemberResponse_Outcome{Message: shareTierReportReply.GetOutcome().GetMessage()},
	}, nil
}

// ApproveLoyaltyMember approves a loyalty member.
func (s *LoyaltyMemberApprovalServer) ApproveLoyaltyMember(ctx context.Context, req *loyaltyv1.ApproveLoyaltyMemberRequest) (*loyaltyv1.ApproveLoyaltyMemberResponse, error) {
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "approve loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "loyalty.approve_loyalty_member.check_user_eligibility",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"details.contact.email": req.GetDetails().GetApprover().GetEmail(),
			"user.audit.updated_by": req.GetLoyaltyMember().GetAudit().GetUpdatedBy(),
			"details.contact.phone": req.GetLoyaltyMember().GetContact().GetPhone(),
			"context.tenant_id":     req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &loyaltyv1.ApproveLoyaltyMemberResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Balance:         req.GetLoyaltyMember().GetBalance(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectLoyaltyMember rejects a loyalty member.
func (s *LoyaltyMemberApprovalServer) RejectLoyaltyMember(ctx context.Context, req *loyaltyv1.RejectLoyaltyMemberRequest) (*loyaltyv1.RejectLoyaltyMemberResponse, error) {
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "reject loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	checkSessionEligibilityRequest := &identityv1.CheckSessionEligibilityRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Requester: &commonv1.ContactInfo{
				Email:            req.GetDetails().GetApprover().GetEmail(),
				PreferredChannel: req.GetLoyaltyMember().GetContact().GetPreferredChannel(),
				Phone:            req.GetLoyaltyMember().GetContact().GetPhone(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetLoyaltyMember().GetHolder().GetDisplayName(),
			},
		},
	}
	checkSessionEligibilityReply, err := grpcx.Call(ctx, s.sessionValidationClient.CheckSessionEligibility, checkSessionEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionValidationService/CheckSessionEligibility")
	}
	return &loyaltyv1.RejectLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			Message: checkSessionEligibilityReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Contact: &commonv1.ContactInfo{
				Email: checkSessionEligibilityReply.GetSession().GetRequester().GetEmail(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
		},
		Outcome: &loyaltyv1.RejectLoyaltyMemberResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// EscalateLoyaltyMember escalates a loyalty member.
func (s *LoyaltyMemberApprovalServer) EscalateLoyaltyMember(ctx context.Context, req *loyaltyv1.EscalateLoyaltyMemberRequest) (*loyaltyv1.EscalateLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateLoyaltyMember", "error", err)
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "escalate loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	for limit := range iterx.Each(req.GetLoyaltyMember().GetLimits(), func(candidate *loyaltyv1.LoyaltyMemberLimit) bool { return candidate != nil }) {
		if _, err := s.pointsLedgerCalculationClient.SimulatePointsLedger(ctx, &loyaltyv1.SimulatePointsLedgerRequest{
			Context: req.GetContext(),
			PointsLedger: &loyaltyv1.PointsLedger{
				Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{
					Amount: limit.GetAmount(),
					Name:   limit.GetName(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/SimulatePointsLedger")
		}
	}
	simulateTierRequest := &loyaltyv1.SimulateTierRequest{
		Context: req.GetContext(),
		Tier: &loyaltyv1.Tier{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	type simulateTierResult struct {
		reply *loyaltyv1.SimulateTierResponse
		err   error
	}
	simulateTierResults := make(chan simulateTierResult, 1)
	go func() {
		reply, err := s.tierCalculationClient.SimulateTier(ctx, simulateTierRequest)
		simulateTierResults <- simulateTierResult{reply, err}
	}()
	var simulateTierReply *loyaltyv1.SimulateTierResponse
	select {
	case result := <-simulateTierResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.loyalty.v1.TierCalculationService/SimulateTier")
		}
		simulateTierReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	normalizeApiCredentialRequest := &identityv1.NormalizeApiCredentialRequest{Context: req.GetContext()}
	send := s.normalizeApiCredentialSender(normalizeApiCredentialRequest)
	normalizeApiCredentialReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/NormalizeApiCredential")
	}
	return &loyaltyv1.EscalateLoyaltyMemberResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				CreatedBy: simulateTierReply.GetTier().GetAudit().GetCreatedBy(),
			},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			OpenedAt:        req.GetLoyaltyMember().GetOpenedAt(),
		},
		Outcome: &loyaltyv1.EscalateLoyaltyMemberResponse_Outcome{
			EffectiveAt: normalizeApiCredentialReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: normalizeApiCredentialReply.GetResult().GetStatus()},
	}, nil
}

// ListPendingLoyaltyMembers lists loyalty members a page at a time.
func (s *LoyaltyMemberApprovalServer) ListPendingLoyaltyMembers(ctx context.Context, req *loyaltyv1.ListPendingLoyaltyMembersRequest) (*loyaltyv1.ListPendingLoyaltyMembersResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending loyalty members", "tenant", req.GetContext().GetTenantId())
	simulateLoyaltyMemberReply, err := s.loyaltyMemberCalculationClient.SimulateLoyaltyMember(ctx, &loyaltyv1.SimulateLoyaltyMemberRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.SimulateLoyaltyMemberRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Labels:  req.GetFilter().GetLabels(),
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberCalculationService/SimulateLoyaltyMember")
	}
	return &loyaltyv1.ListPendingLoyaltyMembersResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: simulateLoyaltyMemberReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// DelegateLoyaltyMemberApproval delegates a loyalty member.
func (s *LoyaltyMemberApprovalServer) DelegateLoyaltyMemberApproval(ctx context.Context, req *loyaltyv1.DelegateLoyaltyMemberApprovalRequest) (*loyaltyv1.DelegateLoyaltyMemberApprovalResponse, error) {
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "delegate loyalty member approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	var holder struct{ Email string }
	if record := req.GetDetails().GetApprover(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	source := req.GetLoyaltyMember().GetAudit().ProtoReflect()
	updatedBy := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("updated_by"))).String()
	scoreRewardReply, err := s.rewardValidationClient.ScoreReward(ctx, &loyaltyv1.ScoreRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetLoyaltyMember().GetAudit().GetEtag(),
				UpdatedBy: updatedBy,
			},
			Owner: &identityv1.UserRef{
				Email:  holder.Email,
				UserId: req.GetDetails().GetApprover().GetUserId(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/ScoreReward")
	}
	scoreVenueRequest := &venuesv1.ScoreVenueRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Owner: &identityv1.UserRef{Email: scoreRewardReply.GetReward().GetOwner().GetEmail()},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetLoyaltyMember().GetAudit().GetCreatedBy()},
		},
	}
	scoreVenueReply, err := s.meteredVenueValidation.ScoreVenue(ctx, scoreVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/ScoreVenue")
	}
	return &loyaltyv1.DelegateLoyaltyMemberApprovalResponse{
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: scoreRewardReply.GetReward().GetAudit().GetUpdatedBy(),
				CreatedBy: scoreRewardReply.GetReward().GetAudit().GetCreatedBy(),
			},
			Limits:          []*loyaltyv1.LoyaltyMemberLimit{&loyaltyv1.LoyaltyMemberLimit{Name: scoreVenueReply.GetVenue().GetName()}},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Labels:          req.GetLoyaltyMember().GetLabels(),
		},
		Result: &commonv1.OperationResult{
			Details: scoreVenueReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// RecallLoyaltyMember recalls a loyalty member.
func (s *LoyaltyMemberApprovalServer) RecallLoyaltyMember(ctx context.Context, req *loyaltyv1.RecallLoyaltyMemberRequest) (*loyaltyv1.RecallLoyaltyMemberResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "recall loyalty member", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	pipelineQuery, ok := s.clients.Get("acme.pipelines.v1.PipelineQueryService").(pipelinesv1.PipelineQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.pipelines.v1.PipelineQueryService")
	}
	labels := make(map[string]string, len(req.GetLoyaltyMember().GetLabels()))
	for key, value := range req.GetLoyaltyMember().GetLabels() {
		labels[key] = value
	}
	findPipelinesRequest := &pipelinesv1.FindPipelinesRequest{
		Context: req.GetContext(),
		Filter: &pipelinesv1.FindPipelinesRequest_Filter{
			Labels:       labels,
			CreatedAfter: req.GetLoyaltyMember().GetAudit().GetUpdatedAt(),
		},
	}
	findPipelinesReply, err := pipelineQuery.FindPipelines(ctx, findPipelinesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pipelines.v1.PipelineQueryService/FindPipelines")
	}
	cloned := proto.Clone(req.GetLoyaltyMember()).(*loyaltyv1.LoyaltyMember)
	ctx = tenancy.With(ctx, "display_name", req.GetLoyaltyMember().GetHolder().GetDisplayName())
	reinstateRewardRequest := &loyaltyv1.ReinstateRewardRequest{}
	reinstateRewardRequest.Context = req.GetContext()
	reinstateRewardRequest.RewardId = cloned.GetHolder().GetRewardId()
	reinstateRewardRequest.Details = &loyaltyv1.ReinstateRewardRequest_Details{
		NewOwner: &identityv1.UserRef{
			Email:       req.GetLoyaltyMember().GetContact().GetEmail(),
			DisplayName: tenancy.Value(ctx, "display_name"),
		},
		NotifyContact: &commonv1.ContactInfo{
			Email:            req.GetDetails().GetApprover().GetEmail(),
			PreferredChannel: req.GetLoyaltyMember().GetContact().GetPreferredChannel(),
		},
	}
	reinstateRewardReply, err := s.rewardLifecycleClient.ReinstateReward(ctx, reinstateRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/ReinstateReward")
	}
	for limit := range iterx.Each(req.GetLoyaltyMember().GetLimits(), func(candidate *loyaltyv1.LoyaltyMemberLimit) bool { return candidate != nil }) {
		if _, err := s.collectionWorkflowClient.ResumeCollectionWorkflow(ctx, &merchandisingv1.ResumeCollectionWorkflowRequest{
			Context: req.GetContext(),
			Collection: &merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
						Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{Name: limit.GetName()}},
					}},
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/ResumeCollectionWorkflow")
		}
	}
	return &loyaltyv1.RecallLoyaltyMemberResponse{
		Result: &commonv1.OperationResult{
			Details: findPipelinesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			Holder:          &loyaltyv1.RewardRef{RewardId: reinstateRewardReply.GetReward().GetRewardId()},
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
		},
		Outcome: &loyaltyv1.RecallLoyaltyMemberResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetLoyaltyMemberApprovalStatus returns a loyalty member.
func (s *LoyaltyMemberApprovalServer) GetLoyaltyMemberApprovalStatus(ctx context.Context, req *loyaltyv1.GetLoyaltyMemberApprovalStatusRequest) (*loyaltyv1.GetLoyaltyMemberApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetLoyaltyMemberApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "loyalty_member", req.GetLoyaltyMemberId(), "get_loyalty_member_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get loyalty member approval status", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	delegateGiftCardBatchApprovalRequest := &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.DelegateGiftCardBatchApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: cloned.GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
	}
	delegateGiftCardBatchApprovalReply := new(giftcardsv1.DelegateGiftCardBatchApprovalResponse)
	if err := grpcx.Invoke(ctx, s.conn, giftcardsv1.GiftCardBatchApprovalService_DelegateGiftCardBatchApproval_FullMethodName, delegateGiftCardBatchApprovalRequest, delegateGiftCardBatchApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	return &loyaltyv1.GetLoyaltyMemberApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Status: delegateGiftCardBatchApprovalReply.GetResult().GetStatus(),
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{LoyaltyMemberId: req.GetLoyaltyMemberId()},
	}, nil
}

// CommentLoyaltyMemberApproval comments a loyalty member.
func (s *LoyaltyMemberApprovalServer) CommentLoyaltyMemberApproval(ctx context.Context, req *loyaltyv1.CommentLoyaltyMemberApprovalRequest) (*loyaltyv1.CommentLoyaltyMemberApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentLoyaltyMemberApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveLoyaltyMember(ctx, req.GetContext().GetTenantId(), req.GetLoyaltyMember()); err != nil {
		return nil, grpcx.Wrap(err, "save loyalty member")
	}
	s.log.InfoContext(ctx, "comment loyalty member approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetLoyaltyMember().GetAudit().GetEtag())
	watchAccessPoliciesStream, err := s.accessPolicyClient.WatchAccessPolicies(ctx, &authorizationv1.WatchAccessPoliciesRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone:            req.GetLoyaltyMember().GetContact().GetPhone(),
			Name:             req.GetLoyaltyMember().GetContact().GetName(),
			PreferredChannel: req.GetLoyaltyMember().GetContact().GetPreferredChannel(),
			Email:            req.GetDetails().GetApprover().GetEmail(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
	}
	for {
		received, err := watchAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
		}
		_ = received
	}
	giftCardTransactionWorkflow, err := registry.Lookup[giftcardsv1.GiftCardTransactionWorkflowServiceClient](s.clients, "acme.giftcards.v1.GiftCardTransactionWorkflowService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	listGiftCardTransactionWorkflowsRequest := &giftcardsv1.ListGiftCardTransactionWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.ListGiftCardTransactionWorkflowsRequest_Filter{
			Labels:  req.GetLoyaltyMember().GetLabels(),
			OwnerId: req.GetLoyaltyMember().GetHolder().GetRewardId(),
		},
	}
	listGiftCardTransactionWorkflowsReply, err := giftCardTransactionWorkflow.ListGiftCardTransactionWorkflows(ctx, listGiftCardTransactionWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionWorkflowService/ListGiftCardTransactionWorkflows")
	}
	return &loyaltyv1.CommentLoyaltyMemberApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: listGiftCardTransactionWorkflowsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &loyaltyv1.CommentLoyaltyMemberApprovalResponse_Outcome{
			Message: listGiftCardTransactionWorkflowsReply.GetResult().GetMessage(),
		},
		LoyaltyMember: &loyaltyv1.LoyaltyMember{
			LoyaltyMemberId: req.GetLoyaltyMemberId(),
			Contact:         req.GetLoyaltyMember().GetContact(),
		},
	}, nil
}

// rewardValidationLazy dials acme.loyalty.v1.RewardValidationService the first time a handler needs it.
func (s *LoyaltyMemberApprovalServer) rewardValidationLazy() loyaltyv1.RewardValidationServiceClient {
	s.rewardValidationOnce.Do(func() { s.rewardValidationCached = loyaltyv1.NewRewardValidationServiceClient(s.conn) })
	return s.rewardValidationCached
}

// normalizeApiCredentialSender returns a function that sends one request to
// acme.identity.v1.ApiCredentialValidationService/NormalizeApiCredential.
func (s *LoyaltyMemberApprovalServer) normalizeApiCredentialSender(request *identityv1.NormalizeApiCredentialRequest) func(context.Context) (*identityv1.NormalizeApiCredentialResponse, error) {
	return func(ctx context.Context) (*identityv1.NormalizeApiCredentialResponse, error) {
		return s.apiCredentialValidationClient.NormalizeApiCredential(ctx, request)
	}
}
