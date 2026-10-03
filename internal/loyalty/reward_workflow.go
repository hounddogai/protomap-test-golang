package loyalty

import (
	"context"
	"log/slog"

	audiencesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audiences/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	localizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/localization/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RewardWorkflowServer implements `acme.loyalty.v1.RewardWorkflowService`.
// Runs the long-lived workflow that takes a reward from start to finish.
type RewardWorkflowServer struct {
	loyaltyv1.UnimplementedRewardWorkflowServiceServer
	store                                 *Store
	log                                   *slog.Logger
	conn                                  *grpc.ClientConn
	synonymHistoryClient                  searchv1.SynonymHistoryServiceClient
	clients                               *registry.Registry
	pointsLedgerCalculationClient         loyaltyv1.PointsLedgerCalculationServiceClient
	pointsLedgerCalculationClientFallback loyaltyv1.PointsLedgerCalculationServiceClient
	pointsLedgerWorkflowClient            loyaltyv1.PointsLedgerWorkflowServiceClient
	pipelineCalculationClient             pipelinesv1.PipelineCalculationServiceClient
	pipelineCalculationClientFallback     pipelinesv1.PipelineCalculationServiceClient
	audienceQueryClient                   audiencesv1.AudienceQueryServiceClient
	rewardLifecycleClient                 loyaltyv1.RewardLifecycleServiceClient
	loyaltyMemberApprovalClient           loyaltyv1.LoyaltyMemberApprovalServiceClient
	collectionWorkflowClient              merchandisingv1.CollectionWorkflowServiceClient
}

// NewRewardWorkflowServer wires a RewardWorkflowServer.
func NewRewardWorkflowServer(deps platform.Deps) *RewardWorkflowServer {
	return &RewardWorkflowServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.loyalty.v1.RewardWorkflowService"),
		conn:                                  deps.Conn,
		synonymHistoryClient:                  searchv1.NewSynonymHistoryServiceClient(deps.Conn),
		clients:                               deps.Clients,
		pointsLedgerCalculationClient:         loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		pointsLedgerCalculationClientFallback: loyaltyv1.NewPointsLedgerCalculationServiceClient(deps.Conn),
		pointsLedgerWorkflowClient:            loyaltyv1.NewPointsLedgerWorkflowServiceClient(deps.Conn),
		pipelineCalculationClient:             pipelinesv1.NewPipelineCalculationServiceClient(deps.Conn),
		pipelineCalculationClientFallback:     pipelinesv1.NewPipelineCalculationServiceClient(deps.Conn),
		audienceQueryClient:                   audiencesv1.NewAudienceQueryServiceClient(deps.Conn),
		rewardLifecycleClient:                 loyaltyv1.NewRewardLifecycleServiceClient(deps.Conn),
		loyaltyMemberApprovalClient:           loyaltyv1.NewLoyaltyMemberApprovalServiceClient(deps.Conn),
		collectionWorkflowClient:              merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
	}
}

// StartRewardWorkflow starts a reward.
func (s *RewardWorkflowServer) StartRewardWorkflow(ctx context.Context, req *loyaltyv1.StartRewardWorkflowRequest) (*loyaltyv1.StartRewardWorkflowResponse, error) {
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "start reward workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	source := req.GetReward().ProtoReflect()
	rewardId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("reward_id"))).String()
	browseTranslationKeysRequest := &localizationv1.BrowseTranslationKeysRequest{
		Context: req.GetContext(),
		Filter: &localizationv1.BrowseTranslationKeysRequest_Filter{
			CreatedAfter: req.GetReward().GetAudit().GetCreatedAt(),
			Labels:       req.GetDetails().GetParameters(),
			OwnerId:      rewardId,
		},
	}
	browseTranslationKeysReply := new(localizationv1.BrowseTranslationKeysResponse)
	if err := s.conn.Invoke(ctx, localizationTranslationKeyQueryServiceBrowseTranslationKeysMethod, browseTranslationKeysRequest, browseTranslationKeysReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.localization.v1.TranslationKeyQueryService/BrowseTranslationKeys")
	}
	return &loyaltyv1.StartRewardWorkflowResponse{
		Outcome: &loyaltyv1.StartRewardWorkflowResponse_Outcome{
			Status:  browseTranslationKeysReply.GetResult().GetStatus(),
			Message: browseTranslationKeysReply.GetResult().GetMessage(),
		},
		Reward: &loyaltyv1.Reward{
			RewardId:    req.GetRewardId(),
			Description: req.GetReward().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceRewardWorkflow advances a reward.
func (s *RewardWorkflowServer) AdvanceRewardWorkflow(ctx context.Context, req *loyaltyv1.AdvanceRewardWorkflowRequest) (*loyaltyv1.AdvanceRewardWorkflowResponse, error) {
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "advance reward workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	listSynonymVersionsRequest := &searchv1.ListSynonymVersionsRequest{}
	listSynonymVersionsRequest.Context = req.GetContext()
	listSynonymVersionsRequest.Filter = &searchv1.ListSynonymVersionsRequest_Filter{
		CreatedAfter: req.GetReward().GetAudit().GetCreatedAt(),
		Labels:       req.GetAnnotations(),
		OwnerId:      req.GetReward().GetRewardId(),
	}
	listSynonymVersionsReply, err := s.synonymHistoryClient.ListSynonymVersions(ctx, listSynonymVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/ListSynonymVersions")
	}
	rewardValidation, err := registry.Lookup[loyaltyv1.RewardValidationServiceClient](s.clients, "acme.loyalty.v1.RewardValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	normalizeRewardRequest := &loyaltyv1.NormalizeRewardRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Audit:  &commonv1.AuditInfo{Etag: req.GetReward().GetAudit().GetEtag()},
			Action: &loyaltyv1.Reward_Route{Route: &loyaltyv1.RewardRouteAction{Target: req.GetReward().GetRoute().GetTarget()}},
		},
	}
	normalizeRewardReply, err := rewardValidation.NormalizeReward(ctx, normalizeRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardValidationService/NormalizeReward")
	}
	return &loyaltyv1.AdvanceRewardWorkflowResponse{
		Outcome: &loyaltyv1.AdvanceRewardWorkflowResponse_Outcome{Status: listSynonymVersionsReply.GetResult().GetStatus()},
		Reward: &loyaltyv1.Reward{
			RewardId: normalizeRewardReply.GetReward().GetRewardId(),
			Owner: &identityv1.UserRef{
				Email: normalizeRewardReply.GetReward().GetOwner().GetEmail(),
			},
			Status:   req.GetReward().GetStatus(),
			Priority: req.GetReward().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelRewardWorkflow cancels a reward.
func (s *RewardWorkflowServer) CancelRewardWorkflow(ctx context.Context, req *loyaltyv1.CancelRewardWorkflowRequest) (*loyaltyv1.CancelRewardWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "cancel reward workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *loyaltyv1.RecalculatePointsLedgersRequest, ...grpc.CallOption) (*loyaltyv1.RecalculatePointsLedgersResponse, error){
		"primary":  s.pointsLedgerCalculationClient.RecalculatePointsLedgers,
		"fallback": s.pointsLedgerCalculationClientFallback.RecalculatePointsLedgers,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	recalculatePointsLedgersRequest := &loyaltyv1.RecalculatePointsLedgersRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetReward().GetAudit().GetCreatedBy(),
				Etag:      req.GetReward().GetAudit().GetEtag(),
			},
			Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{Amount: req.GetReward().GetDiscount().GetAmount()}},
		},
	}
	recalculatePointsLedgersReply, err := route(ctx, recalculatePointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerCalculationService/RecalculatePointsLedgers")
	}
	detailsCache := cache.New[string, *loyaltyv1.CancelRewardWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	startPointsLedgerWorkflowReply, err := s.pointsLedgerWorkflowClient.StartPointsLedgerWorkflow(ctx, &loyaltyv1.StartPointsLedgerWorkflowRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Contact: &commonv1.ContactInfo{
				Email:            recalculatePointsLedgersReply.GetPointsLedger().GetContact().GetEmail(),
				PreferredChannel: cachedDetails.GetNotify().GetPreferredChannel(),
			},
			Funding: &loyaltyv1.PointsLedger_Card{Card: &loyaltyv1.PointsLedgerCardFunding{
				Network: recalculatePointsLedgersReply.GetPointsLedger().GetCard().GetNetwork(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerWorkflowService/StartPointsLedgerWorkflow")
	}
	return &loyaltyv1.CancelRewardWorkflowResponse{
		Reward: &loyaltyv1.Reward{
			Version: recalculatePointsLedgersReply.GetPointsLedger().GetHolder().GetVersion(),
			Audit: &commonv1.AuditInfo{
				UpdatedAt: recalculatePointsLedgersReply.GetPointsLedger().GetAudit().GetUpdatedAt(),
			},
			RewardId: req.GetRewardId(),
			Name:     req.GetReward().GetName(),
		},
		Outcome: &loyaltyv1.CancelRewardWorkflowResponse_Outcome{
			Status: startPointsLedgerWorkflowReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseRewardWorkflow pauses a reward.
func (s *RewardWorkflowServer) PauseRewardWorkflow(ctx context.Context, req *loyaltyv1.PauseRewardWorkflowRequest) (*loyaltyv1.PauseRewardWorkflowResponse, error) {
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "pause reward workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	routes := map[string]func(context.Context, *pipelinesv1.RecalculatePipelinesRequest, ...grpc.CallOption) (*pipelinesv1.RecalculatePipelinesResponse, error){
		"primary":  s.pipelineCalculationClient.RecalculatePipelines,
		"fallback": s.pipelineCalculationClientFallback.RecalculatePipelines,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	recalculatePipelinesRequest := &pipelinesv1.RecalculatePipelinesRequest{
		Context: req.GetContext(),
		Pipeline: &pipelinesv1.Pipeline{
			Participants: []*pipelinesv1.PipelineParticipant{&pipelinesv1.PipelineParticipant{
				Contact: &commonv1.ContactInfo{
					Email:            req.GetReward().GetOwner().GetEmail(),
					PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				},
			}},
		},
	}
	recalculatePipelinesReply, err := route(ctx, recalculatePipelinesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.pipelines.v1.PipelineCalculationService/RecalculatePipelines")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := rewardWorkflowServerSuggestAudiencesSteps[mode]
	suggestAudiencesRequest := &audiencesv1.SuggestAudiencesRequest{
		Context: req.GetContext(),
		Filter: &audiencesv1.SuggestAudiencesRequest_Filter{
			Region: recalculatePipelinesReply.GetPipeline().GetLocation().GetRegion(),
		},
	}
	suggestAudiencesReply, err := run(s, ctx, suggestAudiencesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audiences.v1.AudienceQueryService/SuggestAudiences")
	}
	terminateRewardRequest := &loyaltyv1.TerminateRewardRequest{}
	terminateRewardRequest.Context = req.GetContext()
	terminateRewardRequest.RewardId = req.GetReward().GetRewardId()
	terminateRewardRequest.Details = &loyaltyv1.TerminateRewardRequest_Details{
		NewOwner: &identityv1.UserRef{
			DisplayName: req.GetReward().GetOwner().GetDisplayName(),
			Email:       req.GetDetails().GetNotify().GetEmail(),
		},
		NotifyContact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
	}
	terminateRewardReply, err := s.rewardLifecycleClient.TerminateReward(ctx, terminateRewardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardLifecycleService/TerminateReward")
	}
	return &loyaltyv1.PauseRewardWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message:       recalculatePipelinesReply.GetResult().GetMessage(),
			Status:        recalculatePipelinesReply.GetResult().GetStatus(),
			ReferenceIds:  suggestAudiencesReply.GetResult().GetReferenceIds(),
			CorrelationId: terminateRewardReply.GetResult().GetCorrelationId(),
		},
		Reward: &loyaltyv1.Reward{
			Audit: &commonv1.AuditInfo{
				CreatedBy: terminateRewardReply.GetReward().GetAudit().GetCreatedBy(),
			},
			RewardId: req.GetRewardId(),
			Status:   req.GetReward().GetStatus(),
		},
	}, nil
}

// ResumeRewardWorkflow resumes a reward.
func (s *RewardWorkflowServer) ResumeRewardWorkflow(ctx context.Context, req *loyaltyv1.ResumeRewardWorkflowRequest) (*loyaltyv1.ResumeRewardWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "resume reward workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	cancelPointsLedgerWorkflowRequest := &loyaltyv1.CancelPointsLedgerWorkflowRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit:  &commonv1.AuditInfo{UpdatedAt: req.GetReward().GetAudit().GetUpdatedAt()},
			Limits: []*loyaltyv1.PointsLedgerLimit{&loyaltyv1.PointsLedgerLimit{Amount: req.GetReward().GetDiscount().GetAmount()}},
		},
	}
	type cancelPointsLedgerWorkflowResult struct {
		reply *loyaltyv1.CancelPointsLedgerWorkflowResponse
		err   error
	}
	cancelPointsLedgerWorkflowResults := make(chan cancelPointsLedgerWorkflowResult, 1)
	go func() {
		reply, err := s.pointsLedgerWorkflowClient.CancelPointsLedgerWorkflow(ctx, cancelPointsLedgerWorkflowRequest)
		cancelPointsLedgerWorkflowResults <- cancelPointsLedgerWorkflowResult{reply, err}
	}()
	var cancelPointsLedgerWorkflowReply *loyaltyv1.CancelPointsLedgerWorkflowResponse
	select {
	case result := <-cancelPointsLedgerWorkflowResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.loyalty.v1.PointsLedgerWorkflowService/CancelPointsLedgerWorkflow")
		}
		cancelPointsLedgerWorkflowReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &loyaltyv1.ResumeRewardWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: cancelPointsLedgerWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Reward: &loyaltyv1.Reward{
			RewardId: req.GetRewardId(),
			Owner:    req.GetReward().GetOwner(),
		},
	}, nil
}

// InspectRewardWorkflow returns a reward.
func (s *RewardWorkflowServer) InspectRewardWorkflow(ctx context.Context, req *loyaltyv1.InspectRewardWorkflowRequest) (*loyaltyv1.InspectRewardWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "reward", req.GetRewardId(), "inspect_reward_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect reward workflow", "tenant", req.GetContext().GetTenantId())
	return &loyaltyv1.InspectRewardWorkflowResponse{
		Reward: &loyaltyv1.Reward{RewardId: req.GetRewardId()},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListRewardWorkflows lists rewards a page at a time.
func (s *RewardWorkflowServer) ListRewardWorkflows(ctx context.Context, req *loyaltyv1.ListRewardWorkflowsRequest) (*loyaltyv1.ListRewardWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListRewardWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list reward workflows", "tenant", req.GetContext().GetTenantId())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	recallLoyaltyMemberRequest := &loyaltyv1.RecallLoyaltyMemberRequest{
		Context:       req.GetContext(),
		LoyaltyMember: &loyaltyv1.LoyaltyMember{Labels: labels},
	}
	recallLoyaltyMemberReply, err := grpcx.Call(ctx, s.loyaltyMemberApprovalClient.RecallLoyaltyMember, recallLoyaltyMemberRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.LoyaltyMemberApprovalService/RecallLoyaltyMember")
	}
	return &loyaltyv1.ListRewardWorkflowsResponse{
		Rewards: []*loyaltyv1.Reward{&loyaltyv1.Reward{
			RewardId: recallLoyaltyMemberReply.GetLoyaltyMember().GetHolder().GetRewardId(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalRewardWorkflow signals a reward.
func (s *RewardWorkflowServer) SignalRewardWorkflow(ctx context.Context, req *loyaltyv1.SignalRewardWorkflowRequest) (*loyaltyv1.SignalRewardWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalRewardWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveReward(ctx, req.GetContext().GetTenantId(), req.GetReward()); err != nil {
		return nil, grpcx.Wrap(err, "save reward")
	}
	s.log.InfoContext(ctx, "signal reward workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReward().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetReward())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded loyaltyv1.Reward
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	signalCollectionWorkflowRequest := &merchandisingv1.SignalCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						CreatedBy: req.GetReward().GetAudit().GetCreatedBy(),
						Etag:      req.GetReward().GetAudit().GetEtag(),
						UpdatedAt: (&decoded).GetAudit().GetUpdatedAt(),
					},
				}},
				Manager: &identityv1.UserRef{UserId: req.GetReward().GetOwner().GetUserId()},
			}},
		},
	}
	signalCollectionWorkflowReply, err := merchandisingv1.CollectionWorkflowServiceClient.SignalCollectionWorkflow(s.collectionWorkflowClient, ctx, signalCollectionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/SignalCollectionWorkflow")
	}
	return &loyaltyv1.SignalRewardWorkflowResponse{
		Outcome: &loyaltyv1.SignalRewardWorkflowResponse_Outcome{
			Status: signalCollectionWorkflowReply.GetOutcome().GetStatus(),
		},
		Reward: &loyaltyv1.Reward{
			RewardId:   req.GetRewardId(),
			Parameters: req.GetDetails().GetParameters(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// rewardWorkflowServerSuggestAudiencesSteps are the ways to run
// acme.audiences.v1.AudienceQueryService/SuggestAudiences, by name.
var rewardWorkflowServerSuggestAudiencesSteps = map[string]func(*RewardWorkflowServer, context.Context, *audiencesv1.SuggestAudiencesRequest) (*audiencesv1.SuggestAudiencesResponse, error){
	"default": (*RewardWorkflowServer).suggestAudiencesStep,
	"audited": (*RewardWorkflowServer).suggestAudiencesAuditedStep,
}

func (s *RewardWorkflowServer) suggestAudiencesStep(ctx context.Context, request *audiencesv1.SuggestAudiencesRequest) (*audiencesv1.SuggestAudiencesResponse, error) {
	return s.audienceQueryClient.SuggestAudiences(ctx, request)
}

func (s *RewardWorkflowServer) suggestAudiencesAuditedStep(ctx context.Context, request *audiencesv1.SuggestAudiencesRequest) (*audiencesv1.SuggestAudiencesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.audiences.v1.AudienceQueryService/SuggestAudiences")
	return s.audienceQueryClient.SuggestAudiences(ctx, request)
}
