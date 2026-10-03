package merchandising

import (
	"context"
	"fmt"
	"log/slog"

	checkoutv1 "github.com/hounddogai/protomap-test-golang/gen/acme/checkout/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CollectionWorkflowServer implements `acme.merchandising.v1.CollectionWorkflowService`.
// Runs the long-lived workflow that takes a collection from start to finish.
type CollectionWorkflowServer struct {
	merchandisingv1.UnimplementedCollectionWorkflowServiceServer
	store                             *Store
	log                               *slog.Logger
	sessionHistoryClient              identityv1.SessionHistoryServiceClient
	conn                              *grpc.ClientConn
	merchandisingRuleValidationClient merchandisingv1.MerchandisingRuleValidationServiceClient
	meteredPlacementApproval          meteredPlacementApproval
	giftCardBatchWorkflowClient       giftcardsv1.GiftCardBatchWorkflowServiceClient
	clients                           *registry.Registry
	bus                               *eventbus.Bus
	userQueryClient                   identityv1.UserQueryServiceClient
	collectionQueryClient             merchandisingv1.CollectionQueryServiceClient
	findShippingQuotesFn              func(context.Context, *checkoutv1.FindShippingQuotesRequest, ...grpc.CallOption) (*checkoutv1.FindShippingQuotesResponse, error)
}

// NewCollectionWorkflowServer wires a CollectionWorkflowServer.
func NewCollectionWorkflowServer(deps platform.Deps) *CollectionWorkflowServer {
	shippingQuoteQuery := checkoutv1.NewShippingQuoteQueryServiceClient(deps.Conn)
	return &CollectionWorkflowServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.merchandising.v1.CollectionWorkflowService"),
		sessionHistoryClient:              identityv1.NewSessionHistoryServiceClient(deps.Conn),
		conn:                              deps.Conn,
		merchandisingRuleValidationClient: merchandisingv1.NewMerchandisingRuleValidationServiceClient(deps.Conn),
		meteredPlacementApproval:          meteredPlacementApproval{PlacementApprovalServiceClient: merchandisingv1.NewPlacementApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		giftCardBatchWorkflowClient:       giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
		clients:                           deps.Clients,
		bus:                               deps.Bus,
		userQueryClient:                   identityv1.NewUserQueryServiceClient(deps.Conn),
		collectionQueryClient:             merchandisingv1.NewCollectionQueryServiceClient(deps.Conn),
		findShippingQuotesFn:              shippingQuoteQuery.FindShippingQuotes,
	}
}

// StartCollectionWorkflow starts a collection.
func (s *CollectionWorkflowServer) StartCollectionWorkflow(ctx context.Context, req *merchandisingv1.StartCollectionWorkflowRequest) (*merchandisingv1.StartCollectionWorkflowResponse, error) {
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "start collection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "created_by", req.GetCollection().GetAudit().GetCreatedBy())
	snapshot, err := protojson.Marshal(req.GetCollection())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	step := &collectionWorkflowServerStartCollectionWorkflowCompareSessionVersionsStep{request: &identityv1.CompareSessionVersionsRequest{
		Context: req.GetContext(),
		Session: &identityv1.Session{
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetCollection().GetAudit().GetCreatedAt(),
				Etag:      req.GetCollection().GetAudit().GetEtag(),
				CreatedBy: tenancy.Value(ctx, "created_by"),
			},
			Origin: &identityv1.Session_Customer{Customer: &identityv1.ApiCredentialRef{
				DisplayName: req.GetCollection().GetManager().GetDisplayName(),
			}},
		},
		Details: &identityv1.CompareSessionVersionsRequest_Details{Snapshot: snapshot},
	}}
	if err := s.runStartCollectionWorkflowCompareSessionVersions(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionHistoryService/CompareSessionVersions")
	}
	compareSessionVersionsReply := step.reply
	return &merchandisingv1.StartCollectionWorkflowResponse{
		Outcome: &merchandisingv1.StartCollectionWorkflowResponse_Outcome{
			Message: compareSessionVersionsReply.GetResult().GetMessage(),
		},
		Collection: &merchandisingv1.Collection{
			CollectionId: req.GetCollectionId(),
			Attributes:   req.GetCollection().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceCollectionWorkflow advances a collection.
func (s *CollectionWorkflowServer) AdvanceCollectionWorkflow(ctx context.Context, req *merchandisingv1.AdvanceCollectionWorkflowRequest) (*merchandisingv1.AdvanceCollectionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "advance collection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	previewMerchandisingRuleRequest := &merchandisingv1.PreviewMerchandisingRuleRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetCollection().GetAudit().GetCreatedAt()},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetCollection().GetManager().GetDisplayName(),
			},
		},
	}
	previewMerchandisingRuleReply := new(merchandisingv1.PreviewMerchandisingRuleResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", merchandisingMerchandisingRuleValidationServiceName, "PreviewMerchandisingRule"), previewMerchandisingRuleRequest, previewMerchandisingRuleReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/PreviewMerchandisingRule")
	}
	names := collectCollectionNames(req.GetCollection(), nil)
	checkMerchandisingRuleEligibilityRequest := &merchandisingv1.CheckMerchandisingRuleEligibilityRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Action: &merchandisingv1.MerchandisingRule_Block{Block: &merchandisingv1.MerchandisingRuleBlockAction{
				NotifyEmail: previewMerchandisingRuleReply.GetMerchandisingRule().GetBlock().GetNotifyEmail(),
			}},
			Audit:  &commonv1.AuditInfo{UpdatedBy: req.GetCollection().GetAudit().GetUpdatedBy()},
			Scopes: names,
		},
		Details: &merchandisingv1.CheckMerchandisingRuleEligibilityRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: req.GetDetails().GetNotify().GetPhone()},
		},
	}
	send := s.checkMerchandisingRuleEligibilitySender(checkMerchandisingRuleEligibilityRequest)
	checkMerchandisingRuleEligibilityReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/CheckMerchandisingRuleEligibility")
	}
	names2 := collectCollectionNames(req.GetCollection(), nil)
	commentSellerApprovalRequest := &sellersv1.CommentSellerApprovalRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetCollection().GetManager().GetDisplayName(),
			},
			PrimaryAddress: &commonv1.PostalAddress{Lines: names2},
		},
		Details: &sellersv1.CommentSellerApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: previewMerchandisingRuleReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
		},
	}
	commentSellerApprovalReply, err := grpcx.NewClient(s.conn, sellersv1.NewSellerApprovalServiceClient).CommentSellerApproval(ctx, commentSellerApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/CommentSellerApproval")
	}
	names3 := collectCollectionNames(req.GetCollection(), nil)
	delegatePlacementApprovalRequest := &merchandisingv1.DelegatePlacementApprovalRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Email: checkMerchandisingRuleEligibilityReply.GetMerchandisingRule().GetOwner().GetEmail(),
				},
			}},
			Location: &commonv1.PostalAddress{Lines: names3},
		},
	}
	delegatePlacementApprovalReply, err := s.meteredPlacementApproval.DelegatePlacementApproval(ctx, delegatePlacementApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementApprovalService/DelegatePlacementApproval")
	}
	return &merchandisingv1.AdvanceCollectionWorkflowResponse{
		Collection: &merchandisingv1.Collection{
			Manager: &identityv1.UserRef{
				DisplayName: previewMerchandisingRuleReply.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: checkMerchandisingRuleEligibilityReply.GetMerchandisingRule().GetAudit().GetEtag(),
			},
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Location: &commonv1.PostalAddress{
					Locality: commentSellerApprovalReply.GetSeller().GetPrimaryAddress().GetLocality(),
				},
			}},
			CollectionId: req.GetCollectionId(),
			Attributes:   req.GetCollection().GetAttributes(),
		},
		Outcome: &merchandisingv1.AdvanceCollectionWorkflowResponse_Outcome{
			EffectiveAt: previewMerchandisingRuleReply.GetOutcome().GetEffectiveAt(),
			NextSteps:   delegatePlacementApprovalReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelCollectionWorkflow cancels a collection.
func (s *CollectionWorkflowServer) CancelCollectionWorkflow(ctx context.Context, req *merchandisingv1.CancelCollectionWorkflowRequest) (*merchandisingv1.CancelCollectionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "cancel collection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	pauseGiftCardBatchWorkflowRequest := buildPauseGiftCardBatchWorkflowRequestForCancelCollectionWorkflow(req)
	pauseGiftCardBatchWorkflowReply, err := s.giftCardBatchWorkflowClient.PauseGiftCardBatchWorkflow(ctx, pauseGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/PauseGiftCardBatchWorkflow")
	}
	cloned := proto.Clone(req.GetCollection()).(*merchandisingv1.Collection)
	names := collectCollectionNames(req.GetCollection(), nil)
	validateMerchandisingRuleRequest := &merchandisingv1.ValidateMerchandisingRuleRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetCollection().GetAudit().GetEtag(),
				UpdatedBy: pauseGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
				CreatedAt: cloned.GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: pauseGiftCardBatchWorkflowReply.GetGiftCardBatch().GetCounterparty().GetDisplayName(),
			},
			Scopes: names,
		},
	}
	validateMerchandisingRuleReply, err := merchandisingv1.MerchandisingRuleValidationServiceClient.ValidateMerchandisingRule(s.merchandisingRuleValidationClient, ctx, validateMerchandisingRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/ValidateMerchandisingRule")
	}
	return &merchandisingv1.CancelCollectionWorkflowResponse{
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						CreatedAt: pauseGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
					},
					Manager: &identityv1.UserRef{
						Email: validateMerchandisingRuleReply.GetMerchandisingRule().GetOwner().GetEmail(),
					},
				}},
				Audit: &commonv1.AuditInfo{
					CreatedAt: validateMerchandisingRuleReply.GetMerchandisingRule().GetAudit().GetCreatedAt(),
				},
			}},
			Location: &commonv1.PostalAddress{
				Region: pauseGiftCardBatchWorkflowReply.GetGiftCardBatch().GetBillingAddress().GetRegion(),
			},
			CollectionId: req.GetCollectionId(),
			Attributes:   req.GetCollection().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryCollectionWorkflow retrys a collection.
func (s *CollectionWorkflowServer) RetryCollectionWorkflow(ctx context.Context, req *merchandisingv1.RetryCollectionWorkflowRequest) (*merchandisingv1.RetryCollectionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryCollectionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "retry collection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	sellerWorkflow, err := registry.Lookup[sellersv1.SellerWorkflowServiceClient](s.clients, "acme.sellers.v1.SellerWorkflowService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotify().GetPhone())
	encoded, err := protojson.Marshal(req.GetCollection())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded merchandisingv1.Collection
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	names := collectCollectionNames(req.GetCollection(), nil)
	resumeSellerWorkflowRequest := &sellersv1.ResumeSellerWorkflowRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Contact: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: (&decoded).GetLocation().GetLocality(),
				Lines:    names,
			},
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetCollection().GetAudit().GetCreatedBy()},
		},
	}
	resumeSellerWorkflowReply, err := sellerWorkflow.ResumeSellerWorkflow(ctx, resumeSellerWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerWorkflowService/ResumeSellerWorkflow")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "merchandising.retry_collection_workflow.count_placements",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     resumeSellerWorkflowReply.GetSeller().GetPrimaryAddress().GetRegion(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &merchandisingv1.RetryCollectionWorkflowResponse{
		Collection: &merchandisingv1.Collection{
			Audit: &commonv1.AuditInfo{
				Etag: resumeSellerWorkflowReply.GetSeller().GetAudit().GetEtag(),
			},
			CollectionId: req.GetCollectionId(),
			Location:     req.GetCollection().GetLocation(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// PauseCollectionWorkflow pauses a collection.
func (s *CollectionWorkflowServer) PauseCollectionWorkflow(ctx context.Context, req *merchandisingv1.PauseCollectionWorkflowRequest) (*merchandisingv1.PauseCollectionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseCollectionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "pause collection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "user_id", req.GetCollection().GetManager().GetUserId())
	peekUserRequest := newPeekUserRequest(withPeekUserContext(req.GetContext()), withPeekUserUserId(tenancy.Value(ctx, "user_id")))
	peekUserReply, err := s.userQueryClient.PeekUser(ctx, peekUserRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserQueryService/PeekUser")
	}
	sellerApproval, err := registry.Lookup[sellersv1.SellerApprovalServiceClient](s.clients, "acme.sellers.v1.SellerApprovalService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	collectionCache := cache.New[string, *merchandisingv1.Collection](1)
	collectionCache.Put(req.GetContext().GetTraceId(), req.GetCollection())
	cachedCollection, _ := collectionCache.Get(req.GetContext().GetTraceId())
	names := collectCollectionNames(req.GetCollection(), nil)
	commentSellerApprovalRequest := &sellersv1.CommentSellerApprovalRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			PrimaryAddress: &commonv1.PostalAddress{
				Region:      peekUserReply.GetUser().GetPrimaryAddress().GetRegion(),
				CountryCode: peekUserReply.GetUser().GetPrimaryAddress().GetCountryCode(),
				Lines:       names,
			},
			RelationshipManager: &identityv1.UserRef{UserId: cachedCollection.GetManager().GetUserId()},
			IdentityDocument:    &sellersv1.Seller_Passport{Passport: &sellersv1.SellerPassport{Number: peekUserReply.GetUser().GetPassport().GetNumber()}},
		},
	}
	commentSellerApprovalReply, err := sellerApproval.CommentSellerApproval(ctx, commentSellerApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerApprovalService/CommentSellerApproval")
	}
	return &merchandisingv1.PauseCollectionWorkflowResponse{
		Collection: &merchandisingv1.Collection{
			Audit: &commonv1.AuditInfo{
				CreatedAt: peekUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Manager: &identityv1.UserRef{Email: peekUserReply.GetUser().GetContact().GetEmail()},
			}},
			Location: &commonv1.PostalAddress{
				Region: commentSellerApprovalReply.GetSeller().GetPrimaryAddress().GetRegion(),
			},
			CollectionId: req.GetCollectionId(),
			Status:       req.GetCollection().GetStatus(),
		},
		Outcome: &merchandisingv1.PauseCollectionWorkflowResponse_Outcome{Status: commentSellerApprovalReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeCollectionWorkflow resumes a collection.
func (s *CollectionWorkflowServer) ResumeCollectionWorkflow(ctx context.Context, req *merchandisingv1.ResumeCollectionWorkflowRequest) (*merchandisingv1.ResumeCollectionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeCollectionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "resume collection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	summarizeCollectionsRequest := buildSummarizeCollectionsRequestForResumeCollectionWorkflow(req)
	summarizeCollectionsReply, err := s.collectionQueryClient.SummarizeCollections(ctx, summarizeCollectionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionQueryService/SummarizeCollections")
	}
	explainMerchandisingRuleDecisionRequest := buildExplainMerchandisingRuleDecisionRequestForResumeCollectionWorkflow(req)
	explainMerchandisingRuleDecisionReply, err := s.merchandisingRuleValidationClient.ExplainMerchandisingRuleDecision(ctx, explainMerchandisingRuleDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleValidationService/ExplainMerchandisingRuleDecision")
	}
	return &merchandisingv1.ResumeCollectionWorkflowResponse{
		Result: &commonv1.OperationResult{Status: summarizeCollectionsReply.GetResult().GetStatus()},
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
						Name: explainMerchandisingRuleDecisionReply.GetMerchandisingRule().GetName(),
					}},
				}},
				Name: explainMerchandisingRuleDecisionReply.GetMerchandisingRule().GetName(),
			}},
			CollectionId: req.GetCollectionId(),
			Depth:        req.GetCollection().GetDepth(),
		},
	}, nil
}

// ListCollectionWorkflows lists collections a page at a time.
func (s *CollectionWorkflowServer) ListCollectionWorkflows(ctx context.Context, req *merchandisingv1.ListCollectionWorkflowsRequest) (*merchandisingv1.ListCollectionWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list collection workflows", "tenant", req.GetContext().GetTenantId())
	return &merchandisingv1.ListCollectionWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalCollectionWorkflow signals a collection.
func (s *CollectionWorkflowServer) SignalCollectionWorkflow(ctx context.Context, req *merchandisingv1.SignalCollectionWorkflowRequest) (*merchandisingv1.SignalCollectionWorkflowResponse, error) {
	if err := s.store.SaveCollection(ctx, req.GetContext().GetTenantId(), req.GetCollection()); err != nil {
		return nil, grpcx.Wrap(err, "save collection")
	}
	s.log.InfoContext(ctx, "signal collection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCollection().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCollection())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded merchandisingv1.Collection
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	findShippingQuotesRequest := &checkoutv1.FindShippingQuotesRequest{
		Context: req.GetContext(),
		Filter: &checkoutv1.FindShippingQuotesRequest_Filter{
			Region:       (&decoded).GetLocation().GetRegion(),
			CreatedAfter: req.GetCollection().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetCollection().GetParentId(),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
	findShippingQuotesReply, err := s.findShippingQuotesFn(ctx, findShippingQuotesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.checkout.v1.ShippingQuoteQueryService/FindShippingQuotes")
	}
	return &merchandisingv1.SignalCollectionWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: findShippingQuotesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &merchandisingv1.SignalCollectionWorkflowResponse_Outcome{Status: findShippingQuotesReply.GetResult().GetStatus()},
		Collection: &merchandisingv1.Collection{
			CollectionId: req.GetCollectionId(),
			Depth:        req.GetCollection().GetDepth(),
		},
	}, nil
}

// collectionWorkflowServerStartCollectionWorkflowCompareSessionVersionsStep carries the request of one step of
// StartCollectionWorkflow and, once it ran, its reply.
type collectionWorkflowServerStartCollectionWorkflowCompareSessionVersionsStep struct {
	request *identityv1.CompareSessionVersionsRequest
	reply   *identityv1.CompareSessionVersionsResponse
}

func (s *CollectionWorkflowServer) runStartCollectionWorkflowCompareSessionVersions(ctx context.Context, step *collectionWorkflowServerStartCollectionWorkflowCompareSessionVersionsStep) error {
	reply, err := s.sessionHistoryClient.CompareSessionVersions(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// checkMerchandisingRuleEligibilitySender returns a function that sends one request to
// acme.merchandising.v1.MerchandisingRuleValidationService/CheckMerchandisingRuleEligibility.
func (s *CollectionWorkflowServer) checkMerchandisingRuleEligibilitySender(request *merchandisingv1.CheckMerchandisingRuleEligibilityRequest) func(context.Context) (*merchandisingv1.CheckMerchandisingRuleEligibilityResponse, error) {
	return func(ctx context.Context) (*merchandisingv1.CheckMerchandisingRuleEligibilityResponse, error) {
		return s.merchandisingRuleValidationClient.CheckMerchandisingRuleEligibility(ctx, request)
	}
}
