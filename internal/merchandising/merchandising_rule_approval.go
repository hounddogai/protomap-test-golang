package merchandising

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// MerchandisingRuleApprovalServer implements `acme.merchandising.v1.MerchandisingRuleApprovalService`.
// Routes merchandising rules through approval, with escalation and delegation.
type MerchandisingRuleApprovalServer struct {
	merchandisingv1.UnimplementedMerchandisingRuleApprovalServiceServer
	store                        *Store
	log                          *slog.Logger
	ledgerAccountQueryClient     ledgerv1.LedgerAccountQueryServiceClient
	bus                          *eventbus.Bus
	giftCardBatchWorkflowClient  giftcardsv1.GiftCardBatchWorkflowServiceClient
	collectionClient             merchandisingv1.CollectionServiceClient
	clients                      *registry.Registry
	collectionWorkflowClient     merchandisingv1.CollectionWorkflowServiceClient
	conn                         *grpc.ClientConn
	sellerListingReportingOnce   sync.Once
	sellerListingReportingCached sellersv1.SellerListingReportingServiceClient
	collectionApprovalClient     merchandisingv1.CollectionApprovalServiceClient
	synonymValidationClient      searchv1.SynonymValidationServiceClient
	collectionQueryClient        merchandisingv1.CollectionQueryServiceClient
	ledgerAccountClient          ledgerv1.LedgerAccountServiceClient
	sellerLifecycleClient        sellersv1.SellerLifecycleServiceClient
	meteredPlacementApproval     meteredPlacementApproval
}

// NewMerchandisingRuleApprovalServer wires a MerchandisingRuleApprovalServer.
func NewMerchandisingRuleApprovalServer(deps platform.Deps) *MerchandisingRuleApprovalServer {
	return &MerchandisingRuleApprovalServer{
		store:                       NewStore(deps.DB),
		log:                         deps.Logger.With("service", "acme.merchandising.v1.MerchandisingRuleApprovalService"),
		ledgerAccountQueryClient:    ledgerv1.NewLedgerAccountQueryServiceClient(deps.Conn),
		bus:                         deps.Bus,
		giftCardBatchWorkflowClient: giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
		collectionClient:            merchandisingv1.NewCollectionServiceClient(deps.Conn),
		clients:                     deps.Clients,
		collectionWorkflowClient:    merchandisingv1.NewCollectionWorkflowServiceClient(deps.Conn),
		conn:                        deps.Conn,
		collectionApprovalClient:    merchandisingv1.NewCollectionApprovalServiceClient(deps.Conn),
		synonymValidationClient:     searchv1.NewSynonymValidationServiceClient(deps.Conn),
		collectionQueryClient:       merchandisingv1.NewCollectionQueryServiceClient(deps.Conn),
		ledgerAccountClient:         ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		sellerLifecycleClient:       sellersv1.NewSellerLifecycleServiceClient(deps.Conn),
		meteredPlacementApproval:    meteredPlacementApproval{PlacementApprovalServiceClient: merchandisingv1.NewPlacementApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// SubmitMerchandisingRule submits a merchandising rule.
func (s *MerchandisingRuleApprovalServer) SubmitMerchandisingRule(ctx context.Context, req *merchandisingv1.SubmitMerchandisingRuleRequest) (*merchandisingv1.SubmitMerchandisingRuleResponse, error) {
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "submit merchandising rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetDetails()).(*merchandisingv1.SubmitMerchandisingRuleRequest_Details)
	cloned2 := proto.Clone(req.GetMerchandisingRule()).(*merchandisingv1.MerchandisingRule)
	createCollectionRequest := &merchandisingv1.CreateCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{DisplayName: cloned.GetApprover().GetDisplayName()},
					Audit: &commonv1.AuditInfo{
						Etag:      cloned2.GetAudit().GetEtag(),
						UpdatedBy: req.GetMerchandisingRule().GetAudit().GetUpdatedBy(),
					},
				}},
			}},
		},
	}
	createCollectionReply, err := createCollectionCall(ctx, createCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/CreateCollection")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := merchandisingRuleApprovalServerBrowseLedgerAccountsSteps[mode]
	browseLedgerAccountsRequest := &ledgerv1.BrowseLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.BrowseLedgerAccountsRequest_Filter{
			Region:       createCollectionReply.GetCollection().GetLocation().GetRegion(),
			CreatedAfter: createCollectionReply.GetCollection().GetAudit().GetCreatedAt(),
			OwnerId:      createCollectionReply.GetCollection().GetCollectionId(),
		},
	}
	browseLedgerAccountsReply, err := run(s, ctx, browseLedgerAccountsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/BrowseLedgerAccounts")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "merchandising.submit_merchandising_rule.find_search_indexes",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.region":     createCollectionReply.GetCollection().GetLocation().GetRegion(),
			"filter.owner_id":   req.GetMerchandisingRule().GetOwner().GetUserId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	listGiftCardBatchWorkflowsReply, err := s.giftCardBatchWorkflowClient.ListGiftCardBatchWorkflows(ctx, &giftcardsv1.ListGiftCardBatchWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &giftcardsv1.ListGiftCardBatchWorkflowsRequest_Filter{
			Region:       createCollectionReply.GetCollection().GetLocation().GetRegion(),
			CreatedAfter: createCollectionReply.GetCollection().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/ListGiftCardBatchWorkflows")
	}
	return &merchandisingv1.SubmitMerchandisingRuleResponse{
		Result: &commonv1.OperationResult{
			Details: createCollectionReply.GetResult().GetDetails(),
			Status:  listGiftCardBatchWorkflowsReply.GetResult().GetStatus(),
		},
		Outcome: &merchandisingv1.SubmitMerchandisingRuleResponse_Outcome{Status: browseLedgerAccountsReply.GetResult().GetStatus()},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Priority:            req.GetMerchandisingRule().GetPriority(),
		},
	}, nil
}

// ApproveMerchandisingRule approves a merchandising rule.
func (s *MerchandisingRuleApprovalServer) ApproveMerchandisingRule(ctx context.Context, req *merchandisingv1.ApproveMerchandisingRuleRequest) (*merchandisingv1.ApproveMerchandisingRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "approve merchandising rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	cancelGiftCardBatchWorkflowRequest := &giftcardsv1.CancelGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetMerchandisingRule().GetAudit().GetCreatedBy(),
			},
		},
	}
	cancelGiftCardBatchWorkflowReply, err := grpcx.Call(ctx, s.giftCardBatchWorkflowClient.CancelGiftCardBatchWorkflow, cancelGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/CancelGiftCardBatchWorkflow")
	}
	return &merchandisingv1.ApproveMerchandisingRuleResponse{
		Result: &commonv1.OperationResult{
			Status: cancelGiftCardBatchWorkflowReply.GetOutcome().GetStatus(),
		},
		Outcome: &merchandisingv1.ApproveMerchandisingRuleResponse_Outcome{
			Status: cancelGiftCardBatchWorkflowReply.GetOutcome().GetStatus(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Status:              req.GetMerchandisingRule().GetStatus(),
		},
	}, nil
}

// EscalateMerchandisingRule escalates a merchandising rule.
func (s *MerchandisingRuleApprovalServer) EscalateMerchandisingRule(ctx context.Context, req *merchandisingv1.EscalateMerchandisingRuleRequest) (*merchandisingv1.EscalateMerchandisingRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateMerchandisingRule", "error", err)
		return nil, err
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "escalate merchandising rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	merchandisingRuleCache := cache.New[string, *merchandisingv1.MerchandisingRule](1)
	merchandisingRuleCache.Put(req.GetContext().GetTraceId(), req.GetMerchandisingRule())
	cachedMerchandisingRule, _ := merchandisingRuleCache.Get(req.GetContext().GetTraceId())
	pauseGiftCardBatchWorkflowRequest := &giftcardsv1.PauseGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cachedMerchandisingRule.GetAudit().GetCreatedBy(),
				UpdatedAt: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
			},
			Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{
				Tax: &giftcardsv1.GiftCardBatchLineTax{
					Amount: req.GetMerchandisingRule().GetDiscount().GetAmount(),
				},
			}},
		},
		Details: &giftcardsv1.PauseGiftCardBatchWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetMerchandisingRule().GetOwner().GetEmail()},
		},
	}
	pauseGiftCardBatchWorkflowReply, err := grpcx.Call(ctx, s.giftCardBatchWorkflowClient.PauseGiftCardBatchWorkflow, pauseGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/PauseGiftCardBatchWorkflow")
	}
	cloneCollectionReply, err := s.collectionClient.CloneCollection(ctx, &merchandisingv1.CloneCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: pauseGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetUpdatedBy(),
						UpdatedAt: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
						CreatedAt: pauseGiftCardBatchWorkflowReply.GetGiftCardBatch().GetAudit().GetCreatedAt(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/CloneCollection")
	}
	sellerListingReporting, err := registry.Lookup[sellersv1.SellerListingReportingServiceClient](s.clients, "acme.sellers.v1.SellerListingReportingService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	generateSellerListingReportRequest := &sellersv1.GenerateSellerListingReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetMerchandisingRule().GetBlock().GetReason(),
		Annotations: annotations,
		Details: &sellersv1.GenerateSellerListingReportRequest_Details{
			Filters: cloneCollectionReply.GetResult().GetDetails(),
			Period:  req.GetMerchandisingRule().GetEffective(),
		},
	}
	generateSellerListingReportReply, err := sellerListingReporting.GenerateSellerListingReport(ctx, generateSellerListingReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/GenerateSellerListingReport")
	}
	return &merchandisingv1.EscalateMerchandisingRuleResponse{
		Outcome: &merchandisingv1.EscalateMerchandisingRuleResponse_Outcome{
			Message:     pauseGiftCardBatchWorkflowReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Details:       cloneCollectionReply.GetResult().GetDetails(),
			CorrelationId: generateSellerListingReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				Email: cloneCollectionReply.GetCollection().GetManager().GetEmail(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
		},
	}, nil
}

// ListPendingMerchandisingRules lists merchandising rules a page at a time.
func (s *MerchandisingRuleApprovalServer) ListPendingMerchandisingRules(ctx context.Context, req *merchandisingv1.ListPendingMerchandisingRulesRequest) (*merchandisingv1.ListPendingMerchandisingRulesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingMerchandisingRules", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending merchandising rules", "tenant", req.GetContext().GetTenantId())
	retryCollectionWorkflowRequest := &merchandisingv1.RetryCollectionWorkflowRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Location: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
					Manager: &identityv1.UserRef{
						UserId: req.GetContext().GetActor().GetUserId(),
						Email:  req.GetContext().GetActor().GetEmail(),
					},
				}},
			}},
		},
	}
	send := s.retryCollectionWorkflowSender(retryCollectionWorkflowRequest)
	retryCollectionWorkflowReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionWorkflowService/RetryCollectionWorkflow")
	}
	return &merchandisingv1.ListPendingMerchandisingRulesResponse{
		MerchandisingRules: []*merchandisingv1.MerchandisingRule{&merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: retryCollectionWorkflowReply.GetCollection().GetAudit().GetUpdatedBy(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateMerchandisingRuleApproval delegates a merchandising rule.
func (s *MerchandisingRuleApprovalServer) DelegateMerchandisingRuleApproval(ctx context.Context, req *merchandisingv1.DelegateMerchandisingRuleApprovalRequest) (*merchandisingv1.DelegateMerchandisingRuleApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "delegate merchandising rule approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetMerchandisingRule()).(*merchandisingv1.MerchandisingRule)
	parameters := make(map[string]string, len(req.GetMerchandisingRule().GetParameters()))
	for key, value := range req.GetMerchandisingRule().GetParameters() {
		parameters[key] = value
	}
	countSellerListingTotalsRequest := &sellersv1.CountSellerListingTotalsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.CountSellerListingTotalsRequest_Filter{
			CreatedAfter: cloned.GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
			Labels:       parameters,
		},
	}
	countSellerListingTotalsReply, err := s.sellerListingReportingLazy().CountSellerListingTotals(ctx, countSellerListingTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/CountSellerListingTotals")
	}
	escalateCollectionRequest := &merchandisingv1.EscalateCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager: &identityv1.UserRef{
						UserId: req.GetMerchandisingRule().GetOwner().GetUserId(),
						Email:  req.GetMerchandisingRule().GetOwner().GetEmail(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedAt: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
					},
				}},
				Manager: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
			}},
		},
	}
	escalateCollectionReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*merchandisingv1.EscalateCollectionResponse, error) {
		return s.collectionApprovalClient.EscalateCollection(ctx, escalateCollectionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionApprovalService/EscalateCollection")
	}
	return &merchandisingv1.DelegateMerchandisingRuleApprovalResponse{
		Result: &commonv1.OperationResult{
			Status: countSellerListingTotalsReply.GetResult().GetStatus(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				CreatedBy: escalateCollectionReply.GetCollection().GetAudit().GetCreatedBy(),
				UpdatedAt: escalateCollectionReply.GetCollection().GetAudit().GetUpdatedAt(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
		},
		Outcome: &merchandisingv1.DelegateMerchandisingRuleApprovalResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RecallMerchandisingRule recalls a merchandising rule.
func (s *MerchandisingRuleApprovalServer) RecallMerchandisingRule(ctx context.Context, req *merchandisingv1.RecallMerchandisingRuleRequest) (*merchandisingv1.RecallMerchandisingRuleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "recall merchandising rule", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	explainSynonymDecisionRequest := &searchv1.ExplainSynonymDecisionRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
			},
		},
	}
	explainSynonymDecisionReply, err := searchv1.SynonymValidationServiceClient.ExplainSynonymDecision(s.synonymValidationClient, ctx, explainSynonymDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymValidationService/ExplainSynonymDecision")
	}
	findCollectionsRequest := newFindCollectionsRequest(withFindCollectionsContext(req.GetContext()), withFindCollectionsFilter(&merchandisingv1.FindCollectionsRequest_Filter{
		CreatedAfter: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
	}))
	findCollectionsReply, err := s.collectionQueryClient.FindCollections(ctx, findCollectionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionQueryService/FindCollections")
	}
	watchLedgerAccountsStream, err := s.ledgerAccountClient.WatchLedgerAccounts(ctx, &ledgerv1.WatchLedgerAccountsRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: explainSynonymDecisionReply.GetSynonym().GetOwner().GetEmail(),
		},
		Filter: &ledgerv1.WatchLedgerAccountsRequest_Filter{
			CreatedAfter: explainSynonymDecisionReply.GetOutcome().GetEffectiveAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
	}
	for {
		received, err := watchLedgerAccountsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/WatchLedgerAccounts")
		}
		_ = received
	}
	return &merchandisingv1.RecallMerchandisingRuleResponse{
		Outcome: &merchandisingv1.RecallMerchandisingRuleResponse_Outcome{
			NextSteps:   explainSynonymDecisionReply.GetOutcome().GetNextSteps(),
			Message:     findCollectionsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{MerchandisingRuleId: req.GetMerchandisingRuleId()},
		Result:            &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetMerchandisingRuleApprovalStatus returns a merchandising rule.
func (s *MerchandisingRuleApprovalServer) GetMerchandisingRuleApprovalStatus(ctx context.Context, req *merchandisingv1.GetMerchandisingRuleApprovalStatusRequest) (*merchandisingv1.GetMerchandisingRuleApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "merchandising_rule", req.GetMerchandisingRuleId(), "get_merchandising_rule_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get merchandising rule approval status", "tenant", req.GetContext().GetTenantId())
	cloneCollectionRequest := &merchandisingv1.CloneCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Manager:  &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
					ParentId: req.GetMerchandisingRuleId(),
				}},
			}},
		},
	}
	cloneCollectionReply := new(merchandisingv1.CloneCollectionResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", merchandisingCollectionServiceName, "CloneCollection"), cloneCollectionRequest, cloneCollectionReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionService/CloneCollection")
	}
	escalateCollectionRequest := &merchandisingv1.EscalateCollectionRequest{
		Context: req.GetContext(),
		Collection: &merchandisingv1.Collection{
			Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
				Children: []*merchandisingv1.Collection{&merchandisingv1.Collection{
					Audit: &commonv1.AuditInfo{
						CreatedBy: cloneCollectionReply.GetCollection().GetAudit().GetCreatedBy(),
						UpdatedBy: cloneCollectionReply.GetCollection().GetAudit().GetUpdatedBy(),
					},
					Manager: &identityv1.UserRef{
						UserId: cloneCollectionReply.GetCollection().GetManager().GetUserId(),
					},
				}},
				Location: &commonv1.PostalAddress{
					Region: cloneCollectionReply.GetCollection().GetLocation().GetRegion(),
				},
			}},
		},
	}
	escalateCollectionReply, err := grpcx.Call(ctx, s.collectionApprovalClient.EscalateCollection, escalateCollectionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.CollectionApprovalService/EscalateCollection")
	}
	return &merchandisingv1.GetMerchandisingRuleApprovalStatusResponse{
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{
				UserId: cloneCollectionReply.GetCollection().GetManager().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: cloneCollectionReply.GetCollection().GetAudit().GetCreatedAt(),
				Etag:      escalateCollectionReply.GetCollection().GetAudit().GetEtag(),
			},
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
		},
		Result: &commonv1.OperationResult{
			Details: escalateCollectionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// CommentMerchandisingRuleApproval comments a merchandising rule.
func (s *MerchandisingRuleApprovalServer) CommentMerchandisingRuleApproval(ctx context.Context, req *merchandisingv1.CommentMerchandisingRuleApprovalRequest) (*merchandisingv1.CommentMerchandisingRuleApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveMerchandisingRule(ctx, req.GetContext().GetTenantId(), req.GetMerchandisingRule()); err != nil {
		return nil, grpcx.Wrap(err, "save merchandising rule")
	}
	s.log.InfoContext(ctx, "comment merchandising rule approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetMerchandisingRule().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "merchandising.comment_merchandising_rule_approval.create_collection",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"collection.children.children.manager.email": req.GetMerchandisingRule().GetOwner().GetEmail(),
			"context.tenant_id":                          req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	checkAccessPolicyEligibilityRequest := &authorizationv1.CheckAccessPolicyEligibilityRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{
				UserId:      req.GetMerchandisingRule().GetOwner().GetUserId(),
				DisplayName: req.GetMerchandisingRule().GetOwner().GetDisplayName(),
			},
		},
	}
	checkAccessPolicyEligibilityReply, err := checkAccessPolicyEligibilityCall(ctx, checkAccessPolicyEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/CheckAccessPolicyEligibility")
	}
	scheduleSellerExpiryRequest := &sellersv1.ScheduleSellerExpiryRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetMerchandisingRule().GetOwner().GetDisplayName(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
		},
	}
	scheduleSellerExpiryReply, err := grpcx.Call(ctx, s.sellerLifecycleClient.ScheduleSellerExpiry, scheduleSellerExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerLifecycleService/ScheduleSellerExpiry")
	}
	listPendingPlacementsRequest := &merchandisingv1.ListPendingPlacementsRequest{
		Context: req.GetContext(),
		Filter: &merchandisingv1.ListPendingPlacementsRequest_Filter{
			CreatedAfter: req.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
		},
	}
	listPendingPlacementsReply, err := s.meteredPlacementApproval.ListPendingPlacements(ctx, listPendingPlacementsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementApprovalService/ListPendingPlacements")
	}
	return &merchandisingv1.CommentMerchandisingRuleApprovalResponse{
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Priority:            checkAccessPolicyEligibilityReply.GetAccessPolicy().GetPriority(),
			Name:                checkAccessPolicyEligibilityReply.GetAccessPolicy().GetName(),
			MerchandisingRuleId: req.GetMerchandisingRuleId(),
			Version:             req.GetMerchandisingRule().GetVersion(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: scheduleSellerExpiryReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &merchandisingv1.CommentMerchandisingRuleApprovalResponse_Outcome{
			Message: listPendingPlacementsReply.GetResult().GetMessage(),
		},
	}, nil
}

// merchandisingRuleApprovalServerBrowseLedgerAccountsSteps are the ways to run
// acme.ledger.v1.LedgerAccountQueryService/BrowseLedgerAccounts, by name.
var merchandisingRuleApprovalServerBrowseLedgerAccountsSteps = map[string]func(*MerchandisingRuleApprovalServer, context.Context, *ledgerv1.BrowseLedgerAccountsRequest) (*ledgerv1.BrowseLedgerAccountsResponse, error){
	"default": (*MerchandisingRuleApprovalServer).browseLedgerAccountsStep,
	"audited": (*MerchandisingRuleApprovalServer).browseLedgerAccountsAuditedStep,
}

func (s *MerchandisingRuleApprovalServer) browseLedgerAccountsStep(ctx context.Context, request *ledgerv1.BrowseLedgerAccountsRequest) (*ledgerv1.BrowseLedgerAccountsResponse, error) {
	return s.ledgerAccountQueryClient.BrowseLedgerAccounts(ctx, request)
}

func (s *MerchandisingRuleApprovalServer) browseLedgerAccountsAuditedStep(ctx context.Context, request *ledgerv1.BrowseLedgerAccountsRequest) (*ledgerv1.BrowseLedgerAccountsResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.ledger.v1.LedgerAccountQueryService/BrowseLedgerAccounts")
	return s.ledgerAccountQueryClient.BrowseLedgerAccounts(ctx, request)
}

// retryCollectionWorkflowSender returns a function that sends one request to
// acme.merchandising.v1.CollectionWorkflowService/RetryCollectionWorkflow.
func (s *MerchandisingRuleApprovalServer) retryCollectionWorkflowSender(request *merchandisingv1.RetryCollectionWorkflowRequest) func(context.Context) (*merchandisingv1.RetryCollectionWorkflowResponse, error) {
	return func(ctx context.Context) (*merchandisingv1.RetryCollectionWorkflowResponse, error) {
		return s.collectionWorkflowClient.RetryCollectionWorkflow(ctx, request)
	}
}

// sellerListingReportingLazy dials acme.sellers.v1.SellerListingReportingService the first time a handler needs it.
func (s *MerchandisingRuleApprovalServer) sellerListingReportingLazy() sellersv1.SellerListingReportingServiceClient {
	s.sellerListingReportingOnce.Do(func() { s.sellerListingReportingCached = sellersv1.NewSellerListingReportingServiceClient(s.conn) })
	return s.sellerListingReportingCached
}
