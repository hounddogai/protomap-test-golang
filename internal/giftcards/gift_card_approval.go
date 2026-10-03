package giftcards

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	pricingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pricing/v1"
	procurementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/procurement/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// GiftCardApprovalServer implements `acme.giftcards.v1.GiftCardApprovalService`.
// Routes gift cards through approval, with escalation and delegation.
type GiftCardApprovalServer struct {
	giftcardsv1.UnimplementedGiftCardApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	sellerScorecardCalculationClient   sellersv1.SellerScorecardCalculationServiceClient
	giftCardClient                     giftcardsv1.GiftCardServiceClient
	roleHistoryClient                  identityv1.RoleHistoryServiceClient
	stockItemClient                    inventoryv1.StockItemServiceClient
	giftCardCalculationClient          giftcardsv1.GiftCardCalculationServiceClient
	searchIndexHistoryClient           searchv1.SearchIndexHistoryServiceClient
	priceOverrideCalculationClient     pricingv1.PriceOverrideCalculationServiceClient
	storageBinQueryClient              warehousingv1.StorageBinQueryServiceClient
	documentQueryClient                documentsv1.DocumentQueryServiceClient
	giftCardTransactionLifecycleClient giftcardsv1.GiftCardTransactionLifecycleServiceClient
	clients                            *registry.Registry
}

// NewGiftCardApprovalServer wires a GiftCardApprovalServer.
func NewGiftCardApprovalServer(deps platform.Deps) *GiftCardApprovalServer {
	return &GiftCardApprovalServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.giftcards.v1.GiftCardApprovalService"),
		sellerScorecardCalculationClient:   sellersv1.NewSellerScorecardCalculationServiceClient(deps.Conn),
		giftCardClient:                     giftcardsv1.NewGiftCardServiceClient(deps.Conn),
		roleHistoryClient:                  identityv1.NewRoleHistoryServiceClient(deps.Conn),
		stockItemClient:                    inventoryv1.NewStockItemServiceClient(deps.Conn),
		giftCardCalculationClient:          giftcardsv1.NewGiftCardCalculationServiceClient(deps.Conn),
		searchIndexHistoryClient:           searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		priceOverrideCalculationClient:     pricingv1.NewPriceOverrideCalculationServiceClient(deps.Conn),
		storageBinQueryClient:              warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		documentQueryClient:                documentsv1.NewDocumentQueryServiceClient(deps.Conn),
		giftCardTransactionLifecycleClient: giftcardsv1.NewGiftCardTransactionLifecycleServiceClient(deps.Conn),
		clients:                            deps.Clients,
	}
}

// SubmitGiftCard submits a gift card.
func (s *GiftCardApprovalServer) SubmitGiftCard(ctx context.Context, req *giftcardsv1.SubmitGiftCardRequest) (*giftcardsv1.SubmitGiftCardResponse, error) {
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "submit gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	compareSellerScorecardOptionsRequest := buildCompareSellerScorecardOptionsRequestForSubmitGiftCard(req)
	compareSellerScorecardOptionsReply, err := s.sellerScorecardCalculationClient.CompareSellerScorecardOptions(ctx, compareSellerScorecardOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardCalculationService/CompareSellerScorecardOptions")
	}
	ctx = tenancy.With(ctx, "etag", req.GetGiftCard().GetAudit().GetEtag())
	deleteGiftCardReply, err := s.giftCardClient.DeleteGiftCard(ctx, &giftcardsv1.DeleteGiftCardRequest{
		Context:    req.GetContext(),
		GiftCardId: req.GetGiftCard().GetGiftCardId(),
		Etag:       tenancy.Value(ctx, "etag"),
		Reason:     req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/DeleteGiftCard")
	}
	return &giftcardsv1.SubmitGiftCardResponse{
		GiftCard: &giftcardsv1.GiftCard{
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareSellerScorecardOptionsReply.GetSellerScorecard().GetAudit().GetCreatedAt(),
			},
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: compareSellerScorecardOptionsReply.GetSellerScorecard().GetCounterparty().GetDisplayName(),
			},
			GiftCardId: req.GetGiftCardId(),
			Contact:    req.GetGiftCard().GetContact(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: deleteGiftCardReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ApproveGiftCard approves a gift card.
func (s *GiftCardApprovalServer) ApproveGiftCard(ctx context.Context, req *giftcardsv1.ApproveGiftCardRequest) (*giftcardsv1.ApproveGiftCardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveGiftCard", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "approve gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	listRoleVersionsRequest := &identityv1.ListRoleVersionsRequest{}
	listRoleVersionsRequest.Context = req.GetContext()
	listRoleVersionsRequest.Filter = &identityv1.ListRoleVersionsRequest_Filter{
		Labels:       req.GetGiftCard().GetLabels(),
		CreatedAfter: req.GetGiftCard().GetAudit().GetCreatedAt(),
	}
	listRoleVersionsReply, err := s.roleHistoryClient.ListRoleVersions(ctx, listRoleVersionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/ListRoleVersions")
	}
	step := &giftCardApprovalServerApproveGiftCardDeleteGiftCardStep{request: &giftcardsv1.DeleteGiftCardRequest{
		Context:    req.GetContext(),
		GiftCardId: req.GetGiftCard().GetGiftCardId(),
		Reason:     req.GetReason(),
		Etag:       req.GetGiftCard().GetAudit().GetEtag(),
	}}
	if err := s.runApproveGiftCardDeleteGiftCard(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardService/DeleteGiftCard")
	}
	deleteGiftCardReply := step.reply
	return &giftcardsv1.ApproveGiftCardResponse{
		Result: &commonv1.OperationResult{
			Details:       listRoleVersionsReply.GetResult().GetDetails(),
			CorrelationId: deleteGiftCardReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			OpenedAt:   req.GetGiftCard().GetOpenedAt(),
		},
	}, nil
}

// RejectGiftCard rejects a gift card.
func (s *GiftCardApprovalServer) RejectGiftCard(ctx context.Context, req *giftcardsv1.RejectGiftCardRequest) (*giftcardsv1.RejectGiftCardResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "reject gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	listStockItemsRequest := buildListStockItemsRequestForRejectGiftCard(req)
	listStockItemsReply, err := s.stockItemClient.ListStockItems(ctx, listStockItemsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/ListStockItems")
	}
	for _, limit := range req.GetGiftCard().GetLimits() {
		if _, err := s.giftCardCalculationClient.ExplainGiftCardCalculation(ctx, &giftcardsv1.ExplainGiftCardCalculationRequest{
			Context: req.GetContext(),
			GiftCard: &giftcardsv1.GiftCard{
				Limits: []*giftcardsv1.GiftCardLimit{&giftcardsv1.GiftCardLimit{
					Name:   limit.GetName(),
					Amount: limit.GetAmount(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardCalculationService/ExplainGiftCardCalculation")
		}
	}
	return &giftcardsv1.RejectGiftCardResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listStockItemsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &giftcardsv1.RejectGiftCardResponse_Outcome{
			Status:      listStockItemsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		GiftCard: &giftcardsv1.GiftCard{GiftCardId: req.GetGiftCardId()},
	}, nil
}

// ListPendingGiftCards lists gift cards a page at a time.
func (s *GiftCardApprovalServer) ListPendingGiftCards(ctx context.Context, req *giftcardsv1.ListPendingGiftCardsRequest) (*giftcardsv1.ListPendingGiftCardsResponse, error) {
	s.log.InfoContext(ctx, "list pending gift cards", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	compareSearchIndexVersionsRequest := &searchv1.CompareSearchIndexVersionsRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Locator: &searchv1.SearchIndex_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
		},
		Details: &searchv1.CompareSearchIndexVersionsRequest_Details{Snapshot: snapshot},
	}
	compareSearchIndexVersionsReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*searchv1.CompareSearchIndexVersionsResponse, error) {
		return s.searchIndexHistoryClient.CompareSearchIndexVersions(ctx, compareSearchIndexVersionsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions")
	}
	return &giftcardsv1.ListPendingGiftCardsResponse{
		GiftCards: []*giftcardsv1.GiftCard{&giftcardsv1.GiftCard{
			Holder: &giftcardsv1.GiftCardTransactionRef{
				DisplayName: compareSearchIndexVersionsReply.GetSearchIndex().GetOwner().GetDisplayName(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateGiftCardApproval delegates a gift card.
func (s *GiftCardApprovalServer) DelegateGiftCardApproval(ctx context.Context, req *giftcardsv1.DelegateGiftCardApprovalRequest) (*giftcardsv1.DelegateGiftCardApprovalResponse, error) {
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "delegate gift card approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	for _, limit := range req.GetGiftCard().GetLimits() {
		if _, err := s.priceOverrideCalculationClient.ExplainPriceOverrideCalculation(ctx, &pricingv1.ExplainPriceOverrideCalculationRequest{
			Context: req.GetContext(),
			PriceOverride: &pricingv1.PriceOverride{
				Name:   limit.GetName(),
				Action: &pricingv1.PriceOverride_Discount{Discount: &pricingv1.PriceOverrideDiscountAction{Amount: limit.GetAmount()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.pricing.v1.PriceOverrideCalculationService/ExplainPriceOverrideCalculation")
		}
	}
	labels := make(map[string]string, len(req.GetGiftCard().GetLabels()))
	for key, value := range req.GetGiftCard().GetLabels() {
		labels[key] = value
	}
	cloned := proto.Clone(req.GetGiftCard()).(*giftcardsv1.GiftCard)
	findStorageBinsRequest := &warehousingv1.FindStorageBinsRequest{
		Context: req.GetContext(),
		Filter: &warehousingv1.FindStorageBinsRequest_Filter{
			Labels:       labels,
			CreatedAfter: req.GetGiftCard().GetAudit().GetCreatedAt(),
			OwnerId:      cloned.GetHolder().GetGiftCardTransactionId(),
		},
	}
	type findStorageBinsResult struct {
		reply *warehousingv1.FindStorageBinsResponse
		err   error
	}
	findStorageBinsResults := make(chan findStorageBinsResult, 1)
	go func() {
		reply, err := s.storageBinQueryClient.FindStorageBins(ctx, findStorageBinsRequest)
		findStorageBinsResults <- findStorageBinsResult{reply, err}
	}()
	var findStorageBinsReply *warehousingv1.FindStorageBinsResponse
	select {
	case result := <-findStorageBinsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.warehousing.v1.StorageBinQueryService/FindStorageBins")
		}
		findStorageBinsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &giftcardsv1.DelegateGiftCardApprovalResponse{
		Result: &commonv1.OperationResult{
			Message: findStorageBinsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Balance:    req.GetGiftCard().GetBalance(),
		},
	}, nil
}

// RecallGiftCard recalls a gift card.
func (s *GiftCardApprovalServer) RecallGiftCard(ctx context.Context, req *giftcardsv1.RecallGiftCardRequest) (*giftcardsv1.RecallGiftCardResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallGiftCard", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCard(ctx, req.GetContext().GetTenantId(), req.GetGiftCard()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card")
	}
	s.log.InfoContext(ctx, "recall gift card", "tenant", req.GetContext().GetTenantId(), "etag", req.GetGiftCard().GetAudit().GetEtag())
	resolveDocumentsRequest := newResolveDocumentsRequest(withResolveDocumentsContext(req.GetContext()))
	resolveDocumentsReply, err := s.documentQueryClient.ResolveDocuments(ctx, resolveDocumentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/ResolveDocuments")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := giftCardApprovalServerScheduleGiftCardTransactionExpirySteps[mode]
	encoded, err := protojson.Marshal(req.GetGiftCard())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded giftcardsv1.GiftCard
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetGiftCard())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scheduleGiftCardTransactionExpiryRequest := &giftcardsv1.ScheduleGiftCardTransactionExpiryRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ScheduleGiftCardTransactionExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{Phone: (&decoded).GetContact().GetPhone()},
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{Detail: packed},
	}
	scheduleGiftCardTransactionExpiryReply, err := run(s, ctx, scheduleGiftCardTransactionExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionLifecycleService/ScheduleGiftCardTransactionExpiry")
	}
	purchaseRequisitionQuery, ok := s.clients.Get("acme.procurement.v1.PurchaseRequisitionQueryService").(procurementv1.PurchaseRequisitionQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.procurement.v1.PurchaseRequisitionQueryService")
	}
	resolvePurchaseRequisitionsRequest := &procurementv1.ResolvePurchaseRequisitionsRequest{Context: req.GetContext()}
	resolvePurchaseRequisitionsReply, err := purchaseRequisitionQuery.ResolvePurchaseRequisitions(ctx, resolvePurchaseRequisitionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.procurement.v1.PurchaseRequisitionQueryService/ResolvePurchaseRequisitions")
	}
	return &giftcardsv1.RecallGiftCardResponse{
		Outcome: &giftcardsv1.RecallGiftCardResponse_Outcome{Message: resolveDocumentsReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message:       scheduleGiftCardTransactionExpiryReply.GetOutcome().GetMessage(),
			CorrelationId: resolvePurchaseRequisitionsReply.GetResult().GetCorrelationId(),
			Details:       resolvePurchaseRequisitionsReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		GiftCard: &giftcardsv1.GiftCard{
			GiftCardId: req.GetGiftCardId(),
			Balance:    req.GetGiftCard().GetBalance(),
		},
	}, nil
}

// giftCardApprovalServerApproveGiftCardDeleteGiftCardStep carries the request of one step of ApproveGiftCard and, once
// it ran, its reply.
type giftCardApprovalServerApproveGiftCardDeleteGiftCardStep struct {
	request *giftcardsv1.DeleteGiftCardRequest
	reply   *giftcardsv1.DeleteGiftCardResponse
}

func (s *GiftCardApprovalServer) runApproveGiftCardDeleteGiftCard(ctx context.Context, step *giftCardApprovalServerApproveGiftCardDeleteGiftCardStep) error {
	reply, err := s.giftCardClient.DeleteGiftCard(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// giftCardApprovalServerScheduleGiftCardTransactionExpirySteps are the ways to run
// acme.giftcards.v1.GiftCardTransactionLifecycleService/ScheduleGiftCardTransactionExpiry, by name.
var giftCardApprovalServerScheduleGiftCardTransactionExpirySteps = map[string]func(*GiftCardApprovalServer, context.Context, *giftcardsv1.ScheduleGiftCardTransactionExpiryRequest) (*giftcardsv1.ScheduleGiftCardTransactionExpiryResponse, error){
	"default": (*GiftCardApprovalServer).scheduleGiftCardTransactionExpiryStep,
	"audited": (*GiftCardApprovalServer).scheduleGiftCardTransactionExpiryAuditedStep,
}

func (s *GiftCardApprovalServer) scheduleGiftCardTransactionExpiryStep(ctx context.Context, request *giftcardsv1.ScheduleGiftCardTransactionExpiryRequest) (*giftcardsv1.ScheduleGiftCardTransactionExpiryResponse, error) {
	return s.giftCardTransactionLifecycleClient.ScheduleGiftCardTransactionExpiry(ctx, request)
}

func (s *GiftCardApprovalServer) scheduleGiftCardTransactionExpiryAuditedStep(ctx context.Context, request *giftcardsv1.ScheduleGiftCardTransactionExpiryRequest) (*giftcardsv1.ScheduleGiftCardTransactionExpiryResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.giftcards.v1.GiftCardTransactionLifecycleService/ScheduleGiftCardTransactionExpiry")
	return s.giftCardTransactionLifecycleClient.ScheduleGiftCardTransactionExpiry(ctx, request)
}
