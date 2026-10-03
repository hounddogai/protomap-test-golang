package promotions

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	documentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/documents/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	showsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shows/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// PromotionLifecycleServer implements `acme.promotions.v1.PromotionLifecycleService`.
// Moves a promotion between the states of its lifecycle.
type PromotionLifecycleServer struct {
	promotionsv1.UnimplementedPromotionLifecycleServiceServer
	store                             *Store
	log                               *slog.Logger
	conn                              *grpc.ClientConn
	campaignSyncClient                promotionsv1.CampaignSyncServiceClient
	fraudSignalApprovalClient         fraudv1.FraudSignalApprovalServiceClient
	fraudSignalApprovalClientFallback fraudv1.FraudSignalApprovalServiceClient
	couponReportingClient             promotionsv1.CouponReportingServiceClient
	showClient                        showsv1.ShowServiceClient
	searchIndexNotificationClient     searchv1.SearchIndexNotificationServiceClient
	promotionReportingOnce            sync.Once
	promotionReportingCached          promotionsv1.PromotionReportingServiceClient
	stockItemApprovalClient           inventoryv1.StockItemApprovalServiceClient
	meteredStockItemApproval          meteredStockItemApproval
	orderNoteSyncClient               ordersv1.OrderNoteSyncServiceClient
	ledgerAccountClient               ledgerv1.LedgerAccountServiceClient
}

// NewPromotionLifecycleServer wires a PromotionLifecycleServer.
func NewPromotionLifecycleServer(deps platform.Deps) *PromotionLifecycleServer {
	return &PromotionLifecycleServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.promotions.v1.PromotionLifecycleService"),
		conn:                              deps.Conn,
		campaignSyncClient:                promotionsv1.NewCampaignSyncServiceClient(deps.Conn),
		fraudSignalApprovalClient:         fraudv1.NewFraudSignalApprovalServiceClient(deps.Conn),
		fraudSignalApprovalClientFallback: fraudv1.NewFraudSignalApprovalServiceClient(deps.Conn),
		couponReportingClient:             promotionsv1.NewCouponReportingServiceClient(deps.Conn),
		showClient:                        showsv1.NewShowServiceClient(deps.Conn),
		searchIndexNotificationClient:     searchv1.NewSearchIndexNotificationServiceClient(deps.Conn),
		stockItemApprovalClient:           inventoryv1.NewStockItemApprovalServiceClient(deps.Conn),
		meteredStockItemApproval:          meteredStockItemApproval{StockItemApprovalServiceClient: inventoryv1.NewStockItemApprovalServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		orderNoteSyncClient:               ordersv1.NewOrderNoteSyncServiceClient(deps.Conn),
		ledgerAccountClient:               ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
	}
}

// ActivatePromotion activates a promotion.
func (s *PromotionLifecycleServer) ActivatePromotion(ctx context.Context, req *promotionsv1.ActivatePromotionRequest) (*promotionsv1.ActivatePromotionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "activate_promotion"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate promotion", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotifyContact().GetPhone())
	explainCouponCalculationRequest := &promotionsv1.ExplainCouponCalculationRequest{
		Context: req.GetContext(),
		Coupon: &promotionsv1.Coupon{
			Holder: &promotionsv1.PromotionRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
		},
		Details: &promotionsv1.ExplainCouponCalculationRequest_Details{
			Contact: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
		},
	}
	explainCouponCalculationReply := new(promotionsv1.ExplainCouponCalculationResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", promotionsCouponCalculationServiceName, "ExplainCouponCalculation"), explainCouponCalculationRequest, explainCouponCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponCalculationService/ExplainCouponCalculation")
	}
	pullCampaignsRequest := buildPullCampaignsRequestForActivatePromotion(req, explainCouponCalculationReply)
	pullCampaignsReply, err := s.campaignSyncClient.PullCampaigns(ctx, pullCampaignsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/PullCampaigns")
	}
	routes := map[string]func(context.Context, *fraudv1.RecallFraudSignalRequest, ...grpc.CallOption) (*fraudv1.RecallFraudSignalResponse, error){
		"primary":  s.fraudSignalApprovalClient.RecallFraudSignal,
		"fallback": s.fraudSignalApprovalClientFallback.RecallFraudSignal,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recallFraudSignalRequest := &fraudv1.RecallFraudSignalRequest{
		Context: req.GetContext(),
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				Version:     explainCouponCalculationReply.GetCoupon().GetHolder().GetVersion(),
				DisplayName: pullCampaignsReply.GetCampaign().GetOrganizer().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	recallFraudSignalReply, err := route(ctx, recallFraudSignalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalApprovalService/RecallFraudSignal")
	}
	return &promotionsv1.ActivatePromotionResponse{
		Outcome: &promotionsv1.ActivatePromotionResponse_Outcome{
			NextSteps:   explainCouponCalculationReply.GetOutcome().GetNextSteps(),
			Status:      explainCouponCalculationReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: pullCampaignsReply.GetResult().GetReferenceIds(),
			Message:      recallFraudSignalReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: pullCampaignsReply.GetCampaign().GetAudit().GetUpdatedBy(),
			},
			PromotionId: req.GetPromotionId(),
		},
	}, nil
}

// SuspendPromotion suspends a promotion.
func (s *PromotionLifecycleServer) SuspendPromotion(ctx context.Context, req *promotionsv1.SuspendPromotionRequest) (*promotionsv1.SuspendPromotionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SuspendPromotion", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "suspend_promotion"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend promotion", "tenant", req.GetContext().GetTenantId())
	generateCouponReportRequest := &promotionsv1.GenerateCouponReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
		Details:     &promotionsv1.GenerateCouponReportRequest_Details{Filters: req.GetContext().GetBaggage()},
	}
	generateCouponReportReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*promotionsv1.GenerateCouponReportResponse, error) {
		return s.couponReportingClient.GenerateCouponReport(ctx, generateCouponReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CouponReportingService/GenerateCouponReport")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveSynonymConflictRequest := &searchv1.ResolveSynonymConflictRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				Email:  req.GetDetails().GetNotifyContact().GetEmail(),
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
			},
			Action: &searchv1.Synonym_Block{Block: &searchv1.SynonymBlockAction{
				Reason:      req.GetReason(),
				NotifyEmail: req.GetDetails().GetNewOwner().GetEmail(),
			}},
		},
		Details: &searchv1.ResolveSynonymConflictRequest_Details{Payload: snapshot},
	}
	resolveSynonymConflictReply, err := grpcx.NewClient(s.conn, searchv1.NewSynonymSyncServiceClient).ResolveSynonymConflict(ctx, resolveSynonymConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ResolveSynonymConflict")
	}
	return &promotionsv1.SuspendPromotionResponse{
		Result: &commonv1.OperationResult{
			Status:        generateCouponReportReply.GetResult().GetStatus(),
			CorrelationId: resolveSynonymConflictReply.GetResult().GetCorrelationId(),
		},
		Promotion: &promotionsv1.Promotion{PromotionId: req.GetPromotionId()},
		Outcome:   &promotionsv1.SuspendPromotionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ReinstatePromotion reinstates a promotion.
func (s *PromotionLifecycleServer) ReinstatePromotion(ctx context.Context, req *promotionsv1.ReinstatePromotionRequest) (*promotionsv1.ReinstatePromotionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReinstatePromotion", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "reinstate_promotion"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate promotion", "tenant", req.GetContext().GetTenantId())
	suggestDocumentsRequest := &documentsv1.SuggestDocumentsRequest{
		Context: req.GetContext(),
		Filter:  &documentsv1.SuggestDocumentsRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()},
	}
	suggestDocumentsReply := new(documentsv1.SuggestDocumentsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", documentsDocumentQueryServiceName, "SuggestDocuments"), suggestDocumentsRequest, suggestDocumentsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.documents.v1.DocumentQueryService/SuggestDocuments")
	}
	return &promotionsv1.ReinstatePromotionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: suggestDocumentsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Promotion: &promotionsv1.Promotion{PromotionId: req.GetPromotionId()},
		Outcome:   &promotionsv1.ReinstatePromotionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminatePromotion terminates a promotion.
func (s *PromotionLifecycleServer) TerminatePromotion(ctx context.Context, req *promotionsv1.TerminatePromotionRequest) (*promotionsv1.TerminatePromotionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "terminate_promotion"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate promotion", "tenant", req.GetContext().GetTenantId())
	searchShowsRequest := &showsv1.SearchShowsRequest{}
	searchShowsRequest.Context = req.GetContext()
	searchShowsRequest.Filter = &showsv1.SearchShowsRequest_Filter{
		CreatedAfter: req.GetDetails().GetEffectiveAt(),
		OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
	}
	searchShowsReply, err := s.showClient.SearchShows(ctx, searchShowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shows.v1.ShowService/SearchShows")
	}
	streamSearchIndexNoticesStream, err := s.searchIndexNotificationClient.StreamSearchIndexNotices(ctx, &searchv1.StreamSearchIndexNoticesRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
	}
	for {
		received, err := streamSearchIndexNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexNotificationService/StreamSearchIndexNotices")
		}
		_ = received
	}
	return &promotionsv1.TerminatePromotionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: searchShowsReply.GetResult().GetReferenceIds(),
			Status:       searchShowsReply.GetResult().GetStatus(),
		},
		Promotion: &promotionsv1.Promotion{PromotionId: req.GetPromotionId()},
		Outcome:   &promotionsv1.TerminatePromotionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ExpirePromotion expires a promotion.
func (s *PromotionLifecycleServer) ExpirePromotion(ctx context.Context, req *promotionsv1.ExpirePromotionRequest) (*promotionsv1.ExpirePromotionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpirePromotion", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "expire_promotion"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire promotion", "tenant", req.GetContext().GetTenantId())
	countPromotionTotalsRequest := &promotionsv1.CountPromotionTotalsRequest{
		Context: req.GetContext(),
		Filter: &promotionsv1.CountPromotionTotalsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}
	countPromotionTotalsReply, err := s.promotionReportingLazy().CountPromotionTotals(ctx, countPromotionTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionReportingService/CountPromotionTotals")
	}
	approveStockItemReply, err := s.stockItemApprovalClient.ApproveStockItem(ctx, &inventoryv1.ApproveStockItemRequest{
		Context: req.GetContext(),
		StockItem: &inventoryv1.StockItem{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetNewOwner().GetUserId()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/ApproveStockItem")
	}
	escalateStockItemRequest := &inventoryv1.EscalateStockItemRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.EscalateStockItemRequest_Details{
			Approver: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				DisplayName: approveStockItemReply.GetStockItem().GetOwner().GetDisplayName(),
			},
		},
	}
	escalateStockItemReply, err := s.meteredStockItemApproval.EscalateStockItem(ctx, escalateStockItemRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/EscalateStockItem")
	}
	pushOrderNoteRequest := &ordersv1.PushOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Measurements: []*ordersv1.OrderNoteMeasurement{&ordersv1.OrderNoteMeasurement{
				Unit: approveStockItemReply.GetStockItem().GetDimensions().GetUnit(),
			}},
			Subject: &ordersv1.OrderRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			CorrelationId: approveStockItemReply.GetResult().GetCorrelationId(),
		},
	}
	type pushOrderNoteResult struct {
		reply *ordersv1.PushOrderNoteResponse
		err   error
	}
	pushOrderNoteResults := make(chan pushOrderNoteResult, 1)
	go func() {
		reply, err := s.orderNoteSyncClient.PushOrderNote(ctx, pushOrderNoteRequest)
		pushOrderNoteResults <- pushOrderNoteResult{reply, err}
	}()
	var pushOrderNoteReply *ordersv1.PushOrderNoteResponse
	select {
	case result := <-pushOrderNoteResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderNoteSyncService/PushOrderNote")
		}
		pushOrderNoteReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	_ = escalateStockItemReply
	return &promotionsv1.ExpirePromotionResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: countPromotionTotalsReply.GetResult().GetReferenceIds(),
			Status:       countPromotionTotalsReply.GetResult().GetStatus(),
		},
		Outcome: &promotionsv1.ExpirePromotionResponse_Outcome{
			Message:     approveStockItemReply.GetResult().GetMessage(),
			Status:      pushOrderNoteReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedAt: approveStockItemReply.GetStockItem().GetAudit().GetCreatedAt(),
			},
			PromotionId: req.GetPromotionId(),
		},
	}, nil
}

// GetPromotionLifecycle returns a promotion.
func (s *PromotionLifecycleServer) GetPromotionLifecycle(ctx context.Context, req *promotionsv1.GetPromotionLifecycleRequest) (*promotionsv1.GetPromotionLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPromotionLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "promotion", req.GetPromotionId(), "get_promotion_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get promotion lifecycle", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushCampaignReply, err := s.campaignSyncClient.PushCampaign(ctx, &promotionsv1.PushCampaignRequest{
		Context: req.GetContext(),
		Campaign: &promotionsv1.Campaign{
			Participants: []*promotionsv1.CampaignParticipant{&promotionsv1.CampaignParticipant{
				Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			}},
			Organizer: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
			Audit:     &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
		Details: &promotionsv1.PushCampaignRequest_Details{Payload: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignSyncService/PushCampaign")
	}
	return &promotionsv1.GetPromotionLifecycleResponse{
		Result:    &commonv1.OperationResult{Status: pushCampaignReply.GetOutcome().GetStatus()},
		Promotion: &promotionsv1.Promotion{PromotionId: req.GetPromotionId()},
	}, nil
}

// SchedulePromotionExpiry schedules a promotion.
func (s *PromotionLifecycleServer) SchedulePromotionExpiry(ctx context.Context, req *promotionsv1.SchedulePromotionExpiryRequest) (*promotionsv1.SchedulePromotionExpiryResponse, error) {
	if err := s.store.SavePromotion(ctx, req.GetContext().GetTenantId(), req.GetPromotion()); err != nil {
		return nil, grpcx.Wrap(err, "save promotion")
	}
	s.log.InfoContext(ctx, "schedule promotion expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPromotion().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetPromotion())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded promotionsv1.Promotion
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	createLedgerAccountRequest := &ledgerv1.CreateLedgerAccountRequest{}
	createLedgerAccountRequest.Context = req.GetContext()
	createLedgerAccountRequest.LedgerAccount = &ledgerv1.LedgerAccount{
		Audit: &commonv1.AuditInfo{
			Etag:      (&decoded).GetAudit().GetEtag(),
			UpdatedBy: req.GetPromotion().GetAudit().GetUpdatedBy(),
		},
		Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
	}
	createLedgerAccountReply, err := s.ledgerAccountClient.CreateLedgerAccount(ctx, createLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/CreateLedgerAccount")
	}
	return &promotionsv1.SchedulePromotionExpiryResponse{
		Promotion: &promotionsv1.Promotion{
			Audit: &commonv1.AuditInfo{
				CreatedBy: createLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedBy(),
			},
			PromotionId: req.GetPromotionId(),
			Description: req.GetPromotion().GetDescription(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// promotionReportingLazy dials acme.promotions.v1.PromotionReportingService the first time a handler needs it.
func (s *PromotionLifecycleServer) promotionReportingLazy() promotionsv1.PromotionReportingServiceClient {
	s.promotionReportingOnce.Do(func() { s.promotionReportingCached = promotionsv1.NewPromotionReportingServiceClient(s.conn) })
	return s.promotionReportingCached
}
