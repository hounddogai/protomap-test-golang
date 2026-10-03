package inventory

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	procurementv1 "github.com/hounddogai/protomap-test-golang/gen/acme/procurement/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ReplenishmentPlanValidationServer implements `acme.inventory.v1.ReplenishmentPlanValidationService`.
// Checks replenishment plans against the rules before anything acts on them.
type ReplenishmentPlanValidationServer struct {
	inventoryv1.UnimplementedReplenishmentPlanValidationServiceServer
	store                                      *Store
	log                                        *slog.Logger
	replenishmentPlanCalculationClient         inventoryv1.ReplenishmentPlanCalculationServiceClient
	synonymSyncClient                          searchv1.SynonymSyncServiceClient
	replenishmentPlanCalculationClientFallback inventoryv1.ReplenishmentPlanCalculationServiceClient
	purchaseRequisitionClient                  procurementv1.PurchaseRequisitionServiceClient
	putawayTaskValidationClient                warehousingv1.PutawayTaskValidationServiceClient
	pointsLedgerQueryClient                    loyaltyv1.PointsLedgerQueryServiceClient
	bus                                        *eventbus.Bus
	packingSlipQueryClient                     fulfillmentv1.PackingSlipQueryServiceClient
	stockMovementValidationClient              inventoryv1.StockMovementValidationServiceClient
	conn                                       *grpc.ClientConn
	sellerValidationClient                     sellersv1.SellerValidationServiceClient
	meteredReplenishmentPlanCalculation        meteredReplenishmentPlanCalculation
}

// NewReplenishmentPlanValidationServer wires a ReplenishmentPlanValidationServer.
func NewReplenishmentPlanValidationServer(deps platform.Deps) *ReplenishmentPlanValidationServer {
	return &ReplenishmentPlanValidationServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.inventory.v1.ReplenishmentPlanValidationService"),
		replenishmentPlanCalculationClient: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		synonymSyncClient:                  searchv1.NewSynonymSyncServiceClient(deps.Conn),
		replenishmentPlanCalculationClientFallback: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		purchaseRequisitionClient:                  procurementv1.NewPurchaseRequisitionServiceClient(deps.Conn),
		putawayTaskValidationClient:                warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn),
		pointsLedgerQueryClient:                    loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		bus:                                        deps.Bus,
		packingSlipQueryClient:                     fulfillmentv1.NewPackingSlipQueryServiceClient(deps.Conn),
		stockMovementValidationClient:              inventoryv1.NewStockMovementValidationServiceClient(deps.Conn),
		conn:                                       deps.Conn,
		sellerValidationClient:                     sellersv1.NewSellerValidationServiceClient(deps.Conn),
		meteredReplenishmentPlanCalculation:        meteredReplenishmentPlanCalculation{ReplenishmentPlanCalculationServiceClient: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
	}
}

// ValidateReplenishmentPlan validates a replenishment plan.
func (s *ReplenishmentPlanValidationServer) ValidateReplenishmentPlan(ctx context.Context, req *inventoryv1.ValidateReplenishmentPlanRequest) (*inventoryv1.ValidateReplenishmentPlanResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "validate replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	quoteReplenishmentPlanRequest := newQuoteReplenishmentPlanRequest(withQuoteReplenishmentPlanContext(req.GetContext()), withQuoteReplenishmentPlanReplenishmentPlan(&inventoryv1.ReplenishmentPlan{
		Settlement: &inventoryv1.ReplenishmentPlan_Bank{Bank: &inventoryv1.ReplenishmentPlanBankSettlement{
			Iban: req.GetReplenishmentPlan().GetBank().GetIban(),
			Bic:  req.GetReplenishmentPlan().GetBank().GetBic(),
		}},
		BillingAddress: &commonv1.PostalAddress{
			Lines: req.GetReplenishmentPlan().GetBillingAddress().GetLines(),
		},
	}))
	quoteReplenishmentPlanReply, err := s.replenishmentPlanCalculationClient.QuoteReplenishmentPlan(ctx, quoteReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/QuoteReplenishmentPlan")
	}
	cloned := proto.Clone(req.GetReplenishmentPlan()).(*inventoryv1.ReplenishmentPlan)
	snapshot, err := protojson.Marshal(req.GetReplenishmentPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveSynonymConflictRequest := &searchv1.ResolveSynonymConflictRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: quoteReplenishmentPlanReply.GetReplenishmentPlan().GetAudit().GetUpdatedAt(),
				CreatedAt: quoteReplenishmentPlanReply.GetReplenishmentPlan().GetAudit().GetCreatedAt(),
				UpdatedBy: cloned.GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: req.GetReplenishmentPlan().GetCounterparty().GetDisplayName(),
			},
		},
		Details: &searchv1.ResolveSynonymConflictRequest_Details{Payload: snapshot},
	}
	resolveSynonymConflictReply, err := grpcx.Call(ctx, s.synonymSyncClient.ResolveSynonymConflict, resolveSynonymConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ResolveSynonymConflict")
	}
	routes := map[string]func(context.Context, *inventoryv1.CompareReplenishmentPlanOptionsRequest, ...grpc.CallOption) (*inventoryv1.CompareReplenishmentPlanOptionsResponse, error){
		"primary":  s.replenishmentPlanCalculationClient.CompareReplenishmentPlanOptions,
		"fallback": s.replenishmentPlanCalculationClientFallback.CompareReplenishmentPlanOptions,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	compareReplenishmentPlanOptionsRequest := &inventoryv1.CompareReplenishmentPlanOptionsRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{
				Tax: &inventoryv1.ReplenishmentPlanLineTax{
					Amount: resolveSynonymConflictReply.GetSynonym().GetDiscount().GetAmount(),
				},
			}},
			Audit: &commonv1.AuditInfo{Etag: req.GetReplenishmentPlan().GetAudit().GetEtag()},
			BillingAddress: &commonv1.PostalAddress{
				Lines: req.GetReplenishmentPlan().GetBillingAddress().GetLines(),
			},
			Counterparty: &inventoryv1.StockReservationRef{
				DisplayName: quoteReplenishmentPlanReply.GetReplenishmentPlan().GetCounterparty().GetDisplayName(),
			},
		},
	}
	compareReplenishmentPlanOptionsReply, err := route(ctx, compareReplenishmentPlanOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CompareReplenishmentPlanOptions")
	}
	return &inventoryv1.ValidateReplenishmentPlanResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: quoteReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetCountryCode(),
			},
			Total: quoteReplenishmentPlanReply.GetBreakdown().GetTotal(),
			Audit: &commonv1.AuditInfo{
				Etag: resolveSynonymConflictReply.GetSynonym().GetAudit().GetEtag(),
			},
			Notes:               compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetNotes(),
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Metadata:            req.GetReplenishmentPlan().GetMetadata(),
		},
		Result: &commonv1.OperationResult{
			Message: resolveSynonymConflictReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// PreviewReplenishmentPlan previews a replenishment plan.
func (s *ReplenishmentPlanValidationServer) PreviewReplenishmentPlan(ctx context.Context, req *inventoryv1.PreviewReplenishmentPlanRequest) (*inventoryv1.PreviewReplenishmentPlanResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "preview replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*inventoryv1.QuoteReplenishmentPlanResponse, len(req.GetReplenishmentPlan().GetAttachments()))
	for i, attachment := range req.GetReplenishmentPlan().GetAttachments() {
		group.Go(func() error {
			reply, err := s.replenishmentPlanCalculationClient.QuoteReplenishmentPlan(groupCtx, &inventoryv1.QuoteReplenishmentPlanRequest{
				Context: req.GetContext(),
				ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
					Attachments: []*inventoryv1.ReplenishmentPlanAttachment{&inventoryv1.ReplenishmentPlanAttachment{
						ContentType: attachment.GetContentType(),
						Checksum:    attachment.GetChecksum(),
						SizeBytes:   attachment.GetSizeBytes(),
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
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/QuoteReplenishmentPlan")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	for _, attachment2 := range req.GetReplenishmentPlan().GetAttachments() {
		if _, err := s.purchaseRequisitionClient.UpdatePurchaseRequisition(ctx, &procurementv1.UpdatePurchaseRequisitionRequest{
			Context: req.GetContext(),
			PurchaseRequisition: &procurementv1.PurchaseRequisition{
				Attachments: []*procurementv1.PurchaseRequisitionAttachment{&procurementv1.PurchaseRequisitionAttachment{
					ContentType: attachment2.GetContentType(),
					Checksum:    attachment2.GetChecksum(),
					SizeBytes:   attachment2.GetSizeBytes(),
					FileName:    attachment2.GetFileName(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.procurement.v1.PurchaseRequisitionService/UpdatePurchaseRequisition")
		}
	}
	return &inventoryv1.PreviewReplenishmentPlanResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Counterparty:        req.GetReplenishmentPlan().GetCounterparty(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CheckReplenishmentPlanEligibility checks a replenishment plan.
func (s *ReplenishmentPlanValidationServer) CheckReplenishmentPlanEligibility(ctx context.Context, req *inventoryv1.CheckReplenishmentPlanEligibilityRequest) (*inventoryv1.CheckReplenishmentPlanEligibilityResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "check replenishment plan eligibility", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	previewPutawayTaskRequest := buildPreviewPutawayTaskRequestForCheckReplenishmentPlanEligibility(req)
	previewPutawayTaskReply, err := s.putawayTaskValidationClient.PreviewPutawayTask(ctx, previewPutawayTaskRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/PreviewPutawayTask")
	}
	jobs := make(chan *inventoryv1.ReplenishmentPlanLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.pointsLedgerQueryClient.CountPointsLedgers(workCtx, &loyaltyv1.CountPointsLedgersRequest{
					Context: req.GetContext(),
					Filter: &loyaltyv1.CountPointsLedgersRequest_Filter{
						Region:  req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
						OwnerId: line.GetLineId(),
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetReplenishmentPlan().GetLines() {
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
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/CountPointsLedgers")
	}
	for _, line2 := range req.GetReplenishmentPlan().GetLines() {
		if _, err := s.replenishmentPlanCalculationClient.QuoteReplenishmentPlan(ctx, &inventoryv1.QuoteReplenishmentPlanRequest{
			Context: req.GetContext(),
			ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
				Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{
					Tax: &inventoryv1.ReplenishmentPlanLineTax{
						Rate:       line2.GetTax().GetRate(),
						Exemptions: line2.GetTax().GetExemptions(),
					},
					Description: line2.GetDescription(),
					UnitPrice:   line2.GetUnitPrice(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/QuoteReplenishmentPlan")
		}
	}
	return &inventoryv1.CheckReplenishmentPlanEligibilityResponse{
		Outcome: &inventoryv1.CheckReplenishmentPlanEligibilityResponse_Outcome{
			ReferenceId: previewPutawayTaskReply.GetOutcome().GetReferenceId(),
			Message:     previewPutawayTaskReply.GetResult().GetMessage(),
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Status:              req.GetReplenishmentPlan().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// NormalizeReplenishmentPlan normalizes a replenishment plan.
func (s *ReplenishmentPlanValidationServer) NormalizeReplenishmentPlan(ctx context.Context, req *inventoryv1.NormalizeReplenishmentPlanRequest) (*inventoryv1.NormalizeReplenishmentPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "normalize replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	recalculateReplenishmentPlansRequest := &inventoryv1.RecalculateReplenishmentPlansRequest{}
	recalculateReplenishmentPlansRequest.Context = req.GetContext()
	recalculateReplenishmentPlansRequest.Details = &inventoryv1.RecalculateReplenishmentPlansRequest_Details{
		Destination: &commonv1.PostalAddress{
			Region: req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
		},
	}
	recalculateReplenishmentPlansReply, err := s.replenishmentPlanCalculationClient.RecalculateReplenishmentPlans(ctx, recalculateReplenishmentPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/RecalculateReplenishmentPlans")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "inventory.normalize_replenishment_plan.calculate_replenishment_plan",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"replenishment_plan.counterparty.stock_reservation_id": recalculateReplenishmentPlansReply.GetReplenishmentPlan().GetCounterparty().GetStockReservationId(),
			"replenishment_plan.counterparty.display_name":         req.GetReplenishmentPlan().GetCounterparty().GetDisplayName(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &inventoryv1.NormalizeReplenishmentPlanResponse{
		Result: &commonv1.OperationResult{
			Status: recalculateReplenishmentPlansReply.GetResult().GetStatus(),
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			DueAt:               req.GetReplenishmentPlan().GetDueAt(),
		},
	}, nil
}

// ExplainReplenishmentPlanDecision explains a replenishment plan.
func (s *ReplenishmentPlanValidationServer) ExplainReplenishmentPlanDecision(ctx context.Context, req *inventoryv1.ExplainReplenishmentPlanDecisionRequest) (*inventoryv1.ExplainReplenishmentPlanDecisionResponse, error) {
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "explain replenishment plan decision", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	resolvePackingSlipsRequest := &fulfillmentv1.ResolvePackingSlipsRequest{Context: req.GetContext()}
	resolvePackingSlipsReply, err := grpcx.Call(ctx, s.packingSlipQueryClient.ResolvePackingSlips, resolvePackingSlipsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipQueryService/ResolvePackingSlips")
	}
	ctx = tenancy.With(ctx, "display_name", req.GetReplenishmentPlan().GetCounterparty().GetDisplayName())
	estimateReplenishmentPlanReply, err := s.replenishmentPlanCalculationClient.EstimateReplenishmentPlan(ctx, &inventoryv1.EstimateReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: &inventoryv1.StockReservationRef{DisplayName: tenancy.Value(ctx, "display_name")},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/EstimateReplenishmentPlan")
	}
	return &inventoryv1.ExplainReplenishmentPlanDecisionResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: resolvePackingSlipsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &inventoryv1.ExplainReplenishmentPlanDecisionResponse_Outcome{Message: resolvePackingSlipsReply.GetResult().GetMessage()},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: estimateReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetCountryCode(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Metadata:            req.GetReplenishmentPlan().GetMetadata(),
		},
	}, nil
}

// ScoreReplenishmentPlan scores a replenishment plan.
func (s *ReplenishmentPlanValidationServer) ScoreReplenishmentPlan(ctx context.Context, req *inventoryv1.ScoreReplenishmentPlanRequest) (*inventoryv1.ScoreReplenishmentPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScoreReplenishmentPlan", "error", err)
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "score replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetReplenishmentPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	previewStockMovementRequest := &inventoryv1.PreviewStockMovementRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetReplenishmentPlan().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	type previewStockMovementResult struct {
		reply *inventoryv1.PreviewStockMovementResponse
		err   error
	}
	previewStockMovementResults := make(chan previewStockMovementResult, 1)
	go func() {
		reply, err := s.stockMovementValidationClient.PreviewStockMovement(ctx, previewStockMovementRequest)
		previewStockMovementResults <- previewStockMovementResult{reply, err}
	}()
	var previewStockMovementReply *inventoryv1.PreviewStockMovementResponse
	select {
	case result := <-previewStockMovementResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.inventory.v1.StockMovementValidationService/PreviewStockMovement")
		}
		previewStockMovementReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*inventoryv1.ExplainReplenishmentPlanCalculationResponse, len(req.GetReplenishmentPlan().GetLines()))
	for i, line := range req.GetReplenishmentPlan().GetLines() {
		group.Go(func() error {
			reply, err := s.replenishmentPlanCalculationClient.ExplainReplenishmentPlanCalculation(groupCtx, &inventoryv1.ExplainReplenishmentPlanCalculationRequest{
				Context: req.GetContext(),
				ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
					Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{
						Tax: &inventoryv1.ReplenishmentPlanLineTax{
							Amount:       line.GetTax().GetAmount(),
							Jurisdiction: line.GetTax().GetJurisdiction(),
						},
						Attributes: line.GetAttributes(),
						UnitPrice:  line.GetUnitPrice(),
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
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/ExplainReplenishmentPlanCalculation")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &inventoryv1.ScoreReplenishmentPlanResponse{
		Result: &commonv1.OperationResult{
			Message: previewStockMovementReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			IssuedAt:            req.GetReplenishmentPlan().GetIssuedAt(),
		},
	}, nil
}

// LintReplenishmentPlans lints a replenishment plan.
func (s *ReplenishmentPlanValidationServer) LintReplenishmentPlans(ctx context.Context, req *inventoryv1.LintReplenishmentPlansRequest) (*inventoryv1.LintReplenishmentPlansResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected LintReplenishmentPlans", "error", err)
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "lint replenishment plans", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	var holder struct{ PostalCode string }
	if record := req.GetReplenishmentPlan().GetBillingAddress(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PostalCode"); err != nil {
			s.log.WarnContext(ctx, "copy postal_code", "error", err)
		}
	}
	compareReplenishmentPlanOptionsRequest := &inventoryv1.CompareReplenishmentPlanOptionsRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{PostalCode: holder.PostalCode},
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetReplenishmentPlan().GetAudit().GetCreatedBy(),
			},
			Settlement: &inventoryv1.ReplenishmentPlan_Bank{Bank: &inventoryv1.ReplenishmentPlanBankSettlement{Iban: req.GetReplenishmentPlan().GetBank().GetIban()}},
		},
	}
	compareReplenishmentPlanOptionsReply := new(inventoryv1.CompareReplenishmentPlanOptionsResponse)
	if err := s.conn.Invoke(ctx, inventoryReplenishmentPlanCalculationServiceCompareReplenishmentPlanOptionsMethod, compareReplenishmentPlanOptionsRequest, compareReplenishmentPlanOptionsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CompareReplenishmentPlanOptions")
	}
	explainReplenishmentPlanCalculationRequest := newExplainReplenishmentPlanCalculationRequest(withExplainReplenishmentPlanCalculationContext(req.GetContext()), withExplainReplenishmentPlanCalculationReplenishmentPlan(&inventoryv1.ReplenishmentPlan{
		BillingAddress: &commonv1.PostalAddress{
			Locality: compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetBillingAddress().GetLocality(),
		},
		Counterparty: &inventoryv1.StockReservationRef{
			StartsAt: req.GetReplenishmentPlan().GetCounterparty().GetStartsAt(),
		},
		Audit: &commonv1.AuditInfo{
			CreatedBy: req.GetReplenishmentPlan().GetAudit().GetCreatedBy(),
			CreatedAt: compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetAudit().GetCreatedAt(),
		},
	}))
	explainReplenishmentPlanCalculationReply, err := s.replenishmentPlanCalculationClient.ExplainReplenishmentPlanCalculation(ctx, explainReplenishmentPlanCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/ExplainReplenishmentPlanCalculation")
	}
	detailsCache := cache.New[string, *inventoryv1.LintReplenishmentPlansRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	normalizeSellerRequest := &sellersv1.NormalizeSellerRequest{
		Context: req.GetContext(),
		Seller: &sellersv1.Seller{
			Audit: &commonv1.AuditInfo{
				CreatedAt: compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetAudit().GetCreatedAt(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Recipient: req.GetReplenishmentPlan().GetBillingAddress().GetRecipient(),
			},
		},
		Details: &sellersv1.NormalizeSellerRequest_Details{
			Contact: &commonv1.ContactInfo{Name: cachedDetails.GetContact().GetName()},
		},
	}
	type normalizeSellerResult struct {
		reply *sellersv1.NormalizeSellerResponse
		err   error
	}
	normalizeSellerResults := make(chan normalizeSellerResult, 1)
	go func() {
		reply, err := s.sellerValidationClient.NormalizeSeller(ctx, normalizeSellerRequest)
		normalizeSellerResults <- normalizeSellerResult{reply, err}
	}()
	var normalizeSellerReply *sellersv1.NormalizeSellerResponse
	select {
	case result := <-normalizeSellerResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.sellers.v1.SellerValidationService/NormalizeSeller")
		}
		normalizeSellerReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	cloned := proto.Clone(req.GetReplenishmentPlan()).(*inventoryv1.ReplenishmentPlan)
	scoreStockMovementRequest := &inventoryv1.ScoreStockMovementRequest{}
	scoreStockMovementRequest.Context = req.GetContext()
	scoreStockMovementRequest.Details = &inventoryv1.ScoreStockMovementRequest_Details{
		Destination: &commonv1.PostalAddress{
			Locality:   cloned.GetBillingAddress().GetLocality(),
			Region:     compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
			PostalCode: explainReplenishmentPlanCalculationReply.GetReplenishmentPlan().GetBillingAddress().GetPostalCode(),
		},
	}
	scoreStockMovementRequest.StockMovement = &inventoryv1.StockMovement{
		Subject: &customersv1.CustomerRef{
			Email: normalizeSellerReply.GetSeller().GetRelationshipManager().GetEmail(),
		},
	}
	scoreStockMovementReply, err := s.stockMovementValidationClient.ScoreStockMovement(ctx, scoreStockMovementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/ScoreStockMovement")
	}
	return &inventoryv1.LintReplenishmentPlansResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetCounterparty(),
			BillingAddress: &commonv1.PostalAddress{
				Lines: explainReplenishmentPlanCalculationReply.GetReplenishmentPlan().GetBillingAddress().GetLines(),
			},
			Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{
				Attributes: scoreStockMovementReply.GetStockMovement().GetAttributes(),
			}},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			IssuedAt:            req.GetReplenishmentPlan().GetIssuedAt(),
		},
		Outcome: &inventoryv1.LintReplenishmentPlansResponse_Outcome{Message: normalizeSellerReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: scoreStockMovementReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetReplenishmentPlanRules returns a replenishment plan.
func (s *ReplenishmentPlanValidationServer) GetReplenishmentPlanRules(ctx context.Context, req *inventoryv1.GetReplenishmentPlanRulesRequest) (*inventoryv1.GetReplenishmentPlanRulesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "replenishment_plan", req.GetReplenishmentPlanId(), "get_replenishment_plan_rules"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get replenishment plan rules", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	compareReplenishmentPlanOptionsRequest := &inventoryv1.CompareReplenishmentPlanOptionsRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Counterparty:        &inventoryv1.StockReservationRef{StartsAt: req.GetContext().GetDeadline()},
			Lines:               []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{Attributes: baggage}},
		},
	}
	compareReplenishmentPlanOptionsReply, err := s.meteredReplenishmentPlanCalculation.CompareReplenishmentPlanOptions(ctx, compareReplenishmentPlanOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CompareReplenishmentPlanOptions")
	}
	return &inventoryv1.GetReplenishmentPlanRulesResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetAudit().GetUpdatedAt(),
				UpdatedBy: compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetAudit().GetUpdatedBy(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
