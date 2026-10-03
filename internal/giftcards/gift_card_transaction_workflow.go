package giftcards

import (
	"context"
	"log/slog"

	cartv1 "github.com/hounddogai/protomap-test-golang/gen/acme/cart/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	riskv1 "github.com/hounddogai/protomap-test-golang/gen/acme/risk/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// GiftCardTransactionWorkflowServer implements `acme.giftcards.v1.GiftCardTransactionWorkflowService`.
// Runs the long-lived workflow that takes a gift card transaction from start to finish.
type GiftCardTransactionWorkflowServer struct {
	giftcardsv1.UnimplementedGiftCardTransactionWorkflowServiceServer
	store                                 *Store
	log                                   *slog.Logger
	replenishmentPlanCalculationClient    inventoryv1.ReplenishmentPlanCalculationServiceClient
	giftCardWorkflowPort                  giftCardWorkflowPort
	cartRuleCalculationClient             cartv1.CartRuleCalculationServiceClient
	householdCalculationClient            customersv1.HouseholdCalculationServiceClient
	giftCardBatchWorkflowClient           giftcardsv1.GiftCardBatchWorkflowServiceClient
	sellerPayoutScheduleCalculationClient sellersv1.SellerPayoutScheduleCalculationServiceClient
	riskRegisterClient                    riskv1.RiskRegisterServiceClient
	giftCardBatchApprovalClient           giftcardsv1.GiftCardBatchApprovalServiceClient
	clients                               *registry.Registry
	bus                                   *eventbus.Bus
	giftCardTransactionApprovalClient     giftcardsv1.GiftCardTransactionApprovalServiceClient
	searchQueryQueryClient                searchv1.SearchQueryQueryServiceClient
}

// NewGiftCardTransactionWorkflowServer wires a GiftCardTransactionWorkflowServer.
func NewGiftCardTransactionWorkflowServer(deps platform.Deps) *GiftCardTransactionWorkflowServer {
	return &GiftCardTransactionWorkflowServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.giftcards.v1.GiftCardTransactionWorkflowService"),
		replenishmentPlanCalculationClient:    inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		giftCardWorkflowPort:                  newGiftCardWorkflowPort(deps),
		cartRuleCalculationClient:             cartv1.NewCartRuleCalculationServiceClient(deps.Conn),
		householdCalculationClient:            customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		giftCardBatchWorkflowClient:           giftcardsv1.NewGiftCardBatchWorkflowServiceClient(deps.Conn),
		sellerPayoutScheduleCalculationClient: sellersv1.NewSellerPayoutScheduleCalculationServiceClient(deps.Conn),
		riskRegisterClient:                    riskv1.NewRiskRegisterServiceClient(deps.Conn),
		giftCardBatchApprovalClient:           giftcardsv1.NewGiftCardBatchApprovalServiceClient(deps.Conn),
		clients:                               deps.Clients,
		bus:                                   deps.Bus,
		giftCardTransactionApprovalClient:     giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		searchQueryQueryClient:                searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
	}
}

// StartGiftCardTransactionWorkflow starts a gift card transaction.
func (s *GiftCardTransactionWorkflowServer) StartGiftCardTransactionWorkflow(ctx context.Context, req *giftcardsv1.StartGiftCardTransactionWorkflowRequest) (*giftcardsv1.StartGiftCardTransactionWorkflowResponse, error) {
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "start gift card transaction workflow", "tenant", req.GetContext().GetTenantId())
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	compareReplenishmentPlanOptionsReply, err := s.replenishmentPlanCalculationClient.CompareReplenishmentPlanOptions(ctx, &inventoryv1.CompareReplenishmentPlanOptionsRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: &inventoryv1.StockReservationRef{
				DisplayName: req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
			Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{Attributes: req.GetGiftCardTransaction().GetAttributes()}},
		},
		Reason:              req.GetReason(),
		ReplenishmentPlanId: envelope.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CompareReplenishmentPlanOptions")
	}
	signalGiftCardWorkflowRequest := &giftcardsv1.SignalGiftCardWorkflowRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Funding: &giftcardsv1.GiftCard_Card{Card: &giftcardsv1.GiftCardCardFunding{
				HolderName: compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetCard().GetHolderName(),
				Network:    compareReplenishmentPlanOptionsReply.GetReplenishmentPlan().GetCard().GetNetwork(),
			}},
		},
	}
	signalGiftCardWorkflowReply, err := s.giftCardWorkflowPort.SignalGiftCardWorkflow(ctx, signalGiftCardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardWorkflowService/SignalGiftCardWorkflow")
	}
	return &giftcardsv1.StartGiftCardTransactionWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: compareReplenishmentPlanOptionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &giftcardsv1.StartGiftCardTransactionWorkflowResponse_Outcome{
			Message: compareReplenishmentPlanOptionsReply.GetResult().GetMessage(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Kind:                  signalGiftCardWorkflowReply.GetGiftCard().GetHolder().GetKind(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Subject:               req.GetGiftCardTransaction().GetSubject(),
		},
	}, nil
}

// AdvanceGiftCardTransactionWorkflow advances a gift card transaction.
func (s *GiftCardTransactionWorkflowServer) AdvanceGiftCardTransactionWorkflow(ctx context.Context, req *giftcardsv1.AdvanceGiftCardTransactionWorkflowRequest) (*giftcardsv1.AdvanceGiftCardTransactionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "advance gift card transaction workflow", "tenant", req.GetContext().GetTenantId())
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &giftCardTransactionWorkflowServerAdvanceGiftCardTransactionWorkflowSimulateCartRuleStep{request: &cartv1.SimulateCartRuleRequest{
		Context: req.GetContext(),
		CartRule: &cartv1.CartRule{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNotify().GetEmail(),
				DisplayName: req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
				UserId:      req.GetContext().GetActor().GetUserId(),
			},
			Action: &cartv1.CartRule_Block{Block: &cartv1.CartRuleBlockAction{Reason: req.GetReason()}},
		},
		CartRuleId: envelope.GetDisplayName(),
	}}
	if err := s.runAdvanceGiftCardTransactionWorkflowSimulateCartRule(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.cart.v1.CartRuleCalculationService/SimulateCartRule")
	}
	simulateCartRuleReply := step.reply
	explainHouseholdCalculationRequest := &customersv1.ExplainHouseholdCalculationRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						Etag:      simulateCartRuleReply.GetCartRule().GetAudit().GetEtag(),
						UpdatedAt: simulateCartRuleReply.GetCartRule().GetAudit().GetUpdatedAt(),
					},
					Manager: &identityv1.UserRef{
						Email:       simulateCartRuleReply.GetCartRule().GetOwner().GetEmail(),
						DisplayName: req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
					},
				}},
			}},
		},
	}
	explainHouseholdCalculationReply, err := grpcx.Call(ctx, s.householdCalculationClient.ExplainHouseholdCalculation, explainHouseholdCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation")
	}
	return &giftcardsv1.AdvanceGiftCardTransactionWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: simulateCartRuleReply.GetResult().GetCorrelationId(),
			Status:        simulateCartRuleReply.GetResult().GetStatus(),
		},
		Outcome: &giftcardsv1.AdvanceGiftCardTransactionWorkflowResponse_Outcome{
			Message: explainHouseholdCalculationReply.GetResult().GetMessage(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			OccurredAt:            req.GetGiftCardTransaction().GetOccurredAt(),
		},
	}, nil
}

// CancelGiftCardTransactionWorkflow cancels a gift card transaction.
func (s *GiftCardTransactionWorkflowServer) CancelGiftCardTransactionWorkflow(ctx context.Context, req *giftcardsv1.CancelGiftCardTransactionWorkflowRequest) (*giftcardsv1.CancelGiftCardTransactionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelGiftCardTransactionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "cancel gift card transaction workflow", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetGiftCardTransaction().GetMeasurements() {
		if _, err := s.giftCardBatchWorkflowClient.ResumeGiftCardBatchWorkflow(ctx, &giftcardsv1.ResumeGiftCardBatchWorkflowRequest{
			Context: req.GetContext(),
			GiftCardBatch: &giftcardsv1.GiftCardBatch{
				Audit: &commonv1.AuditInfo{CreatedAt: measurement.GetObservedAt()},
				Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{Attributes: req.GetGiftCardTransaction().GetAttributes()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/ResumeGiftCardBatchWorkflow")
		}
	}
	simulateSellerPayoutScheduleRequest := &sellersv1.SimulateSellerPayoutScheduleRequest{
		Context: req.GetContext(),
		SellerPayoutSchedule: &sellersv1.SellerPayoutSchedule{
			Participants: []*sellersv1.SellerPayoutScheduleParticipant{&sellersv1.SellerPayoutScheduleParticipant{
				Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			}},
		},
	}
	simulateSellerPayoutScheduleReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*sellersv1.SimulateSellerPayoutScheduleResponse, error) {
		return s.sellerPayoutScheduleCalculationClient.SimulateSellerPayoutSchedule(ctx, simulateSellerPayoutScheduleRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleCalculationService/SimulateSellerPayoutSchedule")
	}
	return &giftcardsv1.CancelGiftCardTransactionWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: simulateSellerPayoutScheduleReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId:         simulateSellerPayoutScheduleReply.GetResult().GetCorrelationId(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Kind:                  req.GetGiftCardTransaction().GetKind(),
		},
	}, nil
}

// PauseGiftCardTransactionWorkflow pauses a gift card transaction.
func (s *GiftCardTransactionWorkflowServer) PauseGiftCardTransactionWorkflow(ctx context.Context, req *giftcardsv1.PauseGiftCardTransactionWorkflowRequest) (*giftcardsv1.PauseGiftCardTransactionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseGiftCardTransactionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "pause gift card transaction workflow", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetGiftCardTransaction()).(*giftcardsv1.GiftCardTransaction)
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listRiskRegistersRequest := &riskv1.ListRiskRegistersRequest{
		Context: req.GetContext(),
		Filter: &riskv1.ListRiskRegistersRequest_Filter{
			CreatedAfter: cloned.GetOccurredAt(),
			OwnerId:      req.GetGiftCardTransactionId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	var listRiskRegistersReply *riskv1.ListRiskRegistersResponse
	if err := pipeline.Run(ctx, "acme.risk.v1.RiskRegisterService/ListRiskRegisters", func(ctx context.Context) error {
		answer, err := s.riskRegisterClient.ListRiskRegisters(ctx, listRiskRegistersRequest)
		listRiskRegistersReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.risk.v1.RiskRegisterService/ListRiskRegisters")
	}
	return &giftcardsv1.PauseGiftCardTransactionWorkflowResponse{
		Outcome: &giftcardsv1.PauseGiftCardTransactionWorkflowResponse_Outcome{Status: listRiskRegistersReply.GetResult().GetStatus()},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			OccurredAt:            req.GetGiftCardTransaction().GetOccurredAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeGiftCardTransactionWorkflow resumes a gift card transaction.
func (s *GiftCardTransactionWorkflowServer) ResumeGiftCardTransactionWorkflow(ctx context.Context, req *giftcardsv1.ResumeGiftCardTransactionWorkflowRequest) (*giftcardsv1.ResumeGiftCardTransactionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "resume gift card transaction workflow", "tenant", req.GetContext().GetTenantId())
	attributes := make(map[string]string, len(req.GetGiftCardTransaction().GetAttributes()))
	for key, value := range req.GetGiftCardTransaction().GetAttributes() {
		attributes[key] = value
	}
	delegateGiftCardBatchApprovalReply, err := s.giftCardBatchApprovalClient.DelegateGiftCardBatchApproval(ctx, &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.DelegateGiftCardBatchApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				DisplayName: req.GetGiftCardTransaction().GetSubject().GetDisplayName(),
			},
		},
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Lines:           []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{Attributes: attributes}},
			GiftCardBatchId: req.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId(),
			Number:          req.GetGiftCardTransaction().GetSubject().GetNumber(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	giftCardTransactionApproval, ok := s.clients.Get("acme.giftcards.v1.GiftCardTransactionApprovalService").(giftcardsv1.GiftCardTransactionApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.giftcards.v1.GiftCardTransactionApprovalService")
	}
	encoded, err := protojson.Marshal(req.GetGiftCardTransaction())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded giftcardsv1.GiftCardTransaction
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope giftcardsv1.GiftCardRef
	if err := req.GetGiftCardTransaction().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	submitGiftCardTransactionRequest := &giftcardsv1.SubmitGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				GiftCardBatchId: req.GetGiftCardTransaction().GetSubject().GetGiftCardBatchId(),
				DisplayName:     (&decoded).GetSubject().GetDisplayName(),
			},
		},
		GiftCardTransactionId: envelope.GetDisplayName(),
	}
	submitGiftCardTransactionReply, err := giftCardTransactionApproval.SubmitGiftCardTransaction(ctx, submitGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/SubmitGiftCardTransaction")
	}
	return &giftcardsv1.ResumeGiftCardTransactionWorkflowResponse{
		Outcome: &giftcardsv1.ResumeGiftCardTransactionWorkflowResponse_Outcome{
			Message: delegateGiftCardBatchApprovalReply.GetOutcome().GetMessage(),
		},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			CorrelationId:         submitGiftCardTransactionReply.GetResult().GetCorrelationId(),
			GiftCardTransactionId: req.GetGiftCardTransactionId(),
			Attributes:            req.GetGiftCardTransaction().GetAttributes(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectGiftCardTransactionWorkflow returns a gift card transaction.
func (s *GiftCardTransactionWorkflowServer) InspectGiftCardTransactionWorkflow(ctx context.Context, req *giftcardsv1.InspectGiftCardTransactionWorkflowRequest) (*giftcardsv1.InspectGiftCardTransactionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "gift_card_transaction", req.GetGiftCardTransactionId(), "inspect_gift_card_transaction_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect gift card transaction workflow", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "giftcards.inspect_gift_card_transaction_workflow.list_pending_gift_cards",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   req.GetGiftCardTransactionId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &giftcardsv1.InspectGiftCardTransactionWorkflowResponse{
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{GiftCardTransactionId: req.GetGiftCardTransactionId()},
		Result:              &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListGiftCardTransactionWorkflows lists gift card transactions a page at a time.
func (s *GiftCardTransactionWorkflowServer) ListGiftCardTransactionWorkflows(ctx context.Context, req *giftcardsv1.ListGiftCardTransactionWorkflowsRequest) (*giftcardsv1.ListGiftCardTransactionWorkflowsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "list gift card transaction workflows", "tenant", req.GetContext().GetTenantId())
	return &giftcardsv1.ListGiftCardTransactionWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalGiftCardTransactionWorkflow signals a gift card transaction.
func (s *GiftCardTransactionWorkflowServer) SignalGiftCardTransactionWorkflow(ctx context.Context, req *giftcardsv1.SignalGiftCardTransactionWorkflowRequest) (*giftcardsv1.SignalGiftCardTransactionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalGiftCardTransactionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveGiftCardTransaction(ctx, req.GetContext().GetTenantId(), req.GetGiftCardTransaction()); err != nil {
		return nil, grpcx.Wrap(err, "save gift card transaction")
	}
	s.log.InfoContext(ctx, "signal gift card transaction workflow", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetGiftCardTransaction().GetMeasurements() {
		if _, err := s.giftCardTransactionApprovalClient.CommentGiftCardTransactionApproval(ctx, &giftcardsv1.CommentGiftCardTransactionApprovalRequest{
			Context: req.GetContext(),
			GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
				Measurements: []*giftcardsv1.GiftCardTransactionMeasurement{&giftcardsv1.GiftCardTransactionMeasurement{
					Unit:       measurement.GetUnit(),
					Metric:     measurement.GetMetric(),
					Value:      measurement.GetValue(),
					ObservedAt: measurement.GetObservedAt(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/CommentGiftCardTransactionApproval")
		}
	}
	giftCardTransactionCache := cache.New[string, *giftcardsv1.GiftCardTransaction](1)
	giftCardTransactionCache.Put(req.GetContext().GetTraceId(), req.GetGiftCardTransaction())
	cachedGiftCardTransaction, _ := giftCardTransactionCache.Get(req.GetContext().GetTraceId())
	countSearchQueriesRequest := &searchv1.CountSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.CountSearchQueriesRequest_Filter{
			OwnerId: cachedGiftCardTransaction.GetGiftCardTransactionId(),
		},
	}
	send := s.countSearchQueriesSender(countSearchQueriesRequest)
	countSearchQueriesReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/CountSearchQueries")
	}
	return &giftcardsv1.SignalGiftCardTransactionWorkflowResponse{
		Outcome:             &giftcardsv1.SignalGiftCardTransactionWorkflowResponse_Outcome{Status: countSearchQueriesReply.GetResult().GetStatus()},
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{GiftCardTransactionId: req.GetGiftCardTransactionId()},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetGiftCardTransaction().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// giftCardTransactionWorkflowServerAdvanceGiftCardTransactionWorkflowSimulateCartRuleStep carries the request of one
// step of AdvanceGiftCardTransactionWorkflow and, once it ran, its reply.
type giftCardTransactionWorkflowServerAdvanceGiftCardTransactionWorkflowSimulateCartRuleStep struct {
	request *cartv1.SimulateCartRuleRequest
	reply   *cartv1.SimulateCartRuleResponse
}

func (s *GiftCardTransactionWorkflowServer) runAdvanceGiftCardTransactionWorkflowSimulateCartRule(ctx context.Context, step *giftCardTransactionWorkflowServerAdvanceGiftCardTransactionWorkflowSimulateCartRuleStep) error {
	reply, err := s.cartRuleCalculationClient.SimulateCartRule(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// countSearchQueriesSender returns a function that sends one request to
// acme.search.v1.SearchQueryQueryService/CountSearchQueries.
func (s *GiftCardTransactionWorkflowServer) countSearchQueriesSender(request *searchv1.CountSearchQueriesRequest) func(context.Context) (*searchv1.CountSearchQueriesResponse, error) {
	return func(ctx context.Context) (*searchv1.CountSearchQueriesResponse, error) {
		return s.searchQueryQueryClient.CountSearchQueries(ctx, request)
	}
}
