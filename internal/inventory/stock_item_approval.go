package inventory

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// StockItemApprovalServer implements `acme.inventory.v1.StockItemApprovalService`.
// Routes stock items through approval, with escalation and delegation.
type StockItemApprovalServer struct {
	inventoryv1.UnimplementedStockItemApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	conn                              *grpc.ClientConn
	stockMovementValidationClient     inventoryv1.StockMovementValidationServiceClient
	replenishmentPlanValidationClient inventoryv1.ReplenishmentPlanValidationServiceClient
	bus                               *eventbus.Bus
	validateStockMovementFn           func(context.Context, *inventoryv1.ValidateStockMovementRequest, ...grpc.CallOption) (*inventoryv1.ValidateStockMovementResponse, error)
	packingSlipWorkflowClient         fulfillmentv1.PackingSlipWorkflowServiceClient
	stockItemClient                   inventoryv1.StockItemServiceClient
	preferenceValidationClient        customersv1.PreferenceValidationServiceClient
	giftCardTransactionApprovalClient giftcardsv1.GiftCardTransactionApprovalServiceClient
	putawayTaskApprovalClient         warehousingv1.PutawayTaskApprovalServiceClient
	clients                           *registry.Registry
	householdQueryPort                householdQueryPort
	ledgerAccountValidationClient     ledgerv1.LedgerAccountValidationServiceClient
}

// NewStockItemApprovalServer wires a StockItemApprovalServer.
func NewStockItemApprovalServer(deps platform.Deps) *StockItemApprovalServer {
	stockMovementValidation := inventoryv1.NewStockMovementValidationServiceClient(deps.Conn)
	return &StockItemApprovalServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.inventory.v1.StockItemApprovalService"),
		conn:                              deps.Conn,
		stockMovementValidationClient:     inventoryv1.NewStockMovementValidationServiceClient(deps.Conn),
		replenishmentPlanValidationClient: inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn),
		bus:                               deps.Bus,
		validateStockMovementFn:           stockMovementValidation.ValidateStockMovement,
		packingSlipWorkflowClient:         fulfillmentv1.NewPackingSlipWorkflowServiceClient(deps.Conn),
		stockItemClient:                   inventoryv1.NewStockItemServiceClient(deps.Conn),
		preferenceValidationClient:        customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		giftCardTransactionApprovalClient: giftcardsv1.NewGiftCardTransactionApprovalServiceClient(deps.Conn),
		putawayTaskApprovalClient:         warehousingv1.NewPutawayTaskApprovalServiceClient(deps.Conn),
		clients:                           deps.Clients,
		householdQueryPort:                newHouseholdQueryPort(deps),
		ledgerAccountValidationClient:     ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
	}
}

// SubmitStockItem submits a stock item.
func (s *StockItemApprovalServer) SubmitStockItem(ctx context.Context, req *inventoryv1.SubmitStockItemRequest) (*inventoryv1.SubmitStockItemResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "submit stock item", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetStockItem()).(*inventoryv1.StockItem)
	normalizeReplenishmentPlanRequest := &inventoryv1.NormalizeReplenishmentPlanRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit:        &commonv1.AuditInfo{CreatedBy: req.GetStockItem().GetAudit().GetCreatedBy()},
			Counterparty: &inventoryv1.StockReservationRef{DisplayName: cloned.GetOwner().GetDisplayName()},
		},
		Details: &inventoryv1.NormalizeReplenishmentPlanRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetStockItem().GetOwner().GetEmail()},
		},
	}
	normalizeReplenishmentPlanReply := new(inventoryv1.NormalizeReplenishmentPlanResponse)
	if err := grpcx.Invoke(ctx, s.conn, inventoryv1.ReplenishmentPlanValidationService_NormalizeReplenishmentPlan_FullMethodName, normalizeReplenishmentPlanRequest, normalizeReplenishmentPlanReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/NormalizeReplenishmentPlan")
	}
	packed, err := anypb.New(req.GetStockItem())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	checkStockMovementEligibilityRequest := &inventoryv1.CheckStockMovementEligibilityRequest{}
	checkStockMovementEligibilityRequest.Context = req.GetContext()
	checkStockMovementEligibilityRequest.StockMovement = &inventoryv1.StockMovement{
		Subject:      &customersv1.CustomerRef{Email: req.GetDetails().GetApprover().GetEmail()},
		Measurements: []*inventoryv1.StockMovementMeasurement{&inventoryv1.StockMovementMeasurement{Unit: req.GetStockItem().GetDimensions().GetUnit()}},
		Detail:       packed,
	}
	checkStockMovementEligibilityReply, err := s.stockMovementValidationClient.CheckStockMovementEligibility(ctx, checkStockMovementEligibilityRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/CheckStockMovementEligibility")
	}
	lintReplenishmentPlansReply, err := s.replenishmentPlanValidationClient.LintReplenishmentPlans(ctx, &inventoryv1.LintReplenishmentPlansRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				Lines: normalizeReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetLines(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetStockItem().GetAudit().GetCreatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/LintReplenishmentPlans")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "inventory.submit_stock_item.score_replenishment_plan",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"replenishment_plan.counterparty.display_name":   lintReplenishmentPlansReply.GetReplenishmentPlan().GetCounterparty().GetDisplayName(),
			"replenishment_plan.billing_address.postal_code": req.GetStockItem().GetAddress().GetPostalCode(),
			"replenishment_plan.audit.etag":                  normalizeReplenishmentPlanReply.GetReplenishmentPlan().GetAudit().GetEtag(),
			"context.tenant_id":                              req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &inventoryv1.SubmitStockItemResponse{
		Outcome: &inventoryv1.SubmitStockItemResponse_Outcome{
			NextSteps:   normalizeReplenishmentPlanReply.GetOutcome().GetNextSteps(),
			ReferenceId: checkStockMovementEligibilityReply.GetOutcome().GetReferenceId(),
		},
		StockItem: &inventoryv1.StockItem{
			Audit: &commonv1.AuditInfo{
				Etag: normalizeReplenishmentPlanReply.GetReplenishmentPlan().GetAudit().GetEtag(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: checkStockMovementEligibilityReply.GetStockMovement().GetSubject().GetDisplayName(),
			},
			StockItemId: req.GetStockItemId(),
			Name:        req.GetStockItem().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveStockItem approves a stock item.
func (s *StockItemApprovalServer) ApproveStockItem(ctx context.Context, req *inventoryv1.ApproveStockItemRequest) (*inventoryv1.ApproveStockItemResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveStockItem", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "approve stock item", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	validateStockMovementRequest := &inventoryv1.ValidateStockMovementRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
	}
	validateStockMovementReply, err := s.validateStockMovementFn(ctx, validateStockMovementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/ValidateStockMovement")
	}
	resumePackingSlipWorkflowRequest := buildResumePackingSlipWorkflowRequestForApproveStockItem(req)
	resumePackingSlipWorkflowReply, err := s.packingSlipWorkflowClient.ResumePackingSlipWorkflow(ctx, resumePackingSlipWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.PackingSlipWorkflowService/ResumePackingSlipWorkflow")
	}
	return &inventoryv1.ApproveStockItemResponse{
		Outcome: &inventoryv1.ApproveStockItemResponse_Outcome{
			ReferenceId: validateStockMovementReply.GetOutcome().GetReferenceId(),
			EffectiveAt: resumePackingSlipWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
		StockItem: &inventoryv1.StockItem{
			Owner: &identityv1.UserRef{
				Email: validateStockMovementReply.GetStockMovement().GetSubject().GetEmail(),
			},
			StockItemId: req.GetStockItemId(),
			Name:        req.GetStockItem().GetName(),
		},
		Result: &commonv1.OperationResult{
			Message: resumePackingSlipWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// RejectStockItem rejects a stock item.
func (s *StockItemApprovalServer) RejectStockItem(ctx context.Context, req *inventoryv1.RejectStockItemRequest) (*inventoryv1.RejectStockItemResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "reject stock item", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	specs := make(map[string]string, len(req.GetStockItem().GetSpecs()))
	for key, value := range req.GetStockItem().GetSpecs() {
		specs[key] = value
	}
	step := &stockItemApprovalServerRejectStockItemSearchStockItemsStep{request: &inventoryv1.SearchStockItemsRequest{
		Context: req.GetContext(),
		Filter: &inventoryv1.SearchStockItemsRequest_Filter{
			Region:       req.GetStockItem().GetAddress().GetRegion(),
			CreatedAfter: req.GetStockItem().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetStockItem().GetStockItemId(),
			Labels:       specs,
		},
	}}
	if err := s.runRejectStockItemSearchStockItems(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/SearchStockItems")
	}
	searchStockItemsReply := step.reply
	return &inventoryv1.RejectStockItemResponse{
		Result: &commonv1.OperationResult{
			Message: searchStockItemsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &inventoryv1.RejectStockItemResponse_Outcome{Status: searchStockItemsReply.GetResult().GetStatus()},
		StockItem: &inventoryv1.StockItem{
			StockItemId: req.GetStockItemId(),
			Capacity:    req.GetStockItem().GetCapacity(),
		},
	}, nil
}

// EscalateStockItem escalates a stock item.
func (s *StockItemApprovalServer) EscalateStockItem(ctx context.Context, req *inventoryv1.EscalateStockItemRequest) (*inventoryv1.EscalateStockItemResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected EscalateStockItem", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "escalate stock item", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	source := req.GetStockItem().GetOwner().ProtoReflect()
	email := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("email"))).String()
	cloned := proto.Clone(req.GetStockItem()).(*inventoryv1.StockItem)
	cloned2 := proto.Clone(req.GetStockItem()).(*inventoryv1.StockItem)
	explainPreferenceDecisionReply, err := s.preferenceValidationClient.ExplainPreferenceDecision(ctx, &customersv1.ExplainPreferenceDecisionRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				Email:       email,
				DisplayName: cloned2.GetOwner().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: cloned.GetAudit().GetUpdatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ExplainPreferenceDecision")
	}
	detailsCache := cache.New[string, *inventoryv1.EscalateStockItemRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetStockItem())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	approveGiftCardTransactionRequest := &giftcardsv1.ApproveGiftCardTransactionRequest{
		Context: req.GetContext(),
		GiftCardTransaction: &giftcardsv1.GiftCardTransaction{
			Subject: &giftcardsv1.GiftCardBatchRef{
				DisplayName: explainPreferenceDecisionReply.GetPreference().GetOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &giftcardsv1.ApproveGiftCardTransactionRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       explainPreferenceDecisionReply.GetPreference().GetOwner().GetEmail(),
				DisplayName: cachedDetails.GetApprover().GetDisplayName(),
				UserId:      explainPreferenceDecisionReply.GetPreference().GetOwner().GetUserId(),
			},
		},
	}
	approveGiftCardTransactionReply, err := grpcx.Call(ctx, s.giftCardTransactionApprovalClient.ApproveGiftCardTransaction, approveGiftCardTransactionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardTransactionApprovalService/ApproveGiftCardTransaction")
	}
	scoreStockMovementRequest := &inventoryv1.ScoreStockMovementRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: explainPreferenceDecisionReply.GetPreference().GetOwner().GetDisplayName(),
			},
		},
	}
	type scoreStockMovementResult struct {
		reply *inventoryv1.ScoreStockMovementResponse
		err   error
	}
	scoreStockMovementResults := make(chan scoreStockMovementResult, 1)
	go func() {
		reply, err := s.stockMovementValidationClient.ScoreStockMovement(ctx, scoreStockMovementRequest)
		scoreStockMovementResults <- scoreStockMovementResult{reply, err}
	}()
	var scoreStockMovementReply *inventoryv1.ScoreStockMovementResponse
	select {
	case result := <-scoreStockMovementResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.inventory.v1.StockMovementValidationService/ScoreStockMovement")
		}
		scoreStockMovementReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &inventoryv1.EscalateStockItemResponse{
		StockItem: &inventoryv1.StockItem{
			Audit: &commonv1.AuditInfo{
				CreatedBy: explainPreferenceDecisionReply.GetPreference().GetAudit().GetCreatedBy(),
			},
			StockItemId: req.GetStockItemId(),
		},
		Result: &commonv1.OperationResult{
			Status:        approveGiftCardTransactionReply.GetResult().GetStatus(),
			Message:       approveGiftCardTransactionReply.GetResult().GetMessage(),
			CorrelationId: scoreStockMovementReply.GetStockMovement().GetCorrelationId(),
		},
		Outcome: &inventoryv1.EscalateStockItemResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// ListPendingStockItems lists stock items a page at a time.
func (s *StockItemApprovalServer) ListPendingStockItems(ctx context.Context, req *inventoryv1.ListPendingStockItemsRequest) (*inventoryv1.ListPendingStockItemsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending stock items", "tenant", req.GetContext().GetTenantId())
	submitPutawayTaskRequest := buildSubmitPutawayTaskRequestForListPendingStockItems(req)
	submitPutawayTaskReply, err := s.putawayTaskApprovalClient.SubmitPutawayTask(ctx, submitPutawayTaskRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskApprovalService/SubmitPutawayTask")
	}
	return &inventoryv1.ListPendingStockItemsResponse{
		StockItems: []*inventoryv1.StockItem{&inventoryv1.StockItem{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: submitPutawayTaskReply.GetPutawayTask().GetAudit().GetUpdatedBy(),
				CreatedAt: submitPutawayTaskReply.GetPutawayTask().GetAudit().GetCreatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateStockItemApproval delegates a stock item.
func (s *StockItemApprovalServer) DelegateStockItemApproval(ctx context.Context, req *inventoryv1.DelegateStockItemApprovalRequest) (*inventoryv1.DelegateStockItemApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected DelegateStockItemApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "delegate stock item approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	stockReservationQuery, err := registry.Lookup[inventoryv1.StockReservationQueryServiceClient](s.clients, "acme.inventory.v1.StockReservationQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeStockReservationsRequest := &inventoryv1.SummarizeStockReservationsRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &inventoryv1.SummarizeStockReservationsRequest_Details{Filters: req.GetStockItem().GetSpecs()},
	}
	summarizeStockReservationsReply, err := stockReservationQuery.SummarizeStockReservations(ctx, summarizeStockReservationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/SummarizeStockReservations")
	}
	return &inventoryv1.DelegateStockItemApprovalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeStockReservationsReply.GetResult().GetCorrelationId(),
			Message:       summarizeStockReservationsReply.GetResult().GetMessage(),
			Status:        "ok",
		},
		StockItem: &inventoryv1.StockItem{
			StockItemId: req.GetStockItemId(),
			Specs:       req.GetStockItem().GetSpecs(),
		},
	}, nil
}

// RecallStockItem recalls a stock item.
func (s *StockItemApprovalServer) RecallStockItem(ctx context.Context, req *inventoryv1.RecallStockItemRequest) (*inventoryv1.RecallStockItemResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallStockItem", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "recall stock item", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	retryGiftCardBatchWorkflowRequest := &giftcardsv1.RetryGiftCardBatchWorkflowRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetStockItem().GetAudit().GetUpdatedBy(),
				UpdatedAt: req.GetStockItem().GetAudit().GetUpdatedAt(),
			},
			Counterparty: &giftcardsv1.GiftCardRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			Lines: []*giftcardsv1.GiftCardBatchLine{&giftcardsv1.GiftCardBatchLine{
				Quantity: req.GetStockItem().GetComponents()["primary"].GetQuantity(),
			}},
		},
	}
	retryGiftCardBatchWorkflowReply, err := grpcx.NewClient(s.conn, giftcardsv1.NewGiftCardBatchWorkflowServiceClient).RetryGiftCardBatchWorkflow(ctx, retryGiftCardBatchWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchWorkflowService/RetryGiftCardBatchWorkflow")
	}
	resolveHouseholdsRequest := &customersv1.ResolveHouseholdsRequest{Context: req.GetContext()}
	resolveHouseholdsReply, err := s.householdQueryPort.ResolveHouseholds(ctx, resolveHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ResolveHouseholds")
	}
	_ = resolveHouseholdsReply
	return &inventoryv1.RecallStockItemResponse{
		Result: &commonv1.OperationResult{
			Details: retryGiftCardBatchWorkflowReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &inventoryv1.RecallStockItemResponse_Outcome{
			NextSteps:   retryGiftCardBatchWorkflowReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		StockItem: &inventoryv1.StockItem{StockItemId: req.GetStockItemId()},
	}, nil
}

// GetStockItemApprovalStatus returns a stock item.
func (s *StockItemApprovalServer) GetStockItemApprovalStatus(ctx context.Context, req *inventoryv1.GetStockItemApprovalStatusRequest) (*inventoryv1.GetStockItemApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_item", req.GetStockItemId(), "get_stock_item_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get stock item approval status", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.GetStockItemApprovalStatusResponse{
		StockItem: &inventoryv1.StockItem{StockItemId: req.GetStockItemId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentStockItemApproval comments a stock item.
func (s *StockItemApprovalServer) CommentStockItemApproval(ctx context.Context, req *inventoryv1.CommentStockItemApprovalRequest) (*inventoryv1.CommentStockItemApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "comment stock item approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	normalizeStockMovementRequest := buildNormalizeStockMovementRequestForCommentStockItemApproval(req)
	normalizeStockMovementReply, err := s.stockMovementValidationClient.NormalizeStockMovement(ctx, normalizeStockMovementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/NormalizeStockMovement")
	}
	validateLedgerAccountRequest := &ledgerv1.ValidateLedgerAccountRequest{}
	validateLedgerAccountRequest.Context = req.GetContext()
	validateLedgerAccountRequest.LedgerAccount = &ledgerv1.LedgerAccount{
		Audit: &commonv1.AuditInfo{UpdatedAt: req.GetStockItem().GetAudit().GetUpdatedAt()},
	}
	validateLedgerAccountReply, err := s.ledgerAccountValidationClient.ValidateLedgerAccount(ctx, validateLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ValidateLedgerAccount")
	}
	return &inventoryv1.CommentStockItemApprovalResponse{
		Outcome: &inventoryv1.CommentStockItemApprovalResponse_Outcome{
			Status: normalizeStockMovementReply.GetOutcome().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: validateLedgerAccountReply.GetResult().GetStatus()},
		StockItem: &inventoryv1.StockItem{
			StockItemId: req.GetStockItemId(),
			Code:        req.GetStockItem().GetCode(),
		},
	}, nil
}

// stockItemApprovalServerRejectStockItemSearchStockItemsStep carries the request of one step of RejectStockItem and,
// once it ran, its reply.
type stockItemApprovalServerRejectStockItemSearchStockItemsStep struct {
	request *inventoryv1.SearchStockItemsRequest
	reply   *inventoryv1.SearchStockItemsResponse
}

func (s *StockItemApprovalServer) runRejectStockItemSearchStockItems(ctx context.Context, step *stockItemApprovalServerRejectStockItemSearchStockItemsStep) error {
	reply, err := s.stockItemClient.SearchStockItems(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
