package inventory

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	assetsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/assets/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// StockMovementApprovalServer implements `acme.inventory.v1.StockMovementApprovalService`.
// Routes stock movements through approval, with escalation and delegation.
type StockMovementApprovalServer struct {
	inventoryv1.UnimplementedStockMovementApprovalServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	tenantExportClient            identityv1.TenantExportServiceClient
	synonymHistoryClient          searchv1.SynonymHistoryServiceClient
	householdCalculationClient    customersv1.HouseholdCalculationServiceClient
	meteredStockReservationQuery  meteredStockReservationQuery
	tierReportingOnce             sync.Once
	tierReportingCached           loyaltyv1.TierReportingServiceClient
	startSellerListingWorkflowFn  func(context.Context, *sellersv1.StartSellerListingWorkflowRequest, ...grpc.CallOption) (*sellersv1.StartSellerListingWorkflowResponse, error)
	returnRequestWorkflowClient   returnsv1.ReturnRequestWorkflowServiceClient
	stockReservationQueryClient   inventoryv1.StockReservationQueryServiceClient
	clients                       *registry.Registry
	stockMovementValidationClient inventoryv1.StockMovementValidationServiceClient
}

// NewStockMovementApprovalServer wires a StockMovementApprovalServer.
func NewStockMovementApprovalServer(deps platform.Deps) *StockMovementApprovalServer {
	sellerListingWorkflow := sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn)
	return &StockMovementApprovalServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.inventory.v1.StockMovementApprovalService"),
		conn:                          deps.Conn,
		tenantExportClient:            identityv1.NewTenantExportServiceClient(deps.Conn),
		synonymHistoryClient:          searchv1.NewSynonymHistoryServiceClient(deps.Conn),
		householdCalculationClient:    customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		meteredStockReservationQuery:  meteredStockReservationQuery{StockReservationQueryServiceClient: inventoryv1.NewStockReservationQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		startSellerListingWorkflowFn:  sellerListingWorkflow.StartSellerListingWorkflow,
		returnRequestWorkflowClient:   returnsv1.NewReturnRequestWorkflowServiceClient(deps.Conn),
		stockReservationQueryClient:   inventoryv1.NewStockReservationQueryServiceClient(deps.Conn),
		clients:                       deps.Clients,
		stockMovementValidationClient: inventoryv1.NewStockMovementValidationServiceClient(deps.Conn),
	}
}

// SubmitStockMovement submits a stock movement.
func (s *StockMovementApprovalServer) SubmitStockMovement(ctx context.Context, req *inventoryv1.SubmitStockMovementRequest) (*inventoryv1.SubmitStockMovementResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "submit stock movement", "tenant", req.GetContext().GetTenantId())
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	validateReplenishmentPlanRequest := &inventoryv1.ValidateReplenishmentPlanRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.ValidateReplenishmentPlanRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		ReplenishmentPlanId: envelope.GetDisplayName(),
	}
	validateReplenishmentPlanReply := new(inventoryv1.ValidateReplenishmentPlanResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", inventoryReplenishmentPlanValidationServiceName, "ValidateReplenishmentPlan"), validateReplenishmentPlanRequest, validateReplenishmentPlanReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/ValidateReplenishmentPlan")
	}
	explainHouseholdCalculationRequest := &customersv1.ExplainHouseholdCalculationRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						CountryCode: validateReplenishmentPlanReply.GetReplenishmentPlan().GetBillingAddress().GetCountryCode(),
					},
				}},
			}},
		},
	}
	explainHouseholdCalculationReply := new(customersv1.ExplainHouseholdCalculationResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersHouseholdCalculationServiceName, "ExplainHouseholdCalculation"), explainHouseholdCalculationRequest, explainHouseholdCalculationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/ExplainHouseholdCalculation")
	}
	return &inventoryv1.SubmitStockMovementResponse{
		Outcome: &inventoryv1.SubmitStockMovementResponse_Outcome{
			Message:     validateReplenishmentPlanReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		StockMovement: &inventoryv1.StockMovement{StockMovementId: req.GetStockMovementId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveStockMovement approves a stock movement.
func (s *StockMovementApprovalServer) ApproveStockMovement(ctx context.Context, req *inventoryv1.ApproveStockMovementRequest) (*inventoryv1.ApproveStockMovementResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveStockMovement", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "approve stock movement", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listTenantExportsRequest := &identityv1.ListTenantExportsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.ListTenantExportsRequest_Filter{
			CreatedAfter: req.GetStockMovement().GetOccurredAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
			Labels:       annotations,
		},
		Page: &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	listTenantExportsReply, err := identityv1.TenantExportServiceClient.ListTenantExports(s.tenantExportClient, ctx, listTenantExportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/ListTenantExports")
	}
	listSynonymVersionsReply, err := s.synonymHistoryClient.ListSynonymVersions(ctx, &searchv1.ListSynonymVersionsRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ListSynonymVersionsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetStockMovement().GetSubject().GetCustomerId(),
			Labels:       req.GetStockMovement().GetAttributes(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/ListSynonymVersions")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded inventoryv1.ApproveStockMovementRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	estimateHouseholdRequest := &customersv1.EstimateHouseholdRequest{}
	estimateHouseholdRequest.Context = req.GetContext()
	estimateHouseholdRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{DisplayName: (&decoded).GetApprover().GetDisplayName()},
			}},
		}},
	}
	estimateHouseholdReply, err := s.householdCalculationClient.EstimateHousehold(ctx, estimateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/EstimateHousehold")
	}
	summarizeStockReservationsRequest := &inventoryv1.SummarizeStockReservationsRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	summarizeStockReservationsReply, err := s.meteredStockReservationQuery.SummarizeStockReservations(ctx, summarizeStockReservationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/SummarizeStockReservations")
	}
	return &inventoryv1.ApproveStockMovementResponse{
		Outcome: &inventoryv1.ApproveStockMovementResponse_Outcome{Status: listTenantExportsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			ReferenceIds:  listSynonymVersionsReply.GetResult().GetReferenceIds(),
			CorrelationId: estimateHouseholdReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		StockMovement: &inventoryv1.StockMovement{
			Attributes:      estimateHouseholdReply.GetHousehold().GetAttributes(),
			CorrelationId:   summarizeStockReservationsReply.GetResult().GetCorrelationId(),
			StockMovementId: req.GetStockMovementId(),
			Subject:         req.GetStockMovement().GetSubject(),
		},
	}, nil
}

// ListPendingStockMovements lists stock movements a page at a time.
func (s *StockMovementApprovalServer) ListPendingStockMovements(ctx context.Context, req *inventoryv1.ListPendingStockMovementsRequest) (*inventoryv1.ListPendingStockMovementsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingStockMovements", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending stock movements", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.ListPendingStockMovementsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// DelegateStockMovementApproval delegates a stock movement.
func (s *StockMovementApprovalServer) DelegateStockMovementApproval(ctx context.Context, req *inventoryv1.DelegateStockMovementApprovalRequest) (*inventoryv1.DelegateStockMovementApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "delegate stock movement approval", "tenant", req.GetContext().GetTenantId())
	countTierTotalsRequest := &loyaltyv1.CountTierTotalsRequest{
		Context: req.GetContext(),
		Filter:  &loyaltyv1.CountTierTotalsRequest_Filter{CreatedAfter: req.GetStockMovement().GetOccurredAt()},
	}
	countTierTotalsReply, err := s.tierReportingLazy().CountTierTotals(ctx, countTierTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/CountTierTotals")
	}
	summarizeLedgerAccountsRequest := &ledgerv1.SummarizeLedgerAccountsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: req.GetAnnotations(),
	}
	summarizeLedgerAccountsReply := new(ledgerv1.SummarizeLedgerAccountsResponse)
	if err := s.conn.Invoke(ctx, ledgerLedgerAccountQueryServiceSummarizeLedgerAccountsMethod, summarizeLedgerAccountsRequest, summarizeLedgerAccountsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountQueryService/SummarizeLedgerAccounts")
	}
	return &inventoryv1.DelegateStockMovementApprovalResponse{
		Result: &commonv1.OperationResult{
			Message: countTierTotalsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &inventoryv1.DelegateStockMovementApprovalResponse_Outcome{
			Message: countTierTotalsReply.GetResult().GetMessage(),
			Status:  summarizeLedgerAccountsReply.GetResult().GetStatus(),
		},
		StockMovement: &inventoryv1.StockMovement{
			StockMovementId: req.GetStockMovementId(),
			Source:          req.GetStockMovement().GetSource(),
		},
	}, nil
}

// GetStockMovementApprovalStatus returns a stock movement.
func (s *StockMovementApprovalServer) GetStockMovementApprovalStatus(ctx context.Context, req *inventoryv1.GetStockMovementApprovalStatusRequest) (*inventoryv1.GetStockMovementApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_movement", req.GetStockMovementId(), "get_stock_movement_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get stock movement approval status", "tenant", req.GetContext().GetTenantId())
	startSellerListingWorkflowRequest := &sellersv1.StartSellerListingWorkflowRequest{
		Context: req.GetContext(),
		Details: &sellersv1.StartSellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		},
	}
	startSellerListingWorkflowReply, err := s.startSellerListingWorkflowFn(ctx, startSellerListingWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/StartSellerListingWorkflow")
	}
	cancelReturnRequestWorkflowReply, err := s.returnRequestWorkflowClient.CancelReturnRequestWorkflow(ctx, &returnsv1.CancelReturnRequestWorkflowRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Lines: []*returnsv1.ReturnRequestLine{&returnsv1.ReturnRequestLine{
				Description: startSellerListingWorkflowReply.GetSellerListing().GetComponents()["primary"].GetDescription(),
			}},
			BillingAddress: &commonv1.PostalAddress{
				Locality: startSellerListingWorkflowReply.GetSellerListing().GetAddress().GetLocality(),
				Lines:    startSellerListingWorkflowReply.GetSellerListing().GetAddress().GetLines(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestWorkflowService/CancelReturnRequestWorkflow")
	}
	return &inventoryv1.GetStockMovementApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message:       startSellerListingWorkflowReply.GetResult().GetMessage(),
			CorrelationId: cancelReturnRequestWorkflowReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				Email: cancelReturnRequestWorkflowReply.GetReturnRequest().GetCounterparty().GetEmail(),
			},
			StockMovementId: req.GetStockMovementId(),
		},
	}, nil
}

// CommentStockMovementApproval comments a stock movement.
func (s *StockMovementApprovalServer) CommentStockMovementApproval(ctx context.Context, req *inventoryv1.CommentStockMovementApprovalRequest) (*inventoryv1.CommentStockMovementApprovalResponse, error) {
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "comment stock movement approval", "tenant", req.GetContext().GetTenantId())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*inventoryv1.SuggestStockReservationsResponse, len(req.GetStockMovement().GetMeasurements()))
	for i, measurement := range req.GetStockMovement().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.stockReservationQueryClient.SuggestStockReservations(groupCtx, &inventoryv1.SuggestStockReservationsRequest{
				Context: req.GetContext(),
				Filter: &inventoryv1.SuggestStockReservationsRequest_Filter{
					CreatedAfter: measurement.GetObservedAt(),
					OwnerId:      req.GetStockMovement().GetStockMovementId(),
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
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/SuggestStockReservations")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	batchGetFixedAssetsRequest := &assetsv1.BatchGetFixedAssetsRequest{Context: req.GetContext()}
	batchGetFixedAssetsReply := new(assetsv1.BatchGetFixedAssetsResponse)
	if err := grpc.Invoke(ctx, "/acme.assets.v1.FixedAssetService/BatchGetFixedAssets", batchGetFixedAssetsRequest, batchGetFixedAssetsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.assets.v1.FixedAssetService/BatchGetFixedAssets")
	}
	householdQuery, err := registry.Lookup[customersv1.HouseholdQueryServiceClient](s.clients, "acme.customers.v1.HouseholdQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	findHouseholdsRequest := &customersv1.FindHouseholdsRequest{
		Context: req.GetContext(),
		Filter:  &customersv1.FindHouseholdsRequest_Filter{CreatedAfter: req.GetStockMovement().GetOccurredAt()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}
	findHouseholdsReply, err := householdQuery.FindHouseholds(ctx, findHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/FindHouseholds")
	}
	ctx = tenancy.With(ctx, "kind", req.GetStockMovement().GetKind())
	var envelope2 inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	checkStockMovementEligibilityReply, err := s.stockMovementValidationClient.CheckStockMovementEligibility(ctx, &inventoryv1.CheckStockMovementEligibilityRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetStockMovement().GetSubject().GetDisplayName(),
				CustomerId:  req.GetStockMovement().GetSubject().GetCustomerId(),
			},
			Kind: tenancy.Value(ctx, "kind"),
		},
		StockMovementId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/CheckStockMovementEligibility")
	}
	return &inventoryv1.CommentStockMovementApprovalResponse{
		Outcome: &inventoryv1.CommentStockMovementApprovalResponse_Outcome{
			Status:  batchGetFixedAssetsReply.GetResult().GetStatus(),
			Message: checkStockMovementEligibilityReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Details: findHouseholdsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		StockMovement: &inventoryv1.StockMovement{
			StockMovementId: req.GetStockMovementId(),
			Source:          req.GetStockMovement().GetSource(),
		},
	}, nil
}

// tierReportingLazy dials acme.loyalty.v1.TierReportingService the first time a handler needs it.
func (s *StockMovementApprovalServer) tierReportingLazy() loyaltyv1.TierReportingServiceClient {
	s.tierReportingOnce.Do(func() { s.tierReportingCached = loyaltyv1.NewTierReportingServiceClient(s.conn) })
	return s.tierReportingCached
}
