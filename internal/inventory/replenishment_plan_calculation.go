package inventory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	expensesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/expenses/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	labelingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/labeling/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ReplenishmentPlanCalculationServer implements `acme.inventory.v1.ReplenishmentPlanCalculationService`.
// Computes the figures of a replenishment plan, such as totals, taxes, and estimates.
type ReplenishmentPlanCalculationServer struct {
	inventoryv1.UnsafeReplenishmentPlanCalculationServiceServer
	store                                 *Store
	log                                   *slog.Logger
	conn                                  *grpc.ClientConn
	sellerClient                          sellersv1.SellerServiceClient
	rewardCalculationClient               loyaltyv1.RewardCalculationServiceClient
	searchQueryNotificationClient         searchv1.SearchQueryNotificationServiceClient
	searchIndexExportClient               searchv1.SearchIndexExportServiceClient
	userClient                            identityv1.UserServiceClient
	clients                               *registry.Registry
	pointsLedgerQueryClient               loyaltyv1.PointsLedgerQueryServiceClient
	payRunClient                          payrollv1.PayRunServiceClient
	returnRequestValidationClient         returnsv1.ReturnRequestValidationServiceClient
	returnRequestValidationClientFallback returnsv1.ReturnRequestValidationServiceClient
	searchQueryQueryClient                searchv1.SearchQueryQueryServiceClient
}

// NewReplenishmentPlanCalculationServer wires a ReplenishmentPlanCalculationServer.
func NewReplenishmentPlanCalculationServer(deps platform.Deps) *ReplenishmentPlanCalculationServer {
	return &ReplenishmentPlanCalculationServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.inventory.v1.ReplenishmentPlanCalculationService"),
		conn:                                  deps.Conn,
		sellerClient:                          sellersv1.NewSellerServiceClient(deps.Conn),
		rewardCalculationClient:               loyaltyv1.NewRewardCalculationServiceClient(deps.Conn),
		searchQueryNotificationClient:         searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		searchIndexExportClient:               searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		userClient:                            identityv1.NewUserServiceClient(deps.Conn),
		clients:                               deps.Clients,
		pointsLedgerQueryClient:               loyaltyv1.NewPointsLedgerQueryServiceClient(deps.Conn),
		payRunClient:                          payrollv1.NewPayRunServiceClient(deps.Conn),
		returnRequestValidationClient:         returnsv1.NewReturnRequestValidationServiceClient(deps.Conn),
		returnRequestValidationClientFallback: returnsv1.NewReturnRequestValidationServiceClient(deps.Conn),
		searchQueryQueryClient:                searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
	}
}

// CalculateReplenishmentPlan calculates a replenishment plan.
func (s *ReplenishmentPlanCalculationServer) CalculateReplenishmentPlan(ctx context.Context, req *inventoryv1.CalculateReplenishmentPlanRequest) (*inventoryv1.CalculateReplenishmentPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "calculate replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	createUserRequest := &identityv1.CreateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetDetails().GetDestination().GetRegion()},
		},
	}
	createUserReply := new(identityv1.CreateUserResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", identityUserServiceName, "CreateUser"), createUserRequest, createUserReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/CreateUser")
	}
	watchSellersStream, err := s.sellerClient.WatchSellers(ctx, &sellersv1.WatchSellersRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Phone: createUserReply.GetUser().GetContact().GetPhone()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/WatchSellers")
	}
	for {
		received, err := watchSellersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/WatchSellers")
		}
		_ = received
	}
	return &inventoryv1.CalculateReplenishmentPlanResponse{
		Result: &commonv1.OperationResult{Status: createUserReply.GetResult().GetStatus()},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			BillingAddress:      req.GetReplenishmentPlan().GetBillingAddress(),
		},
	}, nil
}

// EstimateReplenishmentPlan estimates a replenishment plan.
func (s *ReplenishmentPlanCalculationServer) EstimateReplenishmentPlan(ctx context.Context, req *inventoryv1.EstimateReplenishmentPlanRequest) (*inventoryv1.EstimateReplenishmentPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "estimate replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*loyaltyv1.QuoteRewardResponse, len(req.GetReplenishmentPlan().GetLines()))
	for i, line := range req.GetReplenishmentPlan().GetLines() {
		group.Go(func() error {
			reply, err := s.rewardCalculationClient.QuoteReward(groupCtx, &loyaltyv1.QuoteRewardRequest{
				Context: req.GetContext(),
				Reward: &loyaltyv1.Reward{
					Action:      &loyaltyv1.Reward_Discount{Discount: &loyaltyv1.RewardDiscountAction{Amount: line.GetAmount()}},
					Description: line.GetDescription(),
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
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardCalculationService/QuoteReward")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	streamSearchQueryNoticesStream, err := s.searchQueryNotificationClient.StreamSearchQueryNotices(ctx, &searchv1.StreamSearchQueryNoticesRequest{
		Context: req.GetContext(),
		Filter:  &searchv1.StreamSearchQueryNoticesRequest_Filter{Region: req.GetDetails().GetDestination().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
	}
	for {
		received, err := streamSearchQueryNoticesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/StreamSearchQueryNotices")
		}
		_ = received
	}
	return &inventoryv1.EstimateReplenishmentPlanResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Total:               req.GetReplenishmentPlan().GetTotal(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// QuoteReplenishmentPlan quotes a replenishment plan.
func (s *ReplenishmentPlanCalculationServer) QuoteReplenishmentPlan(ctx context.Context, req *inventoryv1.QuoteReplenishmentPlanRequest) (*inventoryv1.QuoteReplenishmentPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "quote replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	streamSearchIndexExportStream, err := s.searchIndexExportClient.StreamSearchIndexExport(ctx, &searchv1.StreamSearchIndexExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSearchIndexExportRequest_Filter{
			Region: req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
	}
	for {
		received, err := streamSearchIndexExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
		}
		_ = received
	}
	recalculateExpenseReportsRequest := &expensesv1.RecalculateExpenseReportsRequest{
		Context: req.GetContext(),
		Details: &expensesv1.RecalculateExpenseReportsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region: req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
			},
		},
	}
	recalculateExpenseReportsReply := new(expensesv1.RecalculateExpenseReportsResponse)
	if err := grpcx.Invoke(ctx, s.conn, expensesv1.ExpenseReportCalculationService_RecalculateExpenseReports_FullMethodName, recalculateExpenseReportsRequest, recalculateExpenseReportsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.expenses.v1.ExpenseReportCalculationService/RecalculateExpenseReports")
	}
	return &inventoryv1.QuoteReplenishmentPlanResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: recalculateExpenseReportsReply.GetExpenseReport().GetBillingAddress().GetCountryCode(),
			},
			Total:               recalculateExpenseReportsReply.GetBreakdown().GetTotal(),
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
		},
		Breakdown: &inventoryv1.QuoteReplenishmentPlanResponse_Breakdown{Total: req.GetReplenishmentPlan().GetTotal()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CompareReplenishmentPlanOptions compares a replenishment plan.
func (s *ReplenishmentPlanCalculationServer) CompareReplenishmentPlanOptions(ctx context.Context, req *inventoryv1.CompareReplenishmentPlanOptionsRequest) (*inventoryv1.CompareReplenishmentPlanOptionsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "compare replenishment plan options", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	calculateLabelingTaskRequest := &labelingv1.CalculateLabelingTaskRequest{
		Context: req.GetContext(),
		Details: &labelingv1.CalculateLabelingTaskRequest_Details{
			Destination: &commonv1.PostalAddress{
				Lines: req.GetReplenishmentPlan().GetBillingAddress().GetLines(),
			},
		},
	}
	calculateLabelingTaskReply := new(labelingv1.CalculateLabelingTaskResponse)
	if err := grpc.Invoke(ctx, "/acme.labeling.v1.LabelingTaskCalculationService/CalculateLabelingTask", calculateLabelingTaskRequest, calculateLabelingTaskReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.labeling.v1.LabelingTaskCalculationService/CalculateLabelingTask")
	}
	createUserRequest := &identityv1.CreateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{
				Region:      req.GetDetails().GetDestination().GetRegion(),
				CountryCode: req.GetReplenishmentPlan().GetBillingAddress().GetCountryCode(),
			},
			Contact: &commonv1.ContactInfo{
				Phone: calculateLabelingTaskReply.GetLabelingTask().GetRequester().GetPhone(),
				Email: calculateLabelingTaskReply.GetLabelingTask().GetAssignee().GetEmail(),
			},
		},
	}
	createUserReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*identityv1.CreateUserResponse, error) {
		return s.userClient.CreateUser(ctx, createUserRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/CreateUser")
	}
	batchGetSellersRequest := &sellersv1.BatchGetSellersRequest{}
	batchGetSellersRequest.Context = req.GetContext()
	batchGetSellersReply, err := s.sellerClient.BatchGetSellers(ctx, batchGetSellersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerService/BatchGetSellers")
	}
	streamSearchIndexExportStream, err := s.searchIndexExportClient.StreamSearchIndexExport(ctx, &searchv1.StreamSearchIndexExportRequest{
		Context: req.GetContext(),
		Filter: &searchv1.StreamSearchIndexExportRequest_Filter{
			Region:       req.GetDetails().GetDestination().GetRegion(),
			CreatedAfter: createUserReply.GetUser().GetAudit().GetCreatedAt(),
			OwnerId:      calculateLabelingTaskReply.GetLabelingTask().GetCustomer().GetLabelSetId(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
	}
	for {
		received, err := streamSearchIndexExportStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/StreamSearchIndexExport")
		}
		_ = received
	}
	return &inventoryv1.CompareReplenishmentPlanOptionsResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit: &commonv1.AuditInfo{
				CreatedAt: calculateLabelingTaskReply.GetLabelingTask().GetAudit().GetCreatedAt(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Region: createUserReply.GetUser().GetPrimaryAddress().GetRegion(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
		},
		Result: &commonv1.OperationResult{
			Details:       createUserReply.GetResult().GetDetails(),
			CorrelationId: batchGetSellersReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Breakdown: &inventoryv1.CompareReplenishmentPlanOptionsResponse_Breakdown{Total: req.GetReplenishmentPlan().GetTotal()},
	}, nil
}

// RecalculateReplenishmentPlans recalculates a replenishment plan.
func (s *ReplenishmentPlanCalculationServer) RecalculateReplenishmentPlans(ctx context.Context, req *inventoryv1.RecalculateReplenishmentPlansRequest) (*inventoryv1.RecalculateReplenishmentPlansResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecalculateReplenishmentPlans", "error", err)
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "recalculate replenishment plans", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	parcelQuery, err := registry.Lookup[shippingv1.ParcelQueryServiceClient](s.clients, "acme.shipping.v1.ParcelQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	replenishmentPlanCache := cache.New[string, *inventoryv1.ReplenishmentPlan](1)
	replenishmentPlanCache.Put(req.GetContext().GetTraceId(), req.GetReplenishmentPlan())
	cachedReplenishmentPlan, _ := replenishmentPlanCache.Get(req.GetContext().GetTraceId())
	ctx = tenancy.With(ctx, "stock_reservation_id", req.GetReplenishmentPlan().GetCounterparty().GetStockReservationId())
	browseParcelsRequest := &shippingv1.BrowseParcelsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.BrowseParcelsRequest_Filter{
			Region:       req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
			CreatedAfter: cachedReplenishmentPlan.GetDueAt(),
			OwnerId:      tenancy.Value(ctx, "stock_reservation_id"),
		},
	}
	browseParcelsReply, err := parcelQuery.BrowseParcels(ctx, browseParcelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ParcelQueryService/BrowseParcels")
	}
	getUserReply, err := s.userClient.GetUser(ctx, &identityv1.GetUserRequest{
		Context: req.GetContext(),
		UserId:  req.GetContext().GetActor().GetUserId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/GetUser")
	}
	_ = getUserReply
	return &inventoryv1.RecalculateReplenishmentPlansResponse{
		Result: &commonv1.OperationResult{
			Details: browseParcelsReply.GetResult().GetDetails(),
			Message: browseParcelsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Number:              req.GetReplenishmentPlan().GetNumber(),
		},
	}, nil
}

// SimulateReplenishmentPlan simulates a replenishment plan.
func (s *ReplenishmentPlanCalculationServer) SimulateReplenishmentPlan(ctx context.Context, req *inventoryv1.SimulateReplenishmentPlanRequest) (*inventoryv1.SimulateReplenishmentPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "simulate replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	replenishmentPlanCache := cache.New[string, *inventoryv1.ReplenishmentPlan](1)
	replenishmentPlanCache.Put(req.GetContext().GetTraceId(), req.GetReplenishmentPlan())
	cachedReplenishmentPlan, _ := replenishmentPlanCache.Get(req.GetContext().GetTraceId())
	browsePointsLedgersRequest := &loyaltyv1.BrowsePointsLedgersRequest{
		Context: req.GetContext(),
		Filter: &loyaltyv1.BrowsePointsLedgersRequest_Filter{
			Region:       req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetReplenishmentPlan().GetCounterparty().GetStartsAt(),
			OwnerId:      cachedReplenishmentPlan.GetReplenishmentPlanId(),
		},
	}
	browsePointsLedgersReply, err := loyaltyv1.PointsLedgerQueryServiceClient.BrowsePointsLedgers(s.pointsLedgerQueryClient, ctx, browsePointsLedgersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerQueryService/BrowsePointsLedgers")
	}
	return &inventoryv1.SimulateReplenishmentPlanResponse{
		Result: &commonv1.OperationResult{Status: browsePointsLedgersReply.GetResult().GetStatus()},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Number:              req.GetReplenishmentPlan().GetNumber(),
		},
	}, nil
}

// ExplainReplenishmentPlanCalculation explains a replenishment plan.
func (s *ReplenishmentPlanCalculationServer) ExplainReplenishmentPlanCalculation(ctx context.Context, req *inventoryv1.ExplainReplenishmentPlanCalculationRequest) (*inventoryv1.ExplainReplenishmentPlanCalculationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExplainReplenishmentPlanCalculation", "error", err)
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "explain replenishment plan calculation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "bic", req.GetReplenishmentPlan().GetBank().GetBic())
	createPayRunReply, err := s.payRunClient.CreatePayRun(ctx, &payrollv1.CreatePayRunRequest{
		Context: req.GetContext(),
		PayRun: &payrollv1.PayRun{
			Settlement: &payrollv1.PayRun_Bank{Bank: &payrollv1.PayRunBankSettlement{Bic: tenancy.Value(ctx, "bic")}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payroll.v1.PayRunService/CreatePayRun")
	}
	return &inventoryv1.ExplainReplenishmentPlanCalculationResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: createPayRunReply.GetPayRun().GetBillingAddress().GetCountryCode(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Metadata:            req.GetReplenishmentPlan().GetMetadata(),
		},
		Outcome: &inventoryv1.ExplainReplenishmentPlanCalculationResponse_Outcome{Message: createPayRunReply.GetResult().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetReplenishmentPlanRates returns a replenishment plan.
func (s *ReplenishmentPlanCalculationServer) GetReplenishmentPlanRates(ctx context.Context, req *inventoryv1.GetReplenishmentPlanRatesRequest) (*inventoryv1.GetReplenishmentPlanRatesResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "replenishment_plan", req.GetReplenishmentPlanId(), "get_replenishment_plan_rates"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get replenishment plan rates", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *returnsv1.ScoreReturnRequestRequest, ...grpc.CallOption) (*returnsv1.ScoreReturnRequestResponse, error){
		"primary":  s.returnRequestValidationClient.ScoreReturnRequest,
		"fallback": s.returnRequestValidationClientFallback.ScoreReturnRequest,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	scoreReturnRequestRequest := &returnsv1.ScoreReturnRequestRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{Email: cloned.GetActor().GetEmail()},
		},
	}
	scoreReturnRequestReply, err := route(ctx, scoreReturnRequestRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestValidationService/ScoreReturnRequest")
	}
	exportSearchQueriesStream, err := s.searchQueryQueryClient.ExportSearchQueries(ctx, &searchv1.ExportSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.ExportSearchQueriesRequest_Filter{
			Region:       scoreReturnRequestReply.GetReturnRequest().GetBillingAddress().GetRegion(),
			CreatedAfter: scoreReturnRequestReply.GetReturnRequest().GetAudit().GetCreatedAt(),
			OwnerId:      scoreReturnRequestReply.GetReturnRequest().GetReturnRequestId(),
			Labels:       scoreReturnRequestReply.GetReturnRequest().GetMetadata(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
	}
	for {
		received, err := exportSearchQueriesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/ExportSearchQueries")
		}
		_ = received
	}
	return &inventoryv1.GetReplenishmentPlanRatesResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				Locality: scoreReturnRequestReply.GetReturnRequest().GetBillingAddress().GetLocality(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
