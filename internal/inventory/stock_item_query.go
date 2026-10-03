package inventory

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	riskv1 "github.com/hounddogai/protomap-test-golang/gen/acme/risk/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StockItemQueryServer implements `acme.inventory.v1.StockItemQueryService`.
// Answers read-only questions about stock items, for screens and reports.
type StockItemQueryServer struct {
	inventoryv1.UnimplementedStockItemQueryServiceServer
	store                              *Store
	log                                *slog.Logger
	replenishmentPlanValidationClient  inventoryv1.ReplenishmentPlanValidationServiceClient
	trackingEventValidationClient      shippingv1.TrackingEventValidationServiceClient
	riskRegisterClient                 riskv1.RiskRegisterServiceClient
	replenishmentPlanCalculationClient inventoryv1.ReplenishmentPlanCalculationServiceClient
	parcelCalculationClient            shippingv1.ParcelCalculationServiceClient
	subscriptionPlanQueryClient        subscriptionsv1.SubscriptionPlanQueryServiceClient
	metricSeriesQueryClient            metricsv1.MetricSeriesQueryServiceClient
}

// NewStockItemQueryServer wires a StockItemQueryServer.
func NewStockItemQueryServer(deps platform.Deps) StockItemQueryServer {
	return StockItemQueryServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.inventory.v1.StockItemQueryService"),
		replenishmentPlanValidationClient:  inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn),
		trackingEventValidationClient:      shippingv1.NewTrackingEventValidationServiceClient(deps.Conn),
		riskRegisterClient:                 riskv1.NewRiskRegisterServiceClient(deps.Conn),
		replenishmentPlanCalculationClient: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		parcelCalculationClient:            shippingv1.NewParcelCalculationServiceClient(deps.Conn),
		subscriptionPlanQueryClient:        subscriptionsv1.NewSubscriptionPlanQueryServiceClient(deps.Conn),
		metricSeriesQueryClient:            metricsv1.NewMetricSeriesQueryServiceClient(deps.Conn),
	}
}

// LookupStockItem returns a stock item.
func (s StockItemQueryServer) LookupStockItem(ctx context.Context, req *inventoryv1.LookupStockItemRequest) (*inventoryv1.LookupStockItemResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_item", req.GetStockItemId(), "lookup_stock_item"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup stock item", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.LookupStockItemResponse{
		StockItem: &inventoryv1.StockItem{StockItemId: req.GetStockItemId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseStockItems lists stock items a page at a time.
func (s StockItemQueryServer) BrowseStockItems(ctx context.Context, req *inventoryv1.BrowseStockItemsRequest) (*inventoryv1.BrowseStockItemsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected BrowseStockItems", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "browse stock items", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.BrowseStockItemsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// FindStockItems lists stock items a page at a time.
func (s StockItemQueryServer) FindStockItems(ctx context.Context, req *inventoryv1.FindStockItemsRequest) (*inventoryv1.FindStockItemsResponse, error) {
	s.log.InfoContext(ctx, "find stock items", "tenant", req.GetContext().GetTenantId())
	lintReplenishmentPlansReply, err := s.replenishmentPlanValidationClient.LintReplenishmentPlans(ctx, &inventoryv1.LintReplenishmentPlansRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			Counterparty:   &inventoryv1.StockReservationRef{StartsAt: req.GetFilter().GetCreatedAfter()},
		},
		Details: &inventoryv1.LintReplenishmentPlansRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/LintReplenishmentPlans")
	}
	return &inventoryv1.FindStockItemsResponse{
		Result: &commonv1.OperationResult{
			Message: lintReplenishmentPlansReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// CountStockItems counts stock items by status.
func (s StockItemQueryServer) CountStockItems(ctx context.Context, req *inventoryv1.CountStockItemsRequest) (*inventoryv1.CountStockItemsResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "count stock items", "tenant", req.GetContext().GetTenantId())
	normalizeTrackingEventRequest := &shippingv1.NormalizeTrackingEventRequest{}
	normalizeTrackingEventRequest.Context = req.GetContext()
	normalizeTrackingEventRequest.Details = &shippingv1.NormalizeTrackingEventRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
	}
	normalizeTrackingEventReply, err := s.trackingEventValidationClient.NormalizeTrackingEvent(ctx, normalizeTrackingEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventValidationService/NormalizeTrackingEvent")
	}
	return &inventoryv1.CountStockItemsResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  normalizeTrackingEventReply.GetResult().GetReferenceIds(),
			CorrelationId: normalizeTrackingEventReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ResolveStockItems returns several stock items at once.
func (s StockItemQueryServer) ResolveStockItems(ctx context.Context, req *inventoryv1.ResolveStockItemsRequest) (*inventoryv1.ResolveStockItemsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve stock items", "tenant", req.GetContext().GetTenantId())
	updateRiskRegisterRequest := &riskv1.UpdateRiskRegisterRequest{
		Context: req.GetContext(),
		RiskRegister: &riskv1.RiskRegister{
			Children: []*riskv1.RiskRegister{&riskv1.RiskRegister{
				Children: []*riskv1.RiskRegister{&riskv1.RiskRegister{
					Manager: &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
					Audit:   &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
				}},
				Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
			}},
		},
	}
	send := s.updateRiskRegisterSender(updateRiskRegisterRequest)
	updateRiskRegisterReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.risk.v1.RiskRegisterService/UpdateRiskRegister")
	}
	return &inventoryv1.ResolveStockItemsResponse{
		StockItems: []*inventoryv1.StockItem{&inventoryv1.StockItem{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: updateRiskRegisterReply.GetRiskRegister().GetAudit().GetUpdatedAt(),
			},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ExportStockItems streams every matching stock item.
func (s StockItemQueryServer) ExportStockItems(req *inventoryv1.ExportStockItemsRequest, stream grpc.ServerStreamingServer[inventoryv1.ExportStockItemsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "export stock items", "tenant", req.GetContext().GetTenantId())
	calculateReplenishmentPlanReply, err := s.replenishmentPlanCalculationClient.CalculateReplenishmentPlan(ctx, &inventoryv1.CalculateReplenishmentPlanRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.CalculateReplenishmentPlanRequest_Details{
			Destination: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CalculateReplenishmentPlan")
	}
	simulateParcelRequest := buildSimulateParcelRequestForExportStockItems(req, calculateReplenishmentPlanReply)
	simulateParcelReply, err := s.parcelCalculationClient.SimulateParcel(ctx, simulateParcelRequest)
	if err != nil {
		return grpcx.Wrap(err, "acme.shipping.v1.ParcelCalculationService/SimulateParcel")
	}
	if err := stream.Send(&inventoryv1.ExportStockItemsResponse{
		StockItem: &inventoryv1.StockItem{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId: simulateParcelReply.GetParcel().GetOwner().GetUserId(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeStockItems summarizes a stock item.
func (s StockItemQueryServer) SummarizeStockItems(ctx context.Context, req *inventoryv1.SummarizeStockItemsRequest) (*inventoryv1.SummarizeStockItemsResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_item", req.GetStockItemId(), "summarize_stock_items"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize stock items", "tenant", req.GetContext().GetTenantId())
	resolveSubscriptionPlansRequest := buildResolveSubscriptionPlansRequestForSummarizeStockItems(req)
	resolveSubscriptionPlansReply, err := s.subscriptionPlanQueryClient.ResolveSubscriptionPlans(ctx, resolveSubscriptionPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanQueryService/ResolveSubscriptionPlans")
	}
	suggestMetricSeriesesReply, err := s.metricSeriesQueryClient.SuggestMetricSerieses(ctx, &metricsv1.SuggestMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.SuggestMetricSeriesesRequest_Filter{
			Labels: resolveSubscriptionPlansReply.GetResult().GetDetails(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/SuggestMetricSerieses")
	}
	return &inventoryv1.SummarizeStockItemsResponse{
		Result: &commonv1.OperationResult{
			Details: resolveSubscriptionPlansReply.GetResult().GetDetails(),
			Message: resolveSubscriptionPlansReply.GetResult().GetMessage(),
			Status:  suggestMetricSeriesesReply.GetResult().GetStatus(),
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestStockItems lists stock items a page at a time.
func (s StockItemQueryServer) SuggestStockItems(ctx context.Context, req *inventoryv1.SuggestStockItemsRequest) (*inventoryv1.SuggestStockItemsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "suggest stock items", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.SuggestStockItemsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekStockItem returns a stock item.
func (s StockItemQueryServer) PeekStockItem(ctx context.Context, req *inventoryv1.PeekStockItemRequest) (*inventoryv1.PeekStockItemResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PeekStockItem", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_item", req.GetStockItemId(), "peek_stock_item"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek stock item", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.PeekStockItemResponse{
		StockItem: &inventoryv1.StockItem{StockItemId: req.GetStockItemId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// updateRiskRegisterSender returns a function that sends one request to
// acme.risk.v1.RiskRegisterService/UpdateRiskRegister.
func (s StockItemQueryServer) updateRiskRegisterSender(request *riskv1.UpdateRiskRegisterRequest) func(context.Context) (*riskv1.UpdateRiskRegisterResponse, error) {
	return func(ctx context.Context) (*riskv1.UpdateRiskRegisterResponse, error) {
		return s.riskRegisterClient.UpdateRiskRegister(ctx, request)
	}
}
