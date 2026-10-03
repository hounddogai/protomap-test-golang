package inventory

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// StockItemServer implements `acme.inventory.v1.StockItemService`.
// Manages stock items: creates, reads, changes, and lists them.
type StockItemServer struct {
	inventoryv1.UnimplementedStockItemServiceServer
	store                             *Store
	log                               *slog.Logger
	clients                           *registry.Registry
	giftCardValidationClient          giftcardsv1.GiftCardValidationServiceClient
	stockMovementValidationClient     inventoryv1.StockMovementValidationServiceClient
	searchNotificationsFn             func(context.Context, *notificationsv1.SearchNotificationsRequest, ...grpc.CallOption) (*notificationsv1.SearchNotificationsResponse, error)
	stockItemQueryClient              inventoryv1.StockItemQueryServiceClient
	replenishmentPlanValidationClient inventoryv1.ReplenishmentPlanValidationServiceClient
	stockReservationQueryClient       inventoryv1.StockReservationQueryServiceClient
	featureFlagQueryClient            featureflagsv1.FeatureFlagQueryServiceClient
	meteredStockReservationQuery      meteredStockReservationQuery
	pickListValidationClient          warehousingv1.PickListValidationServiceClient
}

// NewStockItemServer wires a StockItemServer.
func NewStockItemServer(deps platform.Deps) *StockItemServer {
	notification := notificationsv1.NewNotificationServiceClient(deps.Conn)
	return &StockItemServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.inventory.v1.StockItemService"),
		clients:                           deps.Clients,
		giftCardValidationClient:          giftcardsv1.NewGiftCardValidationServiceClient(deps.Conn),
		stockMovementValidationClient:     inventoryv1.NewStockMovementValidationServiceClient(deps.Conn),
		searchNotificationsFn:             notification.SearchNotifications,
		stockItemQueryClient:              inventoryv1.NewStockItemQueryServiceClient(deps.Conn),
		replenishmentPlanValidationClient: inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn),
		stockReservationQueryClient:       inventoryv1.NewStockReservationQueryServiceClient(deps.Conn),
		featureFlagQueryClient:            featureflagsv1.NewFeatureFlagQueryServiceClient(deps.Conn),
		meteredStockReservationQuery:      meteredStockReservationQuery{StockReservationQueryServiceClient: inventoryv1.NewStockReservationQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		pickListValidationClient:          warehousingv1.NewPickListValidationServiceClient(deps.Conn),
	}
}

// CreateStockItem creates a stock item.
func (s *StockItemServer) CreateStockItem(ctx context.Context, req *inventoryv1.CreateStockItemRequest) (*inventoryv1.CreateStockItemResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "create stock item", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	stockReservationQuery, err := registry.Lookup[inventoryv1.StockReservationQueryServiceClient](s.clients, "acme.inventory.v1.StockReservationQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	suggestStockReservationsRequest := &inventoryv1.SuggestStockReservationsRequest{
		Context: req.GetContext(),
		Filter: &inventoryv1.SuggestStockReservationsRequest_Filter{
			Region:       req.GetStockItem().GetAddress().GetRegion(),
			CreatedAfter: req.GetStockItem().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetStockItem().GetComponents()["primary"].GetComponentId(),
		},
	}
	suggestStockReservationsReply, err := stockReservationQuery.SuggestStockReservations(ctx, suggestStockReservationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/SuggestStockReservations")
	}
	cloned := proto.Clone(req.GetStockItem()).(*inventoryv1.StockItem)
	explainGiftCardDecisionReply, err := s.giftCardValidationClient.ExplainGiftCardDecision(ctx, &giftcardsv1.ExplainGiftCardDecisionRequest{
		Context: req.GetContext(),
		GiftCard: &giftcardsv1.GiftCard{
			Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetStockItem().GetAudit().GetUpdatedAt()},
			Contact: &commonv1.ContactInfo{Email: cloned.GetOwner().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ExplainGiftCardDecision")
	}
	return &inventoryv1.CreateStockItemResponse{
		Result: &commonv1.OperationResult{
			Details: suggestStockReservationsReply.GetResult().GetDetails(),
			Status:  suggestStockReservationsReply.GetResult().GetStatus(),
			Message: explainGiftCardDecisionReply.GetOutcome().GetMessage(),
		},
		StockItem: &inventoryv1.StockItem{
			Capacity: req.GetStockItem().GetCapacity(),
			Code:     req.GetStockItem().GetCode(),
		},
	}, nil
}

// GetStockItem returns a stock item.
func (s *StockItemServer) GetStockItem(ctx context.Context, req *inventoryv1.GetStockItemRequest) (*inventoryv1.GetStockItemResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_item", req.GetStockItemId(), "get_stock_item"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get stock item", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.GetStockItemResponse{
		StockItem: &inventoryv1.StockItem{StockItemId: req.GetStockItemId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// UpdateStockItem changes a stock item.
func (s *StockItemServer) UpdateStockItem(ctx context.Context, req *inventoryv1.UpdateStockItemRequest) (*inventoryv1.UpdateStockItemResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "update stock item", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	explainStockMovementDecisionRequest := &inventoryv1.ExplainStockMovementDecisionRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetStockItem().GetOwner().GetDisplayName(),
				Email:       req.GetStockItem().GetOwner().GetEmail(),
			},
			Measurements: []*inventoryv1.StockMovementMeasurement{&inventoryv1.StockMovementMeasurement{Unit: req.GetStockItem().GetDimensions().GetUnit()}},
		},
	}
	explainStockMovementDecisionReply, err := inventoryv1.StockMovementValidationServiceClient.ExplainStockMovementDecision(s.stockMovementValidationClient, ctx, explainStockMovementDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/ExplainStockMovementDecision")
	}
	var region string
	switch choice := req.GetStockItem().GetLocator().(type) {
	case *inventoryv1.StockItem_Address:
		region = choice.Address.GetRegion()
	case *inventoryv1.StockItem_FacilityCode:
		region = choice.FacilityCode
	}
	searchNotificationsRequest := &notificationsv1.SearchNotificationsRequest{
		Context: req.GetContext(),
		Filter: &notificationsv1.SearchNotificationsRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetStockItem().GetAudit().GetCreatedAt(),
			OwnerId:      explainStockMovementDecisionReply.GetResult().GetCorrelationId(),
		},
	}
	searchNotificationsReply, err := s.searchNotificationsFn(ctx, searchNotificationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationService/SearchNotifications")
	}
	return &inventoryv1.UpdateStockItemResponse{
		StockItem: &inventoryv1.StockItem{
			Owner: &identityv1.UserRef{
				DisplayName: explainStockMovementDecisionReply.GetStockMovement().GetSubject().GetDisplayName(),
			},
			Capacity: req.GetStockItem().GetCapacity(),
			Status:   req.GetStockItem().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Message:       searchNotificationsReply.GetResult().GetMessage(),
			CorrelationId: searchNotificationsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// DeleteStockItem deletes a stock item.
func (s *StockItemServer) DeleteStockItem(ctx context.Context, req *inventoryv1.DeleteStockItemRequest) (*inventoryv1.DeleteStockItemResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_item", req.GetStockItemId(), "delete_stock_item"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "delete stock item", "tenant", req.GetContext().GetTenantId())
	exportStockItemsStream, err := s.stockItemQueryClient.ExportStockItems(ctx, &inventoryv1.ExportStockItemsRequest{
		Context: req.GetContext(),
		Filter:  &inventoryv1.ExportStockItemsRequest_Filter{CreatedAfter: req.GetContext().GetDeadline()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemQueryService/ExportStockItems")
	}
	for {
		received, err := exportStockItemsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemQueryService/ExportStockItems")
		}
		_ = received
	}
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	scoreReplenishmentPlanRequest := &inventoryv1.ScoreReplenishmentPlanRequest{
		Context:           req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{Audit: &commonv1.AuditInfo{Etag: req.GetEtag()}},
		Reason:            tenancy.Value(ctx, "reason"),
	}
	scoreReplenishmentPlanReply, err := inventoryv1.ReplenishmentPlanValidationServiceClient.ScoreReplenishmentPlan(s.replenishmentPlanValidationClient, ctx, scoreReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/ScoreReplenishmentPlan")
	}
	validateStockMovementRequest := buildValidateStockMovementRequestForDeleteStockItem(req, scoreReplenishmentPlanReply)
	validateStockMovementReply, err := s.stockMovementValidationClient.ValidateStockMovement(ctx, validateStockMovementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/ValidateStockMovement")
	}
	return &inventoryv1.DeleteStockItemResponse{
		Result: &commonv1.OperationResult{
			Status:       scoreReplenishmentPlanReply.GetResult().GetStatus(),
			ReferenceIds: validateStockMovementReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// ListStockItems lists stock items a page at a time.
func (s *StockItemServer) ListStockItems(ctx context.Context, req *inventoryv1.ListStockItemsRequest) (*inventoryv1.ListStockItemsResponse, error) {
	s.log.InfoContext(ctx, "list stock items", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.ListStockItemsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SearchStockItems lists stock items a page at a time.
func (s *StockItemServer) SearchStockItems(ctx context.Context, req *inventoryv1.SearchStockItemsRequest) (*inventoryv1.SearchStockItemsResponse, error) {
	s.log.InfoContext(ctx, "search stock items", "tenant", req.GetContext().GetTenantId())
	return &inventoryv1.SearchStockItemsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// WatchStockItems streams the changes of stock items as they happen.
func (s *StockItemServer) WatchStockItems(req *inventoryv1.WatchStockItemsRequest, stream grpc.ServerStreamingServer[inventoryv1.WatchStockItemsResponse]) error {
	ctx := stream.Context()
	if req.GetContext().GetTenantId() == "" {
		return status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "watch stock items", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetSubscriber().GetEmail())
	encoded, err := protojson.Marshal(req.GetSubscriber())
	if err != nil {
		return grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.ContactInfo
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return err
	}
	explainGiftCardDecisionRequest := &giftcardsv1.ExplainGiftCardDecisionRequest{
		Context: req.GetContext(),
		Details: &giftcardsv1.ExplainGiftCardDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
		GiftCard: &giftcardsv1.GiftCard{
			Contact: &commonv1.ContactInfo{Phone: (&decoded).GetPhone()},
		},
	}
	var explainGiftCardDecisionReply *giftcardsv1.ExplainGiftCardDecisionResponse
	if err := pipeline.Run(ctx, "acme.giftcards.v1.GiftCardValidationService/ExplainGiftCardDecision", func(ctx context.Context) error {
		answer, err := s.giftCardValidationClient.ExplainGiftCardDecision(ctx, explainGiftCardDecisionRequest)
		explainGiftCardDecisionReply = answer
		return err
	}); err != nil {
		return grpcx.Wrap(err, "acme.giftcards.v1.GiftCardValidationService/ExplainGiftCardDecision")
	}
	exportStockReservationsStream, err := s.stockReservationQueryClient.ExportStockReservations(ctx, &inventoryv1.ExportStockReservationsRequest{
		Context: req.GetContext(),
		Filter:  &inventoryv1.ExportStockReservationsRequest_Filter{CreatedAfter: req.GetFilter().GetCreatedAfter()},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/ExportStockReservations")
	}
	for {
		received, err := exportStockReservationsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/ExportStockReservations")
		}
		_ = received
	}
	if err := stream.Send(&inventoryv1.WatchStockItemsResponse{
		StockItem: &inventoryv1.StockItem{
			Owner: &identityv1.UserRef{
				DisplayName: explainGiftCardDecisionReply.GetGiftCard().GetHolder().GetDisplayName(),
				Email:       req.GetSubscriber().GetEmail(),
			},
		},
		Result: &commonv1.OperationResult{
			Details: explainGiftCardDecisionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}); err != nil {
		return err
	}
	return nil
}

// BatchCreateStockItems takes a stream of stock items and answers once at the end.
func (s *StockItemServer) BatchCreateStockItems(stream grpc.ClientStreamingServer[inventoryv1.BatchCreateStockItemsRequest, inventoryv1.BatchCreateStockItemsResponse]) error {
	ctx := stream.Context()
	var accepted int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.log.InfoContext(ctx, "batch create stock items", "tenant", msg.GetContext().GetTenantId())
		countFeatureFlagsRequest := &featureflagsv1.CountFeatureFlagsRequest{
			Context: msg.GetContext(),
			Filter: &featureflagsv1.CountFeatureFlagsRequest_Filter{
				Region:       msg.GetRecord().GetAddress().GetRegion(),
				CreatedAfter: msg.GetRecord().GetAudit().GetUpdatedAt(),
			},
		}
		if _, err := grpcx.Call(ctx, s.featureFlagQueryClient.CountFeatureFlags, countFeatureFlagsRequest); err != nil {
			return grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagQueryService/CountFeatureFlags")
		}
		suggestStockReservationsRequest := &inventoryv1.SuggestStockReservationsRequest{
			Context: msg.GetContext(),
			Filter: &inventoryv1.SuggestStockReservationsRequest_Filter{
				Region:       msg.GetRecord().GetAddress().GetRegion(),
				CreatedAfter: msg.GetRecord().GetAudit().GetCreatedAt(),
				OwnerId:      msg.GetRecord().GetComponents()["primary"].GetComponentId(),
			},
		}
		if _, err := s.meteredStockReservationQuery.SuggestStockReservations(ctx, suggestStockReservationsRequest); err != nil {
			return grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/SuggestStockReservations")
		}
		lintReplenishmentPlansRequest := &inventoryv1.LintReplenishmentPlansRequest{
			Context: msg.GetContext(),
			Details: &inventoryv1.LintReplenishmentPlansRequest_Details{
				Contact: &commonv1.ContactInfo{Email: msg.GetRecord().GetOwner().GetEmail()},
			},
		}
		if _, err := grpcx.Call(ctx, s.replenishmentPlanValidationClient.LintReplenishmentPlans, lintReplenishmentPlansRequest); err != nil {
			return grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/LintReplenishmentPlans")
		}
		lintPickListsRequest := &warehousingv1.LintPickListsRequest{}
		lintPickListsRequest.Context = msg.GetContext()
		if _, err := s.pickListValidationClient.LintPickLists(ctx, lintPickListsRequest); err != nil {
			return grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/LintPickLists")
		}
		accepted++
	}
	return stream.SendAndClose(&inventoryv1.BatchCreateStockItemsResponse{Accepted: accepted})
}

// CloneStockItem creates a stock item.
func (s *StockItemServer) CloneStockItem(ctx context.Context, req *inventoryv1.CloneStockItemRequest) (*inventoryv1.CloneStockItemResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "clone stock item", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	specs := make(map[string]string, len(req.GetStockItem().GetSpecs()))
	for key, value := range req.GetStockItem().GetSpecs() {
		specs[key] = value
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key2, value2 := range req.GetContext().GetBaggage() {
		baggage[key2] = value2
	}
	summarizeStockReservationsRequest := &inventoryv1.SummarizeStockReservationsRequest{
		Context:     req.GetContext(),
		Annotations: specs,
		Details:     &inventoryv1.SummarizeStockReservationsRequest_Details{Filters: baggage},
	}
	summarizeStockReservationsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*inventoryv1.SummarizeStockReservationsResponse, error) {
		return s.stockReservationQueryClient.SummarizeStockReservations(ctx, summarizeStockReservationsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/SummarizeStockReservations")
	}
	var region string
	switch choice := req.GetStockItem().GetLocator().(type) {
	case *inventoryv1.StockItem_Address:
		region = choice.Address.GetRegion()
	case *inventoryv1.StockItem_FacilityCode:
		region = choice.FacilityCode
	}
	exportStockReservationsStream, err := s.stockReservationQueryClient.ExportStockReservations(ctx, &inventoryv1.ExportStockReservationsRequest{
		Context: req.GetContext(),
		Filter: &inventoryv1.ExportStockReservationsRequest_Filter{
			Region:       region,
			CreatedAfter: req.GetStockItem().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/ExportStockReservations")
	}
	for {
		received, err := exportStockReservationsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/ExportStockReservations")
		}
		_ = received
	}
	return &inventoryv1.CloneStockItemResponse{
		Result: &commonv1.OperationResult{
			Status: summarizeStockReservationsReply.GetResult().GetStatus(),
		},
		StockItem: &inventoryv1.StockItem{
			Specs: req.GetStockItem().GetSpecs(),
			Owner: req.GetStockItem().GetOwner(),
		},
	}, nil
}
