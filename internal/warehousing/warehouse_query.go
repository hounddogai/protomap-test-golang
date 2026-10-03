package warehousing

import (
	"context"
	"errors"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	travelv1 "github.com/hounddogai/protomap-test-golang/gen/acme/travel/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// WarehouseQueryServer implements `acme.warehousing.v1.WarehouseQueryService`.
// Answers read-only questions about warehouses, for screens and reports.
type WarehouseQueryServer struct {
	warehousingv1.UnimplementedWarehouseQueryServiceServer
	store                        *Store
	log                          *slog.Logger
	tripClient                   travelv1.TripServiceClient
	accessPolicyClient           authorizationv1.AccessPolicyServiceClient
	pickListValidationClient     warehousingv1.PickListValidationServiceClient
	usageRecordCalculationClient subscriptionsv1.UsageRecordCalculationServiceClient
	pointsLedgerValidationPort   pointsLedgerValidationPort
	userHistoryClient            identityv1.UserHistoryServiceClient
	putawayTaskValidationClient  warehousingv1.PutawayTaskValidationServiceClient
	validatePutawayTaskFn        func(context.Context, *warehousingv1.ValidatePutawayTaskRequest, ...grpc.CallOption) (*warehousingv1.ValidatePutawayTaskResponse, error)
	clients                      *registry.Registry
}

// NewWarehouseQueryServer wires a WarehouseQueryServer.
func NewWarehouseQueryServer(deps platform.Deps) *WarehouseQueryServer {
	putawayTaskValidation := warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn)
	return &WarehouseQueryServer{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.warehousing.v1.WarehouseQueryService"),
		tripClient:                   travelv1.NewTripServiceClient(deps.Conn),
		accessPolicyClient:           authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		pickListValidationClient:     warehousingv1.NewPickListValidationServiceClient(deps.Conn),
		usageRecordCalculationClient: subscriptionsv1.NewUsageRecordCalculationServiceClient(deps.Conn),
		pointsLedgerValidationPort:   newPointsLedgerValidationPort(deps),
		userHistoryClient:            identityv1.NewUserHistoryServiceClient(deps.Conn),
		putawayTaskValidationClient:  warehousingv1.NewPutawayTaskValidationServiceClient(deps.Conn),
		validatePutawayTaskFn:        putawayTaskValidation.ValidatePutawayTask,
		clients:                      deps.Clients,
	}
}

// LookupWarehouse returns a warehouse.
func (s *WarehouseQueryServer) LookupWarehouse(ctx context.Context, req *warehousingv1.LookupWarehouseRequest) (*warehousingv1.LookupWarehouseResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "warehouse", req.GetWarehouseId(), "lookup_warehouse"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "lookup warehouse", "tenant", req.GetContext().GetTenantId())
	return &warehousingv1.LookupWarehouseResponse{
		Warehouse: &warehousingv1.Warehouse{WarehouseId: req.GetWarehouseId()},
		Result:    &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// BrowseWarehouses lists warehouses a page at a time.
func (s *WarehouseQueryServer) BrowseWarehouses(ctx context.Context, req *warehousingv1.BrowseWarehousesRequest) (*warehousingv1.BrowseWarehousesResponse, error) {
	s.log.InfoContext(ctx, "browse warehouses", "tenant", req.GetContext().GetTenantId())
	source := req.GetFilter().ProtoReflect()
	query := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("query"))).String()
	listTripsRequest := &travelv1.ListTripsRequest{
		Context: req.GetContext(),
		Filter:  &travelv1.ListTripsRequest_Filter{Query: query},
	}
	listTripsReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*travelv1.ListTripsResponse, error) {
		return s.tripClient.ListTrips(ctx, listTripsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.travel.v1.TripService/ListTrips")
	}
	return &warehousingv1.BrowseWarehousesResponse{
		Result: &commonv1.OperationResult{
			Message: listTripsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// FindWarehouses lists warehouses a page at a time.
func (s *WarehouseQueryServer) FindWarehouses(ctx context.Context, req *warehousingv1.FindWarehousesRequest) (*warehousingv1.FindWarehousesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected FindWarehouses", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "find warehouses", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetFilter()).(*warehousingv1.FindWarehousesRequest_Filter)
	watchAccessPoliciesStream, err := s.accessPolicyClient.WatchAccessPolicies(ctx, &authorizationv1.WatchAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.WatchAccessPoliciesRequest_Filter{
			Region:  req.GetFilter().GetRegion(),
			OwnerId: cloned.GetOwnerId(),
			Query:   req.GetFilter().GetQuery(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
	}
	for {
		received, err := watchAccessPoliciesStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/WatchAccessPolicies")
		}
		_ = received
	}
	return &warehousingv1.FindWarehousesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// CountWarehouses counts warehouses by status.
func (s *WarehouseQueryServer) CountWarehouses(ctx context.Context, req *warehousingv1.CountWarehousesRequest) (*warehousingv1.CountWarehousesResponse, error) {
	s.log.InfoContext(ctx, "count warehouses", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "region", req.GetFilter().GetRegion())
	explainPickListDecisionRequest := &warehousingv1.ExplainPickListDecisionRequest{}
	explainPickListDecisionRequest.Context = req.GetContext()
	explainPickListDecisionRequest.PickList = &warehousingv1.PickList{
		BillingAddress: &commonv1.PostalAddress{Region: tenancy.Value(ctx, "region")},
	}
	explainPickListDecisionRequest.Details = &warehousingv1.ExplainPickListDecisionRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Locale:  req.GetContext().GetLocale(),
	}
	explainPickListDecisionReply, err := s.pickListValidationClient.ExplainPickListDecision(ctx, explainPickListDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/ExplainPickListDecision")
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareUsageRecordOptionsRequest := &subscriptionsv1.CompareUsageRecordOptionsRequest{
		Context: req.GetContext(),
		Details: &subscriptionsv1.CompareUsageRecordOptionsRequest_Details{
			Destination: &commonv1.PostalAddress{
				Region:     explainPickListDecisionReply.GetPickList().GetBillingAddress().GetRegion(),
				PostalCode: explainPickListDecisionReply.GetPickList().GetBillingAddress().GetPostalCode(),
				Locality:   explainPickListDecisionReply.GetPickList().GetBillingAddress().GetLocality(),
			},
		},
		UsageRecord: &subscriptionsv1.UsageRecord{Detail: packed},
	}
	type compareUsageRecordOptionsResult struct {
		reply *subscriptionsv1.CompareUsageRecordOptionsResponse
		err   error
	}
	compareUsageRecordOptionsResults := make(chan compareUsageRecordOptionsResult, 1)
	go func() {
		reply, err := s.usageRecordCalculationClient.CompareUsageRecordOptions(ctx, compareUsageRecordOptionsRequest)
		compareUsageRecordOptionsResults <- compareUsageRecordOptionsResult{reply, err}
	}()
	var compareUsageRecordOptionsReply *subscriptionsv1.CompareUsageRecordOptionsResponse
	select {
	case result := <-compareUsageRecordOptionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.subscriptions.v1.UsageRecordCalculationService/CompareUsageRecordOptions")
		}
		compareUsageRecordOptionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &warehousingv1.CountWarehousesResponse{
		Result: &commonv1.OperationResult{
			Status:        explainPickListDecisionReply.GetOutcome().GetStatus(),
			Details:       compareUsageRecordOptionsReply.GetResult().GetDetails(),
			CorrelationId: compareUsageRecordOptionsReply.GetResult().GetCorrelationId(),
		},
	}, nil
}

// ResolveWarehouses returns several warehouses at once.
func (s *WarehouseQueryServer) ResolveWarehouses(ctx context.Context, req *warehousingv1.ResolveWarehousesRequest) (*warehousingv1.ResolveWarehousesResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "resolve warehouses", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	validatePointsLedgerRequest := &loyaltyv1.ValidatePointsLedgerRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ValidatePointsLedgerRequest_Details{
			Contact: &commonv1.ContactInfo{Email: cloned.GetActor().GetEmail()},
			Locale:  req.GetContext().GetLocale(),
		},
		PointsLedger: &loyaltyv1.PointsLedger{
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
	}
	validatePointsLedgerReply, err := s.pointsLedgerValidationPort.ValidatePointsLedger(ctx, validatePointsLedgerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerValidationService/ValidatePointsLedger")
	}
	snapshot, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	recordUserVersionRequest := &identityv1.RecordUserVersionRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			Contact: &commonv1.ContactInfo{
				Phone: validatePointsLedgerReply.GetPointsLedger().GetContact().GetPhone(),
			},
		},
		Details: &identityv1.RecordUserVersionRequest_Details{Snapshot: snapshot},
	}
	recordUserVersionReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*identityv1.RecordUserVersionResponse, error) {
		return s.userHistoryClient.RecordUserVersion(ctx, recordUserVersionRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserHistoryService/RecordUserVersion")
	}
	_ = recordUserVersionReply
	return &warehousingv1.ResolveWarehousesResponse{
		Result: &commonv1.OperationResult{Status: validatePointsLedgerReply.GetResult().GetStatus()},
	}, nil
}

// ExportWarehouses streams every matching warehouse.
func (s *WarehouseQueryServer) ExportWarehouses(req *warehousingv1.ExportWarehousesRequest, stream grpc.ServerStreamingServer[warehousingv1.ExportWarehousesResponse]) error {
	ctx := stream.Context()
	if err := validateContext(req.GetContext()); err != nil {
		return err
	}
	s.log.InfoContext(ctx, "export warehouses", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetContext()).(*commonv1.RequestContext)
	validatePutawayTaskReply, err := s.putawayTaskValidationClient.ValidatePutawayTask(ctx, &warehousingv1.ValidatePutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Assignee: &identityv1.UserRef{
				UserId: cloned.GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
	})
	if err != nil {
		return grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/ValidatePutawayTask")
	}
	if err := stream.Send(&warehousingv1.ExportWarehousesResponse{
		Warehouse: &warehousingv1.Warehouse{
			Audit: &commonv1.AuditInfo{
				Etag:      validatePutawayTaskReply.GetPutawayTask().GetAudit().GetEtag(),
				UpdatedAt: validatePutawayTaskReply.GetPutawayTask().GetAudit().GetUpdatedAt(),
			},
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}); err != nil {
		return err
	}
	return nil
}

// SummarizeWarehouses summarizes a warehouse.
func (s *WarehouseQueryServer) SummarizeWarehouses(ctx context.Context, req *warehousingv1.SummarizeWarehousesRequest) (*warehousingv1.SummarizeWarehousesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "warehouse", req.GetWarehouseId(), "summarize_warehouses"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "summarize warehouses", "tenant", req.GetContext().GetTenantId())
	contextCache := cache.New[string, *commonv1.RequestContext](1)
	contextCache.Put(req.GetContext().GetTraceId(), req.GetContext())
	cachedContext, _ := contextCache.Get(req.GetContext().GetTraceId())
	validatePutawayTaskRequest := &warehousingv1.ValidatePutawayTaskRequest{
		Context: req.GetContext(),
		PutawayTask: &warehousingv1.PutawayTask{
			Requester: &commonv1.ContactInfo{Email: cachedContext.GetActor().GetEmail()},
		},
	}
	validatePutawayTaskReply, err := s.validatePutawayTaskFn(ctx, validatePutawayTaskRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PutawayTaskValidationService/ValidatePutawayTask")
	}
	return &warehousingv1.SummarizeWarehousesResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: validatePutawayTaskReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Period: req.GetDetails().GetPeriod(),
	}, nil
}

// SuggestWarehouses lists warehouses a page at a time.
func (s *WarehouseQueryServer) SuggestWarehouses(ctx context.Context, req *warehousingv1.SuggestWarehousesRequest) (*warehousingv1.SuggestWarehousesResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	s.log.InfoContext(ctx, "suggest warehouses", "tenant", req.GetContext().GetTenantId())
	return &warehousingv1.SuggestWarehousesResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// PeekWarehouse returns a warehouse.
func (s *WarehouseQueryServer) PeekWarehouse(ctx context.Context, req *warehousingv1.PeekWarehouseRequest) (*warehousingv1.PeekWarehouseResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PeekWarehouse", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "warehouse", req.GetWarehouseId(), "peek_warehouse"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "peek warehouse", "tenant", req.GetContext().GetTenantId())
	pickListValidation, ok := s.clients.Get("acme.warehousing.v1.PickListValidationService").(warehousingv1.PickListValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.warehousing.v1.PickListValidationService")
	}
	explainPickListDecisionRequest := &warehousingv1.ExplainPickListDecisionRequest{
		Context: req.GetContext(),
		PickList: &warehousingv1.PickList{
			Counterparty: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()},
			DueAt:        req.GetContext().GetDeadline(),
		},
		Details: &warehousingv1.ExplainPickListDecisionRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	explainPickListDecisionReply, err := pickListValidation.ExplainPickListDecision(ctx, explainPickListDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.PickListValidationService/ExplainPickListDecision")
	}
	return &warehousingv1.PeekWarehouseResponse{
		Result: &commonv1.OperationResult{
			Details: explainPickListDecisionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Warehouse: &warehousingv1.Warehouse{WarehouseId: req.GetWarehouseId()},
	}, nil
}
