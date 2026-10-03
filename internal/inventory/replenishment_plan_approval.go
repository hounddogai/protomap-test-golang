package inventory

import (
	"context"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	showsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shows/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ReplenishmentPlanApprovalServer implements `acme.inventory.v1.ReplenishmentPlanApprovalService`.
// Routes replenishment plans through approval, with escalation and delegation.
type ReplenishmentPlanApprovalServer struct {
	inventoryv1.UnimplementedReplenishmentPlanApprovalServiceServer
	store                            *Store
	log                              *slog.Logger
	conn                             *grpc.ClientConn
	sellerListingWorkflowClient      sellersv1.SellerListingWorkflowServiceClient
	customerClient                   customersv1.CustomerServiceClient
	stockReservationApprovalClient   inventoryv1.StockReservationApprovalServiceClient
	roleHistoryClient                identityv1.RoleHistoryServiceClient
	stockItemClient                  inventoryv1.StockItemServiceClient
	customerQueryClient              customersv1.CustomerQueryServiceClient
	stockMovementApprovalClient      inventoryv1.StockMovementApprovalServiceClient
	shipmentApprovalClient           shippingv1.ShipmentApprovalServiceClient
	customerCalculationClient        customersv1.CustomerCalculationServiceClient
	userClient                       identityv1.UserServiceClient
	auditEventQueryClient            auditv1.AuditEventQueryServiceClient
	bus                              *eventbus.Bus
	synonymHistoryClient             searchv1.SynonymHistoryServiceClient
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
	clients                          *registry.Registry
	venueValidationClient            venuesv1.VenueValidationServiceClient
}

// NewReplenishmentPlanApprovalServer wires a ReplenishmentPlanApprovalServer.
func NewReplenishmentPlanApprovalServer(deps platform.Deps) *ReplenishmentPlanApprovalServer {
	return &ReplenishmentPlanApprovalServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.inventory.v1.ReplenishmentPlanApprovalService"),
		conn:                             deps.Conn,
		sellerListingWorkflowClient:      sellersv1.NewSellerListingWorkflowServiceClient(deps.Conn),
		customerClient:                   customersv1.NewCustomerServiceClient(deps.Conn),
		stockReservationApprovalClient:   inventoryv1.NewStockReservationApprovalServiceClient(deps.Conn),
		roleHistoryClient:                identityv1.NewRoleHistoryServiceClient(deps.Conn),
		stockItemClient:                  inventoryv1.NewStockItemServiceClient(deps.Conn),
		customerQueryClient:              customersv1.NewCustomerQueryServiceClient(deps.Conn),
		stockMovementApprovalClient:      inventoryv1.NewStockMovementApprovalServiceClient(deps.Conn),
		shipmentApprovalClient:           shippingv1.NewShipmentApprovalServiceClient(deps.Conn),
		customerCalculationClient:        customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		userClient:                       identityv1.NewUserServiceClient(deps.Conn),
		auditEventQueryClient:            auditv1.NewAuditEventQueryServiceClient(deps.Conn),
		bus:                              deps.Bus,
		synonymHistoryClient:             searchv1.NewSynonymHistoryServiceClient(deps.Conn),
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		clients:                          deps.Clients,
		venueValidationClient:            venuesv1.NewVenueValidationServiceClient(deps.Conn),
	}
}

// SubmitReplenishmentPlan submits a replenishment plan.
func (s *ReplenishmentPlanApprovalServer) SubmitReplenishmentPlan(ctx context.Context, req *inventoryv1.SubmitReplenishmentPlanRequest) (*inventoryv1.SubmitReplenishmentPlanResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "submit replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetReplenishmentPlan().GetCounterparty().GetDisplayName())
	cloneShowRequest := &showsv1.CloneShowRequest{
		Context: req.GetContext(),
		Show: &showsv1.Show{
			Organizer: &identityv1.UserRef{
				DisplayName: tenancy.Value(ctx, "display_name"),
				Email:       req.GetDetails().GetApprover().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetReplenishmentPlan().GetAudit().GetUpdatedAt(),
			},
		},
	}
	cloneShowReply := new(showsv1.CloneShowResponse)
	if err := s.conn.Invoke(ctx, showsShowServiceCloneShowMethod, cloneShowRequest, cloneShowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shows.v1.ShowService/CloneShow")
	}
	listSellerListingWorkflowsReply, err := s.sellerListingWorkflowClient.ListSellerListingWorkflows(ctx, &sellersv1.ListSellerListingWorkflowsRequest{
		Context: req.GetContext(),
		Filter:  &sellersv1.ListSellerListingWorkflowsRequest_Filter{Region: cloneShowReply.GetShow().GetLocation().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/ListSellerListingWorkflows")
	}
	submitStockReservationRequest := &inventoryv1.SubmitStockReservationRequest{
		Context: req.GetContext(),
		StockReservation: &inventoryv1.StockReservation{
			Audit: &commonv1.AuditInfo{
				CreatedBy: cloneShowReply.GetShow().GetAudit().GetCreatedBy(),
			},
		},
	}
	submitStockReservationReply := new(inventoryv1.SubmitStockReservationResponse)
	if err := grpcx.Invoke(ctx, s.conn, inventoryv1.StockReservationApprovalService_SubmitStockReservation_FullMethodName, submitStockReservationRequest, submitStockReservationReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationApprovalService/SubmitStockReservation")
	}
	listCustomersRequest := &customersv1.ListCustomersRequest{}
	listCustomersRequest.Context = req.GetContext()
	listCustomersRequest.Filter = &customersv1.ListCustomersRequest_Filter{
		Region: req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
	}
	listCustomersReply, err := s.customerClient.ListCustomers(ctx, listCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/ListCustomers")
	}
	return &inventoryv1.SubmitReplenishmentPlanResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Metadata: cloneShowReply.GetShow().GetMetadata(),
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: cloneShowReply.GetShow().GetLocation().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: submitStockReservationReply.GetStockReservation().GetAudit().GetUpdatedAt(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Total:               req.GetReplenishmentPlan().GetTotal(),
		},
		Result: &commonv1.OperationResult{
			Status:        listSellerListingWorkflowsReply.GetResult().GetStatus(),
			CorrelationId: listSellerListingWorkflowsReply.GetResult().GetCorrelationId(),
		},
		Outcome: &inventoryv1.SubmitReplenishmentPlanResponse_Outcome{Message: listCustomersReply.GetResult().GetMessage()},
	}, nil
}

// ApproveReplenishmentPlan approves a replenishment plan.
func (s *ReplenishmentPlanApprovalServer) ApproveReplenishmentPlan(ctx context.Context, req *inventoryv1.ApproveReplenishmentPlanRequest) (*inventoryv1.ApproveReplenishmentPlanResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "approve replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded inventoryv1.ApproveReplenishmentPlanRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cloned := proto.Clone(req.GetReplenishmentPlan()).(*inventoryv1.ReplenishmentPlan)
	encoded2, err := protojson.Marshal(req.GetReplenishmentPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded2 inventoryv1.ReplenishmentPlan
	if err := protojson.Unmarshal(encoded2, &decoded2); err != nil {
		return nil, err
	}
	submitStockReservationReply, err := s.stockReservationApprovalClient.SubmitStockReservation(ctx, &inventoryv1.SubmitStockReservationRequest{
		Context: req.GetContext(),
		StockReservation: &inventoryv1.StockReservation{
			Location: &commonv1.PostalAddress{
				PostalCode: req.GetReplenishmentPlan().GetBillingAddress().GetPostalCode(),
			},
			Organizer: &identityv1.UserRef{DisplayName: (&decoded).GetApprover().GetDisplayName()},
			Audit: &commonv1.AuditInfo{
				Etag:      cloned.GetAudit().GetEtag(),
				CreatedAt: (&decoded2).GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationApprovalService/SubmitStockReservation")
	}
	replenishmentPlanCache := cache.New[string, *inventoryv1.ReplenishmentPlan](1)
	replenishmentPlanCache.Put(req.GetContext().GetTraceId(), req.GetReplenishmentPlan())
	cachedReplenishmentPlan, _ := replenishmentPlanCache.Get(req.GetContext().GetTraceId())
	ctx = tenancy.With(ctx, "display_name", req.GetReplenishmentPlan().GetCounterparty().GetDisplayName())
	snapshot, err := protojson.Marshal(req.GetReplenishmentPlan())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	annotateRoleVersionReply, err := s.roleHistoryClient.AnnotateRoleVersion(ctx, &identityv1.AnnotateRoleVersionRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Audit: &commonv1.AuditInfo{
				Etag:      cachedReplenishmentPlan.GetAudit().GetEtag(),
				UpdatedBy: req.GetReplenishmentPlan().GetAudit().GetUpdatedBy(),
			},
			Owner: &identityv1.UserRef{
				UserId:      submitStockReservationReply.GetStockReservation().GetOrganizer().GetUserId(),
				DisplayName: tenancy.Value(ctx, "display_name"),
			},
		},
		Details: &identityv1.AnnotateRoleVersionRequest_Details{Snapshot: snapshot},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/AnnotateRoleVersion")
	}
	return &inventoryv1.ApproveReplenishmentPlanResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: submitStockReservationReply.GetStockReservation().GetLocation().GetCountryCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: submitStockReservationReply.GetStockReservation().GetAudit().GetCreatedBy(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Number:              req.GetReplenishmentPlan().GetNumber(),
		},
		Outcome: &inventoryv1.ApproveReplenishmentPlanResponse_Outcome{
			EffectiveAt: annotateRoleVersionReply.GetOutcome().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RejectReplenishmentPlan rejects a replenishment plan.
func (s *ReplenishmentPlanApprovalServer) RejectReplenishmentPlan(ctx context.Context, req *inventoryv1.RejectReplenishmentPlanRequest) (*inventoryv1.RejectReplenishmentPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "reject replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	escalateStockReservationRequest := buildEscalateStockReservationRequestForRejectReplenishmentPlan(req)
	escalateStockReservationReply, err := s.stockReservationApprovalClient.EscalateStockReservation(ctx, escalateStockReservationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationApprovalService/EscalateStockReservation")
	}
	replenishmentPlanCache := cache.New[string, *inventoryv1.ReplenishmentPlan](1)
	replenishmentPlanCache.Put(req.GetContext().GetTraceId(), req.GetReplenishmentPlan())
	cachedReplenishmentPlan, _ := replenishmentPlanCache.Get(req.GetContext().GetTraceId())
	searchStockItemsRequest := &inventoryv1.SearchStockItemsRequest{}
	searchStockItemsRequest.Context = req.GetContext()
	searchStockItemsRequest.Filter = &inventoryv1.SearchStockItemsRequest_Filter{
		Region:       cachedReplenishmentPlan.GetBillingAddress().GetRegion(),
		CreatedAfter: escalateStockReservationReply.GetStockReservation().GetAudit().GetUpdatedAt(),
		OwnerId:      escalateStockReservationReply.GetResult().GetCorrelationId(),
	}
	searchStockItemsReply, err := s.stockItemClient.SearchStockItems(ctx, searchStockItemsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/SearchStockItems")
	}
	cloned := proto.Clone(req.GetReplenishmentPlan()).(*inventoryv1.ReplenishmentPlan)
	browseCustomersRequest := &customersv1.BrowseCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.BrowseCustomersRequest_Filter{
			Region:       req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
			CreatedAfter: cloned.GetCounterparty().GetStartsAt(),
		},
	}
	type browseCustomersResult struct {
		reply *customersv1.BrowseCustomersResponse
		err   error
	}
	browseCustomersResults := make(chan browseCustomersResult, 1)
	go func() {
		reply, err := s.customerQueryClient.BrowseCustomers(ctx, browseCustomersRequest)
		browseCustomersResults <- browseCustomersResult{reply, err}
	}()
	var browseCustomersReply *customersv1.BrowseCustomersResponse
	select {
	case result := <-browseCustomersResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.CustomerQueryService/BrowseCustomers")
		}
		browseCustomersReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &inventoryv1.RejectReplenishmentPlanResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Counterparty: &inventoryv1.StockReservationRef{
				StockReservationId: escalateStockReservationReply.GetStockReservation().GetStockReservationId(),
			},
			BillingAddress: &commonv1.PostalAddress{
				Locality: escalateStockReservationReply.GetStockReservation().GetLocation().GetLocality(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Metadata:            req.GetReplenishmentPlan().GetMetadata(),
		},
		Outcome: &inventoryv1.RejectReplenishmentPlanResponse_Outcome{Status: searchStockItemsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			ReferenceIds: searchStockItemsReply.GetResult().GetReferenceIds(),
			Details:      browseCustomersReply.GetResult().GetDetails(),
			Status:       "ok",
		},
	}, nil
}

// EscalateReplenishmentPlan escalates a replenishment plan.
func (s *ReplenishmentPlanApprovalServer) EscalateReplenishmentPlan(ctx context.Context, req *inventoryv1.EscalateReplenishmentPlanRequest) (*inventoryv1.EscalateReplenishmentPlanResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "escalate replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	jobs := make(chan *inventoryv1.ReplenishmentPlanLine)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range jobs {
				if _, err := s.stockMovementApprovalClient.CommentStockMovementApproval(workCtx, &inventoryv1.CommentStockMovementApprovalRequest{
					Context: req.GetContext(),
					StockMovement: &inventoryv1.StockMovement{
						Attributes: line.GetAttributes(),
						Subject: &customersv1.CustomerRef{
							DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
						},
					},
					Details: &inventoryv1.CommentStockMovementApprovalRequest_Details{
						Conditions: req.GetDetails().GetConditions(),
						Approver:   &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
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
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementApprovalService/CommentStockMovementApproval")
	}
	listPendingShipmentsRequest := &shippingv1.ListPendingShipmentsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListPendingShipmentsRequest_Filter{
			Region: req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
		},
	}
	listPendingShipmentsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*shippingv1.ListPendingShipmentsResponse, error) {
		return s.shipmentApprovalClient.ListPendingShipments(ctx, listPendingShipmentsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ShipmentApprovalService/ListPendingShipments")
	}
	getReplenishmentPlanRulesRequest := &inventoryv1.GetReplenishmentPlanRulesRequest{
		Context:             req.GetContext(),
		ReplenishmentPlanId: req.GetReplenishmentPlanId(),
	}
	getReplenishmentPlanRulesReply, err := getReplenishmentPlanRulesCall(ctx, getReplenishmentPlanRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/GetReplenishmentPlanRules")
	}
	calculateCustomerRequest := &customersv1.CalculateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: getReplenishmentPlanRulesReply.GetReplenishmentPlan().GetAudit().GetUpdatedBy(),
			},
			PrimaryAddress: &commonv1.PostalAddress{
				Locality: req.GetReplenishmentPlan().GetBillingAddress().GetLocality(),
			},
		},
		Details: &customersv1.CalculateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetReplenishmentPlan().GetBillingAddress().GetRecipient(),
			},
		},
	}
	calculateCustomerReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*customersv1.CalculateCustomerResponse, error) {
		return s.customerCalculationClient.CalculateCustomer(ctx, calculateCustomerRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/CalculateCustomer")
	}
	return &inventoryv1.EscalateReplenishmentPlanResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listPendingShipmentsReply.GetResult().GetReferenceIds(),
			Message:      calculateCustomerReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				Recipient: getReplenishmentPlanRulesReply.GetReplenishmentPlan().GetBillingAddress().GetRecipient(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Status:              req.GetReplenishmentPlan().GetStatus(),
		},
	}, nil
}

// ListPendingReplenishmentPlans lists replenishment plans a page at a time.
func (s *ReplenishmentPlanApprovalServer) ListPendingReplenishmentPlans(ctx context.Context, req *inventoryv1.ListPendingReplenishmentPlansRequest) (*inventoryv1.ListPendingReplenishmentPlansResponse, error) {
	s.log.InfoContext(ctx, "list pending replenishment plans", "tenant", req.GetContext().GetTenantId())
	updateUserReply, err := s.userClient.UpdateUser(ctx, &identityv1.UpdateUserRequest{
		Context: req.GetContext(),
		User: &identityv1.User{
			PrimaryAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
			RelationshipManager: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.UserService/UpdateUser")
	}
	return &inventoryv1.ListPendingReplenishmentPlansResponse{
		ReplenishmentPlans: []*inventoryv1.ReplenishmentPlan{&inventoryv1.ReplenishmentPlan{
			Audit: &commonv1.AuditInfo{
				CreatedAt: updateUserReply.GetUser().GetAudit().GetCreatedAt(),
			},
			Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{Attributes: updateUserReply.GetUser().GetAttributes()}},
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// DelegateReplenishmentPlanApproval delegates a replenishment plan.
func (s *ReplenishmentPlanApprovalServer) DelegateReplenishmentPlanApproval(ctx context.Context, req *inventoryv1.DelegateReplenishmentPlanApprovalRequest) (*inventoryv1.DelegateReplenishmentPlanApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "delegate replenishment plan approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	suggestAuditEventsRequest := &auditv1.SuggestAuditEventsRequest{}
	suggestAuditEventsRequest.Context = req.GetContext()
	suggestAuditEventsRequest.Filter = &auditv1.SuggestAuditEventsRequest_Filter{
		Region:       req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
		CreatedAfter: req.GetReplenishmentPlan().GetAudit().GetCreatedAt(),
		OwnerId:      req.GetReplenishmentPlan().GetCounterparty().GetStockReservationId(),
	}
	suggestAuditEventsReply, err := s.auditEventQueryClient.SuggestAuditEvents(ctx, suggestAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/SuggestAuditEvents")
	}
	return &inventoryv1.DelegateReplenishmentPlanApprovalResponse{
		Result:  &commonv1.OperationResult{Status: suggestAuditEventsReply.GetResult().GetStatus()},
		Outcome: &inventoryv1.DelegateReplenishmentPlanApprovalResponse_Outcome{Status: suggestAuditEventsReply.GetResult().GetStatus()},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			Total:               req.GetReplenishmentPlan().GetTotal(),
		},
	}, nil
}

// RecallReplenishmentPlan recalls a replenishment plan.
func (s *ReplenishmentPlanApprovalServer) RecallReplenishmentPlan(ctx context.Context, req *inventoryv1.RecallReplenishmentPlanRequest) (*inventoryv1.RecallReplenishmentPlanResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "recall replenishment plan", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "inventory.recall_replenishment_plan.submit_stock_reservation",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"stock_reservation.audit.created_by": req.GetReplenishmentPlan().GetAudit().GetCreatedBy(),
			"context.tenant_id":                  req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &inventoryv1.RecallReplenishmentPlanResponse{
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
			BillingAddress:      req.GetReplenishmentPlan().GetBillingAddress(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentReplenishmentPlanApproval comments a replenishment plan.
func (s *ReplenishmentPlanApprovalServer) CommentReplenishmentPlanApproval(ctx context.Context, req *inventoryv1.CommentReplenishmentPlanApprovalRequest) (*inventoryv1.CommentReplenishmentPlanApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveReplenishmentPlan(ctx, req.GetContext().GetTenantId(), req.GetReplenishmentPlan()); err != nil {
		return nil, grpcx.Wrap(err, "save replenishment plan")
	}
	s.log.InfoContext(ctx, "comment replenishment plan approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetReplenishmentPlan().GetAudit().GetEtag())
	revertSynonymReply, err := s.synonymHistoryClient.RevertSynonym(ctx, &searchv1.RevertSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetReplenishmentPlan().GetAudit().GetUpdatedBy(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/RevertSynonym")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := replenishmentPlanApprovalServerCalculateCustomerSegmentSteps[mode]
	var holder struct{ PostalCode string }
	if record := req.GetReplenishmentPlan().GetBillingAddress(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PostalCode"); err != nil {
			s.log.WarnContext(ctx, "copy postal_code", "error", err)
		}
	}
	calculateCustomerSegmentRequest := &customersv1.CalculateCustomerSegmentRequest{
		Context: req.GetContext(),
		Details: &customersv1.CalculateCustomerSegmentRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality:   req.GetReplenishmentPlan().GetBillingAddress().GetLocality(),
				PostalCode: holder.PostalCode,
			},
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Action: &customersv1.CustomerSegment_Route{Route: &customersv1.CustomerSegmentRouteAction{
				Weight: revertSynonymReply.GetSynonym().GetRoute().GetWeight(),
			}},
			Owner: &identityv1.UserRef{
				DisplayName: revertSynonymReply.GetSynonym().GetOwner().GetDisplayName(),
			},
		},
	}
	calculateCustomerSegmentReply, err := run(s, ctx, calculateCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/CalculateCustomerSegment")
	}
	consentRecordCalculation, err := registry.Lookup[customersv1.ConsentRecordCalculationServiceClient](s.clients, "acme.customers.v1.ConsentRecordCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	estimateConsentRecordRequest := &customersv1.EstimateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: calculateCustomerSegmentReply.GetCustomerSegment().GetOwner().GetDisplayName(),
			},
		},
		Details: &customersv1.EstimateConsentRecordRequest_Details{
			Destination: &commonv1.PostalAddress{
				Recipient: req.GetReplenishmentPlan().GetBillingAddress().GetRecipient(),
			},
		},
	}
	estimateConsentRecordReply, err := consentRecordCalculation.EstimateConsentRecord(ctx, estimateConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/EstimateConsentRecord")
	}
	step := &replenishmentPlanApprovalServerCommentReplenishmentPlanApprovalCheckVenueEligibilityStep{request: &venuesv1.CheckVenueEligibilityRequest{
		Context: req.GetContext(),
		Venue: &venuesv1.Venue{
			Locator: &venuesv1.Venue_Address{Address: &commonv1.PostalAddress{
				Region: req.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
			}},
		},
	}}
	if err := s.runCommentReplenishmentPlanApprovalCheckVenueEligibility(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/CheckVenueEligibility")
	}
	checkVenueEligibilityReply := step.reply
	return &inventoryv1.CommentReplenishmentPlanApprovalResponse{
		Outcome: &inventoryv1.CommentReplenishmentPlanApprovalResponse_Outcome{
			NextSteps:   revertSynonymReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Lines: []*inventoryv1.ReplenishmentPlanLine{&inventoryv1.ReplenishmentPlanLine{
				Tax: &inventoryv1.ReplenishmentPlanLineTax{
					Amount: calculateCustomerSegmentReply.GetCustomerSegment().GetDiscount().GetAmount(),
				},
			}},
			BillingAddress: &commonv1.PostalAddress{
				Lines: checkVenueEligibilityReply.GetVenue().GetAddress().GetLines(),
			},
			ReplenishmentPlanId: req.GetReplenishmentPlanId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: estimateConsentRecordReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// replenishmentPlanApprovalServerCalculateCustomerSegmentSteps are the ways to run
// acme.customers.v1.CustomerSegmentCalculationService/CalculateCustomerSegment, by name.
var replenishmentPlanApprovalServerCalculateCustomerSegmentSteps = map[string]func(*ReplenishmentPlanApprovalServer, context.Context, *customersv1.CalculateCustomerSegmentRequest) (*customersv1.CalculateCustomerSegmentResponse, error){
	"default": (*ReplenishmentPlanApprovalServer).calculateCustomerSegmentStep,
	"audited": (*ReplenishmentPlanApprovalServer).calculateCustomerSegmentAuditedStep,
}

func (s *ReplenishmentPlanApprovalServer) calculateCustomerSegmentStep(ctx context.Context, request *customersv1.CalculateCustomerSegmentRequest) (*customersv1.CalculateCustomerSegmentResponse, error) {
	return s.customerSegmentCalculationClient.CalculateCustomerSegment(ctx, request)
}

func (s *ReplenishmentPlanApprovalServer) calculateCustomerSegmentAuditedStep(ctx context.Context, request *customersv1.CalculateCustomerSegmentRequest) (*customersv1.CalculateCustomerSegmentResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.CustomerSegmentCalculationService/CalculateCustomerSegment")
	return s.customerSegmentCalculationClient.CalculateCustomerSegment(ctx, request)
}

// replenishmentPlanApprovalServerCommentReplenishmentPlanApprovalCheckVenueEligibilityStep carries the request of one
// step of CommentReplenishmentPlanApproval and, once it ran, its reply.
type replenishmentPlanApprovalServerCommentReplenishmentPlanApprovalCheckVenueEligibilityStep struct {
	request *venuesv1.CheckVenueEligibilityRequest
	reply   *venuesv1.CheckVenueEligibilityResponse
}

func (s *ReplenishmentPlanApprovalServer) runCommentReplenishmentPlanApprovalCheckVenueEligibility(ctx context.Context, step *replenishmentPlanApprovalServerCommentReplenishmentPlanApprovalCheckVenueEligibilityStep) error {
	reply, err := s.venueValidationClient.CheckVenueEligibility(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
