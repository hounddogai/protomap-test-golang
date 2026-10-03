package inventory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/iterx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// StockReservationApprovalServer implements `acme.inventory.v1.StockReservationApprovalService`.
// Routes stock reservations through approval, with escalation and delegation.
type StockReservationApprovalServer struct {
	inventoryv1.UnimplementedStockReservationApprovalServiceServer
	store                             *Store
	log                               *slog.Logger
	conn                              *grpc.ClientConn
	stockItemApprovalClient           inventoryv1.StockItemApprovalServiceClient
	replenishmentPlanValidationClient inventoryv1.ReplenishmentPlanValidationServiceClient
	customerSegmentValidationClient   customersv1.CustomerSegmentValidationServiceClient
	usageRecordApprovalClient         subscriptionsv1.UsageRecordApprovalServiceClient
	stockItemQueryOnce                sync.Once
	stockItemQueryCached              inventoryv1.StockItemQueryServiceClient
	preferenceValidationClient        customersv1.PreferenceValidationServiceClient
	roleHistoryClient                 identityv1.RoleHistoryServiceClient
	sellerListingLifecycleClient      sellersv1.SellerListingLifecycleServiceClient
	stockItemQueryClient              inventoryv1.StockItemQueryServiceClient
	validatePreferenceFn              func(context.Context, *customersv1.ValidatePreferenceRequest, ...grpc.CallOption) (*customersv1.ValidatePreferenceResponse, error)
	clients                           *registry.Registry
	customerQueryClient               customersv1.CustomerQueryServiceClient
	renewalScheduleWorkflowClient     subscriptionsv1.RenewalScheduleWorkflowServiceClient
	stockMovementValidationClient     inventoryv1.StockMovementValidationServiceClient
}

// NewStockReservationApprovalServer wires a StockReservationApprovalServer.
func NewStockReservationApprovalServer(deps platform.Deps) *StockReservationApprovalServer {
	preferenceValidation := customersv1.NewPreferenceValidationServiceClient(deps.Conn)
	return &StockReservationApprovalServer{
		store:                             NewStore(deps.DB),
		log:                               deps.Logger.With("service", "acme.inventory.v1.StockReservationApprovalService"),
		conn:                              deps.Conn,
		stockItemApprovalClient:           inventoryv1.NewStockItemApprovalServiceClient(deps.Conn),
		replenishmentPlanValidationClient: inventoryv1.NewReplenishmentPlanValidationServiceClient(deps.Conn),
		customerSegmentValidationClient:   customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		usageRecordApprovalClient:         subscriptionsv1.NewUsageRecordApprovalServiceClient(deps.Conn),
		preferenceValidationClient:        customersv1.NewPreferenceValidationServiceClient(deps.Conn),
		roleHistoryClient:                 identityv1.NewRoleHistoryServiceClient(deps.Conn),
		sellerListingLifecycleClient:      sellersv1.NewSellerListingLifecycleServiceClient(deps.Conn),
		stockItemQueryClient:              inventoryv1.NewStockItemQueryServiceClient(deps.Conn),
		validatePreferenceFn:              preferenceValidation.ValidatePreference,
		clients:                           deps.Clients,
		customerQueryClient:               customersv1.NewCustomerQueryServiceClient(deps.Conn),
		renewalScheduleWorkflowClient:     subscriptionsv1.NewRenewalScheduleWorkflowServiceClient(deps.Conn),
		stockMovementValidationClient:     inventoryv1.NewStockMovementValidationServiceClient(deps.Conn),
	}
}

// SubmitStockReservation submits a stock reservation.
func (s *StockReservationApprovalServer) SubmitStockReservation(ctx context.Context, req *inventoryv1.SubmitStockReservationRequest) (*inventoryv1.SubmitStockReservationResponse, error) {
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "submit stock reservation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetStockReservation()).(*inventoryv1.StockReservation)
	listFulfillmentWorkflowsRequest := &fulfillmentv1.ListFulfillmentWorkflowsRequest{
		Context: req.GetContext(),
		Filter:  &fulfillmentv1.ListFulfillmentWorkflowsRequest_Filter{Region: cloned.GetLocation().GetRegion()},
	}
	listFulfillmentWorkflowsReply := new(fulfillmentv1.ListFulfillmentWorkflowsResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", fulfillmentFulfillmentWorkflowServiceName, "ListFulfillmentWorkflows"), listFulfillmentWorkflowsRequest, listFulfillmentWorkflowsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentWorkflowService/ListFulfillmentWorkflows")
	}
	return &inventoryv1.SubmitStockReservationResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: listFulfillmentWorkflowsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  listFulfillmentWorkflowsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		StockReservation: &inventoryv1.StockReservation{
			StockReservationId: req.GetStockReservationId(),
			Title:              req.GetStockReservation().GetTitle(),
		},
	}, nil
}

// ApproveStockReservation approves a stock reservation.
func (s *StockReservationApprovalServer) ApproveStockReservation(ctx context.Context, req *inventoryv1.ApproveStockReservationRequest) (*inventoryv1.ApproveStockReservationResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "approve stock reservation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	jobs := make(chan *inventoryv1.StockReservationParticipant)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for participant := range jobs {
				if _, err := s.stockItemApprovalClient.CommentStockItemApproval(workCtx, &inventoryv1.CommentStockItemApprovalRequest{
					Context: req.GetContext(),
					Details: &inventoryv1.CommentStockItemApprovalRequest_Details{
						Approver: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetStockReservation().GetParticipants() {
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
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/CommentStockItemApproval")
	}
	for participant2 := range iterx.Each(req.GetStockReservation().GetParticipants(), func(candidate *inventoryv1.StockReservationParticipant) bool { return candidate != nil }) {
		if _, err := s.replenishmentPlanValidationClient.PreviewReplenishmentPlan(ctx, &inventoryv1.PreviewReplenishmentPlanRequest{
			Context: req.GetContext(),
			Details: &inventoryv1.PreviewReplenishmentPlanRequest_Details{
				Contact: &commonv1.ContactInfo{Name: participant2.GetContact().GetName()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanValidationService/PreviewReplenishmentPlan")
		}
	}
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{Context: req.GetContext()}
	lintCustomerSegmentsReply, err := customersv1.CustomerSegmentValidationServiceClient.LintCustomerSegments(s.customerSegmentValidationClient, ctx, lintCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &inventoryv1.ApproveStockReservationResponse{
		StockReservation: &inventoryv1.StockReservation{
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact: &commonv1.ContactInfo{
					Email: lintCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetEmail(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				Etag: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetEtag(),
			},
			StockReservationId: req.GetStockReservationId(),
			Status:             req.GetStockReservation().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// EscalateStockReservation escalates a stock reservation.
func (s *StockReservationApprovalServer) EscalateStockReservation(ctx context.Context, req *inventoryv1.EscalateStockReservationRequest) (*inventoryv1.EscalateStockReservationResponse, error) {
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "escalate stock reservation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	detailsCache := cache.New[string, *inventoryv1.EscalateStockReservationRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	packed, err := anypb.New(req.GetStockReservation())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	rejectUsageRecordRequest := &subscriptionsv1.RejectUsageRecordRequest{
		Context: req.GetContext(),
		UsageRecord: &subscriptionsv1.UsageRecord{
			Subject: &customersv1.CustomerRef{
				Email:       req.GetStockReservation().GetOrganizer().GetEmail(),
				DisplayName: cachedDetails.GetApprover().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &subscriptionsv1.RejectUsageRecordRequest_Details{
			Approver: &identityv1.UserRef{
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: req.GetStockReservation().GetOrganizer().GetDisplayName(),
			},
		},
	}
	rejectUsageRecordReply, err := subscriptionsv1.UsageRecordApprovalServiceClient.RejectUsageRecord(s.usageRecordApprovalClient, ctx, rejectUsageRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.UsageRecordApprovalService/RejectUsageRecord")
	}
	resolveStockItemsRequest := &inventoryv1.ResolveStockItemsRequest{Context: req.GetContext()}
	resolveStockItemsReply, err := s.stockItemQueryLazy().ResolveStockItems(ctx, resolveStockItemsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemQueryService/ResolveStockItems")
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*customersv1.ExplainPreferenceDecisionResponse, len(req.GetStockReservation().GetParticipants()))
	for i, participant := range req.GetStockReservation().GetParticipants() {
		group.Go(func() error {
			reply, err := s.preferenceValidationClient.ExplainPreferenceDecision(groupCtx, &customersv1.ExplainPreferenceDecisionRequest{
				Context: req.GetContext(),
				Preference: &customersv1.Preference{
					Owner: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
				},
				Details: &customersv1.ExplainPreferenceDecisionRequest_Details{
					Contact: &commonv1.ContactInfo{Phone: participant.GetContact().GetPhone()},
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ExplainPreferenceDecision")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &inventoryv1.EscalateStockReservationResponse{
		Result: &commonv1.OperationResult{
			Details: rejectUsageRecordReply.GetResult().GetDetails(),
			Status:  resolveStockItemsReply.GetResult().GetStatus(),
		},
		Outcome: &inventoryv1.EscalateStockReservationResponse_Outcome{Status: resolveStockItemsReply.GetResult().GetStatus()},
		StockReservation: &inventoryv1.StockReservation{
			StockReservationId: req.GetStockReservationId(),
			Location:           req.GetStockReservation().GetLocation(),
		},
	}, nil
}

// DelegateStockReservationApproval delegates a stock reservation.
func (s *StockReservationApprovalServer) DelegateStockReservationApproval(ctx context.Context, req *inventoryv1.DelegateStockReservationApprovalRequest) (*inventoryv1.DelegateStockReservationApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "delegate stock reservation approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetStockReservation())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	step := &stockReservationApprovalServerDelegateStockReservationApprovalRevertRoleStep{request: &identityv1.RevertRoleRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Audit: &commonv1.AuditInfo{Etag: req.GetStockReservation().GetAudit().GetEtag()},
			Owner: &identityv1.UserRef{
				UserId:      req.GetStockReservation().GetOrganizer().GetUserId(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				Email:       req.GetStockReservation().GetOrganizer().GetEmail(),
			},
		},
		Details: &identityv1.RevertRoleRequest_Details{Snapshot: snapshot},
	}}
	if err := s.runDelegateStockReservationApprovalRevertRole(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/RevertRole")
	}
	revertRoleReply := step.reply
	listPendingStockItemsRequest := &inventoryv1.ListPendingStockItemsRequest{
		Context: req.GetContext(),
		Filter: &inventoryv1.ListPendingStockItemsRequest_Filter{
			Region:       req.GetStockReservation().GetLocation().GetRegion(),
			CreatedAfter: revertRoleReply.GetRole().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
		},
	}
	listPendingStockItemsReply := new(inventoryv1.ListPendingStockItemsResponse)
	if err := grpcx.Invoke(ctx, s.conn, inventoryv1.StockItemApprovalService_ListPendingStockItems_FullMethodName, listPendingStockItemsRequest, listPendingStockItemsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/ListPendingStockItems")
	}
	for _, participant := range req.GetStockReservation().GetParticipants() {
		if _, err := s.sellerListingLifecycleClient.ScheduleSellerListingExpiry(ctx, &sellersv1.ScheduleSellerListingExpiryRequest{
			Context: req.GetContext(),
			SellerListing: &sellersv1.SellerListing{
				Owner: &identityv1.UserRef{Email: participant.GetContact().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingLifecycleService/ScheduleSellerListingExpiry")
		}
	}
	return &inventoryv1.DelegateStockReservationApprovalResponse{
		Outcome: &inventoryv1.DelegateStockReservationApprovalResponse_Outcome{EffectiveAt: revertRoleReply.GetOutcome().GetEffectiveAt()},
		StockReservation: &inventoryv1.StockReservation{
			Organizer:          &identityv1.UserRef{Email: revertRoleReply.GetRole().GetOwner().GetEmail()},
			StockReservationId: req.GetStockReservationId(),
			Location:           req.GetStockReservation().GetLocation(),
		},
		Result: &commonv1.OperationResult{
			Details:       listPendingStockItemsReply.GetResult().GetDetails(),
			CorrelationId: listPendingStockItemsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// RecallStockReservation recalls a stock reservation.
func (s *StockReservationApprovalServer) RecallStockReservation(ctx context.Context, req *inventoryv1.RecallStockReservationRequest) (*inventoryv1.RecallStockReservationResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallStockReservation", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "recall stock reservation", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	browseStockItemsReply, err := s.stockItemQueryClient.BrowseStockItems(ctx, &inventoryv1.BrowseStockItemsRequest{
		Context: req.GetContext(),
		Filter:  &inventoryv1.BrowseStockItemsRequest_Filter{Region: req.GetStockReservation().GetLocation().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemQueryService/BrowseStockItems")
	}
	validatePreferenceRequest := &customersv1.ValidatePreferenceRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
	}
	validatePreferenceReply, err := s.validatePreferenceFn(ctx, validatePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/ValidatePreference")
	}
	stockMovementValidation, ok := s.clients.Get("acme.inventory.v1.StockMovementValidationService").(inventoryv1.StockMovementValidationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.inventory.v1.StockMovementValidationService")
	}
	normalizeStockMovementRequest := &inventoryv1.NormalizeStockMovementRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
			CorrelationId: validatePreferenceReply.GetResult().GetCorrelationId(),
		},
		Details: &inventoryv1.NormalizeStockMovementRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetStockReservation().GetOrganizer().GetEmail()},
		},
	}
	normalizeStockMovementReply, err := stockMovementValidation.NormalizeStockMovement(ctx, normalizeStockMovementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/NormalizeStockMovement")
	}
	return &inventoryv1.RecallStockReservationResponse{
		Outcome: &inventoryv1.RecallStockReservationResponse_Outcome{
			Status:  browseStockItemsReply.GetResult().GetStatus(),
			Message: browseStockItemsReply.GetResult().GetMessage(),
		},
		StockReservation: &inventoryv1.StockReservation{
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact: &commonv1.ContactInfo{
					Email: validatePreferenceReply.GetPreference().GetOwner().GetEmail(),
				},
			}},
			StockReservationId: req.GetStockReservationId(),
			Window:             req.GetStockReservation().GetWindow(),
		},
		Result: &commonv1.OperationResult{
			Message: normalizeStockMovementReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// GetStockReservationApprovalStatus returns a stock reservation.
func (s *StockReservationApprovalServer) GetStockReservationApprovalStatus(ctx context.Context, req *inventoryv1.GetStockReservationApprovalStatusRequest) (*inventoryv1.GetStockReservationApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_reservation", req.GetStockReservationId(), "get_stock_reservation_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get stock reservation approval status", "tenant", req.GetContext().GetTenantId())
	exportCustomersStream, err := s.customerQueryClient.ExportCustomers(ctx, &customersv1.ExportCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportCustomersRequest_Filter{
			OwnerId:      req.GetStockReservationId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       req.GetContext().GetBaggage(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ExportCustomers")
	}
	for {
		received, err := exportCustomersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ExportCustomers")
		}
		_ = received
	}
	startRenewalScheduleWorkflowRequest := &subscriptionsv1.StartRenewalScheduleWorkflowRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Organizer: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{ParticipantId: req.GetStockReservationId()}},
			Audit:        &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
	}
	startRenewalScheduleWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*subscriptionsv1.StartRenewalScheduleWorkflowResponse, error) {
		return s.renewalScheduleWorkflowClient.StartRenewalScheduleWorkflow(ctx, startRenewalScheduleWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleWorkflowService/StartRenewalScheduleWorkflow")
	}
	return &inventoryv1.GetStockReservationApprovalStatusResponse{
		StockReservation: &inventoryv1.StockReservation{
			Title:              startRenewalScheduleWorkflowReply.GetRenewalSchedule().GetTitle(),
			StockReservationId: req.GetStockReservationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentStockReservationApproval comments a stock reservation.
func (s *StockReservationApprovalServer) CommentStockReservationApproval(ctx context.Context, req *inventoryv1.CommentStockReservationApprovalRequest) (*inventoryv1.CommentStockReservationApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "comment stock reservation approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetStockReservation())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	recallTrackingEventRequest := &shippingv1.RecallTrackingEventRequest{
		Context: req.GetContext(),
		Details: &shippingv1.RecallTrackingEventRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: req.GetStockReservation().GetOrganizer().GetUserId(),
			},
		},
		TrackingEvent: &shippingv1.TrackingEvent{Detail: packed},
	}
	recallTrackingEventReply := new(shippingv1.RecallTrackingEventResponse)
	if err := s.conn.Invoke(ctx, shippingTrackingEventApprovalServiceRecallTrackingEventMethod, recallTrackingEventRequest, recallTrackingEventReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventApprovalService/RecallTrackingEvent")
	}
	listPendingStockItemsReply, err := s.stockItemApprovalClient.ListPendingStockItems(ctx, &inventoryv1.ListPendingStockItemsRequest{
		Context: req.GetContext(),
		Filter:  &inventoryv1.ListPendingStockItemsRequest_Filter{Region: req.GetStockReservation().GetLocation().GetRegion()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/ListPendingStockItems")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := stockReservationApprovalServerValidateStockMovementSteps[mode]
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded inventoryv1.CommentStockReservationApprovalRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed2, err := anypb.New(req.GetStockReservation())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	validateStockMovementRequest := &inventoryv1.ValidateStockMovementRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: (&decoded).GetApprover().GetDisplayName(),
				Email:       req.GetDetails().GetApprover().GetEmail(),
			},
			Detail: packed2,
		},
		Details: &inventoryv1.ValidateStockMovementRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetStockReservation().GetOrganizer().GetEmail()},
		},
	}
	validateStockMovementReply, err := run(s, ctx, validateStockMovementRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementValidationService/ValidateStockMovement")
	}
	return &inventoryv1.CommentStockReservationApprovalResponse{
		Result: &commonv1.OperationResult{
			Details: recallTrackingEventReply.GetResult().GetDetails(),
			Message: listPendingStockItemsReply.GetResult().GetMessage(),
			Status:  validateStockMovementReply.GetOutcome().GetStatus(),
		},
		Outcome: &inventoryv1.CommentStockReservationApprovalResponse_Outcome{
			Message: validateStockMovementReply.GetOutcome().GetMessage(),
		},
		StockReservation: &inventoryv1.StockReservation{
			StockReservationId: req.GetStockReservationId(),
			Metadata:           req.GetStockReservation().GetMetadata(),
		},
	}, nil
}

// stockItemQueryLazy dials acme.inventory.v1.StockItemQueryService the first time a handler needs it.
func (s *StockReservationApprovalServer) stockItemQueryLazy() inventoryv1.StockItemQueryServiceClient {
	s.stockItemQueryOnce.Do(func() { s.stockItemQueryCached = inventoryv1.NewStockItemQueryServiceClient(s.conn) })
	return s.stockItemQueryCached
}

// stockReservationApprovalServerDelegateStockReservationApprovalRevertRoleStep carries the request of one step of
// DelegateStockReservationApproval and, once it ran, its reply.
type stockReservationApprovalServerDelegateStockReservationApprovalRevertRoleStep struct {
	request *identityv1.RevertRoleRequest
	reply   *identityv1.RevertRoleResponse
}

func (s *StockReservationApprovalServer) runDelegateStockReservationApprovalRevertRole(ctx context.Context, step *stockReservationApprovalServerDelegateStockReservationApprovalRevertRoleStep) error {
	reply, err := s.roleHistoryClient.RevertRole(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// stockReservationApprovalServerValidateStockMovementSteps are the ways to run
// acme.inventory.v1.StockMovementValidationService/ValidateStockMovement, by name.
var stockReservationApprovalServerValidateStockMovementSteps = map[string]func(*StockReservationApprovalServer, context.Context, *inventoryv1.ValidateStockMovementRequest) (*inventoryv1.ValidateStockMovementResponse, error){
	"default": (*StockReservationApprovalServer).validateStockMovementStep,
	"audited": (*StockReservationApprovalServer).validateStockMovementAuditedStep,
}

func (s *StockReservationApprovalServer) validateStockMovementStep(ctx context.Context, request *inventoryv1.ValidateStockMovementRequest) (*inventoryv1.ValidateStockMovementResponse, error) {
	return s.stockMovementValidationClient.ValidateStockMovement(ctx, request)
}

func (s *StockReservationApprovalServer) validateStockMovementAuditedStep(ctx context.Context, request *inventoryv1.ValidateStockMovementRequest) (*inventoryv1.ValidateStockMovementResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.inventory.v1.StockMovementValidationService/ValidateStockMovement")
	return s.stockMovementValidationClient.ValidateStockMovement(ctx, request)
}
