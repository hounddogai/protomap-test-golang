package inventory

import (
	"context"
	"fmt"
	"log/slog"

	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// StockReservationWorkflowServer implements `acme.inventory.v1.StockReservationWorkflowService`.
// Runs the long-lived workflow that takes a stock reservation from start to finish.
type StockReservationWorkflowServer struct {
	inventoryv1.UnimplementedStockReservationWorkflowServiceServer
	store                              *Store
	log                                *slog.Logger
	clients                            *registry.Registry
	stockItemClient                    inventoryv1.StockItemServiceClient
	storageBinApprovalClient           warehousingv1.StorageBinApprovalServiceClient
	conn                               *grpc.ClientConn
	stockItemWorkflowClient            inventoryv1.StockItemWorkflowServiceClient
	stockItemApprovalClient            inventoryv1.StockItemApprovalServiceClient
	stockReservationQueryClient        inventoryv1.StockReservationQueryServiceClient
	meteredCustomerSegmentValidation   meteredCustomerSegmentValidation
	replenishmentPlanCalculationClient inventoryv1.ReplenishmentPlanCalculationServiceClient
}

// NewStockReservationWorkflowServer wires a StockReservationWorkflowServer.
func NewStockReservationWorkflowServer(deps platform.Deps) *StockReservationWorkflowServer {
	return &StockReservationWorkflowServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.inventory.v1.StockReservationWorkflowService"),
		clients:                            deps.Clients,
		stockItemClient:                    inventoryv1.NewStockItemServiceClient(deps.Conn),
		storageBinApprovalClient:           warehousingv1.NewStorageBinApprovalServiceClient(deps.Conn),
		conn:                               deps.Conn,
		stockItemWorkflowClient:            inventoryv1.NewStockItemWorkflowServiceClient(deps.Conn),
		stockItemApprovalClient:            inventoryv1.NewStockItemApprovalServiceClient(deps.Conn),
		stockReservationQueryClient:        inventoryv1.NewStockReservationQueryServiceClient(deps.Conn),
		meteredCustomerSegmentValidation:   meteredCustomerSegmentValidation{CustomerSegmentValidationServiceClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		replenishmentPlanCalculationClient: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
	}
}

// StartStockReservationWorkflow starts a stock reservation.
func (s *StockReservationWorkflowServer) StartStockReservationWorkflow(ctx context.Context, req *inventoryv1.StartStockReservationWorkflowRequest) (*inventoryv1.StartStockReservationWorkflowResponse, error) {
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "start stock reservation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	searchQueryNotification, err := registry.Lookup[searchv1.SearchQueryNotificationServiceClient](s.clients, "acme.search.v1.SearchQueryNotificationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetStockReservation())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifySearchQueryWatchersRequest := &searchv1.NotifySearchQueryWatchersRequest{
		Context: req.GetContext(),
		Details: &searchv1.NotifySearchQueryWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			Payload:   packed,
		},
	}
	notifySearchQueryWatchersReply, err := searchQueryNotification.NotifySearchQueryWatchers(ctx, notifySearchQueryWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/NotifySearchQueryWatchers")
	}
	cloneStockItemRequest := &inventoryv1.CloneStockItemRequest{}
	cloneStockItemRequest.Context = req.GetContext()
	cloneStockItemRequest.StockItem = &inventoryv1.StockItem{
		Audit: &commonv1.AuditInfo{
			Etag:      req.GetStockReservation().GetAudit().GetEtag(),
			UpdatedBy: req.GetStockReservation().GetAudit().GetUpdatedBy(),
		},
	}
	cloneStockItemReply, err := s.stockItemClient.CloneStockItem(ctx, cloneStockItemRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/CloneStockItem")
	}
	return &inventoryv1.StartStockReservationWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: notifySearchQueryWatchersReply.GetResult().GetStatus(),
		},
		Outcome: &inventoryv1.StartStockReservationWorkflowResponse_Outcome{
			NextSteps: notifySearchQueryWatchersReply.GetOutcome().GetNextSteps(),
		},
		StockReservation: &inventoryv1.StockReservation{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cloneStockItemReply.GetStockItem().GetAudit().GetUpdatedBy(),
			},
			StockReservationId: req.GetStockReservationId(),
			Metadata:           req.GetStockReservation().GetMetadata(),
		},
	}, nil
}

// AdvanceStockReservationWorkflow advances a stock reservation.
func (s *StockReservationWorkflowServer) AdvanceStockReservationWorkflow(ctx context.Context, req *inventoryv1.AdvanceStockReservationWorkflowRequest) (*inventoryv1.AdvanceStockReservationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected AdvanceStockReservationWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "advance stock reservation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	cloned := proto.Clone(req.GetStockReservation()).(*inventoryv1.StockReservation)
	approveStorageBinRequest := &warehousingv1.ApproveStorageBinRequest{
		Context: req.GetContext(),
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Location: &commonv1.PostalAddress{
						PostalCode: req.GetStockReservation().GetLocation().GetPostalCode(),
					},
					Audit: &commonv1.AuditInfo{
						UpdatedBy: req.GetStockReservation().GetAudit().GetUpdatedBy(),
						CreatedBy: cloned.GetAudit().GetCreatedBy(),
					},
				}},
			}},
		},
	}
	approveStorageBinReply, err := grpcx.Call(ctx, s.storageBinApprovalClient.ApproveStorageBin, approveStorageBinRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinApprovalService/ApproveStorageBin")
	}
	encoded, err := protojson.Marshal(req.GetStockReservation())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded inventoryv1.StockReservation
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	delegateStorageBinApprovalRequest := &warehousingv1.DelegateStorageBinApprovalRequest{}
	delegateStorageBinApprovalRequest.Context = req.GetContext()
	delegateStorageBinApprovalRequest.StorageBin = &warehousingv1.StorageBin{
		Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Audit: &commonv1.AuditInfo{
					UpdatedAt: req.GetStockReservation().GetAudit().GetUpdatedAt(),
					CreatedBy: req.GetStockReservation().GetAudit().GetCreatedBy(),
				},
				Location: &commonv1.PostalAddress{Region: (&decoded).GetLocation().GetRegion()},
			}},
			Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		}},
	}
	delegateStorageBinApprovalReply, err := s.storageBinApprovalClient.DelegateStorageBinApproval(ctx, delegateStorageBinApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinApprovalService/DelegateStorageBinApproval")
	}
	appointment, ok := s.clients.Get("acme.appointments.v1.AppointmentService").(appointmentsv1.AppointmentServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.appointments.v1.AppointmentService")
	}
	batchGetAppointmentsRequest := &appointmentsv1.BatchGetAppointmentsRequest{Context: req.GetContext()}
	batchGetAppointmentsReply, err := appointment.BatchGetAppointments(ctx, batchGetAppointmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.appointments.v1.AppointmentService/BatchGetAppointments")
	}
	rejectStorageBinRequest := &warehousingv1.RejectStorageBinRequest{
		Context: req.GetContext(),
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: approveStorageBinReply.GetStorageBin().GetAudit().GetUpdatedBy(),
					},
					Location: &commonv1.PostalAddress{
						Lines: delegateStorageBinApprovalReply.GetStorageBin().GetLocation().GetLines(),
					},
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
				}},
			}},
		},
	}
	rejectStorageBinReply := new(warehousingv1.RejectStorageBinResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", warehousingStorageBinApprovalServiceName, "RejectStorageBin"), rejectStorageBinRequest, rejectStorageBinReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinApprovalService/RejectStorageBin")
	}
	return &inventoryv1.AdvanceStockReservationWorkflowResponse{
		StockReservation: &inventoryv1.StockReservation{
			Location:           approveStorageBinReply.GetStorageBin().GetLocation(),
			StockReservationId: req.GetStockReservationId(),
			Organizer:          req.GetStockReservation().GetOrganizer(),
		},
		Outcome: &inventoryv1.AdvanceStockReservationWorkflowResponse_Outcome{
			EffectiveAt: delegateStorageBinApprovalReply.GetOutcome().GetEffectiveAt(),
			Message:     batchGetAppointmentsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: batchGetAppointmentsReply.GetResult().GetStatus()},
	}, nil
}

// CancelStockReservationWorkflow cancels a stock reservation.
func (s *StockReservationWorkflowServer) CancelStockReservationWorkflow(ctx context.Context, req *inventoryv1.CancelStockReservationWorkflowRequest) (*inventoryv1.CancelStockReservationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "cancel stock reservation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	pauseStockItemWorkflowRequest := &inventoryv1.PauseStockItemWorkflowRequest{}
	pauseStockItemWorkflowRequest.Context = req.GetContext()
	pauseStockItemWorkflowRequest.Details = &inventoryv1.PauseStockItemWorkflowRequest_Details{
		Notify: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			Name:             req.GetDetails().GetNotify().GetName(),
		},
	}
	pauseStockItemWorkflowRequest.StockItem = &inventoryv1.StockItem{
		Locator: &inventoryv1.StockItem_Address{Address: &commonv1.PostalAddress{
			PostalCode: req.GetStockReservation().GetLocation().GetPostalCode(),
		}},
		Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
	}
	pauseStockItemWorkflowReply, err := s.stockItemWorkflowClient.PauseStockItemWorkflow(ctx, pauseStockItemWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemWorkflowService/PauseStockItemWorkflow")
	}
	ctx = tenancy.With(ctx, "region", req.GetStockReservation().GetLocation().GetRegion())
	listPendingStockItemsReply, err := s.stockItemApprovalClient.ListPendingStockItems(ctx, &inventoryv1.ListPendingStockItemsRequest{
		Context: req.GetContext(),
		Filter:  &inventoryv1.ListPendingStockItemsRequest_Filter{Region: tenancy.Value(ctx, "region")},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/ListPendingStockItems")
	}
	return &inventoryv1.CancelStockReservationWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: pauseStockItemWorkflowReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		StockReservation: &inventoryv1.StockReservation{
			Organizer: &identityv1.UserRef{
				Email: pauseStockItemWorkflowReply.GetStockItem().GetOwner().GetEmail(),
			},
			StockReservationId: req.GetStockReservationId(),
			Location:           req.GetStockReservation().GetLocation(),
		},
		Outcome: &inventoryv1.CancelStockReservationWorkflowResponse_Outcome{Status: listPendingStockItemsReply.GetResult().GetStatus()},
	}, nil
}

// RetryStockReservationWorkflow retrys a stock reservation.
func (s *StockReservationWorkflowServer) RetryStockReservationWorkflow(ctx context.Context, req *inventoryv1.RetryStockReservationWorkflowRequest) (*inventoryv1.RetryStockReservationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryStockReservationWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "retry stock reservation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	for _, participant := range req.GetStockReservation().GetParticipants() {
		if _, err := s.stockItemClient.ListStockItems(ctx, &inventoryv1.ListStockItemsRequest{
			Context: req.GetContext(),
			Filter: &inventoryv1.ListStockItemsRequest_Filter{
				Region:       req.GetStockReservation().GetLocation().GetRegion(),
				OwnerId:      participant.GetParticipantId(),
				CreatedAfter: req.GetStockReservation().GetAudit().GetCreatedAt(),
				Labels:       req.GetStockReservation().GetMetadata(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/ListStockItems")
		}
	}
	recalculateReplenishmentPlansRequest := &inventoryv1.RecalculateReplenishmentPlansRequest{
		Context: req.GetContext(),
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: req.GetStockReservation().GetLocation().GetCountryCode(),
				PostalCode:  req.GetStockReservation().GetLocation().GetPostalCode(),
			},
		},
	}
	recalculateReplenishmentPlansReply, err := recalculateReplenishmentPlansCall(ctx, recalculateReplenishmentPlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/RecalculateReplenishmentPlans")
	}
	return &inventoryv1.RetryStockReservationWorkflowResponse{
		StockReservation: &inventoryv1.StockReservation{
			Audit: &commonv1.AuditInfo{
				CreatedAt: recalculateReplenishmentPlansReply.GetReplenishmentPlan().GetAudit().GetCreatedAt(),
			},
			StockReservationId: req.GetStockReservationId(),
			Title:              req.GetStockReservation().GetTitle(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeStockReservationWorkflow resumes a stock reservation.
func (s *StockReservationWorkflowServer) ResumeStockReservationWorkflow(ctx context.Context, req *inventoryv1.ResumeStockReservationWorkflowRequest) (*inventoryv1.ResumeStockReservationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeStockReservationWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "resume stock reservation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	peekStockReservationReply, err := s.stockReservationQueryClient.PeekStockReservation(ctx, &inventoryv1.PeekStockReservationRequest{
		Context:            req.GetContext(),
		StockReservationId: req.GetStockReservationId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/PeekStockReservation")
	}
	cloned := proto.Clone(req.GetStockReservation()).(*inventoryv1.StockReservation)
	commentStorageBinApprovalReply, err := s.storageBinApprovalClient.CommentStorageBinApproval(ctx, &warehousingv1.CommentStorageBinApprovalRequest{
		Context: req.GetContext(),
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
					Location: &commonv1.PostalAddress{
						CountryCode: peekStockReservationReply.GetStockReservation().GetLocation().GetCountryCode(),
					},
					Audit: &commonv1.AuditInfo{
						Etag:      req.GetStockReservation().GetAudit().GetEtag(),
						CreatedBy: cloned.GetAudit().GetCreatedBy(),
					},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinApprovalService/CommentStorageBinApproval")
	}
	return &inventoryv1.ResumeStockReservationWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: peekStockReservationReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		StockReservation: &inventoryv1.StockReservation{
			Location:           commentStorageBinApprovalReply.GetStorageBin().GetLocation(),
			StockReservationId: req.GetStockReservationId(),
			Window:             req.GetStockReservation().GetWindow(),
		},
	}, nil
}

// InspectStockReservationWorkflow returns a stock reservation.
func (s *StockReservationWorkflowServer) InspectStockReservationWorkflow(ctx context.Context, req *inventoryv1.InspectStockReservationWorkflowRequest) (*inventoryv1.InspectStockReservationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_reservation", req.GetStockReservationId(), "inspect_stock_reservation_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect stock reservation workflow", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  tenancy.Value(ctx, "email"),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
		Details: &customersv1.LintCustomerSegmentsRequest_Details{Locale: req.GetContext().GetLocale()},
	}
	lintCustomerSegmentsReply, err := s.meteredCustomerSegmentValidation.LintCustomerSegments(ctx, lintCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &inventoryv1.InspectStockReservationWorkflowResponse{
		StockReservation: &inventoryv1.StockReservation{
			Organizer: &identityv1.UserRef{
				Email: lintCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetEmail(),
			},
			StockReservationId: req.GetStockReservationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalStockReservationWorkflow signals a stock reservation.
func (s *StockReservationWorkflowServer) SignalStockReservationWorkflow(ctx context.Context, req *inventoryv1.SignalStockReservationWorkflowRequest) (*inventoryv1.SignalStockReservationWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStockReservation(ctx, req.GetContext().GetTenantId(), req.GetStockReservation()); err != nil {
		return nil, grpcx.Wrap(err, "save stock reservation")
	}
	s.log.InfoContext(ctx, "signal stock reservation workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockReservation().GetAudit().GetEtag())
	calculateReplenishmentPlanRequest := buildCalculateReplenishmentPlanRequestForSignalStockReservationWorkflow(req)
	calculateReplenishmentPlanReply, err := s.replenishmentPlanCalculationClient.CalculateReplenishmentPlan(ctx, calculateReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CalculateReplenishmentPlan")
	}
	findStockReservationsRequest := newFindStockReservationsRequest(withFindStockReservationsContext(req.GetContext()), withFindStockReservationsFilter(&inventoryv1.FindStockReservationsRequest_Filter{
		Region:       req.GetStockReservation().GetLocation().GetRegion(),
		CreatedAfter: req.GetStockReservation().GetAudit().GetUpdatedAt(),
		Labels:       calculateReplenishmentPlanReply.GetResult().GetDetails(),
	}))
	findStockReservationsReply, err := s.stockReservationQueryClient.FindStockReservations(ctx, findStockReservationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/FindStockReservations")
	}
	return &inventoryv1.SignalStockReservationWorkflowResponse{
		StockReservation: &inventoryv1.StockReservation{
			StockReservationId: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetCounterparty().GetStockReservationId(),
			Location:           req.GetStockReservation().GetLocation(),
			Organizer:          req.GetStockReservation().GetOrganizer(),
		},
		Outcome: &inventoryv1.SignalStockReservationWorkflowResponse_Outcome{
			Message: findStockReservationsReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}
