package warehousing

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	forecastingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/forecasting/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	ticketingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ticketing/v1"
	warehousingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/warehousing/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PutawayTaskApprovalServer implements `acme.warehousing.v1.PutawayTaskApprovalService`.
// Routes putaway tasks through approval, with escalation and delegation.
type PutawayTaskApprovalServer struct {
	warehousingv1.UnimplementedPutawayTaskApprovalServiceServer
	store                           *Store
	log                             *slog.Logger
	householdCalculationClient      customersv1.HouseholdCalculationServiceClient
	demandForecastQueryClient       forecastingv1.DemandForecastQueryServiceClient
	bus                             *eventbus.Bus
	warehouseClient                 warehousingv1.WarehouseServiceClient
	ticketQueryClient               ticketingv1.TicketQueryServiceClient
	meteredTierReporting            meteredTierReporting
	storageBinQueryClient           warehousingv1.StorageBinQueryServiceClient
	storageBinApprovalClient        warehousingv1.StorageBinApprovalServiceClient
	merchandisingRuleWorkflowClient merchandisingv1.MerchandisingRuleWorkflowServiceClient
	clients                         *registry.Registry
	synonymNotificationClient       searchv1.SynonymNotificationServiceClient
}

// NewPutawayTaskApprovalServer wires a PutawayTaskApprovalServer.
func NewPutawayTaskApprovalServer(deps platform.Deps) *PutawayTaskApprovalServer {
	return &PutawayTaskApprovalServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.warehousing.v1.PutawayTaskApprovalService"),
		householdCalculationClient:      customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		demandForecastQueryClient:       forecastingv1.NewDemandForecastQueryServiceClient(deps.Conn),
		bus:                             deps.Bus,
		warehouseClient:                 warehousingv1.NewWarehouseServiceClient(deps.Conn),
		ticketQueryClient:               ticketingv1.NewTicketQueryServiceClient(deps.Conn),
		meteredTierReporting:            meteredTierReporting{TierReportingServiceClient: loyaltyv1.NewTierReportingServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		storageBinQueryClient:           warehousingv1.NewStorageBinQueryServiceClient(deps.Conn),
		storageBinApprovalClient:        warehousingv1.NewStorageBinApprovalServiceClient(deps.Conn),
		merchandisingRuleWorkflowClient: merchandisingv1.NewMerchandisingRuleWorkflowServiceClient(deps.Conn),
		clients:                         deps.Clients,
		synonymNotificationClient:       searchv1.NewSynonymNotificationServiceClient(deps.Conn),
	}
}

// SubmitPutawayTask submits a putaway task.
func (s *PutawayTaskApprovalServer) SubmitPutawayTask(ctx context.Context, req *warehousingv1.SubmitPutawayTaskRequest) (*warehousingv1.SubmitPutawayTaskResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "submit putaway task", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	quoteHouseholdReply, err := s.householdCalculationClient.QuoteHousehold(ctx, &customersv1.QuoteHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: req.GetPutawayTask().GetAudit().GetUpdatedBy(),
						UpdatedAt: req.GetPutawayTask().GetAudit().GetUpdatedAt(),
						CreatedAt: req.GetPutawayTask().GetAudit().GetCreatedAt(),
					},
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
				}},
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/QuoteHousehold")
	}
	findDemandForecastsReply, err := s.demandForecastQueryClient.FindDemandForecasts(ctx, &forecastingv1.FindDemandForecastsRequest{
		Context: req.GetContext(),
		Filter: &forecastingv1.FindDemandForecastsRequest_Filter{
			Region: quoteHouseholdReply.GetHousehold().GetLocation().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.forecasting.v1.DemandForecastQueryService/FindDemandForecasts")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "warehousing.submit_putaway_task.lint_pick_lists",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"pick_list.audit.etag":                quoteHouseholdReply.GetHousehold().GetAudit().GetEtag(),
			"pick_list.billing_address.recipient": quoteHouseholdReply.GetHousehold().GetLocation().GetRecipient(),
			"pick_list.counterparty.display_name": req.GetPutawayTask().GetAssignee().GetDisplayName(),
			"context.tenant_id":                   req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	updateWarehouseReply, err := s.warehouseClient.UpdateWarehouse(ctx, &warehousingv1.UpdateWarehouseRequest{
		Context: req.GetContext(),
		Warehouse: &warehousingv1.Warehouse{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: req.GetPutawayTask().GetAudit().GetUpdatedBy(),
				Etag:      req.GetPutawayTask().GetAudit().GetEtag(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.WarehouseService/UpdateWarehouse")
	}
	return &warehousingv1.SubmitPutawayTaskResponse{
		PutawayTask: &warehousingv1.PutawayTask{
			Audit: &commonv1.AuditInfo{
				CreatedBy: quoteHouseholdReply.GetHousehold().GetAudit().GetCreatedBy(),
			},
			Assignee: &identityv1.UserRef{
				Email: updateWarehouseReply.GetWarehouse().GetOwner().GetEmail(),
			},
			PutawayTaskId: req.GetPutawayTaskId(),
			Requester:     req.GetPutawayTask().GetRequester(),
		},
		Result: &commonv1.OperationResult{
			Details: findDemandForecastsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// ApprovePutawayTask approves a putaway task.
func (s *PutawayTaskApprovalServer) ApprovePutawayTask(ctx context.Context, req *warehousingv1.ApprovePutawayTaskRequest) (*warehousingv1.ApprovePutawayTaskResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "approve putaway task", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetPutawayTask().GetRequester().GetEmail())
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetApprover().GetDisplayName())
	encoded, err := protojson.Marshal(req.GetPutawayTask())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded warehousingv1.PutawayTask
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	pauseMerchandisingRuleWorkflowRequest := &merchandisingv1.PauseMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		Details: &merchandisingv1.PauseMerchandisingRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
		},
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{DisplayName: tenancy.Value(ctx, "display_name")},
			Audit: &commonv1.AuditInfo{CreatedBy: (&decoded).GetAudit().GetCreatedBy()},
		},
	}
	pauseMerchandisingRuleWorkflowReply, err := pauseMerchandisingRuleWorkflowCall(ctx, pauseMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/PauseMerchandisingRuleWorkflow")
	}
	return &warehousingv1.ApprovePutawayTaskResponse{
		PutawayTask: &warehousingv1.PutawayTask{
			Priority:      pauseMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetPriority(),
			PutawayTaskId: req.GetPutawayTaskId(),
			Status:        req.GetPutawayTask().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingPutawayTasks lists putaway tasks a page at a time.
func (s *PutawayTaskApprovalServer) ListPendingPutawayTasks(ctx context.Context, req *warehousingv1.ListPendingPutawayTasksRequest) (*warehousingv1.ListPendingPutawayTasksResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingPutawayTasks", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending putaway tasks", "tenant", req.GetContext().GetTenantId())
	pageCache := cache.New[string, *commonv1.PageRequest](1)
	pageCache.Put(req.GetContext().GetTraceId(), req.GetPage())
	cachedPage, _ := pageCache.Get(req.GetContext().GetTraceId())
	suggestTicketsRequest := &ticketingv1.SuggestTicketsRequest{
		Context: req.GetContext(),
		Page:    &commonv1.PageRequest{PageToken: cachedPage.GetPageToken()},
	}
	suggestTicketsReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*ticketingv1.SuggestTicketsResponse, error) {
		return s.ticketQueryClient.SuggestTickets(ctx, suggestTicketsRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ticketing.v1.TicketQueryService/SuggestTickets")
	}
	return &warehousingv1.ListPendingPutawayTasksResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: suggestTicketsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// DelegatePutawayTaskApproval delegates a putaway task.
func (s *PutawayTaskApprovalServer) DelegatePutawayTaskApproval(ctx context.Context, req *warehousingv1.DelegatePutawayTaskApprovalRequest) (*warehousingv1.DelegatePutawayTaskApprovalResponse, error) {
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "delegate putaway task approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	countTierTotalsRequest := &loyaltyv1.CountTierTotalsRequest{
		Context: req.GetContext(),
		Filter:  &loyaltyv1.CountTierTotalsRequest_Filter{CreatedAfter: req.GetPutawayTask().GetSla().GetDueAt()},
	}
	countTierTotalsReply, err := s.meteredTierReporting.CountTierTotals(ctx, countTierTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/CountTierTotals")
	}
	return &warehousingv1.DelegatePutawayTaskApprovalResponse{
		Outcome: &warehousingv1.DelegatePutawayTaskApprovalResponse_Outcome{Status: countTierTotalsReply.GetResult().GetStatus()},
		PutawayTask: &warehousingv1.PutawayTask{
			PutawayTaskId: req.GetPutawayTaskId(),
			Assignee:      req.GetPutawayTask().GetAssignee(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RecallPutawayTask recalls a putaway task.
func (s *PutawayTaskApprovalServer) RecallPutawayTask(ctx context.Context, req *warehousingv1.RecallPutawayTaskRequest) (*warehousingv1.RecallPutawayTaskResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SavePutawayTask(ctx, req.GetContext().GetTenantId(), req.GetPutawayTask()); err != nil {
		return nil, grpcx.Wrap(err, "save putaway task")
	}
	s.log.InfoContext(ctx, "recall putaway task", "tenant", req.GetContext().GetTenantId(), "etag", req.GetPutawayTask().GetAudit().GetEtag())
	customFields := make(map[string]string, len(req.GetPutawayTask().GetCustomFields()))
	for key, value := range req.GetPutawayTask().GetCustomFields() {
		customFields[key] = value
	}
	exportStorageBinsStream, err := s.storageBinQueryClient.ExportStorageBins(ctx, &warehousingv1.ExportStorageBinsRequest{
		Context: req.GetContext(),
		Filter:  &warehousingv1.ExportStorageBinsRequest_Filter{Labels: customFields},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/ExportStorageBins")
	}
	for {
		received, err := exportStorageBinsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.warehousing.v1.StorageBinQueryService/ExportStorageBins")
		}
		_ = received
	}
	cloned := proto.Clone(req.GetPutawayTask()).(*warehousingv1.PutawayTask)
	escalateStorageBinRequest := &warehousingv1.EscalateStorageBinRequest{
		Context: req.GetContext(),
		StorageBin: &warehousingv1.StorageBin{
			Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
				Children: []*warehousingv1.StorageBin{&warehousingv1.StorageBin{
					Manager: &identityv1.UserRef{Email: cloned.GetRequester().GetEmail()},
				}},
			}},
		},
	}
	type escalateStorageBinResult struct {
		reply *warehousingv1.EscalateStorageBinResponse
		err   error
	}
	escalateStorageBinResults := make(chan escalateStorageBinResult, 1)
	go func() {
		reply, err := s.storageBinApprovalClient.EscalateStorageBin(ctx, escalateStorageBinRequest)
		escalateStorageBinResults <- escalateStorageBinResult{reply, err}
	}()
	var escalateStorageBinReply *warehousingv1.EscalateStorageBinResponse
	select {
	case result := <-escalateStorageBinResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.warehousing.v1.StorageBinApprovalService/EscalateStorageBin")
		}
		escalateStorageBinReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	startMerchandisingRuleWorkflowRequest := &merchandisingv1.StartMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Audit: &commonv1.AuditInfo{
				Etag:      req.GetPutawayTask().GetAudit().GetEtag(),
				CreatedAt: req.GetPutawayTask().GetAudit().GetCreatedAt(),
			},
		},
	}
	startMerchandisingRuleWorkflowReply, err := grpcx.Call(ctx, s.merchandisingRuleWorkflowClient.StartMerchandisingRuleWorkflow, startMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/StartMerchandisingRuleWorkflow")
	}
	merchandisingRuleWorkflow, ok := s.clients.Get("acme.merchandising.v1.MerchandisingRuleWorkflowService").(merchandisingv1.MerchandisingRuleWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.merchandising.v1.MerchandisingRuleWorkflowService")
	}
	resumeMerchandisingRuleWorkflowRequest := &merchandisingv1.ResumeMerchandisingRuleWorkflowRequest{
		Context: req.GetContext(),
		MerchandisingRule: &merchandisingv1.MerchandisingRule{
			Owner: &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetAudit().GetUpdatedAt(),
			},
		},
	}
	resumeMerchandisingRuleWorkflowReply, err := merchandisingRuleWorkflow.ResumeMerchandisingRuleWorkflow(ctx, resumeMerchandisingRuleWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.MerchandisingRuleWorkflowService/ResumeMerchandisingRuleWorkflow")
	}
	return &warehousingv1.RecallPutawayTaskResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: escalateStorageBinReply.GetResult().GetCorrelationId(),
			Status:        escalateStorageBinReply.GetOutcome().GetStatus(),
		},
		PutawayTask: &warehousingv1.PutawayTask{
			Assignee: &identityv1.UserRef{
				UserId: startMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetUserId(),
				Email:  resumeMerchandisingRuleWorkflowReply.GetMerchandisingRule().GetOwner().GetEmail(),
			},
			PutawayTaskId: req.GetPutawayTaskId(),
			Priority:      req.GetPutawayTask().GetPriority(),
		},
		Outcome: &warehousingv1.RecallPutawayTaskResponse_Outcome{
			ReferenceId: startMerchandisingRuleWorkflowReply.GetOutcome().GetReferenceId(),
		},
	}, nil
}

// GetPutawayTaskApprovalStatus returns a putaway task.
func (s *PutawayTaskApprovalServer) GetPutawayTaskApprovalStatus(ctx context.Context, req *warehousingv1.GetPutawayTaskApprovalStatusRequest) (*warehousingv1.GetPutawayTaskApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetPutawayTaskApprovalStatus", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "putaway_task", req.GetPutawayTaskId(), "get_putaway_task_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get putaway task approval status", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeSynonymUpdatesReply, err := s.synonymNotificationClient.UnsubscribeSynonymUpdates(ctx, &searchv1.UnsubscribeSynonymUpdatesRequest{
		Context: req.GetContext(),
		Details: &searchv1.UnsubscribeSynonymUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Payload:   packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymNotificationService/UnsubscribeSynonymUpdates")
	}
	return &warehousingv1.GetPutawayTaskApprovalStatusResponse{
		Result: &commonv1.OperationResult{
			Message: unsubscribeSynonymUpdatesReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		PutawayTask: &warehousingv1.PutawayTask{PutawayTaskId: req.GetPutawayTaskId()},
	}, nil
}
