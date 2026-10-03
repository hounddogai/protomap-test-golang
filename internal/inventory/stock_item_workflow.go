package inventory

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// StockItemWorkflowServer implements `acme.inventory.v1.StockItemWorkflowService`.
// Runs the long-lived workflow that takes a stock item from start to finish.
type StockItemWorkflowServer struct {
	inventoryv1.UnimplementedStockItemWorkflowServiceServer
	store                                      *Store
	log                                        *slog.Logger
	clients                                    *registry.Registry
	replenishmentPlanCalculationClient         inventoryv1.ReplenishmentPlanCalculationServiceClient
	replenishmentPlanCalculationClientFallback inventoryv1.ReplenishmentPlanCalculationServiceClient
	stockReservationQueryClient                inventoryv1.StockReservationQueryServiceClient
	conn                                       *grpc.ClientConn
	subscriptionPlanWorkflowOnce               sync.Once
	subscriptionPlanWorkflowCached             subscriptionsv1.SubscriptionPlanWorkflowServiceClient
	bus                                        *eventbus.Bus
	consentRecordCalculationClient             customersv1.ConsentRecordCalculationServiceClient
	stockItemApprovalClient                    inventoryv1.StockItemApprovalServiceClient
	tierReportingClient                        loyaltyv1.TierReportingServiceClient
	consentRecordValidationClient              customersv1.ConsentRecordValidationServiceClient
	stockItemClient                            inventoryv1.StockItemServiceClient
}

// NewStockItemWorkflowServer wires a StockItemWorkflowServer.
func NewStockItemWorkflowServer(deps platform.Deps) *StockItemWorkflowServer {
	return &StockItemWorkflowServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.inventory.v1.StockItemWorkflowService"),
		clients:                            deps.Clients,
		replenishmentPlanCalculationClient: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		replenishmentPlanCalculationClientFallback: inventoryv1.NewReplenishmentPlanCalculationServiceClient(deps.Conn),
		stockReservationQueryClient:                inventoryv1.NewStockReservationQueryServiceClient(deps.Conn),
		conn:                                       deps.Conn,
		bus:                                        deps.Bus,
		consentRecordCalculationClient:             customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		stockItemApprovalClient:                    inventoryv1.NewStockItemApprovalServiceClient(deps.Conn),
		tierReportingClient:                        loyaltyv1.NewTierReportingServiceClient(deps.Conn),
		consentRecordValidationClient:              customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		stockItemClient:                            inventoryv1.NewStockItemServiceClient(deps.Conn),
	}
}

// StartStockItemWorkflow starts a stock item.
func (s *StockItemWorkflowServer) StartStockItemWorkflow(ctx context.Context, req *inventoryv1.StartStockItemWorkflowRequest) (*inventoryv1.StartStockItemWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "start stock item workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	stockReservationQuery, ok := s.clients.Get("acme.inventory.v1.StockReservationQueryService").(inventoryv1.StockReservationQueryServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.inventory.v1.StockReservationQueryService")
	}
	findStockReservationsRequest := &inventoryv1.FindStockReservationsRequest{
		Context: req.GetContext(),
		Filter: &inventoryv1.FindStockReservationsRequest_Filter{
			Region:       req.GetStockItem().GetAddress().GetRegion(),
			CreatedAfter: req.GetStockItem().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetStockItem().GetComponents()["primary"].GetComponentId(),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
	findStockReservationsReply, err := stockReservationQuery.FindStockReservations(ctx, findStockReservationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/FindStockReservations")
	}
	routes := map[string]func(context.Context, *inventoryv1.CalculateReplenishmentPlanRequest, ...grpc.CallOption) (*inventoryv1.CalculateReplenishmentPlanResponse, error){
		"primary":  s.replenishmentPlanCalculationClient.CalculateReplenishmentPlan,
		"fallback": s.replenishmentPlanCalculationClientFallback.CalculateReplenishmentPlan,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	var countryCode string
	switch choice := req.GetStockItem().GetLocator().(type) {
	case *inventoryv1.StockItem_Address:
		countryCode = choice.Address.GetCountryCode()
	case *inventoryv1.StockItem_FacilityCode:
		countryCode = choice.FacilityCode
	}
	calculateReplenishmentPlanRequest := &inventoryv1.CalculateReplenishmentPlanRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.CalculateReplenishmentPlanRequest_Details{
			Destination: &commonv1.PostalAddress{CountryCode: countryCode},
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			BillingAddress: &commonv1.PostalAddress{PostalCode: req.GetStockItem().GetAddress().GetPostalCode()},
		},
	}
	calculateReplenishmentPlanReply, err := route(ctx, calculateReplenishmentPlanRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanCalculationService/CalculateReplenishmentPlan")
	}
	return &inventoryv1.StartStockItemWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: findStockReservationsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &inventoryv1.StartStockItemWorkflowResponse_Outcome{
			Message: calculateReplenishmentPlanReply.GetResult().GetMessage(),
		},
		StockItem: &inventoryv1.StockItem{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: calculateReplenishmentPlanReply.GetReplenishmentPlan().GetAudit().GetUpdatedBy(),
			},
			StockItemId: req.GetStockItemId(),
			Specs:       req.GetStockItem().GetSpecs(),
		},
	}, nil
}

// AdvanceStockItemWorkflow advances a stock item.
func (s *StockItemWorkflowServer) AdvanceStockItemWorkflow(ctx context.Context, req *inventoryv1.AdvanceStockItemWorkflowRequest) (*inventoryv1.AdvanceStockItemWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "advance stock item workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	summarizeStockReservationsRequest := &inventoryv1.SummarizeStockReservationsRequest{
		Context:     req.GetContext(),
		Reason:      req.GetReason(),
		Annotations: annotations,
		Details:     &inventoryv1.SummarizeStockReservationsRequest_Details{Filters: req.GetStockItem().GetSpecs()},
	}
	summarizeStockReservationsReply, err := grpcx.Call(ctx, s.stockReservationQueryClient.SummarizeStockReservations, summarizeStockReservationsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationQueryService/SummarizeStockReservations")
	}
	return &inventoryv1.AdvanceStockItemWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: summarizeStockReservationsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  summarizeStockReservationsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		StockItem: &inventoryv1.StockItem{
			StockItemId: req.GetStockItemId(),
			Capacity:    req.GetStockItem().GetCapacity(),
		},
	}, nil
}

// CancelStockItemWorkflow cancels a stock item.
func (s *StockItemWorkflowServer) CancelStockItemWorkflow(ctx context.Context, req *inventoryv1.CancelStockItemWorkflowRequest) (*inventoryv1.CancelStockItemWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "cancel stock item workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	advanceSubscriptionPlanWorkflowRequest := &subscriptionsv1.AdvanceSubscriptionPlanWorkflowRequest{
		Context: req.GetContext(),
		SubscriptionPlan: &subscriptionsv1.SubscriptionPlan{
			Owner: &identityv1.UserRef{UserId: req.GetStockItem().GetOwner().GetUserId()},
		},
	}
	advanceSubscriptionPlanWorkflowReply, err := s.subscriptionPlanWorkflowLazy().AdvanceSubscriptionPlanWorkflow(ctx, advanceSubscriptionPlanWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionPlanWorkflowService/AdvanceSubscriptionPlanWorkflow")
	}
	return &inventoryv1.CancelStockItemWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: advanceSubscriptionPlanWorkflowReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		StockItem: &inventoryv1.StockItem{
			Owner:       advanceSubscriptionPlanWorkflowReply.GetSubscriptionPlan().GetOwner(),
			StockItemId: req.GetStockItemId(),
			Name:        req.GetStockItem().GetName(),
		},
	}, nil
}

// PauseStockItemWorkflow pauses a stock item.
func (s *StockItemWorkflowServer) PauseStockItemWorkflow(ctx context.Context, req *inventoryv1.PauseStockItemWorkflowRequest) (*inventoryv1.PauseStockItemWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected PauseStockItemWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "pause stock item workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "inventory.pause_stock_item_workflow.advance_subscription_workflow",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"subscription.holder.email": req.GetStockItem().GetOwner().GetEmail(),
			"subscription.audit.etag":   req.GetStockItem().GetAudit().GetEtag(),
			"context.tenant_id":         req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &inventoryv1.PauseStockItemWorkflowResponse{
		StockItem: &inventoryv1.StockItem{
			StockItemId: req.GetStockItemId(),
			Name:        req.GetStockItem().GetName(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ResumeStockItemWorkflow resumes a stock item.
func (s *StockItemWorkflowServer) ResumeStockItemWorkflow(ctx context.Context, req *inventoryv1.ResumeStockItemWorkflowRequest) (*inventoryv1.ResumeStockItemWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeStockItemWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "resume stock item workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	quoteConsentRecordRequest := &customersv1.QuoteConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{DisplayName: req.GetStockItem().GetOwner().GetDisplayName()},
		},
	}
	type quoteConsentRecordResult struct {
		reply *customersv1.QuoteConsentRecordResponse
		err   error
	}
	quoteConsentRecordResults := make(chan quoteConsentRecordResult, 1)
	go func() {
		reply, err := s.consentRecordCalculationClient.QuoteConsentRecord(ctx, quoteConsentRecordRequest)
		quoteConsentRecordResults <- quoteConsentRecordResult{reply, err}
	}()
	var quoteConsentRecordReply *customersv1.QuoteConsentRecordResponse
	select {
	case result := <-quoteConsentRecordResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordCalculationService/QuoteConsentRecord")
		}
		quoteConsentRecordReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	step := &stockItemWorkflowServerResumeStockItemWorkflowDelegateStockItemApprovalStep{request: &inventoryv1.DelegateStockItemApprovalRequest{
		Context: req.GetContext(),
		StockItem: &inventoryv1.StockItem{
			Components: map[string]*inventoryv1.StockItemComponent{"primary": &inventoryv1.StockItemComponent{
				ComponentId: req.GetStockItem().GetComponents()["primary"].GetComponentId(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: req.GetStockItem().GetAudit().GetCreatedAt(),
				UpdatedAt: req.GetStockItem().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &inventoryv1.DelegateStockItemApprovalRequest_Details{
			Approver: &identityv1.UserRef{UserId: req.GetStockItem().GetOwner().GetUserId()},
		},
	}}
	if err := s.runResumeStockItemWorkflowDelegateStockItemApproval(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/DelegateStockItemApproval")
	}
	delegateStockItemApprovalReply := step.reply
	ctx = tenancy.With(ctx, "phone", req.GetDetails().GetNotify().GetPhone())
	packed, err := anypb.New(req.GetStockItem())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareTierReportRequest := &loyaltyv1.ShareTierReportRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ShareTierReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
			Payload:   packed,
		},
	}
	shareTierReportReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*loyaltyv1.ShareTierReportResponse, error) {
		return s.tierReportingClient.ShareTierReport(ctx, shareTierReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.TierReportingService/ShareTierReport")
	}
	return &inventoryv1.ResumeStockItemWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: quoteConsentRecordReply.GetResult().GetReferenceIds(),
			Details:      delegateStockItemApprovalReply.GetResult().GetDetails(),
			Status:       "ok",
		},
		StockItem: &inventoryv1.StockItem{
			Owner: &identityv1.UserRef{
				UserId: delegateStockItemApprovalReply.GetStockItem().GetOwner().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedBy: shareTierReportReply.GetTier().GetAudit().GetCreatedBy(),
			},
			StockItemId: req.GetStockItemId(),
			Capacity:    req.GetStockItem().GetCapacity(),
		},
	}, nil
}

// InspectStockItemWorkflow returns a stock item.
func (s *StockItemWorkflowServer) InspectStockItemWorkflow(ctx context.Context, req *inventoryv1.InspectStockItemWorkflowRequest) (*inventoryv1.InspectStockItemWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_item", req.GetStockItemId(), "inspect_stock_item_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect stock item workflow", "tenant", req.GetContext().GetTenantId())
	scoreConsentRecordRequest := &customersv1.ScoreConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Measurements: []*customersv1.ConsentRecordMeasurement{&customersv1.ConsentRecordMeasurement{ObservedAt: req.GetContext().GetDeadline()}},
		},
	}
	scoreConsentRecordReply, err := customersv1.ConsentRecordValidationServiceClient.ScoreConsentRecord(s.consentRecordValidationClient, ctx, scoreConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ScoreConsentRecord")
	}
	return &inventoryv1.InspectStockItemWorkflowResponse{
		StockItem: &inventoryv1.StockItem{
			Owner: &identityv1.UserRef{
				DisplayName: scoreConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			StockItemId: req.GetStockItemId(),
		},
		Result: &commonv1.OperationResult{Status: scoreConsentRecordReply.GetResult().GetStatus()},
	}, nil
}

// ListStockItemWorkflows lists stock items a page at a time.
func (s *StockItemWorkflowServer) ListStockItemWorkflows(ctx context.Context, req *inventoryv1.ListStockItemWorkflowsRequest) (*inventoryv1.ListStockItemWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListStockItemWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list stock item workflows", "tenant", req.GetContext().GetTenantId())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "inventory.list_stock_item_workflows.find_customers",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"page.page_token":   req.GetPage().GetPageToken(),
			"filter.owner_id":   req.GetFilter().GetOwnerId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &inventoryv1.ListStockItemWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalStockItemWorkflow signals a stock item.
func (s *StockItemWorkflowServer) SignalStockItemWorkflow(ctx context.Context, req *inventoryv1.SignalStockItemWorkflowRequest) (*inventoryv1.SignalStockItemWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockItem(ctx, req.GetContext().GetTenantId(), req.GetStockItem()); err != nil {
		return nil, grpcx.Wrap(err, "save stock item")
	}
	s.log.InfoContext(ctx, "signal stock item workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetStockItem().GetAudit().GetEtag())
	createStockItemRequest := &inventoryv1.CreateStockItemRequest{
		Context: req.GetContext(),
		StockItem: &inventoryv1.StockItem{
			Components: map[string]*inventoryv1.StockItemComponent{"primary": &inventoryv1.StockItemComponent{
				Description: req.GetStockItem().GetComponents()["primary"].GetDescription(),
			}},
			Dimensions: &inventoryv1.StockItemDimensions{
				Unit:   req.GetStockItem().GetDimensions().GetUnit(),
				Length: req.GetStockItem().GetDimensions().GetLength(),
			},
		},
	}
	createStockItemReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*inventoryv1.CreateStockItemResponse, error) {
		return s.stockItemClient.CreateStockItem(ctx, createStockItemRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemService/CreateStockItem")
	}
	var holder struct{ PreferredChannel string }
	if record := req.GetDetails().GetNotify(); record != nil {
		if err := mapper.CopyFields(&holder, record, "PreferredChannel"); err != nil {
			s.log.WarnContext(ctx, "copy preferred_channel", "error", err)
		}
	}
	delegateStockReservationApprovalRequest := &inventoryv1.DelegateStockReservationApprovalRequest{
		Context: req.GetContext(),
		StockReservation: &inventoryv1.StockReservation{
			Participants: []*inventoryv1.StockReservationParticipant{&inventoryv1.StockReservationParticipant{
				Contact: &commonv1.ContactInfo{
					PreferredChannel: holder.PreferredChannel,
					Email:            req.GetStockItem().GetOwner().GetEmail(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				Etag: createStockItemReply.GetStockItem().GetAudit().GetEtag(),
			},
		},
		Details: &inventoryv1.DelegateStockReservationApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				UserId: createStockItemReply.GetStockItem().GetOwner().GetUserId(),
			},
		},
	}
	delegateStockReservationApprovalReply, err := grpcx.NewClient(s.conn, inventoryv1.NewStockReservationApprovalServiceClient).DelegateStockReservationApproval(ctx, delegateStockReservationApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockReservationApprovalService/DelegateStockReservationApproval")
	}
	return &inventoryv1.SignalStockItemWorkflowResponse{
		StockItem: &inventoryv1.StockItem{
			Owner:       createStockItemReply.GetStockItem().GetOwner(),
			StockItemId: req.GetStockItemId(),
			Specs:       req.GetStockItem().GetSpecs(),
		},
		Result: &commonv1.OperationResult{
			Message: createStockItemReply.GetResult().GetMessage(),
			Details: delegateStockReservationApprovalReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// subscriptionPlanWorkflowLazy dials acme.subscriptions.v1.SubscriptionPlanWorkflowService the first time a handler
// needs it.
func (s *StockItemWorkflowServer) subscriptionPlanWorkflowLazy() subscriptionsv1.SubscriptionPlanWorkflowServiceClient {
	s.subscriptionPlanWorkflowOnce.Do(func() {
		s.subscriptionPlanWorkflowCached = subscriptionsv1.NewSubscriptionPlanWorkflowServiceClient(s.conn)
	})
	return s.subscriptionPlanWorkflowCached
}

// stockItemWorkflowServerResumeStockItemWorkflowDelegateStockItemApprovalStep carries the request of one step of
// ResumeStockItemWorkflow and, once it ran, its reply.
type stockItemWorkflowServerResumeStockItemWorkflowDelegateStockItemApprovalStep struct {
	request *inventoryv1.DelegateStockItemApprovalRequest
	reply   *inventoryv1.DelegateStockItemApprovalResponse
}

func (s *StockItemWorkflowServer) runResumeStockItemWorkflowDelegateStockItemApproval(ctx context.Context, step *stockItemWorkflowServerResumeStockItemWorkflowDelegateStockItemApprovalStep) error {
	reply, err := s.stockItemApprovalClient.DelegateStockItemApproval(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
