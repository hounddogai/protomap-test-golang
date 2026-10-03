package inventory

import (
	"context"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// StockMovementWorkflowServer implements `acme.inventory.v1.StockMovementWorkflowService`.
// Runs the long-lived workflow that takes a stock movement from start to finish.
type StockMovementWorkflowServer struct {
	inventoryv1.UnimplementedStockMovementWorkflowServiceServer
	store                            *Store
	log                              *slog.Logger
	returnRequestWorkflowClient      returnsv1.ReturnRequestWorkflowServiceClient
	clients                          *registry.Registry
	replenishmentPlanApprovalClient  inventoryv1.ReplenishmentPlanApprovalServiceClient
	sellerListingApprovalClient      sellersv1.SellerListingApprovalServiceClient
	stockMovementApprovalClient      inventoryv1.StockMovementApprovalServiceClient
	placementWorkflowClient          merchandisingv1.PlacementWorkflowServiceClient
	tenantNotificationClient         identityv1.TenantNotificationServiceClient
	tenantNotificationClientFallback identityv1.TenantNotificationServiceClient
	stockItemWorkflowClient          inventoryv1.StockItemWorkflowServiceClient
	stockItemApprovalClient          inventoryv1.StockItemApprovalServiceClient
	roleNotificationClient           identityv1.RoleNotificationServiceClient
	householdCalculationClient       customersv1.HouseholdCalculationServiceClient
}

// NewStockMovementWorkflowServer wires a StockMovementWorkflowServer.
func NewStockMovementWorkflowServer(deps platform.Deps) *StockMovementWorkflowServer {
	return &StockMovementWorkflowServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.inventory.v1.StockMovementWorkflowService"),
		returnRequestWorkflowClient:      returnsv1.NewReturnRequestWorkflowServiceClient(deps.Conn),
		clients:                          deps.Clients,
		replenishmentPlanApprovalClient:  inventoryv1.NewReplenishmentPlanApprovalServiceClient(deps.Conn),
		sellerListingApprovalClient:      sellersv1.NewSellerListingApprovalServiceClient(deps.Conn),
		stockMovementApprovalClient:      inventoryv1.NewStockMovementApprovalServiceClient(deps.Conn),
		placementWorkflowClient:          merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn),
		tenantNotificationClient:         identityv1.NewTenantNotificationServiceClient(deps.Conn),
		tenantNotificationClientFallback: identityv1.NewTenantNotificationServiceClient(deps.Conn),
		stockItemWorkflowClient:          inventoryv1.NewStockItemWorkflowServiceClient(deps.Conn),
		stockItemApprovalClient:          inventoryv1.NewStockItemApprovalServiceClient(deps.Conn),
		roleNotificationClient:           identityv1.NewRoleNotificationServiceClient(deps.Conn),
		householdCalculationClient:       customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
	}
}

// StartStockMovementWorkflow starts a stock movement.
func (s *StockMovementWorkflowServer) StartStockMovementWorkflow(ctx context.Context, req *inventoryv1.StartStockMovementWorkflowRequest) (*inventoryv1.StartStockMovementWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "start stock movement workflow", "tenant", req.GetContext().GetTenantId())
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	signalReturnRequestWorkflowRequest := &returnsv1.SignalReturnRequestWorkflowRequest{
		Context: req.GetContext(),
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				Email:       req.GetDetails().GetNotify().GetEmail(),
				DisplayName: req.GetStockMovement().GetSubject().GetDisplayName(),
			},
		},
		Details: &returnsv1.SignalReturnRequestWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetStockMovement().GetSubject().GetEmail()},
		},
		ReturnRequestId: envelope.GetDisplayName(),
	}
	signalReturnRequestWorkflowReply, err := returnsv1.ReturnRequestWorkflowServiceClient.SignalReturnRequestWorkflow(s.returnRequestWorkflowClient, ctx, signalReturnRequestWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestWorkflowService/SignalReturnRequestWorkflow")
	}
	customerValidation, err := registry.Lookup[customersv1.CustomerValidationServiceClient](s.clients, "acme.customers.v1.CustomerValidationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetNotify().GetPreferredChannel())
	explainCustomerDecisionRequest := &customersv1.ExplainCustomerDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotify().GetEmail()},
		},
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{PreferredChannel: tenancy.Value(ctx, "preferred_channel")},
			PrimaryAddress: &commonv1.PostalAddress{
				CountryCode: signalReturnRequestWorkflowReply.GetReturnRequest().GetBillingAddress().GetCountryCode(),
				Region:      signalReturnRequestWorkflowReply.GetReturnRequest().GetBillingAddress().GetRegion(),
			},
		},
	}
	explainCustomerDecisionReply, err := customerValidation.ExplainCustomerDecision(ctx, explainCustomerDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	var envelope2 inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	delegateReplenishmentPlanApprovalReply, err := s.replenishmentPlanApprovalClient.DelegateReplenishmentPlanApproval(ctx, &inventoryv1.DelegateReplenishmentPlanApprovalRequest{
		Context: req.GetContext(),
		Details: &inventoryv1.DelegateReplenishmentPlanApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetStockMovement().GetSubject().GetEmail()},
		},
		ReplenishmentPlan: &inventoryv1.ReplenishmentPlan{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: signalReturnRequestWorkflowReply.GetReturnRequest().GetAudit().GetUpdatedBy(),
			},
		},
		ReplenishmentPlanId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.ReplenishmentPlanApprovalService/DelegateReplenishmentPlanApproval")
	}
	jobs := make(chan *inventoryv1.StockMovementMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.sellerListingApprovalClient.SubmitSellerListing(workCtx, &sellersv1.SubmitSellerListingRequest{
					Context: req.GetContext(),
					SellerListing: &sellersv1.SellerListing{
						Dimensions: &sellersv1.SellerListingDimensions{Unit: measurement.GetUnit()},
						Locator: &sellersv1.SellerListing_Address{Address: &commonv1.PostalAddress{
							Region: delegateReplenishmentPlanApprovalReply.GetReplenishmentPlan().GetBillingAddress().GetRegion(),
						}},
					},
					Details: &sellersv1.SubmitSellerListingRequest_Details{
						Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetStockMovement().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingApprovalService/SubmitSellerListing")
	}
	return &inventoryv1.StartStockMovementWorkflowResponse{
		StockMovement: &inventoryv1.StockMovement{
			CorrelationId:   signalReturnRequestWorkflowReply.GetResult().GetCorrelationId(),
			StockMovementId: req.GetStockMovementId(),
		},
		Outcome: &inventoryv1.StartStockMovementWorkflowResponse_Outcome{
			ReferenceId: explainCustomerDecisionReply.GetOutcome().GetReferenceId(),
			Message:     delegateReplenishmentPlanApprovalReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetStockMovement().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// AdvanceStockMovementWorkflow advances a stock movement.
func (s *StockMovementWorkflowServer) AdvanceStockMovementWorkflow(ctx context.Context, req *inventoryv1.AdvanceStockMovementWorkflowRequest) (*inventoryv1.AdvanceStockMovementWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "advance stock movement workflow", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "customer_id", req.GetStockMovement().GetSubject().GetCustomerId())
	packed, err := anypb.New(req.GetStockMovement())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	commentStockMovementApprovalRequest := &inventoryv1.CommentStockMovementApprovalRequest{}
	commentStockMovementApprovalRequest.Context = req.GetContext()
	commentStockMovementApprovalRequest.StockMovement = &inventoryv1.StockMovement{
		Subject: &customersv1.CustomerRef{
			Email:      req.GetStockMovement().GetSubject().GetEmail(),
			CustomerId: tenancy.Value(ctx, "customer_id"),
		},
		Detail: packed,
	}
	commentStockMovementApprovalRequest.StockMovementId = envelope.GetDisplayName()
	commentStockMovementApprovalReply, err := s.stockMovementApprovalClient.CommentStockMovementApproval(ctx, commentStockMovementApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementApprovalService/CommentStockMovementApproval")
	}
	return &inventoryv1.AdvanceStockMovementWorkflowResponse{
		StockMovement: &inventoryv1.StockMovement{
			Source:          commentStockMovementApprovalReply.GetStockMovement().GetSource(),
			StockMovementId: req.GetStockMovementId(),
			CorrelationId:   req.GetStockMovement().GetCorrelationId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CancelStockMovementWorkflow cancels a stock movement.
func (s *StockMovementWorkflowServer) CancelStockMovementWorkflow(ctx context.Context, req *inventoryv1.CancelStockMovementWorkflowRequest) (*inventoryv1.CancelStockMovementWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelStockMovementWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "cancel stock movement workflow", "tenant", req.GetContext().GetTenantId())
	var holder struct{ Email string }
	if record := req.GetStockMovement().GetSubject(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Email"); err != nil {
			s.log.WarnContext(ctx, "copy email", "error", err)
		}
	}
	pausePlacementWorkflowRequest := &merchandisingv1.PausePlacementWorkflowRequest{
		Context: req.GetContext(),
		Placement: &merchandisingv1.Placement{
			Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
				Contact: &commonv1.ContactInfo{
					Name:  req.GetDetails().GetNotify().GetName(),
					Email: holder.Email,
				},
			}},
		},
	}
	pausePlacementWorkflowReply, err := grpcx.Call(ctx, s.placementWorkflowClient.PausePlacementWorkflow, pausePlacementWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/PausePlacementWorkflow")
	}
	routes := map[string]func(context.Context, *identityv1.UnsubscribeTenantUpdatesRequest, ...grpc.CallOption) (*identityv1.UnsubscribeTenantUpdatesResponse, error){
		"primary":  s.tenantNotificationClient.UnsubscribeTenantUpdates,
		"fallback": s.tenantNotificationClientFallback.UnsubscribeTenantUpdates,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	unsubscribeTenantUpdatesRequest := &identityv1.UnsubscribeTenantUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeTenantUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
				Email:            req.GetDetails().GetNotify().GetEmail(),
				Phone:            req.GetDetails().GetNotify().GetPhone(),
			},
		},
	}
	unsubscribeTenantUpdatesReply, err := route(ctx, unsubscribeTenantUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantNotificationService/UnsubscribeTenantUpdates")
	}
	sellerListingWorkflow, err := registry.Lookup[sellersv1.SellerListingWorkflowServiceClient](s.clients, "acme.sellers.v1.SellerListingWorkflowService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	retrySellerListingWorkflowRequest := &sellersv1.RetrySellerListingWorkflowRequest{
		Context: req.GetContext(),
		Details: &sellersv1.RetrySellerListingWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
		SellerListing: &sellersv1.SellerListing{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: unsubscribeTenantUpdatesReply.GetTenant().GetAudit().GetUpdatedAt(),
				CreatedAt: unsubscribeTenantUpdatesReply.GetTenant().GetAudit().GetCreatedAt(),
			},
		},
	}
	retrySellerListingWorkflowReply, err := sellerListingWorkflow.RetrySellerListingWorkflow(ctx, retrySellerListingWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/RetrySellerListingWorkflow")
	}
	return &inventoryv1.CancelStockMovementWorkflowResponse{
		Outcome: &inventoryv1.CancelStockMovementWorkflowResponse_Outcome{
			Status:      pausePlacementWorkflowReply.GetResult().GetStatus(),
			ReferenceId: retrySellerListingWorkflowReply.GetOutcome().GetReferenceId(),
		},
		Result: &commonv1.OperationResult{
			Message: pausePlacementWorkflowReply.GetResult().GetMessage(),
			Status:  unsubscribeTenantUpdatesReply.GetResult().GetStatus(),
		},
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				Email: retrySellerListingWorkflowReply.GetSellerListing().GetOwner().GetEmail(),
			},
			StockMovementId: req.GetStockMovementId(),
			Sequence:        req.GetStockMovement().GetSequence(),
		},
	}, nil
}

// RetryStockMovementWorkflow retrys a stock movement.
func (s *StockMovementWorkflowServer) RetryStockMovementWorkflow(ctx context.Context, req *inventoryv1.RetryStockMovementWorkflowRequest) (*inventoryv1.RetryStockMovementWorkflowResponse, error) {
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "retry stock movement workflow", "tenant", req.GetContext().GetTenantId())
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	step := &stockMovementWorkflowServerRetryStockMovementWorkflowListStockItemWorkflowsStep{request: &inventoryv1.ListStockItemWorkflowsRequest{
		Context: req.GetContext(),
		Filter:  &inventoryv1.ListStockItemWorkflowsRequest_Filter{CreatedAfter: req.GetStockMovement().GetOccurredAt()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	}}
	if err := s.runRetryStockMovementWorkflowListStockItemWorkflows(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemWorkflowService/ListStockItemWorkflows")
	}
	listStockItemWorkflowsReply := step.reply
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*inventoryv1.CommentStockItemApprovalResponse, len(req.GetStockMovement().GetMeasurements()))
	for i, measurement := range req.GetStockMovement().GetMeasurements() {
		group.Go(func() error {
			reply, err := s.stockItemApprovalClient.CommentStockItemApproval(groupCtx, &inventoryv1.CommentStockItemApprovalRequest{
				Context: req.GetContext(),
				StockItem: &inventoryv1.StockItem{
					Dimensions: &inventoryv1.StockItemDimensions{Unit: measurement.GetUnit()},
					Audit:      &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
					Owner: &identityv1.UserRef{
						DisplayName: req.GetStockMovement().GetSubject().GetDisplayName(),
						Email:       req.GetStockMovement().GetSubject().GetEmail(),
					},
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
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/CommentStockItemApproval")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	return &inventoryv1.RetryStockMovementWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: listStockItemWorkflowsReply.GetResult().GetDetails(),
			Status:  listStockItemWorkflowsReply.GetResult().GetStatus(),
		},
		StockMovement: &inventoryv1.StockMovement{
			StockMovementId: req.GetStockMovementId(),
			Sequence:        req.GetStockMovement().GetSequence(),
		},
	}, nil
}

// ResumeStockMovementWorkflow resumes a stock movement.
func (s *StockMovementWorkflowServer) ResumeStockMovementWorkflow(ctx context.Context, req *inventoryv1.ResumeStockMovementWorkflowRequest) (*inventoryv1.ResumeStockMovementWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeStockMovementWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveStockMovement(ctx, req.GetContext().GetTenantId(), req.GetStockMovement()); err != nil {
		return nil, grpcx.Wrap(err, "save stock movement")
	}
	s.log.InfoContext(ctx, "resume stock movement workflow", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetStockMovement())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	notifyRoleWatchersRequest := &identityv1.NotifyRoleWatchersRequest{}
	notifyRoleWatchersRequest.Context = req.GetContext()
	notifyRoleWatchersRequest.Details = &identityv1.NotifyRoleWatchersRequest_Details{
		Recipient: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		Payload:   packed,
	}
	notifyRoleWatchersReply, err := s.roleNotificationClient.NotifyRoleWatchers(ctx, notifyRoleWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/NotifyRoleWatchers")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded inventoryv1.ResumeStockMovementWorkflowRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var envelope inventoryv1.StockItemRef
	if err := req.GetStockMovement().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	resumePlacementWorkflowRequest := newResumePlacementWorkflowRequest(withResumePlacementWorkflowContext(req.GetContext()), withResumePlacementWorkflowPlacement(&merchandisingv1.Placement{
		Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
			Contact: &commonv1.ContactInfo{Phone: (&decoded).GetNotify().GetPhone()},
		}},
	}), withResumePlacementWorkflowPlacementId(envelope.GetDisplayName()))
	resumePlacementWorkflowReply, err := s.placementWorkflowClient.ResumePlacementWorkflow(ctx, resumePlacementWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/ResumePlacementWorkflow")
	}
	return &inventoryv1.ResumeStockMovementWorkflowResponse{
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: notifyRoleWatchersReply.GetRole().GetOwner().GetDisplayName(),
			},
			StockMovementId: req.GetStockMovementId(),
			Sequence:        req.GetStockMovement().GetSequence(),
		},
		Outcome: &inventoryv1.ResumeStockMovementWorkflowResponse_Outcome{
			Status: resumePlacementWorkflowReply.GetResult().GetStatus(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectStockMovementWorkflow returns a stock movement.
func (s *StockMovementWorkflowServer) InspectStockMovementWorkflow(ctx context.Context, req *inventoryv1.InspectStockMovementWorkflowRequest) (*inventoryv1.InspectStockMovementWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected InspectStockMovementWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "stock_movement", req.GetStockMovementId(), "inspect_stock_movement_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect stock movement workflow", "tenant", req.GetContext().GetTenantId())
	quoteHouseholdRequest := &customersv1.QuoteHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
				Children: []*customersv1.Household{&customersv1.Household{
					Manager:  &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
					Children: []*customersv1.Household{&customersv1.Household{ParentId: req.GetStockMovementId()}},
				}},
			}},
		},
	}
	quoteHouseholdReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.QuoteHouseholdResponse, error) {
		return s.householdCalculationClient.QuoteHousehold(ctx, quoteHouseholdRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/QuoteHousehold")
	}
	return &inventoryv1.InspectStockMovementWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:       quoteHouseholdReply.GetResult().GetStatus(),
			ReferenceIds: quoteHouseholdReply.GetResult().GetReferenceIds(),
		},
		StockMovement: &inventoryv1.StockMovement{StockMovementId: req.GetStockMovementId()},
	}, nil
}

// ListStockMovementWorkflows lists stock movements a page at a time.
func (s *StockMovementWorkflowServer) ListStockMovementWorkflows(ctx context.Context, req *inventoryv1.ListStockMovementWorkflowsRequest) (*inventoryv1.ListStockMovementWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list stock movement workflows", "tenant", req.GetContext().GetTenantId())
	escalateStockItemRequest := &inventoryv1.EscalateStockItemRequest{}
	escalateStockItemRequest.Context = req.GetContext()
	escalateStockItemRequest.StockItem = &inventoryv1.StockItem{
		Locator: &inventoryv1.StockItem_Address{Address: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()}},
		Owner:   &identityv1.UserRef{UserId: req.GetContext().GetActor().GetUserId()},
		Audit:   &commonv1.AuditInfo{UpdatedAt: req.GetFilter().GetCreatedAfter()},
	}
	escalateStockItemRequest.Details = &inventoryv1.EscalateStockItemRequest_Details{
		Approver: &identityv1.UserRef{Email: req.GetContext().GetActor().GetEmail()},
	}
	escalateStockItemReply, err := s.stockItemApprovalClient.EscalateStockItem(ctx, escalateStockItemRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockItemApprovalService/EscalateStockItem")
	}
	return &inventoryv1.ListStockMovementWorkflowsResponse{
		Result: &commonv1.OperationResult{
			Status:       escalateStockItemReply.GetResult().GetStatus(),
			ReferenceIds: escalateStockItemReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// stockMovementWorkflowServerRetryStockMovementWorkflowListStockItemWorkflowsStep carries the request of one step of
// RetryStockMovementWorkflow and, once it ran, its reply.
type stockMovementWorkflowServerRetryStockMovementWorkflowListStockItemWorkflowsStep struct {
	request *inventoryv1.ListStockItemWorkflowsRequest
	reply   *inventoryv1.ListStockItemWorkflowsResponse
}

func (s *StockMovementWorkflowServer) runRetryStockMovementWorkflowListStockItemWorkflows(ctx context.Context, step *stockMovementWorkflowServerRetryStockMovementWorkflowListStockItemWorkflowsStep) error {
	reply, err := s.stockItemWorkflowClient.ListStockItemWorkflows(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
