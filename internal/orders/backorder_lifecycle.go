package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"

	collectionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/collections/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	devicesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/devices/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	pipelinesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/pipelines/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// BackorderLifecycleServer implements `acme.orders.v1.BackorderLifecycleService`.
// Moves a backorder between the states of its lifecycle.
type BackorderLifecycleServer struct {
	ordersv1.UnimplementedBackorderLifecycleServiceServer
	store                                      *Store
	log                                        *slog.Logger
	consentRecordValidationClient              customersv1.ConsentRecordValidationServiceClient
	backorderReportingClient                   ordersv1.BackorderReportingServiceClient
	orderValidationClient                      ordersv1.OrderValidationServiceClient
	searchIndexClient                          searchv1.SearchIndexServiceClient
	orderNoteSyncClient                        ordersv1.OrderNoteSyncServiceClient
	conn                                       *grpc.ClientConn
	pipelineCalculationClient                  pipelinesv1.PipelineCalculationServiceClient
	collectionCaseClient                       collectionsv1.CollectionCaseServiceClient
	orderReportingClient                       ordersv1.OrderReportingServiceClient
	customerClient                             customersv1.CustomerServiceClient
	managedDeviceClient                        devicesv1.ManagedDeviceServiceClient
	subscriptionApprovalClient                 subscriptionsv1.SubscriptionApprovalServiceClient
	sellerPayoutScheduleWorkflowClient         sellersv1.SellerPayoutScheduleWorkflowServiceClient
	sellerPayoutScheduleWorkflowClientFallback sellersv1.SellerPayoutScheduleWorkflowServiceClient
}

// NewBackorderLifecycleServer wires a BackorderLifecycleServer.
func NewBackorderLifecycleServer(deps platform.Deps) *BackorderLifecycleServer {
	return &BackorderLifecycleServer{
		store:                              NewStore(deps.DB),
		log:                                deps.Logger.With("service", "acme.orders.v1.BackorderLifecycleService"),
		consentRecordValidationClient:      customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		backorderReportingClient:           ordersv1.NewBackorderReportingServiceClient(deps.Conn),
		orderValidationClient:              ordersv1.NewOrderValidationServiceClient(deps.Conn),
		searchIndexClient:                  searchv1.NewSearchIndexServiceClient(deps.Conn),
		orderNoteSyncClient:                ordersv1.NewOrderNoteSyncServiceClient(deps.Conn),
		conn:                               deps.Conn,
		pipelineCalculationClient:          pipelinesv1.NewPipelineCalculationServiceClient(deps.Conn),
		collectionCaseClient:               collectionsv1.NewCollectionCaseServiceClient(deps.Conn),
		orderReportingClient:               ordersv1.NewOrderReportingServiceClient(deps.Conn),
		customerClient:                     customersv1.NewCustomerServiceClient(deps.Conn),
		managedDeviceClient:                devicesv1.NewManagedDeviceServiceClient(deps.Conn),
		subscriptionApprovalClient:         subscriptionsv1.NewSubscriptionApprovalServiceClient(deps.Conn),
		sellerPayoutScheduleWorkflowClient: sellersv1.NewSellerPayoutScheduleWorkflowServiceClient(deps.Conn),
		sellerPayoutScheduleWorkflowClientFallback: sellersv1.NewSellerPayoutScheduleWorkflowServiceClient(deps.Conn),
	}
}

// ActivateBackorder activates a backorder.
func (s *BackorderLifecycleServer) ActivateBackorder(ctx context.Context, req *ordersv1.ActivateBackorderRequest) (*ordersv1.ActivateBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "activate_backorder"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate backorder", "tenant", req.GetContext().GetTenantId())
	explainConsentRecordDecisionRequest := buildExplainConsentRecordDecisionRequestForActivateBackorder(req)
	explainConsentRecordDecisionReply, err := s.consentRecordValidationClient.ExplainConsentRecordDecision(ctx, explainConsentRecordDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/ExplainConsentRecordDecision")
	}
	listBackorderReportsRequest := &ordersv1.ListBackorderReportsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.ListBackorderReportsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      explainConsentRecordDecisionReply.GetConsentRecord().GetSubject().GetHouseholdId(),
			Labels:       explainConsentRecordDecisionReply.GetConsentRecord().GetAttributes(),
		},
	}
	listBackorderReportsReply, err := grpcx.Call(ctx, s.backorderReportingClient.ListBackorderReports, listBackorderReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderReportingService/ListBackorderReports")
	}
	return &ordersv1.ActivateBackorderResponse{
		Result: &commonv1.OperationResult{
			Message: explainConsentRecordDecisionReply.GetResult().GetMessage(),
			Status:  listBackorderReportsReply.GetResult().GetStatus(),
		},
		Backorder: &ordersv1.Backorder{BackorderId: req.GetBackorderId()},
		Outcome:   &ordersv1.ActivateBackorderResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// SuspendBackorder suspends a backorder.
func (s *BackorderLifecycleServer) SuspendBackorder(ctx context.Context, req *ordersv1.SuspendBackorderRequest) (*ordersv1.SuspendBackorderResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "suspend_backorder"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend backorder", "tenant", req.GetContext().GetTenantId())
	step := &backorderLifecycleServerSuspendBackorderExplainOrderDecisionStep{request: &ordersv1.ExplainOrderDecisionRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ExplainOrderDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}}
	if err := s.runSuspendBackorderExplainOrderDecision(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/ExplainOrderDecision")
	}
	explainOrderDecisionReply := step.reply
	searchSearchIndexesRequest := &searchv1.SearchSearchIndexesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.SearchSearchIndexesRequest_Filter{
			Region:       explainOrderDecisionReply.GetOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: explainOrderDecisionReply.GetOrder().GetAudit().GetCreatedAt(),
			OwnerId:      explainOrderDecisionReply.GetOrder().GetOrderId(),
		},
	}
	searchSearchIndexesReply, err := grpcx.Call(ctx, s.searchIndexClient.SearchSearchIndexes, searchSearchIndexesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexService/SearchSearchIndexes")
	}
	return &ordersv1.SuspendBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				Email: explainOrderDecisionReply.GetOrder().GetCounterparty().GetEmail(),
			},
			BackorderId: req.GetBackorderId(),
		},
		Outcome: &ordersv1.SuspendBackorderResponse_Outcome{
			Status:      searchSearchIndexesReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: searchSearchIndexesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ReinstateBackorder reinstates a backorder.
func (s *BackorderLifecycleServer) ReinstateBackorder(ctx context.Context, req *ordersv1.ReinstateBackorderRequest) (*ordersv1.ReinstateBackorderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "reinstate_backorder"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate backorder", "tenant", req.GetContext().GetTenantId())
	listOrderNoteConflictsRequest := &ordersv1.ListOrderNoteConflictsRequest{
		Context: req.GetContext(),
		Filter:  &ordersv1.ListOrderNoteConflictsRequest_Filter{CreatedAfter: req.GetDetails().GetEffectiveAt()},
	}
	type listOrderNoteConflictsResult struct {
		reply *ordersv1.ListOrderNoteConflictsResponse
		err   error
	}
	listOrderNoteConflictsResults := make(chan listOrderNoteConflictsResult, 1)
	go func() {
		reply, err := s.orderNoteSyncClient.ListOrderNoteConflicts(ctx, listOrderNoteConflictsRequest)
		listOrderNoteConflictsResults <- listOrderNoteConflictsResult{reply, err}
	}()
	var listOrderNoteConflictsReply *ordersv1.ListOrderNoteConflictsResponse
	select {
	case result := <-listOrderNoteConflictsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.orders.v1.OrderNoteSyncService/ListOrderNoteConflicts")
		}
		listOrderNoteConflictsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	quoteCustomerSegmentRequest := &customersv1.QuoteCustomerSegmentRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetNotifyContact().GetEmail(),
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Action: &customersv1.CustomerSegment_Block{Block: &customersv1.CustomerSegmentBlockAction{Reason: req.GetReason()}},
		},
	}
	quoteCustomerSegmentReply, err := grpcx.NewClient(s.conn, customersv1.NewCustomerSegmentCalculationServiceClient).QuoteCustomerSegment(ctx, quoteCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/QuoteCustomerSegment")
	}
	calculatePipelineRequest := &pipelinesv1.CalculatePipelineRequest{
		Context: req.GetContext(),
		Pipeline: &pipelinesv1.Pipeline{
			Participants: []*pipelinesv1.PipelineParticipant{&pipelinesv1.PipelineParticipant{
				Contact: &commonv1.ContactInfo{
					Name:             req.GetDetails().GetNotifyContact().GetName(),
					PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: quoteCustomerSegmentReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
				Etag:      quoteCustomerSegmentReply.GetCustomerSegment().GetAudit().GetEtag(),
			},
		},
	}
	type calculatePipelineResult struct {
		reply *pipelinesv1.CalculatePipelineResponse
		err   error
	}
	calculatePipelineResults := make(chan calculatePipelineResult, 1)
	go func() {
		reply, err := s.pipelineCalculationClient.CalculatePipeline(ctx, calculatePipelineRequest)
		calculatePipelineResults <- calculatePipelineResult{reply, err}
	}()
	var calculatePipelineReply *pipelinesv1.CalculatePipelineResponse
	select {
	case result2 := <-calculatePipelineResults:
		if result2.err != nil {
			return nil, grpcx.Wrap(result2.err, "acme.pipelines.v1.PipelineCalculationService/CalculatePipeline")
		}
		calculatePipelineReply = result2.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &ordersv1.ReinstateBackorderResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listOrderNoteConflictsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				CreatedBy: calculatePipelineReply.GetPipeline().GetAudit().GetCreatedBy(),
			},
			BackorderId: req.GetBackorderId(),
		},
		Outcome: &ordersv1.ReinstateBackorderResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminateBackorder terminates a backorder.
func (s *BackorderLifecycleServer) TerminateBackorder(ctx context.Context, req *ordersv1.TerminateBackorderRequest) (*ordersv1.TerminateBackorderResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "terminate_backorder"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate backorder", "tenant", req.GetContext().GetTenantId())
	cloneCollectionCaseReply, err := s.collectionCaseClient.CloneCollectionCase(ctx, &collectionsv1.CloneCollectionCaseRequest{
		Context: req.GetContext(),
		CollectionCase: &collectionsv1.CollectionCase{
			Requester: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
				Email:            req.GetDetails().GetNewOwner().GetEmail(),
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.collections.v1.CollectionCaseService/CloneCollectionCase")
	}
	countOrderTotalsRequest := &ordersv1.CountOrderTotalsRequest{}
	countOrderTotalsRequest.Context = req.GetContext()
	countOrderTotalsRequest.Filter = &ordersv1.CountOrderTotalsRequest_Filter{
		CreatedAfter: cloneCollectionCaseReply.GetCollectionCase().GetAudit().GetCreatedAt(),
		OwnerId:      cloneCollectionCaseReply.GetCollectionCase().GetAssignee().GetUserId(),
	}
	countOrderTotalsReply, err := s.orderReportingClient.CountOrderTotals(ctx, countOrderTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/CountOrderTotals")
	}
	return &ordersv1.TerminateBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				Email: cloneCollectionCaseReply.GetCollectionCase().GetAssignee().GetEmail(),
			},
			BackorderId: req.GetBackorderId(),
		},
		Outcome: &ordersv1.TerminateBackorderResponse_Outcome{
			Status:      countOrderTotalsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RenewBackorder renews a backorder.
func (s *BackorderLifecycleServer) RenewBackorder(ctx context.Context, req *ordersv1.RenewBackorderRequest) (*ordersv1.RenewBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewBackorder", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "renew_backorder"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew backorder", "tenant", req.GetContext().GetTenantId())
	browseOrderAmendmentsRequest := &ordersv1.BrowseOrderAmendmentsRequest{
		Context: req.GetContext(),
		Filter: &ordersv1.BrowseOrderAmendmentsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetBackorderId(),
		},
	}
	browseOrderAmendmentsReply, err := browseOrderAmendmentsCall(ctx, browseOrderAmendmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentQueryService/BrowseOrderAmendments")
	}
	cloned := proto.Clone(req.GetDetails()).(*ordersv1.RenewBackorderRequest_Details)
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushOrderNoteRequest := &ordersv1.PushOrderNoteRequest{
		Context: req.GetContext(),
		OrderNote: &ordersv1.OrderNote{
			Subject:       &ordersv1.OrderRef{DisplayName: cloned.GetNewOwner().GetDisplayName()},
			CorrelationId: browseOrderAmendmentsReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Reason:      tenancy.Value(ctx, "reason"),
		Annotations: annotations,
		Details:     &ordersv1.PushOrderNoteRequest_Details{Payload: snapshot},
	}
	pushOrderNoteReply := new(ordersv1.PushOrderNoteResponse)
	if err := grpcx.Invoke(ctx, s.conn, ordersv1.OrderNoteSyncService_PushOrderNote_FullMethodName, pushOrderNoteRequest, pushOrderNoteReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderNoteSyncService/PushOrderNote")
	}
	return &ordersv1.RenewBackorderResponse{
		Outcome: &ordersv1.RenewBackorderResponse_Outcome{
			Status:      browseOrderAmendmentsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  browseOrderAmendmentsReply.GetResult().GetReferenceIds(),
			Details:       pushOrderNoteReply.GetResult().GetDetails(),
			CorrelationId: pushOrderNoteReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Backorder: &ordersv1.Backorder{BackorderId: req.GetBackorderId()},
	}, nil
}

// ExpireBackorder expires a backorder.
func (s *BackorderLifecycleServer) ExpireBackorder(ctx context.Context, req *ordersv1.ExpireBackorderRequest) (*ordersv1.ExpireBackorderResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ExpireBackorder", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "expire_backorder"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire backorder", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNewOwner().GetEmail())
	watchCustomersStream, err := s.customerClient.WatchCustomers(ctx, &customersv1.WatchCustomersRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
			Email:            tenancy.Value(ctx, "email"),
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/WatchCustomers")
	}
	for {
		received, err := watchCustomersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/WatchCustomers")
		}
		_ = received
	}
	source := req.GetDetails().GetNewOwner().ProtoReflect()
	userId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("user_id"))).String()
	createManagedDeviceReply, err := s.managedDeviceClient.CreateManagedDevice(ctx, &devicesv1.CreateManagedDeviceRequest{
		Context:       req.GetContext(),
		ManagedDevice: &devicesv1.ManagedDevice{Owner: &identityv1.UserRef{UserId: userId}},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.devices.v1.ManagedDeviceService/CreateManagedDevice")
	}
	return &ordersv1.ExpireBackorderResponse{
		Backorder: &ordersv1.Backorder{
			Audit: &commonv1.AuditInfo{
				Etag: createManagedDeviceReply.GetManagedDevice().GetAudit().GetEtag(),
			},
			BackorderId: req.GetBackorderId(),
		},
		Result: &commonv1.OperationResult{
			Message: createManagedDeviceReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &ordersv1.ExpireBackorderResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetBackorderLifecycle returns a backorder.
func (s *BackorderLifecycleServer) GetBackorderLifecycle(ctx context.Context, req *ordersv1.GetBackorderLifecycleRequest) (*ordersv1.GetBackorderLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetBackorderLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "backorder", req.GetBackorderId(), "get_backorder_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get backorder lifecycle", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetContext().GetActor().GetUserId())
	createCustomerReply, err := s.customerClient.CreateCustomer(ctx, &customersv1.CreateCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact:             &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			RelationshipManager: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
			Audit:               &commonv1.AuditInfo{CreatedAt: req.GetContext().GetDeadline()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/CreateCustomer")
	}
	return &ordersv1.GetBackorderLifecycleResponse{
		Backorder: &ordersv1.Backorder{
			Requester: &commonv1.ContactInfo{
				Email: createCustomerReply.GetCustomer().GetContact().GetEmail(),
			},
			BackorderId: req.GetBackorderId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: createCustomerReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// ScheduleBackorderExpiry schedules a backorder.
func (s *BackorderLifecycleServer) ScheduleBackorderExpiry(ctx context.Context, req *ordersv1.ScheduleBackorderExpiryRequest) (*ordersv1.ScheduleBackorderExpiryResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveBackorder(ctx, req.GetContext().GetTenantId(), req.GetBackorder()); err != nil {
		return nil, grpcx.Wrap(err, "save backorder")
	}
	s.log.InfoContext(ctx, "schedule backorder expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetBackorder().GetAudit().GetEtag())
	for _, item := range req.GetBackorder().GetHistory() {
		if _, err := s.subscriptionApprovalClient.ListPendingSubscriptions(ctx, &subscriptionsv1.ListPendingSubscriptionsRequest{
			Context: req.GetContext(),
			Filter:  &subscriptionsv1.ListPendingSubscriptionsRequest_Filter{CreatedAfter: item.GetChangedAt()},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.subscriptions.v1.SubscriptionApprovalService/ListPendingSubscriptions")
		}
	}
	var email string
	switch choice := req.GetBackorder().GetOrigin().(type) {
	case *ordersv1.Backorder_Customer:
		email = choice.Customer.GetEmail()
	case *ordersv1.Backorder_ExternalReference:
		email = choice.ExternalReference
	case *ordersv1.Backorder_Channel:
		email = choice.Channel.GetChannel()
	}
	lintOrdersRequest := &ordersv1.LintOrdersRequest{
		Context: req.GetContext(),
		Details: &ordersv1.LintOrdersRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetBackorder().GetAssignee().GetEmail()},
		},
		Order: &ordersv1.Order{
			Audit:        &commonv1.AuditInfo{UpdatedAt: req.GetBackorder().GetAudit().GetUpdatedAt()},
			Counterparty: &customersv1.CustomerRef{Email: email},
		},
	}
	lintOrdersReply, err := lintOrdersCall(ctx, lintOrdersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderValidationService/LintOrders")
	}
	countOrderTotalsRequest := newCountOrderTotalsRequest(withCountOrderTotalsContext(req.GetContext()), withCountOrderTotalsFilter(&ordersv1.CountOrderTotalsRequest_Filter{
		Region:       lintOrdersReply.GetOrder().GetBillingAddress().GetRegion(),
		CreatedAfter: req.GetBackorder().GetAudit().GetCreatedAt(),
		OwnerId:      req.GetBackorder().GetAssignee().GetUserId(),
	}))
	countOrderTotalsReply, err := s.orderReportingClient.CountOrderTotals(ctx, countOrderTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/CountOrderTotals")
	}
	routes := map[string]func(context.Context, *sellersv1.ListSellerPayoutScheduleWorkflowsRequest, ...grpc.CallOption) (*sellersv1.ListSellerPayoutScheduleWorkflowsResponse, error){
		"primary":  s.sellerPayoutScheduleWorkflowClient.ListSellerPayoutScheduleWorkflows,
		"fallback": s.sellerPayoutScheduleWorkflowClientFallback.ListSellerPayoutScheduleWorkflows,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	listSellerPayoutScheduleWorkflowsRequest := &sellersv1.ListSellerPayoutScheduleWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerPayoutScheduleWorkflowsRequest_Filter{
			Region:       lintOrdersReply.GetOrder().GetBillingAddress().GetRegion(),
			CreatedAfter: req.GetBackorder().GetSla().GetDueAt(),
			OwnerId:      req.GetBackorder().GetAssignee().GetUserId(),
			Labels:       lintOrdersReply.GetResult().GetDetails(),
		},
	}
	listSellerPayoutScheduleWorkflowsReply, err := route(ctx, listSellerPayoutScheduleWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleWorkflowService/ListSellerPayoutScheduleWorkflows")
	}
	_ = listSellerPayoutScheduleWorkflowsReply
	return &ordersv1.ScheduleBackorderExpiryResponse{
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{
				Email: lintOrdersReply.GetOrder().GetCounterparty().GetEmail(),
			},
			BackorderId:  req.GetBackorderId(),
			CustomFields: req.GetBackorder().GetCustomFields(),
		},
		Result: &commonv1.OperationResult{Status: countOrderTotalsReply.GetResult().GetStatus()},
	}, nil
}

// backorderLifecycleServerSuspendBackorderExplainOrderDecisionStep carries the request of one step of SuspendBackorder
// and, once it ran, its reply.
type backorderLifecycleServerSuspendBackorderExplainOrderDecisionStep struct {
	request *ordersv1.ExplainOrderDecisionRequest
	reply   *ordersv1.ExplainOrderDecisionResponse
}

func (s *BackorderLifecycleServer) runSuspendBackorderExplainOrderDecision(ctx context.Context, step *backorderLifecycleServerSuspendBackorderExplainOrderDecisionStep) error {
	reply, err := s.orderValidationClient.ExplainOrderDecision(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
