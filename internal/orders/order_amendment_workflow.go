package orders

import (
	"context"
	"errors"
	"io"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ordersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/orders/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	venuesv1 "github.com/hounddogai/protomap-test-golang/gen/acme/venues/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// OrderAmendmentWorkflowServer implements `acme.orders.v1.OrderAmendmentWorkflowService`.
// Runs the long-lived workflow that takes an order amendment from start to finish.
type OrderAmendmentWorkflowServer struct {
	*orderAmendmentWorkflowHandlers
}

// orderAmendmentWorkflowHandlers holds the handlers of OrderAmendmentWorkflowServer, with what they use.
type orderAmendmentWorkflowHandlers struct {
	ordersv1.UnimplementedOrderAmendmentWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	customerCalculationPort        customerCalculationPort
	clients                        *registry.Registry
	householdValidationPort        householdValidationPort
	venueValidationClient          venuesv1.VenueValidationServiceClient
	disputeSyncClient              paymentsv1.DisputeSyncServiceClient
	fraudSignalApprovalClient      fraudv1.FraudSignalApprovalServiceClient
	customerQueryPort              customerQueryPort
	backorderSyncClient            ordersv1.BackorderSyncServiceClient
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
	customerClient                 customersv1.CustomerServiceClient
	searchQueryQueryClient         searchv1.SearchQueryQueryServiceClient
	orderReportingClient           ordersv1.OrderReportingServiceClient
}

// NewOrderAmendmentWorkflowServer wires an OrderAmendmentWorkflowServer.
func NewOrderAmendmentWorkflowServer(deps platform.Deps) *OrderAmendmentWorkflowServer {
	return &OrderAmendmentWorkflowServer{orderAmendmentWorkflowHandlers: &orderAmendmentWorkflowHandlers{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.orders.v1.OrderAmendmentWorkflowService"),
		customerCalculationPort:        newCustomerCalculationPort(deps),
		clients:                        deps.Clients,
		householdValidationPort:        newHouseholdValidationPort(deps),
		venueValidationClient:          venuesv1.NewVenueValidationServiceClient(deps.Conn),
		disputeSyncClient:              paymentsv1.NewDisputeSyncServiceClient(deps.Conn),
		fraudSignalApprovalClient:      fraudv1.NewFraudSignalApprovalServiceClient(deps.Conn),
		customerQueryPort:              newCustomerQueryPort(deps),
		backorderSyncClient:            ordersv1.NewBackorderSyncServiceClient(deps.Conn),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		customerClient:                 customersv1.NewCustomerServiceClient(deps.Conn),
		searchQueryQueryClient:         searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
		orderReportingClient:           ordersv1.NewOrderReportingServiceClient(deps.Conn),
	}}
}

// StartOrderAmendmentWorkflow starts an order amendment.
func (s *orderAmendmentWorkflowHandlers) StartOrderAmendmentWorkflow(ctx context.Context, req *ordersv1.StartOrderAmendmentWorkflowRequest) (*ordersv1.StartOrderAmendmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "start order amendment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	explainCustomerCalculationRequest := &customersv1.ExplainCustomerCalculationRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
	}
	explainCustomerCalculationReply, err := s.customerCalculationPort.ExplainCustomerCalculation(ctx, explainCustomerCalculationRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/ExplainCustomerCalculation")
	}
	householdQuery, err := registry.Lookup[customersv1.HouseholdQueryServiceClient](s.clients, "acme.customers.v1.HouseholdQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	orderAmendmentCache := cache.New[string, *ordersv1.OrderAmendment](1)
	orderAmendmentCache.Put(req.GetContext().GetTraceId(), req.GetOrderAmendment())
	cachedOrderAmendment, _ := orderAmendmentCache.Get(req.GetContext().GetTraceId())
	findHouseholdsRequest := &customersv1.FindHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.FindHouseholdsRequest_Filter{
			Region:       explainCustomerCalculationReply.GetCustomer().GetPrimaryAddress().GetRegion(),
			CreatedAfter: cachedOrderAmendment.GetAudit().GetCreatedAt(),
			OwnerId:      req.GetOrderAmendment().GetOrderAmendmentId(),
			Labels:       explainCustomerCalculationReply.GetCustomer().GetAttributes(),
		},
	}
	findHouseholdsReply, err := householdQuery.FindHouseholds(ctx, findHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/FindHouseholds")
	}
	scoreHouseholdRequest := &customersv1.ScoreHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						PostalCode: req.GetOrderAmendment().GetBillingAddress().GetPostalCode(),
					},
					Audit: &commonv1.AuditInfo{
						CreatedBy: explainCustomerCalculationReply.GetCustomer().GetAudit().GetCreatedBy(),
					},
				}},
			}},
		},
	}
	scoreHouseholdReply, err := s.householdValidationPort.ScoreHousehold(ctx, scoreHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ScoreHousehold")
	}
	validateVenueRequest := buildValidateVenueRequestForStartOrderAmendmentWorkflow(req)
	validateVenueReply, err := s.venueValidationClient.ValidateVenue(ctx, validateVenueRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.venues.v1.VenueValidationService/ValidateVenue")
	}
	_ = scoreHouseholdReply
	return &ordersv1.StartOrderAmendmentWorkflowResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Audit: &commonv1.AuditInfo{
				Etag:      explainCustomerCalculationReply.GetCustomer().GetAudit().GetEtag(),
				UpdatedAt: validateVenueReply.GetVenue().GetAudit().GetUpdatedAt(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Counterparty:     req.GetOrderAmendment().GetCounterparty(),
		},
		Outcome: &ordersv1.StartOrderAmendmentWorkflowResponse_Outcome{
			Status:    findHouseholdsReply.GetResult().GetStatus(),
			NextSteps: validateVenueReply.GetOutcome().GetNextSteps(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceOrderAmendmentWorkflow advances an order amendment.
func (s *orderAmendmentWorkflowHandlers) AdvanceOrderAmendmentWorkflow(ctx context.Context, req *ordersv1.AdvanceOrderAmendmentWorkflowRequest) (*ordersv1.AdvanceOrderAmendmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "advance order amendment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetOrderAmendment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushDisputeRequest := &paymentsv1.PushDisputeRequest{
		Context: req.GetContext(),
		Dispute: &paymentsv1.Dispute{
			Audit: &commonv1.AuditInfo{
				CreatedBy: req.GetOrderAmendment().GetAudit().GetCreatedBy(),
				UpdatedBy: req.GetOrderAmendment().GetAudit().GetUpdatedBy(),
			},
			Requester: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
		},
		Details: &paymentsv1.PushDisputeRequest_Details{Payload: snapshot},
	}
	type pushDisputeResult struct {
		reply *paymentsv1.PushDisputeResponse
		err   error
	}
	pushDisputeResults := make(chan pushDisputeResult, 1)
	go func() {
		reply, err := s.disputeSyncClient.PushDispute(ctx, pushDisputeRequest)
		pushDisputeResults <- pushDisputeResult{reply, err}
	}()
	var pushDisputeReply *paymentsv1.PushDisputeResponse
	select {
	case result := <-pushDisputeResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.payments.v1.DisputeSyncService/PushDispute")
		}
		pushDisputeReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &ordersv1.AdvanceOrderAmendmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:  pushDisputeReply.GetResult().GetStatus(),
			Message: pushDisputeReply.GetResult().GetMessage(),
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Status:           req.GetOrderAmendment().GetStatus(),
		},
	}, nil
}

// CancelOrderAmendmentWorkflow cancels an order amendment.
func (s *orderAmendmentWorkflowHandlers) CancelOrderAmendmentWorkflow(ctx context.Context, req *ordersv1.CancelOrderAmendmentWorkflowRequest) (*ordersv1.CancelOrderAmendmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelOrderAmendmentWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "cancel order amendment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "display_name", req.GetOrderAmendment().GetCounterparty().GetDisplayName())
	recallFraudSignalReply, err := s.fraudSignalApprovalClient.RecallFraudSignal(ctx, &fraudv1.RecallFraudSignalRequest{
		Context: req.GetContext(),
		Details: &fraudv1.RecallFraudSignalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:       req.GetDetails().GetNotify().GetEmail(),
				DisplayName: tenancy.Value(ctx, "display_name"),
			},
		},
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalApprovalService/RecallFraudSignal")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	scheduleOrderAmendmentReportRequest := &ordersv1.ScheduleOrderAmendmentReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
	}
	scheduleOrderAmendmentReportReply, err := scheduleOrderAmendmentReportCall(ctx, scheduleOrderAmendmentReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderAmendmentReportingService/ScheduleOrderAmendmentReport")
	}
	ctx = tenancy.With(ctx, "region", req.GetOrderAmendment().GetBillingAddress().GetRegion())
	browseCustomersRequest := &customersv1.BrowseCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.BrowseCustomersRequest_Filter{
			Region:       tenancy.Value(ctx, "region"),
			CreatedAfter: req.GetOrderAmendment().GetIssuedAt(),
		},
	}
	browseCustomersReply, err := s.customerQueryPort.BrowseCustomers(ctx, browseCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/BrowseCustomers")
	}
	return &ordersv1.CancelOrderAmendmentWorkflowResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: recallFraudSignalReply.GetFraudSignal().GetSubject().GetDisplayName(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Notes:            req.GetOrderAmendment().GetNotes(),
		},
		Result: &commonv1.OperationResult{
			Status:       recallFraudSignalReply.GetResult().GetStatus(),
			ReferenceIds: browseCustomersReply.GetResult().GetReferenceIds(),
		},
		Outcome: &ordersv1.CancelOrderAmendmentWorkflowResponse_Outcome{
			Message: scheduleOrderAmendmentReportReply.GetResult().GetMessage(),
		},
	}, nil
}

// PauseOrderAmendmentWorkflow pauses an order amendment.
func (s *orderAmendmentWorkflowHandlers) PauseOrderAmendmentWorkflow(ctx context.Context, req *ordersv1.PauseOrderAmendmentWorkflowRequest) (*ordersv1.PauseOrderAmendmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "pause order amendment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetOrderAmendment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveBackorderConflictRequest := &ordersv1.ResolveBackorderConflictRequest{
		Context: req.GetContext(),
		Backorder: &ordersv1.Backorder{
			Assignee: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetOrderAmendment().GetAudit().GetUpdatedAt(),
			},
		},
		Details: &ordersv1.ResolveBackorderConflictRequest_Details{Payload: snapshot},
	}
	send := s.resolveBackorderConflictSender(resolveBackorderConflictRequest)
	resolveBackorderConflictReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.BackorderSyncService/ResolveBackorderConflict")
	}
	return &ordersv1.PauseOrderAmendmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: resolveBackorderConflictReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: resolveBackorderConflictReply.GetBackorder().GetCustomer().GetDisplayName(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Metadata:         req.GetOrderAmendment().GetMetadata(),
		},
	}, nil
}

// ResumeOrderAmendmentWorkflow resumes an order amendment.
func (s *orderAmendmentWorkflowHandlers) ResumeOrderAmendmentWorkflow(ctx context.Context, req *ordersv1.ResumeOrderAmendmentWorkflowRequest) (*ordersv1.ResumeOrderAmendmentWorkflowResponse, error) {
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "resume order amendment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	packed, err := anypb.New(req.GetOrderAmendment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareConsentRecordOptionsRequest := &customersv1.CompareConsentRecordOptionsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetOrderAmendment().GetCounterparty().GetDisplayName(),
			},
			Detail: packed,
		},
	}
	type compareConsentRecordOptionsResult struct {
		reply *customersv1.CompareConsentRecordOptionsResponse
		err   error
	}
	compareConsentRecordOptionsResults := make(chan compareConsentRecordOptionsResult, 1)
	go func() {
		reply, err := s.consentRecordCalculationClient.CompareConsentRecordOptions(ctx, compareConsentRecordOptionsRequest)
		compareConsentRecordOptionsResults <- compareConsentRecordOptionsResult{reply, err}
	}()
	var compareConsentRecordOptionsReply *customersv1.CompareConsentRecordOptionsResponse
	select {
	case result := <-compareConsentRecordOptionsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordCalculationService/CompareConsentRecordOptions")
		}
		compareConsentRecordOptionsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	ctx = tenancy.With(ctx, "region", req.GetOrderAmendment().GetBillingAddress().GetRegion())
	watchCustomersStream, err := s.customerClient.WatchCustomers(ctx, &customersv1.WatchCustomersRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			Email: req.GetDetails().GetNotify().GetEmail(),
			Name:  req.GetDetails().GetNotify().GetName(),
		},
		Filter: &customersv1.WatchCustomersRequest_Filter{Region: tenancy.Value(ctx, "region")},
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
	summarizeSearchQueriesRequest := &searchv1.SummarizeSearchQueriesRequest{}
	summarizeSearchQueriesRequest.Context = req.GetContext()
	summarizeSearchQueriesRequest.Reason = req.GetReason()
	summarizeSearchQueriesReply, err := s.searchQueryQueryClient.SummarizeSearchQueries(ctx, summarizeSearchQueriesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/SummarizeSearchQueries")
	}
	detailsCache := cache.New[string, *ordersv1.ResumeOrderAmendmentWorkflowRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	shareOrderReportReply, err := s.orderReportingClient.ShareOrderReport(ctx, &ordersv1.ShareOrderReportRequest{
		Context: req.GetContext(),
		Details: &ordersv1.ShareOrderReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Phone: cachedDetails.GetNotify().GetPhone(),
				Email: req.GetDetails().GetNotify().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.orders.v1.OrderReportingService/ShareOrderReport")
	}
	return &ordersv1.ResumeOrderAmendmentWorkflowResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			Counterparty: &ordersv1.BackorderRef{
				DisplayName: compareConsentRecordOptionsReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			IssuedAt:         shareOrderReportReply.GetOrder().GetIssuedAt(),
			OrderAmendmentId: req.GetOrderAmendmentId(),
			Number:           req.GetOrderAmendment().GetNumber(),
		},
		Outcome: &ordersv1.ResumeOrderAmendmentWorkflowResponse_Outcome{
			Status:  compareConsentRecordOptionsReply.GetResult().GetStatus(),
			Message: summarizeSearchQueriesReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{Status: shareOrderReportReply.GetOutcome().GetStatus()},
	}, nil
}

// InspectOrderAmendmentWorkflow returns an order amendment.
func (s *orderAmendmentWorkflowHandlers) InspectOrderAmendmentWorkflow(ctx context.Context, req *ordersv1.InspectOrderAmendmentWorkflowRequest) (*ordersv1.InspectOrderAmendmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "order_amendment", req.GetOrderAmendmentId(), "inspect_order_amendment_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect order amendment workflow", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.InspectOrderAmendmentWorkflowResponse{
		OrderAmendment: &ordersv1.OrderAmendment{OrderAmendmentId: req.GetOrderAmendmentId()},
		Result:         &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListOrderAmendmentWorkflows lists order amendments a page at a time.
func (s *orderAmendmentWorkflowHandlers) ListOrderAmendmentWorkflows(ctx context.Context, req *ordersv1.ListOrderAmendmentWorkflowsRequest) (*ordersv1.ListOrderAmendmentWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list order amendment workflows", "tenant", req.GetContext().GetTenantId())
	return &ordersv1.ListOrderAmendmentWorkflowsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// SignalOrderAmendmentWorkflow signals an order amendment.
func (s *orderAmendmentWorkflowHandlers) SignalOrderAmendmentWorkflow(ctx context.Context, req *ordersv1.SignalOrderAmendmentWorkflowRequest) (*ordersv1.SignalOrderAmendmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveOrderAmendment(ctx, req.GetContext().GetTenantId(), req.GetOrderAmendment()); err != nil {
		return nil, grpcx.Wrap(err, "save order amendment")
	}
	s.log.InfoContext(ctx, "signal order amendment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetOrderAmendment().GetAudit().GetEtag())
	customerCalculation, err := registry.Lookup[customersv1.CustomerCalculationServiceClient](s.clients, "acme.customers.v1.CustomerCalculationService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	estimateCustomerRequest := &customersv1.EstimateCustomerRequest{
		Context: req.GetContext(),
		Details: &customersv1.EstimateCustomerRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: req.GetOrderAmendment().GetBillingAddress().GetLocality(),
			},
		},
	}
	estimateCustomerReply, err := customerCalculation.EstimateCustomer(ctx, estimateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	batchGetCustomersRequest := &customersv1.BatchGetCustomersRequest{Context: req.GetContext()}
	send := s.batchGetCustomersSender(batchGetCustomersRequest)
	batchGetCustomersReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/BatchGetCustomers")
	}
	return &ordersv1.SignalOrderAmendmentWorkflowResponse{
		OrderAmendment: &ordersv1.OrderAmendment{
			BillingAddress: &commonv1.PostalAddress{
				Region: estimateCustomerReply.GetCustomer().GetPrimaryAddress().GetRegion(),
			},
			OrderAmendmentId: req.GetOrderAmendmentId(),
			IssuedAt:         req.GetOrderAmendment().GetIssuedAt(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: batchGetCustomersReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Outcome: &ordersv1.SignalOrderAmendmentWorkflowResponse_Outcome{Message: batchGetCustomersReply.GetResult().GetMessage()},
	}, nil
}

// resolveBackorderConflictSender returns a function that sends one request to
// acme.orders.v1.BackorderSyncService/ResolveBackorderConflict.
func (s *orderAmendmentWorkflowHandlers) resolveBackorderConflictSender(request *ordersv1.ResolveBackorderConflictRequest) func(context.Context) (*ordersv1.ResolveBackorderConflictResponse, error) {
	return func(ctx context.Context) (*ordersv1.ResolveBackorderConflictResponse, error) {
		return s.backorderSyncClient.ResolveBackorderConflict(ctx, request)
	}
}

// batchGetCustomersSender returns a function that sends one request to
// acme.customers.v1.CustomerService/BatchGetCustomers.
func (s *orderAmendmentWorkflowHandlers) batchGetCustomersSender(request *customersv1.BatchGetCustomersRequest) func(context.Context) (*customersv1.BatchGetCustomersResponse, error) {
	return func(ctx context.Context) (*customersv1.BatchGetCustomersResponse, error) {
		return s.customerClient.BatchGetCustomers(ctx, request)
	}
}
