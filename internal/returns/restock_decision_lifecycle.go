package returns

import (
	"context"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	invoicingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/invoicing/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// RestockDecisionLifecycleServer implements `acme.returns.v1.RestockDecisionLifecycleService`.
// Moves a restock decision between the states of its lifecycle.
type RestockDecisionLifecycleServer struct {
	returnsv1.UnimplementedRestockDecisionLifecycleServiceServer
	store                            *Store
	log                              *slog.Logger
	inspectionCalculationClient      returnsv1.InspectionCalculationServiceClient
	restockDecisionCalculationClient returnsv1.RestockDecisionCalculationServiceClient
	sellerScorecardReportingClient   sellersv1.SellerScorecardReportingServiceClient
	conn                             *grpc.ClientConn
	restockDecisionReportingPort     restockDecisionReportingPort
	clients                          *registry.Registry
	customerCalculationClient        customersv1.CustomerCalculationServiceClient
	preferenceCalculationClient      customersv1.PreferenceCalculationServiceClient
	invoiceBatchCalculationOnce      sync.Once
	invoiceBatchCalculationCached    invoicingv1.InvoiceBatchCalculationServiceClient
	householdQueryClient             customersv1.HouseholdQueryServiceClient
	returnLabelCalculationClient     returnsv1.ReturnLabelCalculationServiceClient
}

// NewRestockDecisionLifecycleServer wires a RestockDecisionLifecycleServer.
func NewRestockDecisionLifecycleServer(deps platform.Deps) *RestockDecisionLifecycleServer {
	return &RestockDecisionLifecycleServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.returns.v1.RestockDecisionLifecycleService"),
		inspectionCalculationClient:      returnsv1.NewInspectionCalculationServiceClient(deps.Conn),
		restockDecisionCalculationClient: returnsv1.NewRestockDecisionCalculationServiceClient(deps.Conn),
		sellerScorecardReportingClient:   sellersv1.NewSellerScorecardReportingServiceClient(deps.Conn),
		conn:                             deps.Conn,
		restockDecisionReportingPort:     newRestockDecisionReportingPort(deps),
		clients:                          deps.Clients,
		customerCalculationClient:        customersv1.NewCustomerCalculationServiceClient(deps.Conn),
		preferenceCalculationClient:      customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		householdQueryClient:             customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		returnLabelCalculationClient:     returnsv1.NewReturnLabelCalculationServiceClient(deps.Conn),
	}
}

// ActivateRestockDecision activates a restock decision.
func (s *RestockDecisionLifecycleServer) ActivateRestockDecision(ctx context.Context, req *returnsv1.ActivateRestockDecisionRequest) (*returnsv1.ActivateRestockDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "activate_restock_decision"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate restock decision", "tenant", req.GetContext().GetTenantId())
	calculateInspectionReply, err := s.inspectionCalculationClient.CalculateInspection(ctx, &returnsv1.CalculateInspectionRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				UserId: req.GetDetails().GetNewOwner().GetUserId(),
				Email:  req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionCalculationService/CalculateInspection")
	}
	estimateRestockDecisionRequest := buildEstimateRestockDecisionRequestForActivateRestockDecision(req, calculateInspectionReply)
	estimateRestockDecisionReply, err := s.restockDecisionCalculationClient.EstimateRestockDecision(ctx, estimateRestockDecisionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionCalculationService/EstimateRestockDecision")
	}
	return &returnsv1.ActivateRestockDecisionResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				Etag: calculateInspectionReply.GetInspection().GetAudit().GetEtag(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
		},
		Outcome: &returnsv1.ActivateRestockDecisionResponse_Outcome{
			Message:     estimateRestockDecisionReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: estimateRestockDecisionReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SuspendRestockDecision suspends a restock decision.
func (s *RestockDecisionLifecycleServer) SuspendRestockDecision(ctx context.Context, req *returnsv1.SuspendRestockDecisionRequest) (*returnsv1.SuspendRestockDecisionResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "suspend_restock_decision"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend restock decision", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetNewOwner().GetUserId())
	countSellerScorecardTotalsRequest := &sellersv1.CountSellerScorecardTotalsRequest{}
	countSellerScorecardTotalsRequest.Context = req.GetContext()
	countSellerScorecardTotalsRequest.Filter = &sellersv1.CountSellerScorecardTotalsRequest_Filter{
		OwnerId:      tenancy.Value(ctx, "user_id"),
		CreatedAfter: req.GetDetails().GetEffectiveAt(),
		Labels:       req.GetAnnotations(),
	}
	countSellerScorecardTotalsReply, err := s.sellerScorecardReportingClient.CountSellerScorecardTotals(ctx, countSellerScorecardTotalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardReportingService/CountSellerScorecardTotals")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	searchCustomersRequest := &customersv1.SearchCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.SearchCustomersRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			Labels:       annotations,
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}
	searchCustomersReply, err := grpcx.NewClient(s.conn, customersv1.NewCustomerServiceClient).SearchCustomers(ctx, searchCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerService/SearchCustomers")
	}
	return &returnsv1.SuspendRestockDecisionResponse{
		Result: &commonv1.OperationResult{
			Message: countSellerScorecardTotalsReply.GetResult().GetMessage(),
			Details: countSellerScorecardTotalsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &returnsv1.SuspendRestockDecisionResponse_Outcome{
			Status:      searchCustomersReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		RestockDecision: &returnsv1.RestockDecision{RestockDecisionId: req.GetRestockDecisionId()},
	}, nil
}

// TerminateRestockDecision terminates a restock decision.
func (s *RestockDecisionLifecycleServer) TerminateRestockDecision(ctx context.Context, req *returnsv1.TerminateRestockDecisionRequest) (*returnsv1.TerminateRestockDecisionResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "terminate_restock_decision"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate restock decision", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *returnsv1.TerminateRestockDecisionRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	calculateRestockDecisionRequest := &returnsv1.CalculateRestockDecisionRequest{
		Context: req.GetContext(),
		RestockDecision: &returnsv1.RestockDecision{
			Owner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetNewOwner().GetUserId(),
				Email:       cachedDetails.GetNotifyContact().GetEmail(),
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	type calculateRestockDecisionResult struct {
		reply *returnsv1.CalculateRestockDecisionResponse
		err   error
	}
	calculateRestockDecisionResults := make(chan calculateRestockDecisionResult, 1)
	go func() {
		reply, err := s.restockDecisionCalculationClient.CalculateRestockDecision(ctx, calculateRestockDecisionRequest)
		calculateRestockDecisionResults <- calculateRestockDecisionResult{reply, err}
	}()
	var calculateRestockDecisionReply *returnsv1.CalculateRestockDecisionResponse
	select {
	case result := <-calculateRestockDecisionResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.returns.v1.RestockDecisionCalculationService/CalculateRestockDecision")
		}
		calculateRestockDecisionReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	deleteRestockDecisionReportRequest := &returnsv1.DeleteRestockDecisionReportRequest{
		Context:           req.GetContext(),
		RestockDecisionId: req.GetRestockDecisionId(),
		Reason:            req.GetReason(),
	}
	deleteRestockDecisionReportReply, err := s.restockDecisionReportingPort.DeleteRestockDecisionReport(ctx, deleteRestockDecisionReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.RestockDecisionReportingService/DeleteRestockDecisionReport")
	}
	sellerListingWorkflow, ok := s.clients.Get("acme.sellers.v1.SellerListingWorkflowService").(sellersv1.SellerListingWorkflowServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.sellers.v1.SellerListingWorkflowService")
	}
	listSellerListingWorkflowsRequest := &sellersv1.ListSellerListingWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &sellersv1.ListSellerListingWorkflowsRequest_Filter{
			CreatedAfter: calculateRestockDecisionReply.GetRestockDecision().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
			Labels:       calculateRestockDecisionReply.GetRestockDecision().GetParameters(),
		},
	}
	listSellerListingWorkflowsReply, err := sellerListingWorkflow.ListSellerListingWorkflows(ctx, listSellerListingWorkflowsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingWorkflowService/ListSellerListingWorkflows")
	}
	_ = listSellerListingWorkflowsReply
	return &returnsv1.TerminateRestockDecisionResponse{
		RestockDecision: &returnsv1.RestockDecision{
			Effective:         calculateRestockDecisionReply.GetRestockDecision().GetEffective(),
			Priority:          calculateRestockDecisionReply.GetRestockDecision().GetPriority(),
			RestockDecisionId: req.GetRestockDecisionId(),
		},
		Result: &commonv1.OperationResult{
			Message: deleteRestockDecisionReportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &returnsv1.TerminateRestockDecisionResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewRestockDecision renews a restock decision.
func (s *RestockDecisionLifecycleServer) RenewRestockDecision(ctx context.Context, req *returnsv1.RenewRestockDecisionRequest) (*returnsv1.RenewRestockDecisionResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "renew_restock_decision"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew restock decision", "tenant", req.GetContext().GetTenantId())
	source := req.GetDetails().GetNotifyContact().ProtoReflect()
	phone := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("phone"))).String()
	simulateCustomerRequest := newSimulateCustomerRequest(withSimulateCustomerContext(req.GetContext()), withSimulateCustomerCustomer(&customersv1.Customer{
		Contact: &commonv1.ContactInfo{
			Phone: phone,
			Email: req.GetDetails().GetNotifyContact().GetEmail(),
		},
	}))
	simulateCustomerReply, err := s.customerCalculationClient.SimulateCustomer(ctx, simulateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/SimulateCustomer")
	}
	step := &restockDecisionLifecycleServerRenewRestockDecisionRecalculatePreferencesStep{request: &customersv1.RecalculatePreferencesRequest{
		Context: req.GetContext(),
		Details: &customersv1.RecalculatePreferencesRequest_Details{
			Destination: &commonv1.PostalAddress{
				Locality: simulateCustomerReply.GetCustomer().GetPrimaryAddress().GetLocality(),
			},
		},
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: simulateCustomerReply.GetCustomer().GetAudit().GetUpdatedAt(),
			},
		},
	}}
	if err := s.runRenewRestockDecisionRecalculatePreferences(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/RecalculatePreferences")
	}
	recalculatePreferencesReply := step.reply
	compareInvoiceBatchOptionsRequest := &invoicingv1.CompareInvoiceBatchOptionsRequest{
		Context: req.GetContext(),
		InvoiceBatch: &invoicingv1.InvoiceBatch{
			BillingAddress: &commonv1.PostalAddress{
				PostalCode: simulateCustomerReply.GetCustomer().GetPrimaryAddress().GetPostalCode(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: simulateCustomerReply.GetCustomer().GetAudit().GetCreatedAt(),
				CreatedBy: simulateCustomerReply.GetCustomer().GetAudit().GetCreatedBy(),
			},
		},
	}
	compareInvoiceBatchOptionsReply, err := s.invoiceBatchCalculationLazy().CompareInvoiceBatchOptions(ctx, compareInvoiceBatchOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.invoicing.v1.InvoiceBatchCalculationService/CompareInvoiceBatchOptions")
	}
	pointsLedgerApproval, ok := s.clients.Get("acme.loyalty.v1.PointsLedgerApprovalService").(loyaltyv1.PointsLedgerApprovalServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.loyalty.v1.PointsLedgerApprovalService")
	}
	rejectPointsLedgerRequest := &loyaltyv1.RejectPointsLedgerRequest{
		Context: req.GetContext(),
		PointsLedger: &loyaltyv1.PointsLedger{
			Funding: &loyaltyv1.PointsLedger_Bank{Bank: &loyaltyv1.PointsLedgerBankFunding{
				Bic: compareInvoiceBatchOptionsReply.GetInvoiceBatch().GetBank().GetBic(),
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: recalculatePreferencesReply.GetPreference().GetAudit().GetCreatedAt(),
			},
		},
	}
	rejectPointsLedgerReply, err := pointsLedgerApproval.RejectPointsLedger(ctx, rejectPointsLedgerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.PointsLedgerApprovalService/RejectPointsLedger")
	}
	return &returnsv1.RenewRestockDecisionResponse{
		Result: &commonv1.OperationResult{Status: simulateCustomerReply.GetResult().GetStatus()},
		RestockDecision: &returnsv1.RestockDecision{
			Effective: recalculatePreferencesReply.GetPreference().GetEffective(),
			Owner: &identityv1.UserRef{
				DisplayName: compareInvoiceBatchOptionsReply.GetInvoiceBatch().GetCounterparty().GetDisplayName(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: rejectPointsLedgerReply.GetPointsLedger().GetAudit().GetEtag(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
		},
		Outcome: &returnsv1.RenewRestockDecisionResponse_Outcome{
			Message:     compareInvoiceBatchOptionsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// GetRestockDecisionLifecycle returns a restock decision.
func (s *RestockDecisionLifecycleServer) GetRestockDecisionLifecycle(ctx context.Context, req *returnsv1.GetRestockDecisionLifecycleRequest) (*returnsv1.GetRestockDecisionLifecycleResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "restock_decision", req.GetRestockDecisionId(), "get_restock_decision_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get restock decision lifecycle", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	findHouseholdsRequest := &customersv1.FindHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.FindHouseholdsRequest_Filter{
			OwnerId:      req.GetRestockDecisionId(),
			CreatedAfter: req.GetContext().GetDeadline(),
			Labels:       baggage,
		},
	}
	findHouseholdsReply, err := grpcx.Call(ctx, s.householdQueryClient.FindHouseholds, findHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/FindHouseholds")
	}
	return &returnsv1.GetRestockDecisionLifecycleResponse{
		Result: &commonv1.OperationResult{
			Details: findHouseholdsReply.GetResult().GetDetails(),
			Message: findHouseholdsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		RestockDecision: &returnsv1.RestockDecision{RestockDecisionId: req.GetRestockDecisionId()},
	}, nil
}

// ScheduleRestockDecisionExpiry schedules a restock decision.
func (s *RestockDecisionLifecycleServer) ScheduleRestockDecisionExpiry(ctx context.Context, req *returnsv1.ScheduleRestockDecisionExpiryRequest) (*returnsv1.ScheduleRestockDecisionExpiryResponse, error) {
	if err := s.store.SaveRestockDecision(ctx, req.GetContext().GetTenantId(), req.GetRestockDecision()); err != nil {
		return nil, grpcx.Wrap(err, "save restock decision")
	}
	s.log.InfoContext(ctx, "schedule restock decision expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRestockDecision().GetAudit().GetEtag())
	auditEventQuery, err := registry.Lookup[auditv1.AuditEventQueryServiceClient](s.clients, "acme.audit.v1.AuditEventQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	findAuditEventsRequest := &auditv1.FindAuditEventsRequest{
		Context: req.GetContext(),
		Filter:  &auditv1.FindAuditEventsRequest_Filter{OwnerId: req.GetRestockDecision().GetOwner().GetUserId()},
	}
	findAuditEventsReply, err := auditEventQuery.FindAuditEvents(ctx, findAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/FindAuditEvents")
	}
	packed, err := anypb.New(req.GetRestockDecision())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	calculateConsentRecordRequest := &customersv1.CalculateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetRestockDecision().GetOwner().GetDisplayName(),
			},
			CorrelationId: findAuditEventsReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
	}
	calculateConsentRecordReply := new(customersv1.CalculateConsentRecordResponse)
	if err := grpcx.Invoke(ctx, s.conn, customersv1.ConsentRecordCalculationService_CalculateConsentRecord_FullMethodName, calculateConsentRecordRequest, calculateConsentRecordReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
	}
	estimateReturnLabelRequest := &returnsv1.EstimateReturnLabelRequest{
		Context: req.GetContext(),
		ReturnLabel: &returnsv1.ReturnLabel{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: req.GetRestockDecision().GetAudit().GetUpdatedAt(),
				UpdatedBy: req.GetRestockDecision().GetAudit().GetUpdatedBy(),
			},
		},
	}
	estimateReturnLabelReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*returnsv1.EstimateReturnLabelResponse, error) {
		return s.returnLabelCalculationClient.EstimateReturnLabel(ctx, estimateReturnLabelRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelCalculationService/EstimateReturnLabel")
	}
	cloned := proto.Clone(req.GetRestockDecision()).(*returnsv1.RestockDecision)
	estimateCustomerRequest := newEstimateCustomerRequest(withEstimateCustomerContext(req.GetContext()), withEstimateCustomerCustomer(&customersv1.Customer{
		Audit:               &commonv1.AuditInfo{UpdatedAt: cloned.GetAudit().GetUpdatedAt()},
		RelationshipManager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
	}), withEstimateCustomerDetails(&customersv1.EstimateCustomerRequest_Details{
		Destination: &commonv1.PostalAddress{
			PostalCode: estimateReturnLabelReply.GetReturnLabel().GetBillingAddress().GetPostalCode(),
			Recipient:  estimateReturnLabelReply.GetReturnLabel().GetBillingAddress().GetRecipient(),
		},
	}))
	estimateCustomerReply, err := s.customerCalculationClient.EstimateCustomer(ctx, estimateCustomerRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/EstimateCustomer")
	}
	return &returnsv1.ScheduleRestockDecisionExpiryResponse{
		Result: &commonv1.OperationResult{
			Status:        findAuditEventsReply.GetResult().GetStatus(),
			Details:       calculateConsentRecordReply.GetResult().GetDetails(),
			CorrelationId: estimateCustomerReply.GetResult().GetCorrelationId(),
		},
		RestockDecision: &returnsv1.RestockDecision{
			Audit: &commonv1.AuditInfo{
				CreatedAt: estimateReturnLabelReply.GetReturnLabel().GetAudit().GetCreatedAt(),
			},
			Owner: &identityv1.UserRef{
				DisplayName: estimateReturnLabelReply.GetReturnLabel().GetCounterparty().GetDisplayName(),
			},
			RestockDecisionId: req.GetRestockDecisionId(),
			Parameters:        req.GetRestockDecision().GetParameters(),
		},
	}, nil
}

// restockDecisionLifecycleServerRenewRestockDecisionRecalculatePreferencesStep carries the request of one step of
// RenewRestockDecision and, once it ran, its reply.
type restockDecisionLifecycleServerRenewRestockDecisionRecalculatePreferencesStep struct {
	request *customersv1.RecalculatePreferencesRequest
	reply   *customersv1.RecalculatePreferencesResponse
}

func (s *RestockDecisionLifecycleServer) runRenewRestockDecisionRecalculatePreferences(ctx context.Context, step *restockDecisionLifecycleServerRenewRestockDecisionRecalculatePreferencesStep) error {
	reply, err := s.preferenceCalculationClient.RecalculatePreferences(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// invoiceBatchCalculationLazy dials acme.invoicing.v1.InvoiceBatchCalculationService the first time a handler needs it.
func (s *RestockDecisionLifecycleServer) invoiceBatchCalculationLazy() invoicingv1.InvoiceBatchCalculationServiceClient {
	s.invoiceBatchCalculationOnce.Do(func() { s.invoiceBatchCalculationCached = invoicingv1.NewInvoiceBatchCalculationServiceClient(s.conn) })
	return s.invoiceBatchCalculationCached
}
