package fraud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// RiskAssessmentWorkflowServer implements `acme.fraud.v1.RiskAssessmentWorkflowService`.
// Runs the long-lived workflow that takes a risk assessment from start to finish.
type RiskAssessmentWorkflowServer struct {
	fraudv1.UnimplementedRiskAssessmentWorkflowServiceServer
	store                          *Store
	log                            *slog.Logger
	householdCalculationClient     customersv1.HouseholdCalculationServiceClient
	stockMovementWorkflowClient    inventoryv1.StockMovementWorkflowServiceClient
	conn                           *grpc.ClientConn
	searchIndexValidationClient    searchv1.SearchIndexValidationServiceClient
	householdQueryClient           customersv1.HouseholdQueryServiceClient
	bus                            *eventbus.Bus
	ledgerAccountClient            ledgerv1.LedgerAccountServiceClient
	fraudSignalLifecycleClient     fraudv1.FraudSignalLifecycleServiceClient
	searchIndexExportClient        searchv1.SearchIndexExportServiceClient
	clients                        *registry.Registry
	fraudSignalQueryClient         fraudv1.FraudSignalQueryServiceClient
	consentRecordCalculationClient customersv1.ConsentRecordCalculationServiceClient
	riskAssessmentApprovalClient   fraudv1.RiskAssessmentApprovalServiceClient
	watchlistReportingClient       fraudv1.WatchlistReportingServiceClient
	customerClient                 customersv1.CustomerServiceClient
	accessPolicyClient             authorizationv1.AccessPolicyServiceClient
	preferenceCalculationClient    customersv1.PreferenceCalculationServiceClient
}

// NewRiskAssessmentWorkflowServer wires a RiskAssessmentWorkflowServer.
func NewRiskAssessmentWorkflowServer(deps platform.Deps) *RiskAssessmentWorkflowServer {
	return &RiskAssessmentWorkflowServer{
		store:                          NewStore(deps.DB),
		log:                            deps.Logger.With("service", "acme.fraud.v1.RiskAssessmentWorkflowService"),
		householdCalculationClient:     customersv1.NewHouseholdCalculationServiceClient(deps.Conn),
		stockMovementWorkflowClient:    inventoryv1.NewStockMovementWorkflowServiceClient(deps.Conn),
		conn:                           deps.Conn,
		searchIndexValidationClient:    searchv1.NewSearchIndexValidationServiceClient(deps.Conn),
		householdQueryClient:           customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		bus:                            deps.Bus,
		ledgerAccountClient:            ledgerv1.NewLedgerAccountServiceClient(deps.Conn),
		fraudSignalLifecycleClient:     fraudv1.NewFraudSignalLifecycleServiceClient(deps.Conn),
		searchIndexExportClient:        searchv1.NewSearchIndexExportServiceClient(deps.Conn),
		clients:                        deps.Clients,
		fraudSignalQueryClient:         fraudv1.NewFraudSignalQueryServiceClient(deps.Conn),
		consentRecordCalculationClient: customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		riskAssessmentApprovalClient:   fraudv1.NewRiskAssessmentApprovalServiceClient(deps.Conn),
		watchlistReportingClient:       fraudv1.NewWatchlistReportingServiceClient(deps.Conn),
		customerClient:                 customersv1.NewCustomerServiceClient(deps.Conn),
		accessPolicyClient:             authorizationv1.NewAccessPolicyServiceClient(deps.Conn),
		preferenceCalculationClient:    customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
	}
}

// StartRiskAssessmentWorkflow starts a risk assessment.
func (s *RiskAssessmentWorkflowServer) StartRiskAssessmentWorkflow(ctx context.Context, req *fraudv1.StartRiskAssessmentWorkflowRequest) (*fraudv1.StartRiskAssessmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "start risk assessment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	calculateHouseholdRequest := buildCalculateHouseholdRequestForStartRiskAssessmentWorkflow(req)
	calculateHouseholdReply, err := s.householdCalculationClient.CalculateHousehold(ctx, calculateHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/CalculateHousehold")
	}
	packed, err := anypb.New(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	retryStockMovementWorkflowReply, err := s.stockMovementWorkflowClient.RetryStockMovementWorkflow(ctx, &inventoryv1.RetryStockMovementWorkflowRequest{
		Context: req.GetContext(),
		StockMovement: &inventoryv1.StockMovement{
			Subject: &customersv1.CustomerRef{
				DisplayName: req.GetRiskAssessment().GetAssignee().GetDisplayName(),
				Email:       req.GetRiskAssessment().GetAssignee().GetEmail(),
			},
			Detail: packed,
		},
		Details: &inventoryv1.RetryStockMovementWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotify().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementWorkflowService/RetryStockMovementWorkflow")
	}
	recalculateHouseholdsRequest := &customersv1.RecalculateHouseholdsRequest{}
	recalculateHouseholdsRequest.Context = req.GetContext()
	recalculateHouseholdsRequest.Household = &customersv1.Household{
		Children: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Audit: &commonv1.AuditInfo{
					Etag: calculateHouseholdReply.GetHousehold().GetAudit().GetEtag(),
				},
				Location: &commonv1.PostalAddress{
					Locality: calculateHouseholdReply.GetHousehold().GetLocation().GetLocality(),
					Lines:    calculateHouseholdReply.GetHousehold().GetLocation().GetLines(),
				},
			}},
		}},
	}
	recalculateHouseholdsReply, err := s.householdCalculationClient.RecalculateHouseholds(ctx, recalculateHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/RecalculateHouseholds")
	}
	return &fraudv1.StartRiskAssessmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: calculateHouseholdReply.GetResult().GetMessage(),
			Status:  retryStockMovementWorkflowReply.GetOutcome().GetStatus(),
		},
		Outcome: &fraudv1.StartRiskAssessmentWorkflowResponse_Outcome{
			EffectiveAt: retryStockMovementWorkflowReply.GetOutcome().GetEffectiveAt(),
			Status:      recalculateHouseholdsReply.GetResult().GetStatus(),
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Status:           req.GetRiskAssessment().GetStatus(),
		},
	}, nil
}

// AdvanceRiskAssessmentWorkflow advances a risk assessment.
func (s *RiskAssessmentWorkflowServer) AdvanceRiskAssessmentWorkflow(ctx context.Context, req *fraudv1.AdvanceRiskAssessmentWorkflowRequest) (*fraudv1.AdvanceRiskAssessmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "advance risk assessment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	validateWatchlistRequest := &fraudv1.ValidateWatchlistRequest{
		Context: req.GetContext(),
		Watchlist: &fraudv1.Watchlist{
			Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
				Children: []*fraudv1.Watchlist{&fraudv1.Watchlist{
					Audit: &commonv1.AuditInfo{
						CreatedAt: req.GetRiskAssessment().GetAudit().GetCreatedAt(),
						CreatedBy: req.GetRiskAssessment().GetAudit().GetCreatedBy(),
					},
					Manager: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
				}},
			}},
		},
	}
	validateWatchlistReply, err := grpcx.NewClient(s.conn, fraudv1.NewWatchlistValidationServiceClient).ValidateWatchlist(ctx, validateWatchlistRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistValidationService/ValidateWatchlist")
	}
	cloned := proto.Clone(req.GetRiskAssessment()).(*fraudv1.RiskAssessment)
	explainSearchIndexDecisionReply, err := s.searchIndexValidationClient.ExplainSearchIndexDecision(ctx, &searchv1.ExplainSearchIndexDecisionRequest{
		Context: req.GetContext(),
		SearchIndex: &searchv1.SearchIndex{
			Owner: &identityv1.UserRef{Email: req.GetRiskAssessment().GetCustomer().GetEmail()},
			Audit: &commonv1.AuditInfo{UpdatedAt: cloned.GetAudit().GetUpdatedAt()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexValidationService/ExplainSearchIndexDecision")
	}
	exportHouseholdsStream, err := s.householdQueryClient.ExportHouseholds(ctx, &customersv1.ExportHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportHouseholdsRequest_Filter{
			Region: explainSearchIndexDecisionReply.GetSearchIndex().GetAddress().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
	}
	for {
		received, err := exportHouseholdsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/ExportHouseholds")
		}
		_ = received
	}
	return &fraudv1.AdvanceRiskAssessmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: validateWatchlistReply.GetResult().GetCorrelationId(),
			ReferenceIds:  explainSearchIndexDecisionReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: explainSearchIndexDecisionReply.GetSearchIndex().GetAudit().GetUpdatedBy(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Priority:         req.GetRiskAssessment().GetPriority(),
		},
	}, nil
}

// CancelRiskAssessmentWorkflow cancels a risk assessment.
func (s *RiskAssessmentWorkflowServer) CancelRiskAssessmentWorkflow(ctx context.Context, req *fraudv1.CancelRiskAssessmentWorkflowRequest) (*fraudv1.CancelRiskAssessmentWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CancelRiskAssessmentWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "cancel risk assessment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "fraud.cancel_risk_assessment_workflow.score_consent_record",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"consent_record.subject.display_name": req.GetRiskAssessment().GetAssignee().GetDisplayName(),
			"context.tenant_id":                   req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	parameters := make(map[string]string, len(req.GetDetails().GetParameters()))
	for key, value := range req.GetDetails().GetParameters() {
		parameters[key] = value
	}
	ctx = tenancy.With(ctx, "customer_id", req.GetRiskAssessment().GetCustomer().GetCustomerId())
	listLedgerAccountsReply, err := s.ledgerAccountClient.ListLedgerAccounts(ctx, &ledgerv1.ListLedgerAccountsRequest{
		Context: req.GetContext(),
		Filter: &ledgerv1.ListLedgerAccountsRequest_Filter{
			CreatedAfter: req.GetRiskAssessment().GetAudit().GetCreatedAt(),
			Labels:       parameters,
			OwnerId:      tenancy.Value(ctx, "customer_id"),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountService/ListLedgerAccounts")
	}
	packed, err := anypb.New(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	scheduleFraudSignalExpiryRequest := &fraudv1.ScheduleFraudSignalExpiryRequest{
		Context: req.GetContext(),
		Details: &fraudv1.ScheduleFraudSignalExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:  req.GetRiskAssessment().GetRequester().GetName(),
				Phone: req.GetRiskAssessment().GetRequester().GetPhone(),
			},
			Timeout: req.GetDetails().GetTimeout(),
		},
		FraudSignal: &fraudv1.FraudSignal{Detail: packed},
	}
	var scheduleFraudSignalExpiryReply *fraudv1.ScheduleFraudSignalExpiryResponse
	if err := pipeline.Run(ctx, "acme.fraud.v1.FraudSignalLifecycleService/ScheduleFraudSignalExpiry", func(ctx context.Context) error {
		answer, err := s.fraudSignalLifecycleClient.ScheduleFraudSignalExpiry(ctx, scheduleFraudSignalExpiryRequest)
		scheduleFraudSignalExpiryReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalLifecycleService/ScheduleFraudSignalExpiry")
	}
	return &fraudv1.CancelRiskAssessmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  listLedgerAccountsReply.GetResult().GetReferenceIds(),
			CorrelationId: scheduleFraudSignalExpiryReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &fraudv1.CancelRiskAssessmentWorkflowResponse_Outcome{
			Status: scheduleFraudSignalExpiryReply.GetResult().GetStatus(),
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Assignee:         req.GetRiskAssessment().GetAssignee(),
		},
	}, nil
}

// RetryRiskAssessmentWorkflow retrys a risk assessment.
func (s *RiskAssessmentWorkflowServer) RetryRiskAssessmentWorkflow(ctx context.Context, req *fraudv1.RetryRiskAssessmentWorkflowRequest) (*fraudv1.RetryRiskAssessmentWorkflowResponse, error) {
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "retry risk assessment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	recallReturnRequestRequest := &returnsv1.RecallReturnRequestRequest{
		Context: req.GetContext(),
		Details: &returnsv1.RecallReturnRequestRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  req.GetDetails().GetNotify().GetEmail(),
				UserId: req.GetRiskAssessment().GetAssignee().GetUserId(),
			},
		},
		ReturnRequest: &returnsv1.ReturnRequest{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: req.GetRiskAssessment().GetCustomer().GetDisplayName(),
			},
		},
	}
	recallReturnRequestReply := new(returnsv1.RecallReturnRequestResponse)
	if err := s.conn.Invoke(ctx, returnsReturnRequestApprovalServiceRecallReturnRequestMethod, recallReturnRequestRequest, recallReturnRequestReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestApprovalService/RecallReturnRequest")
	}
	packed, err := anypb.New(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareSearchIndexExportReply, err := s.searchIndexExportClient.ShareSearchIndexExport(ctx, &searchv1.ShareSearchIndexExportRequest{
		Context: req.GetContext(),
		Details: &searchv1.ShareSearchIndexExportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: req.GetRiskAssessment().GetRequester().GetPreferredChannel(),
			},
			Payload: packed,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexExportService/ShareSearchIndexExport")
	}
	householdQuery, err := registry.Lookup[customersv1.HouseholdQueryServiceClient](s.clients, "acme.customers.v1.HouseholdQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	browseHouseholdsRequest := &customersv1.BrowseHouseholdsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.BrowseHouseholdsRequest_Filter{
			Region: shareSearchIndexExportReply.GetSearchIndex().GetAddress().GetRegion(),
		},
	}
	browseHouseholdsReply, err := householdQuery.BrowseHouseholds(ctx, browseHouseholdsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/BrowseHouseholds")
	}
	return &fraudv1.RetryRiskAssessmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status:  recallReturnRequestReply.GetOutcome().GetStatus(),
			Message: browseHouseholdsReply.GetResult().GetMessage(),
		},
		Outcome: &fraudv1.RetryRiskAssessmentWorkflowResponse_Outcome{
			Message: recallReturnRequestReply.GetResult().GetMessage(),
			Status:  shareSearchIndexExportReply.GetOutcome().GetStatus(),
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: shareSearchIndexExportReply.GetSearchIndex().GetAudit().GetCreatedBy(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Priority:         req.GetRiskAssessment().GetPriority(),
		},
	}, nil
}

// PauseRiskAssessmentWorkflow pauses a risk assessment.
func (s *RiskAssessmentWorkflowServer) PauseRiskAssessmentWorkflow(ctx context.Context, req *fraudv1.PauseRiskAssessmentWorkflowRequest) (*fraudv1.PauseRiskAssessmentWorkflowResponse, error) {
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "pause risk assessment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	customFields := make(map[string]string, len(req.GetRiskAssessment().GetCustomFields()))
	for key, value := range req.GetRiskAssessment().GetCustomFields() {
		customFields[key] = value
	}
	findFraudSignalsRequest := &fraudv1.FindFraudSignalsRequest{
		Context: req.GetContext(),
		Filter: &fraudv1.FindFraudSignalsRequest_Filter{
			CreatedAfter: req.GetRiskAssessment().GetSla().GetDueAt(),
			Labels:       customFields,
			OwnerId:      req.GetRiskAssessment().GetCustomer().GetCustomerId(),
		},
	}
	findFraudSignalsReply, err := fraudv1.FraudSignalQueryServiceClient.FindFraudSignals(s.fraudSignalQueryClient, ctx, findFraudSignalsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalQueryService/FindFraudSignals")
	}
	riskAssessmentCalculation, ok := s.clients.Get("acme.fraud.v1.RiskAssessmentCalculationService").(fraudv1.RiskAssessmentCalculationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.fraud.v1.RiskAssessmentCalculationService")
	}
	ctx = tenancy.With(ctx, "phone", req.GetRiskAssessment().GetRequester().GetPhone())
	source := req.GetRiskAssessment().GetSla().ProtoReflect()
	policyId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("policy_id"))).String()
	quoteRiskAssessmentRequest := &fraudv1.QuoteRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Requester: &commonv1.ContactInfo{Phone: tenancy.Value(ctx, "phone")},
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				DisplayName: req.GetRiskAssessment().GetAssignee().GetDisplayName(),
				Email:       req.GetRiskAssessment().GetAssignee().GetEmail(),
			}},
			Sla: &fraudv1.RiskAssessmentSla{PolicyId: policyId},
		},
	}
	quoteRiskAssessmentReply, err := riskAssessmentCalculation.QuoteRiskAssessment(ctx, quoteRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentCalculationService/QuoteRiskAssessment")
	}
	compareConsentRecordOptionsRequest := buildCompareConsentRecordOptionsRequestForPauseRiskAssessmentWorkflow(req, quoteRiskAssessmentReply)
	compareConsentRecordOptionsReply, err := s.consentRecordCalculationClient.CompareConsentRecordOptions(ctx, compareConsentRecordOptionsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CompareConsentRecordOptions")
	}
	approveRiskAssessmentRequest := &fraudv1.ApproveRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: quoteRiskAssessmentReply.GetRiskAssessment().GetAssignee().GetDisplayName(),
			},
		},
	}
	type approveRiskAssessmentResult struct {
		reply *fraudv1.ApproveRiskAssessmentResponse
		err   error
	}
	approveRiskAssessmentResults := make(chan approveRiskAssessmentResult, 1)
	go func() {
		reply, err := s.riskAssessmentApprovalClient.ApproveRiskAssessment(ctx, approveRiskAssessmentRequest)
		approveRiskAssessmentResults <- approveRiskAssessmentResult{reply, err}
	}()
	var approveRiskAssessmentReply *fraudv1.ApproveRiskAssessmentResponse
	select {
	case result := <-approveRiskAssessmentResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.fraud.v1.RiskAssessmentApprovalService/ApproveRiskAssessment")
		}
		approveRiskAssessmentReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	_ = approveRiskAssessmentReply
	return &fraudv1.PauseRiskAssessmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: findFraudSignalsReply.GetResult().GetReferenceIds(),
			Message:      compareConsentRecordOptionsReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		Outcome: &fraudv1.PauseRiskAssessmentWorkflowResponse_Outcome{Status: findFraudSignalsReply.GetResult().GetStatus()},
		RiskAssessment: &fraudv1.RiskAssessment{
			Requester: &commonv1.ContactInfo{
				Email: quoteRiskAssessmentReply.GetRiskAssessment().GetAssignee().GetEmail(),
			},
			Assignee: &identityv1.UserRef{
				DisplayName: compareConsentRecordOptionsReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Status:           req.GetRiskAssessment().GetStatus(),
		},
	}, nil
}

// ResumeRiskAssessmentWorkflow resumes a risk assessment.
func (s *RiskAssessmentWorkflowServer) ResumeRiskAssessmentWorkflow(ctx context.Context, req *fraudv1.ResumeRiskAssessmentWorkflowRequest) (*fraudv1.ResumeRiskAssessmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "resume risk assessment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	listWatchlistReportsRequest := &fraudv1.ListWatchlistReportsRequest{}
	listWatchlistReportsRequest.Context = req.GetContext()
	listWatchlistReportsRequest.Filter = &fraudv1.ListWatchlistReportsRequest_Filter{CreatedAfter: req.GetRiskAssessment().GetSla().GetDueAt()}
	listWatchlistReportsReply, err := s.watchlistReportingClient.ListWatchlistReports(ctx, listWatchlistReportsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.WatchlistReportingService/ListWatchlistReports")
	}
	listAccessPoliciesRequest := &authorizationv1.ListAccessPoliciesRequest{
		Context: req.GetContext(),
		Filter: &authorizationv1.ListAccessPoliciesRequest_Filter{
			CreatedAfter: req.GetRiskAssessment().GetAudit().GetUpdatedAt(),
		},
	}
	listAccessPoliciesReply := new(authorizationv1.ListAccessPoliciesResponse)
	if err := grpcx.Invoke(ctx, s.conn, authorizationv1.AccessPolicyService_ListAccessPolicies_FullMethodName, listAccessPoliciesRequest, listAccessPoliciesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/ListAccessPolicies")
	}
	return &fraudv1.ResumeRiskAssessmentWorkflowResponse{
		Outcome: &fraudv1.ResumeRiskAssessmentWorkflowResponse_Outcome{Status: listWatchlistReportsReply.GetResult().GetStatus()},
		Result: &commonv1.OperationResult{
			CorrelationId: listAccessPoliciesReply.GetResult().GetCorrelationId(),
			Status:        listAccessPoliciesReply.GetResult().GetStatus(),
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Priority:         req.GetRiskAssessment().GetPriority(),
		},
	}, nil
}

// InspectRiskAssessmentWorkflow returns a risk assessment.
func (s *RiskAssessmentWorkflowServer) InspectRiskAssessmentWorkflow(ctx context.Context, req *fraudv1.InspectRiskAssessmentWorkflowRequest) (*fraudv1.InspectRiskAssessmentWorkflowResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "risk_assessment", req.GetRiskAssessmentId(), "inspect_risk_assessment_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect risk assessment workflow", "tenant", req.GetContext().GetTenantId())
	quoteCustomerRequest := &customersv1.QuoteCustomerRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{ExpiresAt: req.GetContext().GetDeadline()}},
		},
	}
	quoteCustomerReply := new(customersv1.QuoteCustomerResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", customersCustomerCalculationServiceName, "QuoteCustomer"), quoteCustomerRequest, quoteCustomerReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerCalculationService/QuoteCustomer")
	}
	return &fraudv1.InspectRiskAssessmentWorkflowResponse{
		Result: &commonv1.OperationResult{Status: quoteCustomerReply.GetResult().GetStatus()},
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				Email: quoteCustomerReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
		},
	}, nil
}

// ListRiskAssessmentWorkflows lists risk assessments a page at a time.
func (s *RiskAssessmentWorkflowServer) ListRiskAssessmentWorkflows(ctx context.Context, req *fraudv1.ListRiskAssessmentWorkflowsRequest) (*fraudv1.ListRiskAssessmentWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListRiskAssessmentWorkflows", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list risk assessment workflows", "tenant", req.GetContext().GetTenantId())
	normalizeRiskAssessmentRequest := &fraudv1.NormalizeRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{Email: req.GetContext().GetActor().GetEmail()}},
			Audit:  &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
		},
	}
	normalizeRiskAssessmentReply, err := normalizeRiskAssessmentCall(ctx, normalizeRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/NormalizeRiskAssessment")
	}
	watchCustomersStream, err := s.customerClient.WatchCustomers(ctx, &customersv1.WatchCustomersRequest{
		Context: req.GetContext(),
		Subscriber: &commonv1.ContactInfo{
			PreferredChannel: normalizeRiskAssessmentReply.GetRiskAssessment().GetRequester().GetPreferredChannel(),
			Email:            normalizeRiskAssessmentReply.GetRiskAssessment().GetCustomer().GetEmail(),
			Phone:            normalizeRiskAssessmentReply.GetRiskAssessment().GetRequester().GetPhone(),
		},
		Filter: &customersv1.WatchCustomersRequest_Filter{Region: req.GetFilter().GetRegion()},
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
	return &fraudv1.ListRiskAssessmentWorkflowsResponse{
		RiskAssessments: []*fraudv1.RiskAssessment{&fraudv1.RiskAssessment{
			Requester: &commonv1.ContactInfo{
				Name: normalizeRiskAssessmentReply.GetRiskAssessment().GetRequester().GetName(),
			},
			Tags: normalizeRiskAssessmentReply.GetRiskAssessment().GetTags(),
		}},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SignalRiskAssessmentWorkflow signals a risk assessment.
func (s *RiskAssessmentWorkflowServer) SignalRiskAssessmentWorkflow(ctx context.Context, req *fraudv1.SignalRiskAssessmentWorkflowRequest) (*fraudv1.SignalRiskAssessmentWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveRiskAssessment(ctx, req.GetContext().GetTenantId(), req.GetRiskAssessment()); err != nil {
		return nil, grpcx.Wrap(err, "save risk assessment")
	}
	s.log.InfoContext(ctx, "signal risk assessment workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetRiskAssessment().GetAudit().GetEtag())
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	replies := make([]*authorizationv1.CreateAccessPolicyResponse, len(req.GetRiskAssessment().GetComments()))
	for i, comment := range req.GetRiskAssessment().GetComments() {
		group.Go(func() error {
			reply, err := s.accessPolicyClient.CreateAccessPolicy(groupCtx, &authorizationv1.CreateAccessPolicyRequest{
				Context: req.GetContext(),
				AccessPolicy: &authorizationv1.AccessPolicy{
					Action: &authorizationv1.AccessPolicy_Block{Block: &authorizationv1.AccessPolicyBlockAction{NotifyEmail: comment.GetAuthorEmail()}},
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
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyService/CreateAccessPolicy")
	}
	s.log.DebugContext(ctx, "fan-out done", "replies", len(replies))
	for _, comment2 := range req.GetRiskAssessment().GetComments() {
		if _, err := s.preferenceCalculationClient.RecalculatePreferences(ctx, &customersv1.RecalculatePreferencesRequest{
			Context: req.GetContext(),
			Preference: &customersv1.Preference{
				Audit:  &commonv1.AuditInfo{CreatedAt: comment2.GetCreatedAt()},
				Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{NotifyEmail: comment2.GetAuthorEmail()}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/RecalculatePreferences")
		}
	}
	packed, err := anypb.New(req.GetRiskAssessment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	compareConsentRecordOptionsRequest := &customersv1.CompareConsentRecordOptionsRequest{
		Context:       req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{Detail: packed},
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
	timesheetQuery, err := registry.Lookup[timekeepingv1.TimesheetQueryServiceClient](s.clients, "acme.timekeeping.v1.TimesheetQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	suggestTimesheetsRequest := &timekeepingv1.SuggestTimesheetsRequest{
		Context: req.GetContext(),
		Filter: &timekeepingv1.SuggestTimesheetsRequest_Filter{
			OwnerId: req.GetRiskAssessment().GetCustomer().GetCustomerId(),
		},
	}
	suggestTimesheetsReply, err := timesheetQuery.SuggestTimesheets(ctx, suggestTimesheetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetQueryService/SuggestTimesheets")
	}
	return &fraudv1.SignalRiskAssessmentWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: compareConsentRecordOptionsReply.GetResult().GetStatus(),
		},
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{
				DisplayName: compareConsentRecordOptionsReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			RiskAssessmentId: req.GetRiskAssessmentId(),
			Status:           req.GetRiskAssessment().GetStatus(),
		},
		Outcome: &fraudv1.SignalRiskAssessmentWorkflowResponse_Outcome{Status: suggestTimesheetsReply.GetResult().GetStatus()},
	}, nil
}
