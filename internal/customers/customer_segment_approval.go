package customers

import (
	"context"
	"log/slog"
	"sync"

	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	fulfillmentv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fulfillment/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	insurancev1 "github.com/hounddogai/protomap-test-golang/gen/acme/insurance/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	metricsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/metrics/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/pipeline"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// CustomerSegmentApprovalServer implements `acme.customers.v1.CustomerSegmentApprovalService`.
// Routes customer segments through approval, with escalation and delegation.
type CustomerSegmentApprovalServer struct {
	customersv1.UnimplementedCustomerSegmentApprovalServiceServer
	store                              *Store
	log                                *slog.Logger
	roleHistoryPort                    roleHistoryPort
	synonymSyncClient                  searchv1.SynonymSyncServiceClient
	consentRecordReportingClient       customersv1.ConsentRecordReportingServiceClient
	preferenceImportClient             customersv1.PreferenceImportServiceClient
	householdWorkflowClient            customersv1.HouseholdWorkflowServiceClient
	ledgerAccountValidationClient      ledgerv1.LedgerAccountValidationServiceClient
	brandReportingClient               catalogv1.BrandReportingServiceClient
	conn                               *grpc.ClientConn
	fulfillmentExceptionWorkflowOnce   sync.Once
	fulfillmentExceptionWorkflowCached fulfillmentv1.FulfillmentExceptionWorkflowServiceClient
	customerSegmentImportPort          customerSegmentImportPort
	customerSegmentLifecycleOnce       sync.Once
	customerSegmentLifecycleCached     customersv1.CustomerSegmentLifecycleServiceClient
	clients                            *registry.Registry
	fraudRuleWorkflowClient            fraudv1.FraudRuleWorkflowServiceClient
	customerSegmentImportClient        customersv1.CustomerSegmentImportServiceClient
	apiCredentialNotificationPort      apiCredentialNotificationPort
	policyQueryClient                  insurancev1.PolicyQueryServiceClient
}

// NewCustomerSegmentApprovalServer wires a CustomerSegmentApprovalServer.
func NewCustomerSegmentApprovalServer(deps platform.Deps) *CustomerSegmentApprovalServer {
	return &CustomerSegmentApprovalServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.customers.v1.CustomerSegmentApprovalService"),
		roleHistoryPort:               newRoleHistoryPort(deps),
		synonymSyncClient:             searchv1.NewSynonymSyncServiceClient(deps.Conn),
		consentRecordReportingClient:  customersv1.NewConsentRecordReportingServiceClient(deps.Conn),
		preferenceImportClient:        customersv1.NewPreferenceImportServiceClient(deps.Conn),
		householdWorkflowClient:       customersv1.NewHouseholdWorkflowServiceClient(deps.Conn),
		ledgerAccountValidationClient: ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		brandReportingClient:          catalogv1.NewBrandReportingServiceClient(deps.Conn),
		conn:                          deps.Conn,
		customerSegmentImportPort:     newCustomerSegmentImportPort(deps),
		clients:                       deps.Clients,
		fraudRuleWorkflowClient:       fraudv1.NewFraudRuleWorkflowServiceClient(deps.Conn),
		customerSegmentImportClient:   customersv1.NewCustomerSegmentImportServiceClient(deps.Conn),
		apiCredentialNotificationPort: newApiCredentialNotificationPort(deps),
		policyQueryClient:             insurancev1.NewPolicyQueryServiceClient(deps.Conn),
	}
}

// SubmitCustomerSegment submits a customer segment.
func (s *CustomerSegmentApprovalServer) SubmitCustomerSegment(ctx context.Context, req *customersv1.SubmitCustomerSegmentRequest) (*customersv1.SubmitCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitCustomerSegment", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "submit customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	snapshot, err := protojson.Marshal(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	revertRoleRequest := &identityv1.RevertRoleRequest{
		Context: req.GetContext(),
		Role: &identityv1.Role{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			},
		},
		Details: &identityv1.RevertRoleRequest_Details{Snapshot: snapshot},
	}
	revertRoleReply, err := s.roleHistoryPort.RevertRole(ctx, revertRoleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleHistoryService/RevertRole")
	}
	snapshot2, err := protojson.Marshal(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileSynonymsRequest := &searchv1.ReconcileSynonymsRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Action: &searchv1.Synonym_Discount{Discount: &searchv1.SynonymDiscountAction{Amount: revertRoleReply.GetRole().GetDiscount().GetAmount()}},
			Audit: &commonv1.AuditInfo{
				CreatedBy: revertRoleReply.GetRole().GetAudit().GetCreatedBy(),
			},
		},
		Details: &searchv1.ReconcileSynonymsRequest_Details{Payload: snapshot2},
	}
	reconcileSynonymsReply, err := searchv1.SynonymSyncServiceClient.ReconcileSynonyms(s.synonymSyncClient, ctx, reconcileSynonymsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymSyncService/ReconcileSynonyms")
	}
	return &customersv1.SubmitCustomerSegmentResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Version:           revertRoleReply.GetRole().GetVersion(),
			Scopes:            reconcileSynonymsReply.GetSynonym().GetScopes(),
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Priority:          req.GetCustomerSegment().GetPriority(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveCustomerSegment approves a customer segment.
func (s *CustomerSegmentApprovalServer) ApproveCustomerSegment(ctx context.Context, req *customersv1.ApproveCustomerSegmentRequest) (*customersv1.ApproveCustomerSegmentResponse, error) {
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "approve customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	listConsentRecordReportsRequest := &customersv1.ListConsentRecordReportsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListConsentRecordReportsRequest_Filter{
			CreatedAfter: req.GetCustomerSegment().GetAudit().GetCreatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
			Labels:       req.GetAnnotations(),
		},
	}
	var listConsentRecordReportsReply *customersv1.ListConsentRecordReportsResponse
	if err := pipeline.Run(ctx, "acme.customers.v1.ConsentRecordReportingService/ListConsentRecordReports", func(ctx context.Context) error {
		answer, err := s.consentRecordReportingClient.ListConsentRecordReports(ctx, listConsentRecordReportsRequest)
		listConsentRecordReportsReply = answer
		return err
	}); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordReportingService/ListConsentRecordReports")
	}
	validatePreferenceImportRequest := &customersv1.ValidatePreferenceImportRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Action: &customersv1.Preference_Route{Route: &customersv1.PreferenceRouteAction{Target: req.GetCustomerSegment().GetRoute().GetTarget()}},
			Owner:  &identityv1.UserRef{Email: req.GetCustomerSegment().GetOwner().GetEmail()},
		},
	}
	validatePreferenceImportReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.ValidatePreferenceImportResponse, error) {
		return s.preferenceImportClient.ValidatePreferenceImport(ctx, validatePreferenceImportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceImportService/ValidatePreferenceImport")
	}
	signalHouseholdWorkflowRequest := &customersv1.SignalHouseholdWorkflowRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{Etag: req.GetCustomerSegment().GetAudit().GetEtag()},
					Manager: &identityv1.UserRef{
						DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
					},
				}},
			}},
		},
	}
	signalHouseholdWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*customersv1.SignalHouseholdWorkflowResponse, error) {
		return s.householdWorkflowClient.SignalHouseholdWorkflow(ctx, signalHouseholdWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdWorkflowService/SignalHouseholdWorkflow")
	}
	_ = signalHouseholdWorkflowReply
	return &customersv1.ApproveCustomerSegmentResponse{
		Result: &commonv1.OperationResult{
			Details: listConsentRecordReportsReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Outcome: &customersv1.ApproveCustomerSegmentResponse_Outcome{
			Message: listConsentRecordReportsReply.GetResult().GetMessage(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Version:           validatePreferenceImportReply.GetPreference().GetVersion(),
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Owner:             req.GetCustomerSegment().GetOwner(),
		},
	}, nil
}

// RejectCustomerSegment rejects a customer segment.
func (s *CustomerSegmentApprovalServer) RejectCustomerSegment(ctx context.Context, req *customersv1.RejectCustomerSegmentRequest) (*customersv1.RejectCustomerSegmentResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectCustomerSegment", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "reject customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	encoded, err := protojson.Marshal(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.CustomerSegment
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	validateLedgerAccountRequest := &ledgerv1.ValidateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Audit: &commonv1.AuditInfo{
				CreatedAt: (&decoded).GetAudit().GetCreatedAt(),
				CreatedBy: req.GetCustomerSegment().GetAudit().GetCreatedBy(),
			},
		},
	}
	validateLedgerAccountReply, err := grpcx.Call(ctx, s.ledgerAccountValidationClient.ValidateLedgerAccount, validateLedgerAccountRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ValidateLedgerAccount")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	parameters := make(map[string]string, len(req.GetCustomerSegment().GetParameters()))
	for key2, value2 := range req.GetCustomerSegment().GetParameters() {
		parameters[key2] = value2
	}
	generateBrandReportRequest := &catalogv1.GenerateBrandReportRequest{
		Context:     req.GetContext(),
		Reason:      req.GetCustomerSegment().GetBlock().GetReason(),
		Annotations: annotations,
		Details: &catalogv1.GenerateBrandReportRequest_Details{
			Filters: parameters,
			Period:  req.GetCustomerSegment().GetEffective(),
		},
	}
	generateBrandReportReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*catalogv1.GenerateBrandReportResponse, error) {
		return s.brandReportingClient.GenerateBrandReport(ctx, generateBrandReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandReportingService/GenerateBrandReport")
	}
	resumeFulfillmentExceptionWorkflowRequest := &fulfillmentv1.ResumeFulfillmentExceptionWorkflowRequest{
		Context: req.GetContext(),
		FulfillmentException: &fulfillmentv1.FulfillmentException{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedBy(),
			},
			Comments: []*fulfillmentv1.FulfillmentExceptionComment{&fulfillmentv1.FulfillmentExceptionComment{
				CreatedAt: req.GetCustomerSegment().GetAudit().GetCreatedAt(),
			}},
			Origin: &fulfillmentv1.FulfillmentException_Customer{Customer: &fulfillmentv1.PackingSlipRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
			}},
		},
		Details: &fulfillmentv1.ResumeFulfillmentExceptionWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name: validateLedgerAccountReply.GetLedgerAccount().GetContact().GetName(),
			},
		},
	}
	resumeFulfillmentExceptionWorkflowReply, err := s.fulfillmentExceptionWorkflowLazy().ResumeFulfillmentExceptionWorkflow(ctx, resumeFulfillmentExceptionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fulfillment.v1.FulfillmentExceptionWorkflowService/ResumeFulfillmentExceptionWorkflow")
	}
	cancelCustomerSegmentImportRequest := &customersv1.CancelCustomerSegmentImportRequest{
		Context:           req.GetContext(),
		CustomerSegmentId: req.GetCustomerSegment().GetCustomerSegmentId(),
		Details: &customersv1.CancelCustomerSegmentImportRequest_Details{
			NewOwner: &identityv1.UserRef{
				DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
				Email:       validateLedgerAccountReply.GetLedgerAccount().GetContact().GetEmail(),
				UserId:      resumeFulfillmentExceptionWorkflowReply.GetFulfillmentException().GetAssignee().GetUserId(),
			},
			NotifyContact: &commonv1.ContactInfo{
				Phone: validateLedgerAccountReply.GetLedgerAccount().GetContact().GetPhone(),
			},
		},
	}
	cancelCustomerSegmentImportReply, err := s.customerSegmentImportPort.CancelCustomerSegmentImport(ctx, cancelCustomerSegmentImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/CancelCustomerSegmentImport")
	}
	return &customersv1.RejectCustomerSegmentResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				CreatedBy: validateLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedBy(),
				CreatedAt: cancelCustomerSegmentImportReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: generateBrandReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &customersv1.RejectCustomerSegmentResponse_Outcome{
			Status:      generateBrandReportReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
	}, nil
}

// EscalateCustomerSegment escalates a customer segment.
func (s *CustomerSegmentApprovalServer) EscalateCustomerSegment(ctx context.Context, req *customersv1.EscalateCustomerSegmentRequest) (*customersv1.EscalateCustomerSegmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "escalate customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "customer_segment_id", req.GetCustomerSegment().GetCustomerSegmentId())
	renewCustomerSegmentRequest := &customersv1.RenewCustomerSegmentRequest{
		Context:           req.GetContext(),
		CustomerSegmentId: tenancy.Value(ctx, "customer_segment_id"),
		Details: &customersv1.RenewCustomerSegmentRequest_Details{
			NotifyContact: &commonv1.ContactInfo{Email: req.GetCustomerSegment().GetOwner().GetEmail()},
			NewOwner: &identityv1.UserRef{
				UserId:      req.GetDetails().GetApprover().GetUserId(),
				DisplayName: req.GetCustomerSegment().GetOwner().GetDisplayName(),
				Email:       req.GetCustomerSegment().GetBlock().GetNotifyEmail(),
			},
		},
	}
	renewCustomerSegmentReply, err := s.customerSegmentLifecycleLazy().RenewCustomerSegment(ctx, renewCustomerSegmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentLifecycleService/RenewCustomerSegment")
	}
	apiCredentialNotification, ok := s.clients.Get("acme.identity.v1.ApiCredentialNotificationService").(identityv1.ApiCredentialNotificationServiceClient)
	if !ok {
		return nil, status.Errorf(codes.Internal, "no client of %s", "acme.identity.v1.ApiCredentialNotificationService")
	}
	packed, err := anypb.New(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeApiCredentialUpdatesRequest := &identityv1.SubscribeApiCredentialUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeApiCredentialUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetCustomerSegment().GetOwner().GetEmail()},
			Payload:   packed,
		},
	}
	subscribeApiCredentialUpdatesReply, err := apiCredentialNotification.SubscribeApiCredentialUpdates(ctx, subscribeApiCredentialUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/SubscribeApiCredentialUpdates")
	}
	_ = subscribeApiCredentialUpdatesReply
	return &customersv1.EscalateCustomerSegmentResponse{
		Result: &commonv1.OperationResult{Status: renewCustomerSegmentReply.GetOutcome().GetStatus()},
		CustomerSegment: &customersv1.CustomerSegment{
			Priority:          renewCustomerSegmentReply.GetCustomerSegment().GetPriority(),
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Name:              req.GetCustomerSegment().GetName(),
		},
	}, nil
}

// ListPendingCustomerSegments lists customer segments a page at a time.
func (s *CustomerSegmentApprovalServer) ListPendingCustomerSegments(ctx context.Context, req *customersv1.ListPendingCustomerSegmentsRequest) (*customersv1.ListPendingCustomerSegmentsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ListPendingCustomerSegments", "error", err)
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending customer segments", "tenant", req.GetContext().GetTenantId())
	return &customersv1.ListPendingCustomerSegmentsResponse{Result: &commonv1.OperationResult{Status: "ok"}}, nil
}

// RecallCustomerSegment recalls a customer segment.
func (s *CustomerSegmentApprovalServer) RecallCustomerSegment(ctx context.Context, req *customersv1.RecallCustomerSegmentRequest) (*customersv1.RecallCustomerSegmentResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "recall customer segment", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	parameters := make(map[string]string, len(req.GetCustomerSegment().GetParameters()))
	for key, value := range req.GetCustomerSegment().GetParameters() {
		parameters[key] = value
	}
	countMetricSeriesesRequest := &metricsv1.CountMetricSeriesesRequest{
		Context: req.GetContext(),
		Filter: &metricsv1.CountMetricSeriesesRequest_Filter{
			CreatedAfter: req.GetCustomerSegment().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetDetails().GetApprover().GetUserId(),
			Labels:       parameters,
		},
	}
	countMetricSeriesesReply, err := countMetricSeriesesCall(ctx, countMetricSeriesesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.metrics.v1.MetricSeriesQueryService/CountMetricSerieses")
	}
	encoded, err := protojson.Marshal(req.GetCustomerSegment())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.CustomerSegment
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	cancelFraudRuleWorkflowReply, err := s.fraudRuleWorkflowClient.CancelFraudRuleWorkflow(ctx, &fraudv1.CancelFraudRuleWorkflowRequest{
		Context: req.GetContext(),
		Details: &fraudv1.CancelFraudRuleWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
		FraudRule: &fraudv1.FraudRule{
			Audit: &commonv1.AuditInfo{UpdatedBy: (&decoded).GetAudit().GetUpdatedBy()},
			Action: &fraudv1.FraudRule_Discount{Discount: &fraudv1.FraudRuleDiscountAction{
				Percent: req.GetCustomerSegment().GetDiscount().GetPercent(),
			}},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleWorkflowService/CancelFraudRuleWorkflow")
	}
	return &customersv1.RecallCustomerSegmentResponse{
		Result: &commonv1.OperationResult{
			Status:  countMetricSeriesesReply.GetResult().GetStatus(),
			Message: countMetricSeriesesReply.GetResult().GetMessage(),
		},
		CustomerSegment: &customersv1.CustomerSegment{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: cancelFraudRuleWorkflowReply.GetFraudRule().GetAudit().GetUpdatedBy(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Parameters:        req.GetCustomerSegment().GetParameters(),
		},
	}, nil
}

// GetCustomerSegmentApprovalStatus returns a customer segment.
func (s *CustomerSegmentApprovalServer) GetCustomerSegmentApprovalStatus(ctx context.Context, req *customersv1.GetCustomerSegmentApprovalStatusRequest) (*customersv1.GetCustomerSegmentApprovalStatusResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "customer_segment", req.GetCustomerSegmentId(), "get_customer_segment_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get customer segment approval status", "tenant", req.GetContext().GetTenantId())
	retryCustomerSegmentImportRequest := &customersv1.RetryCustomerSegmentImportRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
	}
	retryCustomerSegmentImportReply, err := grpcx.Call(ctx, s.customerSegmentImportClient.RetryCustomerSegmentImport, retryCustomerSegmentImportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentImportService/RetryCustomerSegmentImport")
	}
	delegateHouseholdApprovalRequest := &customersv1.DelegateHouseholdApprovalRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						UpdatedBy: retryCustomerSegmentImportReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
					},
					Manager: &identityv1.UserRef{
						DisplayName: retryCustomerSegmentImportReply.GetCustomerSegment().GetOwner().GetDisplayName(),
						UserId:      retryCustomerSegmentImportReply.GetCustomerSegment().GetOwner().GetUserId(),
					},
				}},
			}},
		},
	}
	delegateHouseholdApprovalReply := new(customersv1.DelegateHouseholdApprovalResponse)
	if err := s.conn.Invoke(ctx, customersHouseholdApprovalServiceDelegateHouseholdApprovalMethod, delegateHouseholdApprovalRequest, delegateHouseholdApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdApprovalService/DelegateHouseholdApproval")
	}
	return &customersv1.GetCustomerSegmentApprovalStatusResponse{
		CustomerSegment: &customersv1.CustomerSegment{
			Effective: retryCustomerSegmentImportReply.GetCustomerSegment().GetEffective(),
			Owner: &identityv1.UserRef{
				UserId:      retryCustomerSegmentImportReply.GetCustomerSegment().GetOwner().GetUserId(),
				DisplayName: delegateHouseholdApprovalReply.GetHousehold().GetManager().GetDisplayName(),
			},
			CustomerSegmentId: req.GetCustomerSegmentId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentCustomerSegmentApproval comments a customer segment.
func (s *CustomerSegmentApprovalServer) CommentCustomerSegmentApproval(ctx context.Context, req *customersv1.CommentCustomerSegmentApprovalRequest) (*customersv1.CommentCustomerSegmentApprovalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected CommentCustomerSegmentApproval", "error", err)
		return nil, err
	}
	if err := s.store.SaveCustomerSegment(ctx, req.GetContext().GetTenantId(), req.GetCustomerSegment()); err != nil {
		return nil, grpcx.Wrap(err, "save customer segment")
	}
	s.log.InfoContext(ctx, "comment customer segment approval", "tenant", req.GetContext().GetTenantId(), "etag", req.GetCustomerSegment().GetAudit().GetEtag())
	notifyApiCredentialWatchersRequest := &identityv1.NotifyApiCredentialWatchersRequest{
		Context: req.GetContext(),
		Details: &identityv1.NotifyApiCredentialWatchersRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	notifyApiCredentialWatchersReply, err := s.apiCredentialNotificationPort.NotifyApiCredentialWatchers(ctx, notifyApiCredentialWatchersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialNotificationService/NotifyApiCredentialWatchers")
	}
	step := &customerSegmentApprovalServerCommentCustomerSegmentApprovalBrowsePoliciesStep{request: &insurancev1.BrowsePoliciesRequest{
		Context: req.GetContext(),
		Filter: &insurancev1.BrowsePoliciesRequest_Filter{
			Labels:       notifyApiCredentialWatchersReply.GetApiCredential().GetLabels(),
			CreatedAfter: notifyApiCredentialWatchersReply.GetApiCredential().GetAudit().GetUpdatedAt(),
			OwnerId:      req.GetCustomerSegment().GetCustomerSegmentId(),
		},
	}}
	if err := s.runCommentCustomerSegmentApprovalBrowsePolicies(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.insurance.v1.PolicyQueryService/BrowsePolicies")
	}
	browsePoliciesReply := step.reply
	return &customersv1.CommentCustomerSegmentApprovalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: notifyApiCredentialWatchersReply.GetResult().GetCorrelationId(),
			Details:       notifyApiCredentialWatchersReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Outcome: &customersv1.CommentCustomerSegmentApprovalResponse_Outcome{Message: browsePoliciesReply.GetResult().GetMessage()},
		CustomerSegment: &customersv1.CustomerSegment{
			CustomerSegmentId: req.GetCustomerSegmentId(),
			Version:           req.GetCustomerSegment().GetVersion(),
		},
	}, nil
}

// fulfillmentExceptionWorkflowLazy dials acme.fulfillment.v1.FulfillmentExceptionWorkflowService the first time a
// handler needs it.
func (s *CustomerSegmentApprovalServer) fulfillmentExceptionWorkflowLazy() fulfillmentv1.FulfillmentExceptionWorkflowServiceClient {
	s.fulfillmentExceptionWorkflowOnce.Do(func() {
		s.fulfillmentExceptionWorkflowCached = fulfillmentv1.NewFulfillmentExceptionWorkflowServiceClient(s.conn)
	})
	return s.fulfillmentExceptionWorkflowCached
}

// customerSegmentLifecycleLazy dials acme.customers.v1.CustomerSegmentLifecycleService the first time a handler needs
// it.
func (s *CustomerSegmentApprovalServer) customerSegmentLifecycleLazy() customersv1.CustomerSegmentLifecycleServiceClient {
	s.customerSegmentLifecycleOnce.Do(func() {
		s.customerSegmentLifecycleCached = customersv1.NewCustomerSegmentLifecycleServiceClient(s.conn)
	})
	return s.customerSegmentLifecycleCached
}

// customerSegmentApprovalServerCommentCustomerSegmentApprovalBrowsePoliciesStep carries the request of one step of
// CommentCustomerSegmentApproval and, once it ran, its reply.
type customerSegmentApprovalServerCommentCustomerSegmentApprovalBrowsePoliciesStep struct {
	request *insurancev1.BrowsePoliciesRequest
	reply   *insurancev1.BrowsePoliciesResponse
}

func (s *CustomerSegmentApprovalServer) runCommentCustomerSegmentApprovalBrowsePolicies(ctx context.Context, step *customerSegmentApprovalServerCommentCustomerSegmentApprovalBrowsePoliciesStep) error {
	reply, err := s.policyQueryClient.BrowsePolicies(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
