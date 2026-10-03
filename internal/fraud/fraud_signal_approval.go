package fraud

import (
	"context"
	"log/slog"
	"sync"

	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	notificationsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/notifications/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// FraudSignalApprovalServer implements `acme.fraud.v1.FraudSignalApprovalService`.
// Routes fraud signals through approval, with escalation and delegation.
type FraudSignalApprovalServer struct {
	fraudv1.UnimplementedFraudSignalApprovalServiceServer
	store                            *Store
	log                              *slog.Logger
	customerSegmentValidationClient  customersv1.CustomerSegmentValidationServiceClient
	riskAssessmentValidationClient   fraudv1.RiskAssessmentValidationServiceClient
	clients                          *registry.Registry
	fraudSignalValidationClient      fraudv1.FraudSignalValidationServiceClient
	fraudRuleLifecycleClient         fraudv1.FraudRuleLifecycleServiceClient
	conn                             *grpc.ClientConn
	auditEventValidationOnce         sync.Once
	auditEventValidationCached       auditv1.AuditEventValidationServiceClient
	customerSegmentCalculationClient customersv1.CustomerSegmentCalculationServiceClient
	searchQueryNotificationClient    searchv1.SearchQueryNotificationServiceClient
	tenantExportClient               identityv1.TenantExportServiceClient
	preferenceCalculationClient      customersv1.PreferenceCalculationServiceClient
	notificationQueryClient          notificationsv1.NotificationQueryServiceClient
	manifestSyncClient               shippingv1.ManifestSyncServiceClient
}

// NewFraudSignalApprovalServer wires a FraudSignalApprovalServer.
func NewFraudSignalApprovalServer(deps platform.Deps) *FraudSignalApprovalServer {
	return &FraudSignalApprovalServer{
		store:                            NewStore(deps.DB),
		log:                              deps.Logger.With("service", "acme.fraud.v1.FraudSignalApprovalService"),
		customerSegmentValidationClient:  customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		riskAssessmentValidationClient:   fraudv1.NewRiskAssessmentValidationServiceClient(deps.Conn),
		clients:                          deps.Clients,
		fraudSignalValidationClient:      fraudv1.NewFraudSignalValidationServiceClient(deps.Conn),
		fraudRuleLifecycleClient:         fraudv1.NewFraudRuleLifecycleServiceClient(deps.Conn),
		conn:                             deps.Conn,
		customerSegmentCalculationClient: customersv1.NewCustomerSegmentCalculationServiceClient(deps.Conn),
		searchQueryNotificationClient:    searchv1.NewSearchQueryNotificationServiceClient(deps.Conn),
		tenantExportClient:               identityv1.NewTenantExportServiceClient(deps.Conn),
		preferenceCalculationClient:      customersv1.NewPreferenceCalculationServiceClient(deps.Conn),
		notificationQueryClient:          notificationsv1.NewNotificationQueryServiceClient(deps.Conn),
		manifestSyncClient:               shippingv1.NewManifestSyncServiceClient(deps.Conn),
	}
}

// SubmitFraudSignal submits a fraud signal.
func (s *FraudSignalApprovalServer) SubmitFraudSignal(ctx context.Context, req *fraudv1.SubmitFraudSignalRequest) (*fraudv1.SubmitFraudSignalResponse, error) {
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "submit fraud signal", "tenant", req.GetContext().GetTenantId())
	jobs := make(chan *fraudv1.FraudSignalMeasurement)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for measurement := range jobs {
				if _, err := s.customerSegmentValidationClient.CheckCustomerSegmentEligibility(workCtx, &customersv1.CheckCustomerSegmentEligibilityRequest{
					Context: req.GetContext(),
					CustomerSegment: &customersv1.CustomerSegment{
						Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
						Owner: &identityv1.UserRef{
							DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
							UserId:      req.GetDetails().GetApprover().GetUserId(),
						},
					},
					Details: &customersv1.CheckCustomerSegmentEligibilityRequest_Details{
						Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetFraudSignal().GetMeasurements() {
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
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility")
	}
	return &fraudv1.SubmitFraudSignalResponse{
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			Kind:          req.GetFraudSignal().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ApproveFraudSignal approves a fraud signal.
func (s *FraudSignalApprovalServer) ApproveFraudSignal(ctx context.Context, req *fraudv1.ApproveFraudSignalRequest) (*fraudv1.ApproveFraudSignalResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveFraudSignal", "error", err)
		return nil, err
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "approve fraud signal", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "user_id", req.GetDetails().GetApprover().GetUserId())
	detailsCache := cache.New[string, *fraudv1.ApproveFraudSignalRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	source := req.GetDetails().GetApprover().ProtoReflect()
	displayName := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("display_name"))).String()
	previewRiskAssessmentRequest := &fraudv1.PreviewRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Assignee: &identityv1.UserRef{UserId: tenancy.Value(ctx, "user_id")},
			Origin: &fraudv1.RiskAssessment_Customer{Customer: &customersv1.CustomerRef{
				Email:       cachedDetails.GetApprover().GetEmail(),
				DisplayName: displayName,
			}},
		},
	}
	previewRiskAssessmentReply, err := grpcx.Call(ctx, s.riskAssessmentValidationClient.PreviewRiskAssessment, previewRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentValidationService/PreviewRiskAssessment")
	}
	customerQuery, err := registry.Lookup[customersv1.CustomerQueryServiceClient](s.clients, "acme.customers.v1.CustomerQueryService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	summarizeCustomersRequest := &customersv1.SummarizeCustomersRequest{
		Context:     req.GetContext(),
		CustomerId:  previewRiskAssessmentReply.GetRiskAssessment().GetCustomer().GetCustomerId(),
		Annotations: req.GetAnnotations(),
		Reason:      req.GetReason(),
	}
	summarizeCustomersReply, err := customerQuery.SummarizeCustomers(ctx, summarizeCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/SummarizeCustomers")
	}
	return &fraudv1.ApproveFraudSignalResponse{
		Outcome: &fraudv1.ApproveFraudSignalResponse_Outcome{
			ReferenceId: previewRiskAssessmentReply.GetOutcome().GetReferenceId(),
			Message:     summarizeCustomersReply.GetResult().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Message:       previewRiskAssessmentReply.GetResult().GetMessage(),
			Status:        summarizeCustomersReply.GetResult().GetStatus(),
			CorrelationId: req.GetFraudSignal().GetCorrelationId(),
		},
		FraudSignal: &fraudv1.FraudSignal{FraudSignalId: req.GetFraudSignalId()},
	}, nil
}

// RejectFraudSignal rejects a fraud signal.
func (s *FraudSignalApprovalServer) RejectFraudSignal(ctx context.Context, req *fraudv1.RejectFraudSignalRequest) (*fraudv1.RejectFraudSignalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "reject fraud signal", "tenant", req.GetContext().GetTenantId())
	getFraudSignalRulesRequest := buildGetFraudSignalRulesRequestForRejectFraudSignal(req)
	getFraudSignalRulesReply, err := s.fraudSignalValidationClient.GetFraudSignalRules(ctx, getFraudSignalRulesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudSignalValidationService/GetFraudSignalRules")
	}
	suspendFraudRuleRequest := &fraudv1.SuspendFraudRuleRequest{}
	suspendFraudRuleRequest.Context = req.GetContext()
	suspendFraudRuleRequest.FraudRuleId = req.GetFraudSignal().GetSubject().GetFraudRuleId()
	suspendFraudRuleRequest.Details = &fraudv1.SuspendFraudRuleRequest_Details{
		NewOwner: &identityv1.UserRef{
			DisplayName: getFraudSignalRulesReply.GetFraudSignal().GetSubject().GetDisplayName(),
		},
		EffectiveAt: req.GetDetails().GetEffectiveAt(),
	}
	suspendFraudRuleReply, err := s.fraudRuleLifecycleClient.SuspendFraudRule(ctx, suspendFraudRuleRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.FraudRuleLifecycleService/SuspendFraudRule")
	}
	ctx = tenancy.With(ctx, "kind", req.GetFraudSignal().GetKind())
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	normalizeAuditEventRequest := &auditv1.NormalizeAuditEventRequest{
		Context: req.GetContext(),
		AuditEvent: &auditv1.AuditEvent{
			Subject: &auditv1.AuditTrailRef{
				DisplayName: getFraudSignalRulesReply.GetFraudSignal().GetSubject().GetDisplayName(),
			},
			Kind: tenancy.Value(ctx, "kind"),
		},
		AuditEventId: envelope.GetDisplayName(),
	}
	normalizeAuditEventReply, err := s.auditEventValidationLazy().NormalizeAuditEvent(ctx, normalizeAuditEventRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/NormalizeAuditEvent")
	}
	_ = normalizeAuditEventReply
	return &fraudv1.RejectFraudSignalResponse{
		FraudSignal: &fraudv1.FraudSignal{
			CorrelationId: getFraudSignalRulesReply.GetResult().GetCorrelationId(),
			FraudSignalId: req.GetFraudSignalId(),
			Kind:          req.GetFraudSignal().GetKind(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds:  getFraudSignalRulesReply.GetResult().GetReferenceIds(),
			CorrelationId: suspendFraudRuleReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &fraudv1.RejectFraudSignalResponse_Outcome{
			EffectiveAt: suspendFraudRuleReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// EscalateFraudSignal escalates a fraud signal.
func (s *FraudSignalApprovalServer) EscalateFraudSignal(ctx context.Context, req *fraudv1.EscalateFraudSignalRequest) (*fraudv1.EscalateFraudSignalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "escalate fraud signal", "tenant", req.GetContext().GetTenantId())
	for _, measurement := range req.GetFraudSignal().GetMeasurements() {
		if _, err := s.customerSegmentCalculationClient.EstimateCustomerSegment(ctx, &customersv1.EstimateCustomerSegmentRequest{
			Context: req.GetContext(),
			CustomerSegment: &customersv1.CustomerSegment{
				Owner: &identityv1.UserRef{
					DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				},
				Audit:   &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
				Version: req.GetFraudSignal().GetSubject().GetVersion(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentCalculationService/EstimateCustomerSegment")
		}
	}
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	explainHouseholdDecisionRequest := &customersv1.ExplainHouseholdDecisionRequest{
		Context:     req.GetContext(),
		HouseholdId: envelope.GetDisplayName(),
	}
	explainHouseholdDecisionReply := new(customersv1.ExplainHouseholdDecisionResponse)
	if err := grpc.Invoke(ctx, "/acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision", explainHouseholdDecisionRequest, explainHouseholdDecisionReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdValidationService/ExplainHouseholdDecision")
	}
	return &fraudv1.EscalateFraudSignalResponse{
		Result: &commonv1.OperationResult{
			Details: explainHouseholdDecisionReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		FraudSignal: &fraudv1.FraudSignal{
			Subject: &fraudv1.FraudRuleRef{
				DisplayName: explainHouseholdDecisionReply.GetHousehold().GetManager().GetDisplayName(),
			},
			FraudSignalId: req.GetFraudSignalId(),
			Sequence:      req.GetFraudSignal().GetSequence(),
		},
	}, nil
}

// ListPendingFraudSignals lists fraud signals a page at a time.
func (s *FraudSignalApprovalServer) ListPendingFraudSignals(ctx context.Context, req *fraudv1.ListPendingFraudSignalsRequest) (*fraudv1.ListPendingFraudSignalsResponse, error) {
	s.log.InfoContext(ctx, "list pending fraud signals", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	sendSearchQueryTestNoticeReply, err := s.searchQueryNotificationClient.SendSearchQueryTestNotice(ctx, &searchv1.SendSearchQueryTestNoticeRequest{
		Context: req.GetContext(),
		Details: &searchv1.SendSearchQueryTestNoticeRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: (&decoded).GetActor().GetEmail()},
			Variables: req.GetContext().GetBaggage(),
		},
		Annotations: labels,
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryNotificationService/SendSearchQueryTestNotice")
	}
	return &fraudv1.ListPendingFraudSignalsResponse{
		FraudSignals: []*fraudv1.FraudSignal{&fraudv1.FraudSignal{
			Sequence: sendSearchQueryTestNoticeReply.GetSearchQuery().GetSequence(),
		}},
		Result: &commonv1.OperationResult{
			Details: sendSearchQueryTestNoticeReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// RecallFraudSignal recalls a fraud signal.
func (s *FraudSignalApprovalServer) RecallFraudSignal(ctx context.Context, req *fraudv1.RecallFraudSignalRequest) (*fraudv1.RecallFraudSignalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "recall fraud signal", "tenant", req.GetContext().GetTenantId())
	getTenantExportRequest := &identityv1.GetTenantExportRequest{
		Context:  req.GetContext(),
		TenantId: req.GetContext().GetTenantId(),
	}
	getTenantExportReply, err := grpcx.Retry(ctx, 2, func(ctx context.Context) (*identityv1.GetTenantExportResponse, error) {
		return s.tenantExportClient.GetTenantExport(ctx, getTenantExportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.TenantExportService/GetTenantExport")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetApprover().GetEmail())
	quotePreferenceRequest := &customersv1.QuotePreferenceRequest{}
	quotePreferenceRequest.Context = req.GetContext()
	quotePreferenceRequest.Preference = &customersv1.Preference{
		Owner: &identityv1.UserRef{
			DisplayName: getTenantExportReply.GetTenant().GetManager().GetDisplayName(),
			Email:       tenancy.Value(ctx, "email"),
		},
	}
	quotePreferenceRequest.Details = &customersv1.QuotePreferenceRequest_Details{
		Destination: &commonv1.PostalAddress{
			CountryCode: getTenantExportReply.GetTenant().GetLocation().GetCountryCode(),
		},
	}
	quotePreferenceReply, err := s.preferenceCalculationClient.QuotePreference(ctx, quotePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceCalculationService/QuotePreference")
	}
	return &fraudv1.RecallFraudSignalResponse{
		FraudSignal: &fraudv1.FraudSignal{
			Attributes:    getTenantExportReply.GetTenant().GetAttributes(),
			Subject:       &fraudv1.FraudRuleRef{Version: quotePreferenceReply.GetPreference().GetVersion()},
			FraudSignalId: req.GetFraudSignalId(),
			Sequence:      req.GetFraudSignal().GetSequence(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetFraudSignalApprovalStatus returns a fraud signal.
func (s *FraudSignalApprovalServer) GetFraudSignalApprovalStatus(ctx context.Context, req *fraudv1.GetFraudSignalApprovalStatusRequest) (*fraudv1.GetFraudSignalApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "fraud_signal", req.GetFraudSignalId(), "get_fraud_signal_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get fraud signal approval status", "tenant", req.GetContext().GetTenantId())
	return &fraudv1.GetFraudSignalApprovalStatusResponse{
		FraudSignal: &fraudv1.FraudSignal{FraudSignalId: req.GetFraudSignalId()},
		Result:      &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// CommentFraudSignalApproval comments a fraud signal.
func (s *FraudSignalApprovalServer) CommentFraudSignalApproval(ctx context.Context, req *fraudv1.CommentFraudSignalApprovalRequest) (*fraudv1.CommentFraudSignalApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveFraudSignal(ctx, req.GetContext().GetTenantId(), req.GetFraudSignal()); err != nil {
		return nil, grpcx.Wrap(err, "save fraud signal")
	}
	s.log.InfoContext(ctx, "comment fraud signal approval", "tenant", req.GetContext().GetTenantId())
	var envelope fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	findNotificationsReply, err := s.notificationQueryClient.FindNotifications(ctx, &notificationsv1.FindNotificationsRequest{
		Context: req.GetContext(),
		Filter:  &notificationsv1.FindNotificationsRequest_Filter{CreatedAfter: req.GetFraudSignal().GetOccurredAt()},
		Page:    &commonv1.PageRequest{PageToken: envelope.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.notifications.v1.NotificationQueryService/FindNotifications")
	}
	var envelope2 fraudv1.RiskAssessmentRef
	if err := req.GetFraudSignal().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	listManifestConflictsReply, err := s.manifestSyncClient.ListManifestConflicts(ctx, &shippingv1.ListManifestConflictsRequest{
		Context: req.GetContext(),
		Filter: &shippingv1.ListManifestConflictsRequest_Filter{
			OwnerId: findNotificationsReply.GetResult().GetCorrelationId(),
		},
		Page: &commonv1.PageRequest{PageToken: envelope2.GetDisplayName()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.ManifestSyncService/ListManifestConflicts")
	}
	return &fraudv1.CommentFraudSignalApprovalResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: findNotificationsReply.GetResult().GetCorrelationId(),
			ReferenceIds:  listManifestConflictsReply.GetResult().GetReferenceIds(),
			Status:        "ok",
		},
		FraudSignal: &fraudv1.FraudSignal{
			FraudSignalId: req.GetFraudSignalId(),
			CorrelationId: req.GetFraudSignal().GetCorrelationId(),
		},
	}, nil
}

// auditEventValidationLazy dials acme.audit.v1.AuditEventValidationService the first time a handler needs it.
func (s *FraudSignalApprovalServer) auditEventValidationLazy() auditv1.AuditEventValidationServiceClient {
	s.auditEventValidationOnce.Do(func() { s.auditEventValidationCached = auditv1.NewAuditEventValidationServiceClient(s.conn) })
	return s.auditEventValidationCached
}
