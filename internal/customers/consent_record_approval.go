package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	analyticsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/analytics/v1"
	auditv1 "github.com/hounddogai/protomap-test-golang/gen/acme/audit/v1"
	catalogv1 "github.com/hounddogai/protomap-test-golang/gen/acme/catalog/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/cache"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// ConsentRecordApprovalServer implements `acme.customers.v1.ConsentRecordApprovalService`.
// Routes consent records through approval, with escalation and delegation.
type ConsentRecordApprovalServer struct {
	*consentRecordApprovalHandlers
}

// consentRecordApprovalHandlers holds the handlers of ConsentRecordApprovalServer, with what they use.
type consentRecordApprovalHandlers struct {
	customersv1.UnimplementedConsentRecordApprovalServiceServer
	store                        *Store
	log                          *slog.Logger
	meteredAuditEventQuery       meteredAuditEventQuery
	searchIndexHistoryClient     searchv1.SearchIndexHistoryServiceClient
	customerWorkflowClient       customersv1.CustomerWorkflowServiceClient
	householdQueryClient         customersv1.HouseholdQueryServiceClient
	consentRecordLifecycleClient customersv1.ConsentRecordLifecycleServiceClient
	conn                         *grpc.ClientConn
	productVariantWorkflowClient catalogv1.ProductVariantWorkflowServiceClient
	redemptionLifecycleOnce      sync.Once
	redemptionLifecycleCached    promotionsv1.RedemptionLifecycleServiceClient
	customerApprovalClient       customersv1.CustomerApprovalServiceClient
	synonymSyncClient            searchv1.SynonymSyncServiceClient
	preferenceImportClient       customersv1.PreferenceImportServiceClient
	featureFlagValidationClient  featureflagsv1.FeatureFlagValidationServiceClient
	roleNotificationClient       identityv1.RoleNotificationServiceClient
	analyticsEventQueryClient    analyticsv1.AnalyticsEventQueryServiceClient
	customerSegmentSyncClient    customersv1.CustomerSegmentSyncServiceClient
}

// NewConsentRecordApprovalServer wires a ConsentRecordApprovalServer.
func NewConsentRecordApprovalServer(deps platform.Deps) *ConsentRecordApprovalServer {
	return &ConsentRecordApprovalServer{consentRecordApprovalHandlers: &consentRecordApprovalHandlers{
		store:                        NewStore(deps.DB),
		log:                          deps.Logger.With("service", "acme.customers.v1.ConsentRecordApprovalService"),
		meteredAuditEventQuery:       meteredAuditEventQuery{AuditEventQueryServiceClient: auditv1.NewAuditEventQueryServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		searchIndexHistoryClient:     searchv1.NewSearchIndexHistoryServiceClient(deps.Conn),
		customerWorkflowClient:       customersv1.NewCustomerWorkflowServiceClient(deps.Conn),
		householdQueryClient:         customersv1.NewHouseholdQueryServiceClient(deps.Conn),
		consentRecordLifecycleClient: customersv1.NewConsentRecordLifecycleServiceClient(deps.Conn),
		conn:                         deps.Conn,
		productVariantWorkflowClient: catalogv1.NewProductVariantWorkflowServiceClient(deps.Conn),
		customerApprovalClient:       customersv1.NewCustomerApprovalServiceClient(deps.Conn),
		synonymSyncClient:            searchv1.NewSynonymSyncServiceClient(deps.Conn),
		preferenceImportClient:       customersv1.NewPreferenceImportServiceClient(deps.Conn),
		featureFlagValidationClient:  featureflagsv1.NewFeatureFlagValidationServiceClient(deps.Conn),
		roleNotificationClient:       identityv1.NewRoleNotificationServiceClient(deps.Conn),
		analyticsEventQueryClient:    analyticsv1.NewAnalyticsEventQueryServiceClient(deps.Conn),
		customerSegmentSyncClient:    customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
	}}
}

// SubmitConsentRecord submits a consent record.
func (s *consentRecordApprovalHandlers) SubmitConsentRecord(ctx context.Context, req *customersv1.SubmitConsentRecordRequest) (*customersv1.SubmitConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SubmitConsentRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "submit consent record", "tenant", req.GetContext().GetTenantId())
	countAuditEventsRequest := &auditv1.CountAuditEventsRequest{
		Context: req.GetContext(),
		Filter:  &auditv1.CountAuditEventsRequest_Filter{CreatedAfter: req.GetConsentRecord().GetOccurredAt()},
	}
	countAuditEventsReply, err := s.meteredAuditEventQuery.CountAuditEvents(ctx, countAuditEventsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventQueryService/CountAuditEvents")
	}
	for _, measurement := range req.GetConsentRecord().GetMeasurements() {
		if _, err := s.searchIndexHistoryClient.CompareSearchIndexVersions(ctx, &searchv1.CompareSearchIndexVersionsRequest{
			Context: req.GetContext(),
			SearchIndex: &searchv1.SearchIndex{
				Dimensions: &searchv1.SearchIndexDimensions{Unit: measurement.GetUnit()},
				Owner:      &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.search.v1.SearchIndexHistoryService/CompareSearchIndexVersions")
		}
	}
	return &customersv1.SubmitConsentRecordResponse{
		Result: &commonv1.OperationResult{
			Status:  countAuditEventsReply.GetResult().GetStatus(),
			Message: countAuditEventsReply.GetResult().GetMessage(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Attributes:      req.GetConsentRecord().GetAttributes(),
		},
	}, nil
}

// ApproveConsentRecord approves a consent record.
func (s *consentRecordApprovalHandlers) ApproveConsentRecord(ctx context.Context, req *customersv1.ApproveConsentRecordRequest) (*customersv1.ApproveConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ApproveConsentRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "approve consent record", "tenant", req.GetContext().GetTenantId())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := consentRecordApprovalHandlersPauseCustomerWorkflowSteps[mode]
	pauseCustomerWorkflowRequest := &customersv1.PauseCustomerWorkflowRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			RelationshipManager: &identityv1.UserRef{
				DisplayName: req.GetDetails().GetApprover().GetDisplayName(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
		},
	}
	pauseCustomerWorkflowReply, err := run(s, ctx, pauseCustomerWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerWorkflowService/PauseCustomerWorkflow")
	}
	return &customersv1.ApproveConsentRecordResponse{
		Result: &commonv1.OperationResult{
			Status:        pauseCustomerWorkflowReply.GetResult().GetStatus(),
			CorrelationId: req.GetConsentRecord().GetCorrelationId(),
		},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
	}, nil
}

// RejectConsentRecord rejects a consent record.
func (s *consentRecordApprovalHandlers) RejectConsentRecord(ctx context.Context, req *customersv1.RejectConsentRecordRequest) (*customersv1.RejectConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RejectConsentRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "reject consent record", "tenant", req.GetContext().GetTenantId())
	peekHouseholdReply, err := s.householdQueryClient.PeekHousehold(ctx, &customersv1.PeekHouseholdRequest{
		Context:     req.GetContext(),
		HouseholdId: req.GetConsentRecord().GetSubject().GetHouseholdId(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdQueryService/PeekHousehold")
	}
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	renewConsentRecordRequest := &customersv1.RenewConsentRecordRequest{
		Context:         req.GetContext(),
		ConsentRecordId: req.GetConsentRecord().GetConsentRecordId(),
		Details: &customersv1.RenewConsentRecordRequest_Details{
			NewOwner:   &identityv1.UserRef{Email: req.GetDetails().GetApprover().GetEmail()},
			ReasonCode: envelope.GetDisplayName(),
		},
	}
	renewConsentRecordReply, err := grpcx.Call(ctx, s.consentRecordLifecycleClient.RenewConsentRecord, renewConsentRecordRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordLifecycleService/RenewConsentRecord")
	}
	return &customersv1.RejectConsentRecordResponse{
		Result: &commonv1.OperationResult{
			Details:       peekHouseholdReply.GetResult().GetDetails(),
			CorrelationId: renewConsentRecordReply.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Source:          req.GetConsentRecord().GetSource(),
		},
	}, nil
}

// EscalateConsentRecord escalates a consent record.
func (s *consentRecordApprovalHandlers) EscalateConsentRecord(ctx context.Context, req *customersv1.EscalateConsentRecordRequest) (*customersv1.EscalateConsentRecordResponse, error) {
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "escalate consent record", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.EscalateConsentRecordRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	packed, err := anypb.New(req.GetConsentRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	lintAuditEventsRequest := &auditv1.LintAuditEventsRequest{
		Context: req.GetContext(),
		Details: &auditv1.LintAuditEventsRequest_Details{
			Contact: &commonv1.ContactInfo{Email: (&decoded).GetApprover().GetEmail()},
		},
		AuditEvent:   &auditv1.AuditEvent{Detail: packed},
		AuditEventId: envelope.GetDisplayName(),
	}
	lintAuditEventsReply := new(auditv1.LintAuditEventsResponse)
	if err := grpc.Invoke(ctx, "/acme.audit.v1.AuditEventValidationService/LintAuditEvents", lintAuditEventsRequest, lintAuditEventsReply, s.conn); err != nil {
		return nil, grpcx.Wrap(err, "acme.audit.v1.AuditEventValidationService/LintAuditEvents")
	}
	var envelope2 customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	signalProductVariantWorkflowReply, err := s.productVariantWorkflowClient.SignalProductVariantWorkflow(ctx, &catalogv1.SignalProductVariantWorkflowRequest{
		Context: req.GetContext(),
		ProductVariant: &catalogv1.ProductVariant{
			Owner: &identityv1.UserRef{
				Email:       req.GetDetails().GetApprover().GetEmail(),
				DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
			},
		},
		ProductVariantId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.ProductVariantWorkflowService/SignalProductVariantWorkflow")
	}
	packed2, err := anypb.New(req.GetConsentRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	var envelope3 customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope3); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	scheduleRedemptionExpiryRequest := &promotionsv1.ScheduleRedemptionExpiryRequest{
		Context: req.GetContext(),
		Details: &promotionsv1.ScheduleRedemptionExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: signalProductVariantWorkflowReply.GetProductVariant().GetOwner().GetEmail(),
			},
		},
		Redemption:   &promotionsv1.Redemption{Detail: packed2},
		RedemptionId: envelope3.GetDisplayName(),
	}
	scheduleRedemptionExpiryReply, err := s.redemptionLifecycleLazy().ScheduleRedemptionExpiry(ctx, scheduleRedemptionExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionLifecycleService/ScheduleRedemptionExpiry")
	}
	for _, measurement := range req.GetConsentRecord().GetMeasurements() {
		if _, err := s.customerApprovalClient.ListPendingCustomers(ctx, &customersv1.ListPendingCustomersRequest{
			Context: req.GetContext(),
			Filter: &customersv1.ListPendingCustomersRequest_Filter{
				CreatedAfter: measurement.GetObservedAt(),
				Region:       signalProductVariantWorkflowReply.GetProductVariant().GetAddress().GetRegion(),
				OwnerId:      signalProductVariantWorkflowReply.GetProductVariant().GetComponents()["primary"].GetComponentId(),
				Labels:       lintAuditEventsReply.GetAuditEvent().GetAttributes(),
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerApprovalService/ListPendingCustomers")
		}
	}
	return &customersv1.EscalateConsentRecordResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			OccurredAt:      lintAuditEventsReply.GetAuditEvent().GetOccurredAt(),
			CorrelationId:   lintAuditEventsReply.GetAuditEvent().GetCorrelationId(),
			Sequence:        scheduleRedemptionExpiryReply.GetRedemption().GetSequence(),
			ConsentRecordId: req.GetConsentRecordId(),
			Kind:            req.GetConsentRecord().GetKind(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListPendingConsentRecords lists consent records a page at a time.
func (s *consentRecordApprovalHandlers) ListPendingConsentRecords(ctx context.Context, req *customersv1.ListPendingConsentRecordsRequest) (*customersv1.ListPendingConsentRecordsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list pending consent records", "tenant", req.GetContext().GetTenantId())
	snapshot, err := protojson.Marshal(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pushSynonymRequest := &searchv1.PushSynonymRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner: &identityv1.UserRef{
				UserId: req.GetContext().GetActor().GetUserId(),
				Email:  req.GetContext().GetActor().GetEmail(),
			},
		},
		Details: &searchv1.PushSynonymRequest_Details{Payload: snapshot},
	}
	type pushSynonymResult struct {
		reply *searchv1.PushSynonymResponse
		err   error
	}
	pushSynonymResults := make(chan pushSynonymResult, 1)
	go func() {
		reply, err := s.synonymSyncClient.PushSynonym(ctx, pushSynonymRequest)
		pushSynonymResults <- pushSynonymResult{reply, err}
	}()
	var pushSynonymReply *searchv1.PushSynonymResponse
	select {
	case result := <-pushSynonymResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.search.v1.SynonymSyncService/PushSynonym")
		}
		pushSynonymReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &customersv1.ListPendingConsentRecordsResponse{
		ConsentRecords: []*customersv1.ConsentRecord{&customersv1.ConsentRecord{
			CorrelationId: pushSynonymReply.GetResult().GetCorrelationId(),
		}},
		Result: &commonv1.OperationResult{
			Details: pushSynonymReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// DelegateConsentRecordApproval delegates a consent record.
func (s *consentRecordApprovalHandlers) DelegateConsentRecordApproval(ctx context.Context, req *customersv1.DelegateConsentRecordApprovalRequest) (*customersv1.DelegateConsentRecordApprovalResponse, error) {
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "delegate consent record approval", "tenant", req.GetContext().GetTenantId())
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	submitBrandRequest := &catalogv1.SubmitBrandRequest{
		Context: req.GetContext(),
		Brand: &catalogv1.Brand{
			RelationshipManager: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
		},
		BrandId: envelope.GetDisplayName(),
	}
	submitBrandReply := new(catalogv1.SubmitBrandResponse)
	if err := s.conn.Invoke(ctx, catalogBrandApprovalServiceSubmitBrandMethod, submitBrandRequest, submitBrandReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.catalog.v1.BrandApprovalService/SubmitBrand")
	}
	var envelope2 customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope2); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	pauseCustomerWorkflowReply, err := s.customerWorkflowClient.PauseCustomerWorkflow(ctx, &customersv1.PauseCustomerWorkflowRequest{
		Context: req.GetContext(),
		Customer: &customersv1.Customer{
			IdentityDocument: &customersv1.Customer_Passport{Passport: &customersv1.CustomerPassport{
				IssuingCountry: submitBrandReply.GetBrand().GetNationalId().GetIssuingCountry(),
			}},
		},
		CustomerId: envelope2.GetDisplayName(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerWorkflowService/PauseCustomerWorkflow")
	}
	return &customersv1.DelegateConsentRecordApprovalResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			Attributes:      submitBrandReply.GetBrand().GetAttributes(),
			ConsentRecordId: req.GetConsentRecordId(),
			Source:          req.GetConsentRecord().GetSource(),
		},
		Outcome: &customersv1.DelegateConsentRecordApprovalResponse_Outcome{Message: submitBrandReply.GetResult().GetMessage()},
		Result: &commonv1.OperationResult{
			Message: pauseCustomerWorkflowReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// RecallConsentRecord recalls a consent record.
func (s *consentRecordApprovalHandlers) RecallConsentRecord(ctx context.Context, req *customersv1.RecallConsentRecordRequest) (*customersv1.RecallConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RecallConsentRecord", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "recall consent record", "tenant", req.GetContext().GetTenantId())
	detailsCache := cache.New[string, *customersv1.RecallConsentRecordRequest_Details](1)
	detailsCache.Put(req.GetContext().GetTraceId(), req.GetDetails())
	cachedDetails, _ := detailsCache.Get(req.GetContext().GetTraceId())
	step := &consentRecordApprovalHandlersRecallConsentRecordValidatePreferenceImportStep{request: &customersv1.ValidatePreferenceImportRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Owner: &identityv1.UserRef{
				UserId:      cachedDetails.GetApprover().GetUserId(),
				DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Action: &customersv1.Preference_Block{Block: &customersv1.PreferenceBlockAction{NotifyEmail: req.GetDetails().GetApprover().GetEmail()}},
		},
		Annotations: req.GetAnnotations(),
	}}
	if err := s.runRecallConsentRecordValidatePreferenceImport(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceImportService/ValidatePreferenceImport")
	}
	validatePreferenceImportReply := step.reply
	for _, measurement := range req.GetConsentRecord().GetMeasurements() {
		if _, err := s.featureFlagValidationClient.ValidateFeatureFlag(ctx, &featureflagsv1.ValidateFeatureFlagRequest{
			Context: req.GetContext(),
			FeatureFlag: &featureflagsv1.FeatureFlag{
				Owner: &identityv1.UserRef{UserId: req.GetDetails().GetApprover().GetUserId()},
				Audit: &commonv1.AuditInfo{UpdatedAt: measurement.GetObservedAt()},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagValidationService/ValidateFeatureFlag")
		}
	}
	return &customersv1.RecallConsentRecordResponse{
		Outcome: &customersv1.RecallConsentRecordResponse_Outcome{
			Status: validatePreferenceImportReply.GetOutcome().GetStatus(),
		},
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			OccurredAt:      req.GetConsentRecord().GetOccurredAt(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// GetConsentRecordApprovalStatus returns a consent record.
func (s *consentRecordApprovalHandlers) GetConsentRecordApprovalStatus(ctx context.Context, req *customersv1.GetConsentRecordApprovalStatusRequest) (*customersv1.GetConsentRecordApprovalStatusResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "get_consent_record_approval_status"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get consent record approval status", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeRoleUpdatesRequest := newSubscribeRoleUpdatesRequest(withSubscribeRoleUpdatesContext(req.GetContext()), withSubscribeRoleUpdatesDetails(&identityv1.SubscribeRoleUpdatesRequest_Details{
		Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Variables: req.GetContext().GetBaggage(),
		Payload:   packed,
	}))
	subscribeRoleUpdatesReply, err := s.roleNotificationClient.SubscribeRoleUpdates(ctx, subscribeRoleUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.RoleNotificationService/SubscribeRoleUpdates")
	}
	exportAnalyticsEventsStream, err := s.analyticsEventQueryClient.ExportAnalyticsEvents(ctx, &analyticsv1.ExportAnalyticsEventsRequest{
		Context: req.GetContext(),
		Filter: &analyticsv1.ExportAnalyticsEventsRequest_Filter{
			CreatedAfter: subscribeRoleUpdatesReply.GetRole().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.analytics.v1.AnalyticsEventQueryService/ExportAnalyticsEvents")
	}
	for {
		received, err := exportAnalyticsEventsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.analytics.v1.AnalyticsEventQueryService/ExportAnalyticsEvents")
		}
		_ = received
	}
	return &customersv1.GetConsentRecordApprovalStatusResponse{
		Result:        &commonv1.OperationResult{Status: subscribeRoleUpdatesReply.GetResult().GetStatus()},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
	}, nil
}

// CommentConsentRecordApproval comments a consent record.
func (s *consentRecordApprovalHandlers) CommentConsentRecordApproval(ctx context.Context, req *customersv1.CommentConsentRecordApprovalRequest) (*customersv1.CommentConsentRecordApprovalResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "comment consent record approval", "tenant", req.GetContext().GetTenantId())
	validateFeatureFlagRequest := &featureflagsv1.ValidateFeatureFlagRequest{
		Context: req.GetContext(),
		FeatureFlag: &featureflagsv1.FeatureFlag{
			Owner: &identityv1.UserRef{
				DisplayName: req.GetConsentRecord().GetSubject().GetDisplayName(),
				UserId:      req.GetDetails().GetApprover().GetUserId(),
			},
			Action: &featureflagsv1.FeatureFlag_Block{Block: &featureflagsv1.FeatureFlagBlockAction{Reason: req.GetReason()}},
		},
		Details: &featureflagsv1.ValidateFeatureFlagRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetApprover().GetEmail()},
		},
	}
	type validateFeatureFlagResult struct {
		reply *featureflagsv1.ValidateFeatureFlagResponse
		err   error
	}
	validateFeatureFlagResults := make(chan validateFeatureFlagResult, 1)
	go func() {
		reply, err := s.featureFlagValidationClient.ValidateFeatureFlag(ctx, validateFeatureFlagRequest)
		validateFeatureFlagResults <- validateFeatureFlagResult{reply, err}
	}()
	var validateFeatureFlagReply *featureflagsv1.ValidateFeatureFlagResponse
	select {
	case result := <-validateFeatureFlagResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.featureflags.v1.FeatureFlagValidationService/ValidateFeatureFlag")
		}
		validateFeatureFlagReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	snapshot, err := protojson.Marshal(req.GetConsentRecord())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	var envelope customersv1.CustomerRef
	if err := req.GetConsentRecord().GetDetail().UnmarshalTo(&envelope); err != nil {
		s.log.WarnContext(ctx, "unexpected envelope", "error", err)
	}
	syncCustomerSegmentsRequest := &customersv1.SyncCustomerSegmentsRequest{}
	syncCustomerSegmentsRequest.Context = req.GetContext()
	syncCustomerSegmentsRequest.CustomerSegment = &customersv1.CustomerSegment{
		Audit: &commonv1.AuditInfo{
			CreatedAt: validateFeatureFlagReply.GetFeatureFlag().GetAudit().GetCreatedAt(),
		},
	}
	syncCustomerSegmentsRequest.Details = &customersv1.SyncCustomerSegmentsRequest_Details{Payload: snapshot}
	syncCustomerSegmentsRequest.CustomerSegmentId = envelope.GetDisplayName()
	syncCustomerSegmentsReply, err := s.customerSegmentSyncClient.SyncCustomerSegments(ctx, syncCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/SyncCustomerSegments")
	}
	return &customersv1.CommentConsentRecordApprovalResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId:   validateFeatureFlagReply.GetResult().GetCorrelationId(),
			ConsentRecordId: req.GetConsentRecordId(),
			Kind:            req.GetConsentRecord().GetKind(),
		},
		Result: &commonv1.OperationResult{
			Message: validateFeatureFlagReply.GetResult().GetMessage(),
			Status:  syncCustomerSegmentsReply.GetOutcome().GetStatus(),
		},
		Outcome: &customersv1.CommentConsentRecordApprovalResponse_Outcome{
			ReferenceId: syncCustomerSegmentsReply.GetOutcome().GetReferenceId(),
		},
	}, nil
}

// consentRecordApprovalHandlersPauseCustomerWorkflowSteps are the ways to run
// acme.customers.v1.CustomerWorkflowService/PauseCustomerWorkflow, by name.
var consentRecordApprovalHandlersPauseCustomerWorkflowSteps = map[string]func(*consentRecordApprovalHandlers, context.Context, *customersv1.PauseCustomerWorkflowRequest) (*customersv1.PauseCustomerWorkflowResponse, error){
	"default": (*consentRecordApprovalHandlers).pauseCustomerWorkflowStep,
	"audited": (*consentRecordApprovalHandlers).pauseCustomerWorkflowAuditedStep,
}

func (s *consentRecordApprovalHandlers) pauseCustomerWorkflowStep(ctx context.Context, request *customersv1.PauseCustomerWorkflowRequest) (*customersv1.PauseCustomerWorkflowResponse, error) {
	return s.customerWorkflowClient.PauseCustomerWorkflow(ctx, request)
}

func (s *consentRecordApprovalHandlers) pauseCustomerWorkflowAuditedStep(ctx context.Context, request *customersv1.PauseCustomerWorkflowRequest) (*customersv1.PauseCustomerWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.CustomerWorkflowService/PauseCustomerWorkflow")
	return s.customerWorkflowClient.PauseCustomerWorkflow(ctx, request)
}

// redemptionLifecycleLazy dials acme.promotions.v1.RedemptionLifecycleService the first time a handler needs it.
func (s *consentRecordApprovalHandlers) redemptionLifecycleLazy() promotionsv1.RedemptionLifecycleServiceClient {
	s.redemptionLifecycleOnce.Do(func() { s.redemptionLifecycleCached = promotionsv1.NewRedemptionLifecycleServiceClient(s.conn) })
	return s.redemptionLifecycleCached
}

// consentRecordApprovalHandlersRecallConsentRecordValidatePreferenceImportStep carries the request of one step of
// RecallConsentRecord and, once it ran, its reply.
type consentRecordApprovalHandlersRecallConsentRecordValidatePreferenceImportStep struct {
	request *customersv1.ValidatePreferenceImportRequest
	reply   *customersv1.ValidatePreferenceImportResponse
}

func (s *consentRecordApprovalHandlers) runRecallConsentRecordValidatePreferenceImport(ctx context.Context, step *consentRecordApprovalHandlersRecallConsentRecordValidatePreferenceImportStep) error {
	reply, err := s.preferenceImportClient.ValidatePreferenceImport(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
