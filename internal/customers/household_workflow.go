package customers

import (
	"context"
	"errors"
	"io"
	"log/slog"

	appointmentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/appointments/v1"
	authorizationv1 "github.com/hounddogai/protomap-test-golang/gen/acme/authorization/v1"
	careplansv1 "github.com/hounddogai/protomap-test-golang/gen/acme/careplans/v1"
	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	fraudv1 "github.com/hounddogai/protomap-test-golang/gen/acme/fraud/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	inventoryv1 "github.com/hounddogai/protomap-test-golang/gen/acme/inventory/v1"
	searchv1 "github.com/hounddogai/protomap-test-golang/gen/acme/search/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	shippingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/shipping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/registry"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// HouseholdWorkflowServer implements `acme.customers.v1.HouseholdWorkflowService`.
// Runs the long-lived workflow that takes a household from start to finish.
type HouseholdWorkflowServer struct {
	customersv1.UnimplementedHouseholdWorkflowServiceServer
	store                           *Store
	log                             *slog.Logger
	sellerListingReportingClient    sellersv1.SellerListingReportingServiceClient
	householdReportingClient        customersv1.HouseholdReportingServiceClient
	schedulePreferenceExpiryFn      func(context.Context, *customersv1.SchedulePreferenceExpiryRequest, ...grpc.CallOption) (*customersv1.SchedulePreferenceExpiryResponse, error)
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	trackingEventReportingClient    shippingv1.TrackingEventReportingServiceClient
	stockMovementWorkflowClient     inventoryv1.StockMovementWorkflowServiceClient
	carePlanClient                  careplansv1.CarePlanServiceClient
	synonymHistoryClient            searchv1.SynonymHistoryServiceClient
	clients                         *registry.Registry
	accessPolicyValidationClient    authorizationv1.AccessPolicyValidationServiceClient
	conn                            *grpc.ClientConn
	sessionNotificationClient       identityv1.SessionNotificationServiceClient
	consentRecordReportingClient    customersv1.ConsentRecordReportingServiceClient
	searchQueryQueryClient          searchv1.SearchQueryQueryServiceClient
}

// NewHouseholdWorkflowServer wires a HouseholdWorkflowServer.
func NewHouseholdWorkflowServer(deps platform.Deps) *HouseholdWorkflowServer {
	preferenceLifecycle := customersv1.NewPreferenceLifecycleServiceClient(deps.Conn)
	return &HouseholdWorkflowServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.customers.v1.HouseholdWorkflowService"),
		sellerListingReportingClient:    sellersv1.NewSellerListingReportingServiceClient(deps.Conn),
		householdReportingClient:        customersv1.NewHouseholdReportingServiceClient(deps.Conn),
		schedulePreferenceExpiryFn:      preferenceLifecycle.SchedulePreferenceExpiry,
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		trackingEventReportingClient:    shippingv1.NewTrackingEventReportingServiceClient(deps.Conn),
		stockMovementWorkflowClient:     inventoryv1.NewStockMovementWorkflowServiceClient(deps.Conn),
		carePlanClient:                  careplansv1.NewCarePlanServiceClient(deps.Conn),
		synonymHistoryClient:            searchv1.NewSynonymHistoryServiceClient(deps.Conn),
		clients:                         deps.Clients,
		accessPolicyValidationClient:    authorizationv1.NewAccessPolicyValidationServiceClient(deps.Conn),
		conn:                            deps.Conn,
		sessionNotificationClient:       identityv1.NewSessionNotificationServiceClient(deps.Conn),
		consentRecordReportingClient:    customersv1.NewConsentRecordReportingServiceClient(deps.Conn),
		searchQueryQueryClient:          searchv1.NewSearchQueryQueryServiceClient(deps.Conn),
	}
}

// StartHouseholdWorkflow starts a household.
func (s *HouseholdWorkflowServer) StartHouseholdWorkflow(ctx context.Context, req *customersv1.StartHouseholdWorkflowRequest) (*customersv1.StartHouseholdWorkflowResponse, error) {
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "start household workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	rejectRiskAssessmentRequest := &fraudv1.RejectRiskAssessmentRequest{
		Context: req.GetContext(),
		RiskAssessment: &fraudv1.RiskAssessment{
			Audit: &commonv1.AuditInfo{CreatedBy: req.GetHousehold().GetAudit().GetCreatedBy()},
		},
	}
	rejectRiskAssessmentReply, err := rejectRiskAssessmentCall(ctx, rejectRiskAssessmentRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.fraud.v1.RiskAssessmentApprovalService/RejectRiskAssessment")
	}
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	generateSellerListingReportReply, err := s.sellerListingReportingClient.GenerateSellerListingReport(ctx, &sellersv1.GenerateSellerListingReportRequest{
		Context:     req.GetContext(),
		Annotations: annotations,
		Reason:      req.GetReason(),
		Details:     &sellersv1.GenerateSellerListingReportRequest_Details{RecipientEmails: names},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerListingReportingService/GenerateSellerListingReport")
	}
	source := req.GetHousehold().ProtoReflect()
	householdId := source.Get(source.Descriptor().Fields().ByName(protoreflect.Name("household_id"))).String()
	deleteHouseholdReportRequest := &customersv1.DeleteHouseholdReportRequest{
		Context:     req.GetContext(),
		HouseholdId: householdId,
		Etag:        req.GetHousehold().GetAudit().GetEtag(),
	}
	deleteHouseholdReportReply, err := grpcx.Call(ctx, s.householdReportingClient.DeleteHouseholdReport, deleteHouseholdReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdReportingService/DeleteHouseholdReport")
	}
	_ = deleteHouseholdReportReply
	return &customersv1.StartHouseholdWorkflowResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						UserId: rejectRiskAssessmentReply.GetRiskAssessment().GetAssignee().GetUserId(),
					},
				}},
				Audit: &commonv1.AuditInfo{
					Etag: rejectRiskAssessmentReply.GetRiskAssessment().GetAudit().GetEtag(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
			Depth:       req.GetHousehold().GetDepth(),
		},
		Result: &commonv1.OperationResult{
			Details: generateSellerListingReportReply.GetResult().GetDetails(),
			Status:  "ok",
		},
	}, nil
}

// AdvanceHouseholdWorkflow advances a household.
func (s *HouseholdWorkflowServer) AdvanceHouseholdWorkflow(ctx context.Context, req *customersv1.AdvanceHouseholdWorkflowRequest) (*customersv1.AdvanceHouseholdWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "advance household workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "created_by", req.GetHousehold().GetAudit().GetCreatedBy())
	schedulePreferenceExpiryRequest := &customersv1.SchedulePreferenceExpiryRequest{
		Context: req.GetContext(),
		Preference: &customersv1.Preference{
			Audit: &commonv1.AuditInfo{
				CreatedBy: tenancy.Value(ctx, "created_by"),
				Etag:      req.GetHousehold().GetAudit().GetEtag(),
			},
		},
		Details: &customersv1.SchedulePreferenceExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone:            req.GetDetails().GetNotify().GetPhone(),
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	schedulePreferenceExpiryReply, err := s.schedulePreferenceExpiryFn(ctx, schedulePreferenceExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceLifecycleService/SchedulePreferenceExpiry")
	}
	encoded, err := protojson.Marshal(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.Household
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	lintCustomerSegmentsReply, err := s.customerSegmentValidationClient.LintCustomerSegments(ctx, &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Audit:  &commonv1.AuditInfo{CreatedBy: (&decoded).GetAudit().GetCreatedBy()},
			Scopes: names,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &customersv1.AdvanceHouseholdWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: schedulePreferenceExpiryReply.GetOutcome().GetStatus(),
		},
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Audit: &commonv1.AuditInfo{
					CreatedBy: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedBy(),
				},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetCreatedAt(),
			},
			HouseholdId: req.GetHouseholdId(),
			Location:    req.GetHousehold().GetLocation(),
		},
	}, nil
}

// CancelHouseholdWorkflow cancels a household.
func (s *HouseholdWorkflowServer) CancelHouseholdWorkflow(ctx context.Context, req *customersv1.CancelHouseholdWorkflowRequest) (*customersv1.CancelHouseholdWorkflowResponse, error) {
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "cancel household workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	generateTrackingEventReportRequest := newGenerateTrackingEventReportRequest(withGenerateTrackingEventReportContext(req.GetContext()), withGenerateTrackingEventReportAnnotations(req.GetAnnotations()), withGenerateTrackingEventReportReason(req.GetReason()), withGenerateTrackingEventReportDetails(&shippingv1.GenerateTrackingEventReportRequest_Details{Filters: req.GetHousehold().GetAttributes()}))
	generateTrackingEventReportReply, err := s.trackingEventReportingClient.GenerateTrackingEventReport(ctx, generateTrackingEventReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.shipping.v1.TrackingEventReportingService/GenerateTrackingEventReport")
	}
	advanceStockMovementWorkflowRequest := buildAdvanceStockMovementWorkflowRequestForCancelHouseholdWorkflow(req)
	advanceStockMovementWorkflowReply, err := s.stockMovementWorkflowClient.AdvanceStockMovementWorkflow(ctx, advanceStockMovementWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.inventory.v1.StockMovementWorkflowService/AdvanceStockMovementWorkflow")
	}
	return &customersv1.CancelHouseholdWorkflowResponse{
		Result: &commonv1.OperationResult{
			Message: generateTrackingEventReportReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Attributes: advanceStockMovementWorkflowReply.GetStockMovement().GetAttributes(),
			}},
			HouseholdId: req.GetHouseholdId(),
			ParentId:    req.GetHousehold().GetParentId(),
		},
	}, nil
}

// RetryHouseholdWorkflow retrys a household.
func (s *HouseholdWorkflowServer) RetryHouseholdWorkflow(ctx context.Context, req *customersv1.RetryHouseholdWorkflowRequest) (*customersv1.RetryHouseholdWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryHouseholdWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "retry household workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	batchGetCarePlansRequest := buildBatchGetCarePlansRequestForRetryHouseholdWorkflow(req)
	batchGetCarePlansReply, err := s.carePlanClient.BatchGetCarePlans(ctx, batchGetCarePlansRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanService/BatchGetCarePlans")
	}
	names := collectHouseholdNames(req.GetHousehold(), nil)
	recordSynonymVersionRequest := &searchv1.RecordSynonymVersionRequest{
		Context: req.GetContext(),
		Synonym: &searchv1.Synonym{
			Owner:  &identityv1.UserRef{UserId: req.GetHousehold().GetManager().GetUserId()},
			Scopes: names,
		},
	}
	recordSynonymVersionReply, err := grpcx.Call(ctx, s.synonymHistoryClient.RecordSynonymVersion, recordSynonymVersionRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SynonymHistoryService/RecordSynonymVersion")
	}
	consentRecordSync, err := registry.Lookup[customersv1.ConsentRecordSyncServiceClient](s.clients, "acme.customers.v1.ConsentRecordSyncService")
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	packed, err := anypb.New(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullConsentRecordsRequest := &customersv1.PullConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: recordSynonymVersionReply.GetSynonym().GetOwner().GetDisplayName(),
				HouseholdId: req.GetHouseholdId(),
			},
			Detail: packed,
		},
		Details: &customersv1.PullConsentRecordsRequest_Details{Payload: snapshot},
	}
	pullConsentRecordsReply, err := consentRecordSync.PullConsentRecords(ctx, pullConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/PullConsentRecords")
	}
	return &customersv1.RetryHouseholdWorkflowResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetCarePlansReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &customersv1.RetryHouseholdWorkflowResponse_Outcome{Status: recordSynonymVersionReply.GetOutcome().GetStatus()},
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{
					DisplayName: pullConsentRecordsReply.GetConsentRecord().GetSubject().GetDisplayName(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
			Name:        req.GetHousehold().GetName(),
		},
	}, nil
}

// PauseHouseholdWorkflow pauses a household.
func (s *HouseholdWorkflowServer) PauseHouseholdWorkflow(ctx context.Context, req *customersv1.PauseHouseholdWorkflowRequest) (*customersv1.PauseHouseholdWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "pause household workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotify().GetEmail())
	previewAccessPolicyReply, err := s.accessPolicyValidationClient.PreviewAccessPolicy(ctx, &authorizationv1.PreviewAccessPolicyRequest{
		Context: req.GetContext(),
		AccessPolicy: &authorizationv1.AccessPolicy{
			Owner: &identityv1.UserRef{Email: tenancy.Value(ctx, "email")},
			Audit: &commonv1.AuditInfo{Etag: req.GetHousehold().GetAudit().GetEtag()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.authorization.v1.AccessPolicyValidationService/PreviewAccessPolicy")
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetNotify().GetPreferredChannel())
	subscribeSessionUpdatesRequest := &identityv1.SubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
				Email:            previewAccessPolicyReply.GetAccessPolicy().GetOwner().GetEmail(),
			},
		},
	}
	subscribeSessionUpdatesReply := new(identityv1.SubscribeSessionUpdatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.SessionNotificationService_SubscribeSessionUpdates_FullMethodName, subscribeSessionUpdatesRequest, subscribeSessionUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates")
	}
	return &customersv1.PauseHouseholdWorkflowResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Audit: &commonv1.AuditInfo{
						Etag: previewAccessPolicyReply.GetAccessPolicy().GetAudit().GetEtag(),
					},
				}},
			}},
			Audit: &commonv1.AuditInfo{
				CreatedAt: previewAccessPolicyReply.GetAccessPolicy().GetAudit().GetCreatedAt(),
			},
			HouseholdId: req.GetHouseholdId(),
			Name:        req.GetHousehold().GetName(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: subscribeSessionUpdatesReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// ResumeHouseholdWorkflow resumes a household.
func (s *HouseholdWorkflowServer) ResumeHouseholdWorkflow(ctx context.Context, req *customersv1.ResumeHouseholdWorkflowRequest) (*customersv1.ResumeHouseholdWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ResumeHouseholdWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "resume household workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := householdWorkflowServerSubscribeSessionUpdatesSteps[mode]
	packed, err := anypb.New(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	subscribeSessionUpdatesRequest := &identityv1.SubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.SubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Name: req.GetDetails().GetNotify().GetName()},
			Payload:   packed,
		},
	}
	subscribeSessionUpdatesReply, err := run(s, ctx, subscribeSessionUpdatesRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates")
	}
	cloneCarePlanRequest := &careplansv1.CloneCarePlanRequest{
		Context: req.GetContext(),
		CarePlan: &careplansv1.CarePlan{
			Counterparty: &appointmentsv1.AppointmentRef{
				DisplayName: req.GetHousehold().GetManager().GetDisplayName(),
			},
		},
	}
	cloneCarePlanReply := new(careplansv1.CloneCarePlanResponse)
	if err := grpcx.Invoke(ctx, s.conn, careplansv1.CarePlanService_CloneCarePlan_FullMethodName, cloneCarePlanRequest, cloneCarePlanReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.careplans.v1.CarePlanService/CloneCarePlan")
	}
	return &customersv1.ResumeHouseholdWorkflowResponse{
		Household: &customersv1.Household{
			Manager: &identityv1.UserRef{
				UserId: subscribeSessionUpdatesReply.GetSession().GetAssignee().GetUserId(),
			},
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Locality: cloneCarePlanReply.GetCarePlan().GetBillingAddress().GetLocality(),
					},
				}},
			}},
			HouseholdId: req.GetHouseholdId(),
			Location:    req.GetHousehold().GetLocation(),
		},
		Result: &commonv1.OperationResult{
			Message: cloneCarePlanReply.GetResult().GetMessage(),
			Status:  "ok",
		},
	}, nil
}

// InspectHouseholdWorkflow returns a household.
func (s *HouseholdWorkflowServer) InspectHouseholdWorkflow(ctx context.Context, req *customersv1.InspectHouseholdWorkflowRequest) (*customersv1.InspectHouseholdWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "inspect_household_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect household workflow", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	packed, err := anypb.New(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	unsubscribeSessionUpdatesRequest := &identityv1.UnsubscribeSessionUpdatesRequest{
		Context: req.GetContext(),
		Details: &identityv1.UnsubscribeSessionUpdatesRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
			Variables: baggage,
			Payload:   packed,
		},
		SessionId: req.GetHouseholdId(),
	}
	unsubscribeSessionUpdatesReply := new(identityv1.UnsubscribeSessionUpdatesResponse)
	if err := grpcx.Invoke(ctx, s.conn, identityv1.SessionNotificationService_UnsubscribeSessionUpdates_FullMethodName, unsubscribeSessionUpdatesRequest, unsubscribeSessionUpdatesReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionNotificationService/UnsubscribeSessionUpdates")
	}
	return &customersv1.InspectHouseholdWorkflowResponse{
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Manager: &identityv1.UserRef{
					Email: unsubscribeSessionUpdatesReply.GetSession().GetAssignee().GetEmail(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
		},
		Result: &commonv1.OperationResult{
			Status: unsubscribeSessionUpdatesReply.GetResult().GetStatus(),
		},
	}, nil
}

// ListHouseholdWorkflows lists households a page at a time.
func (s *HouseholdWorkflowServer) ListHouseholdWorkflows(ctx context.Context, req *customersv1.ListHouseholdWorkflowsRequest) (*customersv1.ListHouseholdWorkflowsResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "list household workflows", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "email", req.GetContext().GetActor().GetEmail())
	labels := make(map[string]string, len(req.GetFilter().GetLabels()))
	for key, value := range req.GetFilter().GetLabels() {
		labels[key] = value
	}
	packed, err := anypb.New(req.GetPage())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareConsentRecordReportRequest := &customersv1.ShareConsentRecordReportRequest{
		Context: req.GetContext(),
		Details: &customersv1.ShareConsentRecordReportRequest_Details{
			Recipient: &commonv1.ContactInfo{Email: tenancy.Value(ctx, "email")},
			Payload:   packed,
		},
		Annotations: labels,
	}
	shareConsentRecordReportReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*customersv1.ShareConsentRecordReportResponse, error) {
		return s.consentRecordReportingClient.ShareConsentRecordReport(ctx, shareConsentRecordReportRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordReportingService/ShareConsentRecordReport")
	}
	streamConsentRecordMetricsStream, err := s.consentRecordReportingClient.StreamConsentRecordMetrics(ctx, &customersv1.StreamConsentRecordMetricsRequest{
		Context: req.GetContext(),
		Filter:  &customersv1.StreamConsentRecordMetricsRequest_Filter{Query: req.GetFilter().GetQuery()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordReportingService/StreamConsentRecordMetrics")
	}
	for {
		received, err := streamConsentRecordMetricsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordReportingService/StreamConsentRecordMetrics")
		}
		_ = received
	}
	return &customersv1.ListHouseholdWorkflowsResponse{
		Households: []*customersv1.Household{&customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					HouseholdId: shareConsentRecordReportReply.GetConsentRecord().GetSubject().GetHouseholdId(),
				}},
			}},
		}},
		Result: &commonv1.OperationResult{
			CorrelationId: shareConsentRecordReportReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// SignalHouseholdWorkflow signals a household.
func (s *HouseholdWorkflowServer) SignalHouseholdWorkflow(ctx context.Context, req *customersv1.SignalHouseholdWorkflowRequest) (*customersv1.SignalHouseholdWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalHouseholdWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "signal household workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	countSearchQueriesReply, err := s.searchQueryQueryClient.CountSearchQueries(ctx, &searchv1.CountSearchQueriesRequest{
		Context: req.GetContext(),
		Filter: &searchv1.CountSearchQueriesRequest_Filter{
			Region:       req.GetHousehold().GetLocation().GetRegion(),
			CreatedAfter: req.GetHousehold().GetAudit().GetUpdatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.search.v1.SearchQueryQueryService/CountSearchQueries")
	}
	return &customersv1.SignalHouseholdWorkflowResponse{
		Result: &commonv1.OperationResult{
			Details: countSearchQueriesReply.GetResult().GetDetails(),
			Status:  "ok",
		},
		Household: &customersv1.Household{
			HouseholdId: req.GetHouseholdId(),
			Depth:       req.GetHousehold().GetDepth(),
		},
	}, nil
}

// householdWorkflowServerSubscribeSessionUpdatesSteps are the ways to run
// acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates, by name.
var householdWorkflowServerSubscribeSessionUpdatesSteps = map[string]func(*HouseholdWorkflowServer, context.Context, *identityv1.SubscribeSessionUpdatesRequest) (*identityv1.SubscribeSessionUpdatesResponse, error){
	"default": (*HouseholdWorkflowServer).subscribeSessionUpdatesStep,
	"audited": (*HouseholdWorkflowServer).subscribeSessionUpdatesAuditedStep,
}

func (s *HouseholdWorkflowServer) subscribeSessionUpdatesStep(ctx context.Context, request *identityv1.SubscribeSessionUpdatesRequest) (*identityv1.SubscribeSessionUpdatesResponse, error) {
	return s.sessionNotificationClient.SubscribeSessionUpdates(ctx, request)
}

func (s *HouseholdWorkflowServer) subscribeSessionUpdatesAuditedStep(ctx context.Context, request *identityv1.SubscribeSessionUpdatesRequest) (*identityv1.SubscribeSessionUpdatesResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.identity.v1.SessionNotificationService/SubscribeSessionUpdates")
	return s.sessionNotificationClient.SubscribeSessionUpdates(ctx, request)
}
