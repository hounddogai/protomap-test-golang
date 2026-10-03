package customers

import (
	"context"
	"log/slog"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	payrollv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payroll/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	subscriptionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/subscriptions/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/eventbus"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
)

// ConsentRecordLifecycleServer implements `acme.customers.v1.ConsentRecordLifecycleService`.
// Moves a consent record between the states of its lifecycle.
type ConsentRecordLifecycleServer struct {
	customersv1.UnsafeConsentRecordLifecycleServiceServer
	store                                 *Store
	log                                   *slog.Logger
	consentRecordCalculationClient        customersv1.ConsentRecordCalculationServiceClient
	bus                                   *eventbus.Bus
	conn                                  *grpc.ClientConn
	payRunCalculationClient               payrollv1.PayRunCalculationServiceClient
	sessionQueryClient                    identityv1.SessionQueryServiceClient
	consentRecordSyncClient               customersv1.ConsentRecordSyncServiceClient
	refundWorkflowClient                  paymentsv1.RefundWorkflowServiceClient
	apiCredentialValidationClient         identityv1.ApiCredentialValidationServiceClient
	apiCredentialValidationClientFallback identityv1.ApiCredentialValidationServiceClient
	campaignApprovalClient                promotionsv1.CampaignApprovalServiceClient
	householdReportingClient              customersv1.HouseholdReportingServiceClient
	promotionWorkflowClient               promotionsv1.PromotionWorkflowServiceClient
}

// NewConsentRecordLifecycleServer wires a ConsentRecordLifecycleServer.
func NewConsentRecordLifecycleServer(deps platform.Deps) *ConsentRecordLifecycleServer {
	return &ConsentRecordLifecycleServer{
		store:                                 NewStore(deps.DB),
		log:                                   deps.Logger.With("service", "acme.customers.v1.ConsentRecordLifecycleService"),
		consentRecordCalculationClient:        customersv1.NewConsentRecordCalculationServiceClient(deps.Conn),
		bus:                                   deps.Bus,
		conn:                                  deps.Conn,
		payRunCalculationClient:               payrollv1.NewPayRunCalculationServiceClient(deps.Conn),
		sessionQueryClient:                    identityv1.NewSessionQueryServiceClient(deps.Conn),
		consentRecordSyncClient:               customersv1.NewConsentRecordSyncServiceClient(deps.Conn),
		refundWorkflowClient:                  paymentsv1.NewRefundWorkflowServiceClient(deps.Conn),
		apiCredentialValidationClient:         identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		apiCredentialValidationClientFallback: identityv1.NewApiCredentialValidationServiceClient(deps.Conn),
		campaignApprovalClient:                promotionsv1.NewCampaignApprovalServiceClient(deps.Conn),
		householdReportingClient:              customersv1.NewHouseholdReportingServiceClient(deps.Conn),
		promotionWorkflowClient:               promotionsv1.NewPromotionWorkflowServiceClient(deps.Conn),
	}
}

// ActivateConsentRecord activates a consent record.
func (s *ConsentRecordLifecycleServer) ActivateConsentRecord(ctx context.Context, req *customersv1.ActivateConsentRecordRequest) (*customersv1.ActivateConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ActivateConsentRecord", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "activate_consent_record"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate consent record", "tenant", req.GetContext().GetTenantId())
	calculateConsentRecordReply, err := s.consentRecordCalculationClient.CalculateConsentRecord(ctx, &customersv1.CalculateConsentRecordRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			ConsentRecordId: req.GetConsentRecordId(),
		},
		Reason: req.GetReason(),
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordCalculationService/CalculateConsentRecord")
	}
	if err := s.bus.Publish(ctx, eventbus.Event{
		Topic: "customers.activate_consent_record.list_customer_segment_conflicts",
		Key:   req.GetContext().GetTraceId(),
		Attributes: map[string]string{
			"filter.owner_id":   calculateConsentRecordReply.GetConsentRecord().GetSubject().GetHouseholdId(),
			"context.tenant_id": req.GetContext().GetTenantId(),
		},
	}); err != nil {
		return nil, grpcx.Wrap(err, "publish")
	}
	return &customersv1.ActivateConsentRecordResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			OccurredAt: calculateConsentRecordReply.GetConsentRecord().GetOccurredAt(),
			Subject: &customersv1.HouseholdRef{
				DisplayName: calculateConsentRecordReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			ConsentRecordId: req.GetConsentRecordId(),
		},
		Outcome: &customersv1.ActivateConsentRecordResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// SuspendConsentRecord suspends a consent record.
func (s *ConsentRecordLifecycleServer) SuspendConsentRecord(ctx context.Context, req *customersv1.SuspendConsentRecordRequest) (*customersv1.SuspendConsentRecordResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "suspend_consent_record"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend consent record", "tenant", req.GetContext().GetTenantId())
	signalRenewalScheduleWorkflowRequest := &subscriptionsv1.SignalRenewalScheduleWorkflowRequest{
		Context: req.GetContext(),
		RenewalSchedule: &subscriptionsv1.RenewalSchedule{
			Participants: []*subscriptionsv1.RenewalScheduleParticipant{&subscriptionsv1.RenewalScheduleParticipant{
				Contact: &commonv1.ContactInfo{
					Name:  req.GetDetails().GetNotifyContact().GetName(),
					Email: req.GetDetails().GetNewOwner().GetEmail(),
				},
			}},
		},
	}
	signalRenewalScheduleWorkflowReply := new(subscriptionsv1.SignalRenewalScheduleWorkflowResponse)
	if err := s.conn.Invoke(ctx, subscriptionsRenewalScheduleWorkflowServiceSignalRenewalScheduleWorkflowMethod, signalRenewalScheduleWorkflowRequest, signalRenewalScheduleWorkflowReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.subscriptions.v1.RenewalScheduleWorkflowService/SignalRenewalScheduleWorkflow")
	}
	return &customersv1.SuspendConsentRecordResponse{
		Outcome: &customersv1.SuspendConsentRecordResponse_Outcome{
			NextSteps:   signalRenewalScheduleWorkflowReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
		Result:        &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ReinstateConsentRecord reinstates a consent record.
func (s *ConsentRecordLifecycleServer) ReinstateConsentRecord(ctx context.Context, req *customersv1.ReinstateConsentRecordRequest) (*customersv1.ReinstateConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "reinstate_consent_record"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate consent record", "tenant", req.GetContext().GetTenantId())
	simulatePayRunRequest := buildSimulatePayRunRequestForReinstateConsentRecord(req)
	simulatePayRunReply, err := s.payRunCalculationClient.SimulatePayRun(ctx, simulatePayRunRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payroll.v1.PayRunCalculationService/SimulatePayRun")
	}
	suggestSessionsReply, err := s.sessionQueryClient.SuggestSessions(ctx, &identityv1.SuggestSessionsRequest{
		Context: req.GetContext(),
		Filter: &identityv1.SuggestSessionsRequest_Filter{
			Region:       simulatePayRunReply.GetPayRun().GetBillingAddress().GetRegion(),
			CreatedAfter: simulatePayRunReply.GetPayRun().GetAudit().GetCreatedAt(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.SessionQueryService/SuggestSessions")
	}
	return &customersv1.ReinstateConsentRecordResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds:  simulatePayRunReply.GetResult().GetReferenceIds(),
			CorrelationId: suggestSessionsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Outcome: &customersv1.ReinstateConsentRecordResponse_Outcome{
			Status:      suggestSessionsReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
	}, nil
}

// TerminateConsentRecord terminates a consent record.
func (s *ConsentRecordLifecycleServer) TerminateConsentRecord(ctx context.Context, req *customersv1.TerminateConsentRecordRequest) (*customersv1.TerminateConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "terminate_consent_record"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate consent record", "tenant", req.GetContext().GetTenantId())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	reconcileConsentRecordsRequest := &customersv1.ReconcileConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			ConsentRecordId: req.GetConsentRecordId(),
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			Detail: packed,
		},
		Details: &customersv1.ReconcileConsentRecordsRequest_Details{Payload: snapshot},
	}
	reconcileConsentRecordsReply, err := grpcx.Call(ctx, s.consentRecordSyncClient.ReconcileConsentRecords, reconcileConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/ReconcileConsentRecords")
	}
	ctx = tenancy.With(ctx, "email", req.GetDetails().GetNotifyContact().GetEmail())
	startRefundWorkflowRequest := &paymentsv1.StartRefundWorkflowRequest{
		Context: req.GetContext(),
		Refund: &paymentsv1.Refund{
			Counterparty: &customersv1.CustomerRef{
				DisplayName: reconcileConsentRecordsReply.GetConsentRecord().GetSubject().GetDisplayName(),
			},
			Lines: []*paymentsv1.RefundLine{&paymentsv1.RefundLine{
				Attributes: reconcileConsentRecordsReply.GetConsentRecord().GetAttributes(),
			}},
		},
		Details: &paymentsv1.StartRefundWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
				Email: tenancy.Value(ctx, "email"),
			},
		},
	}
	startRefundWorkflowReply, err := grpcx.Retry(ctx, 3, func(ctx context.Context) (*paymentsv1.StartRefundWorkflowResponse, error) {
		return s.refundWorkflowClient.StartRefundWorkflow(ctx, startRefundWorkflowRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.RefundWorkflowService/StartRefundWorkflow")
	}
	_ = startRefundWorkflowReply
	return &customersv1.TerminateConsentRecordResponse{
		ConsentRecord: &customersv1.ConsentRecord{
			Attributes:      reconcileConsentRecordsReply.GetConsentRecord().GetAttributes(),
			ConsentRecordId: req.GetConsentRecordId(),
		},
		Result: &commonv1.OperationResult{
			Message: reconcileConsentRecordsReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Outcome: &customersv1.TerminateConsentRecordResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// RenewConsentRecord renews a consent record.
func (s *ConsentRecordLifecycleServer) RenewConsentRecord(ctx context.Context, req *customersv1.RenewConsentRecordRequest) (*customersv1.RenewConsentRecordResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewConsentRecord", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "renew_consent_record"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew consent record", "tenant", req.GetContext().GetTenantId())
	routes := map[string]func(context.Context, *identityv1.PreviewApiCredentialRequest, ...grpc.CallOption) (*identityv1.PreviewApiCredentialResponse, error){
		"primary":  s.apiCredentialValidationClient.PreviewApiCredential,
		"fallback": s.apiCredentialValidationClientFallback.PreviewApiCredential,
	}
	route, ok := routes[req.GetContext().GetLocale()]
	if !ok {
		route = routes["primary"]
	}
	previewApiCredentialRequest := &identityv1.PreviewApiCredentialRequest{
		Context: req.GetContext(),
		Details: &identityv1.PreviewApiCredentialRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
		ApiCredential: &identityv1.ApiCredential{
			Contact: &commonv1.ContactInfo{Email: req.GetDetails().GetNewOwner().GetEmail()},
			Holder: &identityv1.SessionRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
		},
	}
	previewApiCredentialReply, err := route(ctx, previewApiCredentialRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.identity.v1.ApiCredentialValidationService/PreviewApiCredential")
	}
	commentCampaignApprovalRequest := buildCommentCampaignApprovalRequestForRenewConsentRecord(req, previewApiCredentialReply)
	commentCampaignApprovalReply, err := s.campaignApprovalClient.CommentCampaignApproval(ctx, commentCampaignApprovalRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignApprovalService/CommentCampaignApproval")
	}
	_ = commentCampaignApprovalReply
	return &customersv1.RenewConsentRecordResponse{
		Result:        &commonv1.OperationResult{Status: previewApiCredentialReply.GetResult().GetStatus()},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
		Outcome:       &customersv1.RenewConsentRecordResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// GetConsentRecordLifecycle returns a consent record.
func (s *ConsentRecordLifecycleServer) GetConsentRecordLifecycle(ctx context.Context, req *customersv1.GetConsentRecordLifecycleRequest) (*customersv1.GetConsentRecordLifecycleResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "consent_record", req.GetConsentRecordId(), "get_consent_record_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get consent record lifecycle", "tenant", req.GetContext().GetTenantId())
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key, value := range req.GetContext().GetBaggage() {
		baggage[key] = value
	}
	listPendingCampaignsRequest := &promotionsv1.ListPendingCampaignsRequest{}
	listPendingCampaignsRequest.Context = req.GetContext()
	listPendingCampaignsRequest.Filter = &promotionsv1.ListPendingCampaignsRequest_Filter{
		OwnerId:      req.GetConsentRecordId(),
		CreatedAfter: req.GetContext().GetDeadline(),
		Labels:       baggage,
	}
	listPendingCampaignsReply, err := s.campaignApprovalClient.ListPendingCampaigns(ctx, listPendingCampaignsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.CampaignApprovalService/ListPendingCampaigns")
	}
	step := &consentRecordLifecycleServerGetConsentRecordLifecycleScheduleHouseholdReportStep{request: &customersv1.ScheduleHouseholdReportRequest{
		Context:     req.GetContext(),
		Details:     &customersv1.ScheduleHouseholdReportRequest_Details{Filters: listPendingCampaignsReply.GetResult().GetDetails()},
		Annotations: req.GetContext().GetBaggage(),
	}}
	if err := s.runGetConsentRecordLifecycleScheduleHouseholdReport(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdReportingService/ScheduleHouseholdReport")
	}
	scheduleHouseholdReportReply := step.reply
	return &customersv1.GetConsentRecordLifecycleResponse{
		Result: &commonv1.OperationResult{
			ReferenceIds: listPendingCampaignsReply.GetResult().GetReferenceIds(),
			Message:      scheduleHouseholdReportReply.GetResult().GetMessage(),
			Status:       "ok",
		},
		ConsentRecord: &customersv1.ConsentRecord{
			CorrelationId:   listPendingCampaignsReply.GetResult().GetCorrelationId(),
			ConsentRecordId: req.GetConsentRecordId(),
		},
	}, nil
}

// ScheduleConsentRecordExpiry schedules a consent record.
func (s *ConsentRecordLifecycleServer) ScheduleConsentRecordExpiry(ctx context.Context, req *customersv1.ScheduleConsentRecordExpiryRequest) (*customersv1.ScheduleConsentRecordExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleConsentRecordExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SaveConsentRecord(ctx, req.GetContext().GetTenantId(), req.GetConsentRecord()); err != nil {
		return nil, grpcx.Wrap(err, "save consent record")
	}
	s.log.InfoContext(ctx, "schedule consent record expiry", "tenant", req.GetContext().GetTenantId())
	resumePromotionWorkflowRequest := buildResumePromotionWorkflowRequestForScheduleConsentRecordExpiry(req)
	resumePromotionWorkflowReply, err := s.promotionWorkflowClient.ResumePromotionWorkflow(ctx, resumePromotionWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.PromotionWorkflowService/ResumePromotionWorkflow")
	}
	return &customersv1.ScheduleConsentRecordExpiryResponse{
		Outcome: &customersv1.ScheduleConsentRecordExpiryResponse_Outcome{
			ReferenceId: resumePromotionWorkflowReply.GetOutcome().GetReferenceId(),
		},
		ConsentRecord: &customersv1.ConsentRecord{ConsentRecordId: req.GetConsentRecordId()},
		Result: &commonv1.OperationResult{
			CorrelationId: req.GetConsentRecord().GetCorrelationId(),
			Status:        "ok",
		},
	}, nil
}

// consentRecordLifecycleServerGetConsentRecordLifecycleScheduleHouseholdReportStep carries the request of one step of
// GetConsentRecordLifecycle and, once it ran, its reply.
type consentRecordLifecycleServerGetConsentRecordLifecycleScheduleHouseholdReportStep struct {
	request *customersv1.ScheduleHouseholdReportRequest
	reply   *customersv1.ScheduleHouseholdReportResponse
}

func (s *ConsentRecordLifecycleServer) runGetConsentRecordLifecycleScheduleHouseholdReport(ctx context.Context, step *consentRecordLifecycleServerGetConsentRecordLifecycleScheduleHouseholdReportStep) error {
	reply, err := s.householdReportingClient.ScheduleHouseholdReport(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}
