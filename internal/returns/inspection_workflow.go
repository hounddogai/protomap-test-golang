package returns

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	featureflagsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/featureflags/v1"
	giftcardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/giftcards/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	loyaltyv1 "github.com/hounddogai/protomap-test-golang/gen/acme/loyalty/v1"
	merchandisingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/merchandising/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	timekeepingv1 "github.com/hounddogai/protomap-test-golang/gen/acme/timekeeping/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// InspectionWorkflowServer implements `acme.returns.v1.InspectionWorkflowService`.
// Runs the long-lived workflow that takes an inspection from start to finish.
type InspectionWorkflowServer struct {
	returnsv1.UnimplementedInspectionWorkflowServiceServer
	store                         *Store
	log                           *slog.Logger
	conn                          *grpc.ClientConn
	timesheetQueryPort            timesheetQueryPort
	rewardWorkflowClient          loyaltyv1.RewardWorkflowServiceClient
	inspectionLifecycleClient     returnsv1.InspectionLifecycleServiceClient
	returnRequestQueryClient      returnsv1.ReturnRequestQueryServiceClient
	householdCalculationPort      householdCalculationPort
	customerValidationClient      customersv1.CustomerValidationServiceClient
	meteredPreferenceValidation   meteredPreferenceValidation
	returnRequestWorkflowClient   returnsv1.ReturnRequestWorkflowServiceClient
	placementWorkflowClient       merchandisingv1.PlacementWorkflowServiceClient
	customerQueryClient           customersv1.CustomerQueryServiceClient
	consentRecordValidationClient customersv1.ConsentRecordValidationServiceClient
	sellerScorecardApprovalClient sellersv1.SellerScorecardApprovalServiceClient
	featureFlagClient             featureflagsv1.FeatureFlagServiceClient
	returnLabelValidationClient   returnsv1.ReturnLabelValidationServiceClient
}

// NewInspectionWorkflowServer wires an InspectionWorkflowServer.
func NewInspectionWorkflowServer(deps platform.Deps) *InspectionWorkflowServer {
	return &InspectionWorkflowServer{
		store:                         NewStore(deps.DB),
		log:                           deps.Logger.With("service", "acme.returns.v1.InspectionWorkflowService"),
		conn:                          deps.Conn,
		timesheetQueryPort:            newTimesheetQueryPort(deps),
		rewardWorkflowClient:          loyaltyv1.NewRewardWorkflowServiceClient(deps.Conn),
		inspectionLifecycleClient:     returnsv1.NewInspectionLifecycleServiceClient(deps.Conn),
		returnRequestQueryClient:      returnsv1.NewReturnRequestQueryServiceClient(deps.Conn),
		householdCalculationPort:      newHouseholdCalculationPort(deps),
		customerValidationClient:      customersv1.NewCustomerValidationServiceClient(deps.Conn),
		meteredPreferenceValidation:   meteredPreferenceValidation{PreferenceValidationServiceClient: customersv1.NewPreferenceValidationServiceClient(deps.Conn), recorder: metrics.NewRecorder(deps.Logger)},
		returnRequestWorkflowClient:   returnsv1.NewReturnRequestWorkflowServiceClient(deps.Conn),
		placementWorkflowClient:       merchandisingv1.NewPlacementWorkflowServiceClient(deps.Conn),
		customerQueryClient:           customersv1.NewCustomerQueryServiceClient(deps.Conn),
		consentRecordValidationClient: customersv1.NewConsentRecordValidationServiceClient(deps.Conn),
		sellerScorecardApprovalClient: sellersv1.NewSellerScorecardApprovalServiceClient(deps.Conn),
		featureFlagClient:             featureflagsv1.NewFeatureFlagServiceClient(deps.Conn),
		returnLabelValidationClient:   returnsv1.NewReturnLabelValidationServiceClient(deps.Conn),
	}
}

// StartInspectionWorkflow starts an inspection.
func (s *InspectionWorkflowServer) StartInspectionWorkflow(ctx context.Context, req *returnsv1.StartInspectionWorkflowRequest) (*returnsv1.StartInspectionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "start inspection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	var email string
	switch choice := req.GetInspection().GetOrigin().(type) {
	case *returnsv1.Inspection_Customer:
		email = choice.Customer.GetEmail()
	case *returnsv1.Inspection_ExternalReference:
		email = choice.ExternalReference
	case *returnsv1.Inspection_Channel:
		email = choice.Channel.GetChannel()
	}
	delegateGiftCardBatchApprovalRequest := &giftcardsv1.DelegateGiftCardBatchApprovalRequest{
		Context: req.GetContext(),
		GiftCardBatch: &giftcardsv1.GiftCardBatch{
			Audit: &commonv1.AuditInfo{Etag: req.GetInspection().GetAudit().GetEtag()},
		},
		Details: &giftcardsv1.DelegateGiftCardBatchApprovalRequest_Details{
			Approver: &identityv1.UserRef{
				Email:  email,
				UserId: req.GetInspection().GetAssignee().GetUserId(),
			},
		},
	}
	delegateGiftCardBatchApprovalReply := new(giftcardsv1.DelegateGiftCardBatchApprovalResponse)
	if err := grpcx.LegacyInvoke(ctx, s.conn, "/acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval", delegateGiftCardBatchApprovalRequest, delegateGiftCardBatchApprovalReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.giftcards.v1.GiftCardBatchApprovalService/DelegateGiftCardBatchApproval")
	}
	summarizeTimesheetsRequest := &timekeepingv1.SummarizeTimesheetsRequest{
		Context: req.GetContext(),
		Reason:  req.GetReason(),
	}
	summarizeTimesheetsReply, err := s.timesheetQueryPort.SummarizeTimesheets(ctx, summarizeTimesheetsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.timekeeping.v1.TimesheetQueryService/SummarizeTimesheets")
	}
	return &returnsv1.StartInspectionWorkflowResponse{
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				Etag: delegateGiftCardBatchApprovalReply.GetGiftCardBatch().GetAudit().GetEtag(),
			},
			InspectionId: req.GetInspectionId(),
			Requester:    req.GetInspection().GetRequester(),
		},
		Outcome: &returnsv1.StartInspectionWorkflowResponse_Outcome{Status: summarizeTimesheetsReply.GetResult().GetStatus()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// AdvanceInspectionWorkflow advances an inspection.
func (s *InspectionWorkflowServer) AdvanceInspectionWorkflow(ctx context.Context, req *returnsv1.AdvanceInspectionWorkflowRequest) (*returnsv1.AdvanceInspectionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "advance inspection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := inspectionWorkflowServerCancelRewardWorkflowSteps[mode]
	cancelRewardWorkflowRequest := &loyaltyv1.CancelRewardWorkflowRequest{
		Context: req.GetContext(),
		Reward: &loyaltyv1.Reward{
			Owner: &identityv1.UserRef{Email: req.GetInspection().GetAssignee().GetEmail()},
		},
	}
	cancelRewardWorkflowReply, err := run(s, ctx, cancelRewardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardWorkflowService/CancelRewardWorkflow")
	}
	for _, comment := range req.GetInspection().GetComments() {
		if _, err := s.inspectionLifecycleClient.ScheduleInspectionExpiry(ctx, &returnsv1.ScheduleInspectionExpiryRequest{
			Context: req.GetContext(),
			Inspection: &returnsv1.Inspection{
				Comments: []*returnsv1.InspectionComment{&returnsv1.InspectionComment{
					Body:      comment.GetBody(),
					CreatedAt: comment.GetCreatedAt(),
				}},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionLifecycleService/ScheduleInspectionExpiry")
		}
	}
	resumeRewardWorkflowRequest := &loyaltyv1.ResumeRewardWorkflowRequest{
		Context: req.GetContext(),
		Details: &loyaltyv1.ResumeRewardWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Email: cancelRewardWorkflowReply.GetReward().GetOwner().GetEmail(),
			},
		},
	}
	resumeRewardWorkflowReply, err := loyaltyv1.RewardWorkflowServiceClient.ResumeRewardWorkflow(s.rewardWorkflowClient, ctx, resumeRewardWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.loyalty.v1.RewardWorkflowService/ResumeRewardWorkflow")
	}
	customFields := make(map[string]string, len(req.GetInspection().GetCustomFields()))
	for key, value := range req.GetInspection().GetCustomFields() {
		customFields[key] = value
	}
	browseReturnRequestsReply, err := s.returnRequestQueryClient.BrowseReturnRequests(ctx, &returnsv1.BrowseReturnRequestsRequest{
		Context: req.GetContext(),
		Filter: &returnsv1.BrowseReturnRequestsRequest_Filter{
			CreatedAfter: req.GetInspection().GetAudit().GetCreatedAt(),
			Labels:       customFields,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestQueryService/BrowseReturnRequests")
	}
	return &returnsv1.AdvanceInspectionWorkflowResponse{
		Inspection: &returnsv1.Inspection{
			Audit: &commonv1.AuditInfo{
				UpdatedAt: cancelRewardWorkflowReply.GetReward().GetAudit().GetUpdatedAt(),
			},
			Assignee: &identityv1.UserRef{
				Email: resumeRewardWorkflowReply.GetReward().GetOwner().GetEmail(),
			},
			InspectionId: req.GetInspectionId(),
			Priority:     req.GetInspection().GetPriority(),
		},
		Outcome: &returnsv1.AdvanceInspectionWorkflowResponse_Outcome{
			Message: resumeRewardWorkflowReply.GetOutcome().GetMessage(),
		},
		Result: &commonv1.OperationResult{
			Details:      browseReturnRequestsReply.GetResult().GetDetails(),
			ReferenceIds: browseReturnRequestsReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// CancelInspectionWorkflow cancels an inspection.
func (s *InspectionWorkflowServer) CancelInspectionWorkflow(ctx context.Context, req *returnsv1.CancelInspectionWorkflowRequest) (*returnsv1.CancelInspectionWorkflowResponse, error) {
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "cancel inspection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	quoteHouseholdRequest := &customersv1.QuoteHouseholdRequest{
		Context: req.GetContext(),
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						UserId:      req.GetInspection().GetAssignee().GetUserId(),
						Email:       req.GetInspection().GetAssignee().GetEmail(),
						DisplayName: req.GetInspection().GetAssignee().GetDisplayName(),
					},
				}},
			}},
		},
	}
	quoteHouseholdReply, err := s.householdCalculationPort.QuoteHousehold(ctx, quoteHouseholdRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdCalculationService/QuoteHousehold")
	}
	return &returnsv1.CancelInspectionWorkflowResponse{
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				UserId: quoteHouseholdReply.GetHousehold().GetManager().GetUserId(),
			},
			InspectionId: req.GetInspectionId(),
			Subject:      req.GetInspection().GetSubject(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// RetryInspectionWorkflow retrys an inspection.
func (s *InspectionWorkflowServer) RetryInspectionWorkflow(ctx context.Context, req *returnsv1.RetryInspectionWorkflowRequest) (*returnsv1.RetryInspectionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RetryInspectionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "retry inspection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	explainCustomerDecisionReply, err := s.customerValidationClient.ExplainCustomerDecision(ctx, &customersv1.ExplainCustomerDecisionRequest{
		Context: req.GetContext(),
		Details: &customersv1.ExplainCustomerDecisionRequest_Details{
			Contact: &commonv1.ContactInfo{Email: req.GetInspection().GetRequester().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/ExplainCustomerDecision")
	}
	scheduleInspectionExpiryRequest := &returnsv1.ScheduleInspectionExpiryRequest{
		Context: req.GetContext(),
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				Email:  req.GetInspection().GetCustomer().GetEmail(),
				UserId: explainCustomerDecisionReply.GetCustomer().GetRelationshipManager().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{
				Etag: explainCustomerDecisionReply.GetCustomer().GetAudit().GetEtag(),
			},
		},
		Details: &returnsv1.ScheduleInspectionExpiryRequest_Details{
			Notify: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotify().GetPreferredChannel(),
			},
		},
	}
	scheduleInspectionExpiryReply, err := grpcx.NewClient(s.conn, returnsv1.NewInspectionLifecycleServiceClient).ScheduleInspectionExpiry(ctx, scheduleInspectionExpiryRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionLifecycleService/ScheduleInspectionExpiry")
	}
	normalizePreferenceRequest := &customersv1.NormalizePreferenceRequest{
		Context: req.GetContext(),
		Details: &customersv1.NormalizePreferenceRequest_Details{
			Contact: &commonv1.ContactInfo{
				Email: scheduleInspectionExpiryReply.GetInspection().GetCustomer().GetEmail(),
			},
		},
	}
	normalizePreferenceReply, err := s.meteredPreferenceValidation.NormalizePreference(ctx, normalizePreferenceRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceValidationService/NormalizePreference")
	}
	signalReturnRequestWorkflowRequest := buildSignalReturnRequestWorkflowRequestForRetryInspectionWorkflow(req, scheduleInspectionExpiryReply)
	signalReturnRequestWorkflowReply, err := s.returnRequestWorkflowClient.SignalReturnRequestWorkflow(ctx, signalReturnRequestWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestWorkflowService/SignalReturnRequestWorkflow")
	}
	return &returnsv1.RetryInspectionWorkflowResponse{
		Inspection: &returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				Email: explainCustomerDecisionReply.GetCustomer().GetRelationshipManager().GetEmail(),
			},
			Audit: &commonv1.AuditInfo{
				CreatedAt: explainCustomerDecisionReply.GetCustomer().GetAudit().GetCreatedAt(),
				UpdatedBy: scheduleInspectionExpiryReply.GetInspection().GetAudit().GetUpdatedBy(),
			},
			InspectionId: req.GetInspectionId(),
			Priority:     req.GetInspection().GetPriority(),
		},
		Result: &commonv1.OperationResult{
			CorrelationId: scheduleInspectionExpiryReply.GetResult().GetCorrelationId(),
			Details:       normalizePreferenceReply.GetResult().GetDetails(),
			Status:        "ok",
		},
		Outcome: &returnsv1.RetryInspectionWorkflowResponse_Outcome{
			EffectiveAt: signalReturnRequestWorkflowReply.GetOutcome().GetEffectiveAt(),
		},
	}, nil
}

// PauseInspectionWorkflow pauses an inspection.
func (s *InspectionWorkflowServer) PauseInspectionWorkflow(ctx context.Context, req *returnsv1.PauseInspectionWorkflowRequest) (*returnsv1.PauseInspectionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "pause inspection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	pausePlacementWorkflowRequest := &merchandisingv1.PausePlacementWorkflowRequest{}
	pausePlacementWorkflowRequest.Context = req.GetContext()
	pausePlacementWorkflowRequest.Placement = &merchandisingv1.Placement{
		Participants: []*merchandisingv1.PlacementParticipant{&merchandisingv1.PlacementParticipant{
			Contact: &commonv1.ContactInfo{
				Email:            req.GetInspection().GetCustomer().GetEmail(),
				PreferredChannel: req.GetInspection().GetRequester().GetPreferredChannel(),
			},
		}},
	}
	pausePlacementWorkflowReply, err := s.placementWorkflowClient.PausePlacementWorkflow(ctx, pausePlacementWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.merchandising.v1.PlacementWorkflowService/PausePlacementWorkflow")
	}
	exportCustomersStream, err := s.customerQueryClient.ExportCustomers(ctx, &customersv1.ExportCustomersRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ExportCustomersRequest_Filter{
			Region: pausePlacementWorkflowReply.GetPlacement().GetLocation().GetRegion(),
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ExportCustomers")
	}
	for {
		received, err := exportCustomersStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerQueryService/ExportCustomers")
		}
		_ = received
	}
	return &returnsv1.PauseInspectionWorkflowResponse{
		Result: &commonv1.OperationResult{Status: pausePlacementWorkflowReply.GetResult().GetStatus()},
		Inspection: &returnsv1.Inspection{
			Requester: &commonv1.ContactInfo{
				Email: pausePlacementWorkflowReply.GetPlacement().GetOrganizer().GetEmail(),
			},
			InspectionId: req.GetInspectionId(),
			Subject:      req.GetInspection().GetSubject(),
		},
	}, nil
}

// ResumeInspectionWorkflow resumes an inspection.
func (s *InspectionWorkflowServer) ResumeInspectionWorkflow(ctx context.Context, req *returnsv1.ResumeInspectionWorkflowRequest) (*returnsv1.ResumeInspectionWorkflowResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "resume inspection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	var email string
	switch choice := req.GetInspection().GetOrigin().(type) {
	case *returnsv1.Inspection_Customer:
		email = choice.Customer.GetEmail()
	case *returnsv1.Inspection_ExternalReference:
		email = choice.ExternalReference
	case *returnsv1.Inspection_Channel:
		email = choice.Channel.GetChannel()
	}
	var displayName string
	switch choice2 := req.GetInspection().GetOrigin().(type) {
	case *returnsv1.Inspection_Customer:
		displayName = choice2.Customer.GetDisplayName()
	case *returnsv1.Inspection_ExternalReference:
		displayName = choice2.ExternalReference
	case *returnsv1.Inspection_Channel:
		displayName = choice2.Channel.GetChannel()
	}
	recallReturnLabelRequest := &returnsv1.RecallReturnLabelRequest{
		Context: req.GetContext(),
		Details: &returnsv1.RecallReturnLabelRequest_Details{Approver: &identityv1.UserRef{Email: email}},
		ReturnLabel: &returnsv1.ReturnLabel{
			Counterparty: &returnsv1.InspectionRef{DisplayName: displayName},
		},
	}
	recallReturnLabelReply := new(returnsv1.RecallReturnLabelResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", returnsReturnLabelApprovalServiceName, "RecallReturnLabel"), recallReturnLabelRequest, recallReturnLabelReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelApprovalService/RecallReturnLabel")
	}
	for _, comment := range req.GetInspection().GetComments() {
		if _, err := s.consentRecordValidationClient.PreviewConsentRecord(ctx, &customersv1.PreviewConsentRecordRequest{
			Context: req.GetContext(),
			Details: &customersv1.PreviewConsentRecordRequest_Details{
				Contact: &commonv1.ContactInfo{
					Email: comment.GetAuthorEmail(),
					Phone: req.GetDetails().GetNotify().GetPhone(),
				},
			},
			ConsentRecord: &customersv1.ConsentRecord{
				Subject: &customersv1.HouseholdRef{
					DisplayName: req.GetInspection().GetCustomer().GetDisplayName(),
				},
			},
		}); err != nil {
			return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordValidationService/PreviewConsentRecord")
		}
	}
	approveSellerScorecardReply, err := s.sellerScorecardApprovalClient.ApproveSellerScorecard(ctx, &sellersv1.ApproveSellerScorecardRequest{
		Context: req.GetContext(),
		SellerScorecard: &sellersv1.SellerScorecard{
			BillingAddress: &commonv1.PostalAddress{
				CountryCode: recallReturnLabelReply.GetReturnLabel().GetBillingAddress().GetCountryCode(),
			},
		},
		Details: &sellersv1.ApproveSellerScorecardRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetInspection().GetCustomer().GetEmail()},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerScorecardApprovalService/ApproveSellerScorecard")
	}
	_ = approveSellerScorecardReply
	return &returnsv1.ResumeInspectionWorkflowResponse{
		Inspection: &returnsv1.Inspection{
			InspectionId: recallReturnLabelReply.GetReturnLabel().GetCounterparty().GetInspectionId(),
			Requester:    req.GetInspection().GetRequester(),
			CustomFields: req.GetInspection().GetCustomFields(),
		},
		Outcome: &returnsv1.ResumeInspectionWorkflowResponse_Outcome{Message: recallReturnLabelReply.GetOutcome().GetMessage()},
		Result:  &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// InspectInspectionWorkflow returns an inspection.
func (s *InspectionWorkflowServer) InspectInspectionWorkflow(ctx context.Context, req *returnsv1.InspectInspectionWorkflowRequest) (*returnsv1.InspectInspectionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "inspection", req.GetInspectionId(), "inspect_inspection_workflow"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "inspect inspection workflow", "tenant", req.GetContext().GetTenantId())
	return &returnsv1.InspectInspectionWorkflowResponse{
		Inspection: &returnsv1.Inspection{InspectionId: req.GetInspectionId()},
		Result:     &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ListInspectionWorkflows lists inspections a page at a time.
func (s *InspectionWorkflowServer) ListInspectionWorkflows(ctx context.Context, req *returnsv1.ListInspectionWorkflowsRequest) (*returnsv1.ListInspectionWorkflowsResponse, error) {
	s.log.InfoContext(ctx, "list inspection workflows", "tenant", req.GetContext().GetTenantId())
	batchGetFeatureFlagsReply, err := s.featureFlagClient.BatchGetFeatureFlags(ctx, &featureflagsv1.BatchGetFeatureFlagsRequest{Context: req.GetContext()})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.featureflags.v1.FeatureFlagService/BatchGetFeatureFlags")
	}
	lintReturnLabelsRequest := &returnsv1.LintReturnLabelsRequest{}
	lintReturnLabelsRequest.Context = req.GetContext()
	lintReturnLabelsRequest.ReturnLabel = &returnsv1.ReturnLabel{
		BillingAddress: &commonv1.PostalAddress{Region: req.GetFilter().GetRegion()},
		Audit:          &commonv1.AuditInfo{CreatedAt: req.GetFilter().GetCreatedAfter()},
	}
	lintReturnLabelsRequest.Details = &returnsv1.LintReturnLabelsRequest_Details{
		Contact: &commonv1.ContactInfo{Email: req.GetContext().GetActor().GetEmail()},
		Locale:  req.GetContext().GetLocale(),
	}
	lintReturnLabelsReply, err := s.returnLabelValidationClient.LintReturnLabels(ctx, lintReturnLabelsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelValidationService/LintReturnLabels")
	}
	return &returnsv1.ListInspectionWorkflowsResponse{
		Result: &commonv1.OperationResult{
			CorrelationId: batchGetFeatureFlagsReply.GetResult().GetCorrelationId(),
			Status:        "ok",
		},
		Inspections: []*returnsv1.Inspection{&returnsv1.Inspection{
			Assignee: &identityv1.UserRef{
				DisplayName: lintReturnLabelsReply.GetReturnLabel().GetCounterparty().GetDisplayName(),
			},
		}},
	}, nil
}

// SignalInspectionWorkflow signals an inspection.
func (s *InspectionWorkflowServer) SignalInspectionWorkflow(ctx context.Context, req *returnsv1.SignalInspectionWorkflowRequest) (*returnsv1.SignalInspectionWorkflowResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected SignalInspectionWorkflow", "error", err)
		return nil, err
	}
	if err := s.store.SaveInspection(ctx, req.GetContext().GetTenantId(), req.GetInspection()); err != nil {
		return nil, grpcx.Wrap(err, "save inspection")
	}
	s.log.InfoContext(ctx, "signal inspection workflow", "tenant", req.GetContext().GetTenantId(), "etag", req.GetInspection().GetAudit().GetEtag())
	jobs := make(chan *returnsv1.InspectionComment)
	failures := make(chan error, 4)
	workCtx, stopWork := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for comment := range jobs {
				if _, err := s.inspectionLifecycleClient.ScheduleInspectionExpiry(workCtx, &returnsv1.ScheduleInspectionExpiryRequest{
					Context: req.GetContext(),
					Inspection: &returnsv1.Inspection{
						Comments: []*returnsv1.InspectionComment{&returnsv1.InspectionComment{
							CreatedAt:   comment.GetCreatedAt(),
							AuthorEmail: comment.GetAuthorEmail(),
						}},
					},
				}); err != nil {
					failures <- err
					stopWork()
					return
				}
			}
		}()
	}
	for _, queued := range req.GetInspection().GetComments() {
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
		return nil, grpcx.Wrap(err, "acme.returns.v1.InspectionLifecycleService/ScheduleInspectionExpiry")
	}
	listReturnRequestWorkflowsRequest := &returnsv1.ListReturnRequestWorkflowsRequest{
		Context: req.GetContext(),
		Filter: &returnsv1.ListReturnRequestWorkflowsRequest_Filter{
			CreatedAfter: req.GetInspection().GetSla().GetDueAt(),
			Labels:       req.GetDetails().GetParameters(),
		},
	}
	listReturnRequestWorkflowsReply := new(returnsv1.ListReturnRequestWorkflowsResponse)
	if err := grpcx.Invoke(ctx, s.conn, returnsv1.ReturnRequestWorkflowService_ListReturnRequestWorkflows_FullMethodName, listReturnRequestWorkflowsRequest, listReturnRequestWorkflowsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnRequestWorkflowService/ListReturnRequestWorkflows")
	}
	return &returnsv1.SignalInspectionWorkflowResponse{
		Result: &commonv1.OperationResult{
			Status: listReturnRequestWorkflowsReply.GetResult().GetStatus(),
		},
		Inspection: &returnsv1.Inspection{
			InspectionId: req.GetInspectionId(),
			Subject:      req.GetInspection().GetSubject(),
		},
	}, nil
}

// inspectionWorkflowServerCancelRewardWorkflowSteps are the ways to run
// acme.loyalty.v1.RewardWorkflowService/CancelRewardWorkflow, by name.
var inspectionWorkflowServerCancelRewardWorkflowSteps = map[string]func(*InspectionWorkflowServer, context.Context, *loyaltyv1.CancelRewardWorkflowRequest) (*loyaltyv1.CancelRewardWorkflowResponse, error){
	"default": (*InspectionWorkflowServer).cancelRewardWorkflowStep,
	"audited": (*InspectionWorkflowServer).cancelRewardWorkflowAuditedStep,
}

func (s *InspectionWorkflowServer) cancelRewardWorkflowStep(ctx context.Context, request *loyaltyv1.CancelRewardWorkflowRequest) (*loyaltyv1.CancelRewardWorkflowResponse, error) {
	return s.rewardWorkflowClient.CancelRewardWorkflow(ctx, request)
}

func (s *InspectionWorkflowServer) cancelRewardWorkflowAuditedStep(ctx context.Context, request *loyaltyv1.CancelRewardWorkflowRequest) (*loyaltyv1.CancelRewardWorkflowResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.loyalty.v1.RewardWorkflowService/CancelRewardWorkflow")
	return s.rewardWorkflowClient.CancelRewardWorkflow(ctx, request)
}
