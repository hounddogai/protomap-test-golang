package customers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	commonv1 "github.com/hounddogai/protomap-test-golang/gen/acme/common/v1"
	customersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/customers/v1"
	identityv1 "github.com/hounddogai/protomap-test-golang/gen/acme/identity/v1"
	ledgerv1 "github.com/hounddogai/protomap-test-golang/gen/acme/ledger/v1"
	paymentsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/payments/v1"
	promotionsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/promotions/v1"
	returnsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/returns/v1"
	sellersv1 "github.com/hounddogai/protomap-test-golang/gen/acme/sellers/v1"
	telehealthv1 "github.com/hounddogai/protomap-test-golang/gen/acme/telehealth/v1"
	yardsv1 "github.com/hounddogai/protomap-test-golang/gen/acme/yards/v1"
	"github.com/hounddogai/protomap-test-golang/internal/platform"
	"github.com/hounddogai/protomap-test-golang/internal/platform/grpcx"
	"github.com/hounddogai/protomap-test-golang/internal/platform/mapper"
	"github.com/hounddogai/protomap-test-golang/internal/platform/tenancy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// HouseholdLifecycleServer implements `acme.customers.v1.HouseholdLifecycleService`.
// Moves a household between the states of its lifecycle.
type HouseholdLifecycleServer struct {
	customersv1.UnimplementedHouseholdLifecycleServiceServer
	store                           *Store
	log                             *slog.Logger
	redemptionReportingClient       promotionsv1.RedemptionReportingServiceClient
	yardClient                      yardsv1.YardServiceClient
	ledgerAccountValidationClient   ledgerv1.LedgerAccountValidationServiceClient
	preferenceImportClient          customersv1.PreferenceImportServiceClient
	conn                            *grpc.ClientConn
	returnLabelWorkflowClient       returnsv1.ReturnLabelWorkflowServiceClient
	videoVisitClient                telehealthv1.VideoVisitServiceClient
	paymentApprovalClient           paymentsv1.PaymentApprovalServiceClient
	paymentWorkflowClient           paymentsv1.PaymentWorkflowServiceClient
	customerValidationClient        customersv1.CustomerValidationServiceClient
	householdReportingClient        customersv1.HouseholdReportingServiceClient
	customerSegmentValidationClient customersv1.CustomerSegmentValidationServiceClient
	householdReportingOnce          sync.Once
	householdReportingCached        customersv1.HouseholdReportingServiceClient
	consentRecordReportingClient    customersv1.ConsentRecordReportingServiceClient
	customerSegmentSyncClient       customersv1.CustomerSegmentSyncServiceClient
}

// NewHouseholdLifecycleServer wires a HouseholdLifecycleServer.
func NewHouseholdLifecycleServer(deps platform.Deps) *HouseholdLifecycleServer {
	return &HouseholdLifecycleServer{
		store:                           NewStore(deps.DB),
		log:                             deps.Logger.With("service", "acme.customers.v1.HouseholdLifecycleService"),
		redemptionReportingClient:       promotionsv1.NewRedemptionReportingServiceClient(deps.Conn),
		yardClient:                      yardsv1.NewYardServiceClient(deps.Conn),
		ledgerAccountValidationClient:   ledgerv1.NewLedgerAccountValidationServiceClient(deps.Conn),
		preferenceImportClient:          customersv1.NewPreferenceImportServiceClient(deps.Conn),
		conn:                            deps.Conn,
		returnLabelWorkflowClient:       returnsv1.NewReturnLabelWorkflowServiceClient(deps.Conn),
		videoVisitClient:                telehealthv1.NewVideoVisitServiceClient(deps.Conn),
		paymentApprovalClient:           paymentsv1.NewPaymentApprovalServiceClient(deps.Conn),
		paymentWorkflowClient:           paymentsv1.NewPaymentWorkflowServiceClient(deps.Conn),
		customerValidationClient:        customersv1.NewCustomerValidationServiceClient(deps.Conn),
		householdReportingClient:        customersv1.NewHouseholdReportingServiceClient(deps.Conn),
		customerSegmentValidationClient: customersv1.NewCustomerSegmentValidationServiceClient(deps.Conn),
		consentRecordReportingClient:    customersv1.NewConsentRecordReportingServiceClient(deps.Conn),
		customerSegmentSyncClient:       customersv1.NewCustomerSegmentSyncServiceClient(deps.Conn),
	}
}

// ActivateHousehold activates a household.
func (s *HouseholdLifecycleServer) ActivateHousehold(ctx context.Context, req *customersv1.ActivateHouseholdRequest) (*customersv1.ActivateHouseholdResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "activate_household"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "activate household", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "reason", req.GetReason())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	baggage := make(map[string]string, len(req.GetContext().GetBaggage()))
	for key2, value2 := range req.GetContext().GetBaggage() {
		baggage[key2] = value2
	}
	generateRedemptionReportRequest := &promotionsv1.GenerateRedemptionReportRequest{
		Context:     req.GetContext(),
		Reason:      tenancy.Value(ctx, "reason"),
		Annotations: annotations,
		Details:     &promotionsv1.GenerateRedemptionReportRequest_Details{Filters: baggage},
	}
	generateRedemptionReportReply, err := promotionsv1.RedemptionReportingServiceClient.GenerateRedemptionReport(s.redemptionReportingClient, ctx, generateRedemptionReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.promotions.v1.RedemptionReportingService/GenerateRedemptionReport")
	}
	cloneYardRequest := &yardsv1.CloneYardRequest{}
	cloneYardRequest.Context = req.GetContext()
	cloneYardRequest.Yard = &yardsv1.Yard{
		Owner: &identityv1.UserRef{
			UserId: req.GetDetails().GetNewOwner().GetUserId(),
			Email:  req.GetDetails().GetNotifyContact().GetEmail(),
		},
	}
	cloneYardReply, err := s.yardClient.CloneYard(ctx, cloneYardRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.yards.v1.YardService/CloneYard")
	}
	scoreLedgerAccountRequest := &ledgerv1.ScoreLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{
				Phone: req.GetDetails().GetNotifyContact().GetPhone(),
				Email: req.GetDetails().GetNotifyContact().GetEmail(),
			},
			Holder: &ledgerv1.PostingBatchRef{
				DisplayName: cloneYardReply.GetYard().GetOwner().GetDisplayName(),
			},
		},
		Details: &ledgerv1.ScoreLedgerAccountRequest_Details{
			Destination: &commonv1.PostalAddress{
				PostalCode: cloneYardReply.GetYard().GetAddress().GetPostalCode(),
			},
		},
	}
	scoreLedgerAccountReply, err := grpcx.Retry(ctx, 5, func(ctx context.Context) (*ledgerv1.ScoreLedgerAccountResponse, error) {
		return s.ledgerAccountValidationClient.ScoreLedgerAccount(ctx, scoreLedgerAccountRequest)
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ScoreLedgerAccount")
	}
	return &customersv1.ActivateHouseholdResponse{
		Outcome: &customersv1.ActivateHouseholdResponse_Outcome{
			Status:      generateRedemptionReportReply.GetResult().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Location: &commonv1.PostalAddress{
						Recipient: cloneYardReply.GetYard().GetAddress().GetRecipient(),
					},
					Audit: &commonv1.AuditInfo{
						Etag: scoreLedgerAccountReply.GetLedgerAccount().GetAudit().GetEtag(),
					},
				}},
			}},
			HouseholdId: req.GetHouseholdId(),
		},
		Result: &commonv1.OperationResult{
			ReferenceIds: scoreLedgerAccountReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
	}, nil
}

// SuspendHousehold suspends a household.
func (s *HouseholdLifecycleServer) SuspendHousehold(ctx context.Context, req *customersv1.SuspendHouseholdRequest) (*customersv1.SuspendHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "suspend_household"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "suspend household", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	listPreferenceImportsReply, err := s.preferenceImportClient.ListPreferenceImports(ctx, &customersv1.ListPreferenceImportsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.ListPreferenceImportsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			OwnerId:      req.GetHouseholdId(),
			Labels:       annotations,
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.PreferenceImportService/ListPreferenceImports")
	}
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	resolveConsentRecordConflictRequest := &customersv1.ResolveConsentRecordConflictRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: req.GetDetails().GetNewOwner().GetDisplayName(),
			},
			CorrelationId: listPreferenceImportsReply.GetResult().GetCorrelationId(),
			Detail:        packed,
		},
		Details: &customersv1.ResolveConsentRecordConflictRequest_Details{Payload: snapshot},
	}
	resolveConsentRecordConflictReply, err := resolveConsentRecordConflictCall(ctx, resolveConsentRecordConflictRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/ResolveConsentRecordConflict")
	}
	ctx = tenancy.With(ctx, "preferred_channel", req.GetDetails().GetNotifyContact().GetPreferredChannel())
	packed2, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	shareSellerPayoutScheduleReportRequest := &sellersv1.ShareSellerPayoutScheduleReportRequest{
		Context: req.GetContext(),
		Details: &sellersv1.ShareSellerPayoutScheduleReportRequest_Details{
			Recipient: &commonv1.ContactInfo{
				Name:             req.GetDetails().GetNotifyContact().GetName(),
				PreferredChannel: tenancy.Value(ctx, "preferred_channel"),
				Phone:            req.GetDetails().GetNotifyContact().GetPhone(),
			},
			Payload: packed2,
		},
	}
	shareSellerPayoutScheduleReportReply := new(sellersv1.ShareSellerPayoutScheduleReportResponse)
	if err := s.conn.Invoke(ctx, fmt.Sprintf("/%s/%s", sellersSellerPayoutScheduleReportingServiceName, "ShareSellerPayoutScheduleReport"), shareSellerPayoutScheduleReportRequest, shareSellerPayoutScheduleReportReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.sellers.v1.SellerPayoutScheduleReportingService/ShareSellerPayoutScheduleReport")
	}
	cloned := proto.Clone(req.GetDetails()).(*customersv1.SuspendHouseholdRequest_Details)
	pauseReturnLabelWorkflowRequest := &returnsv1.PauseReturnLabelWorkflowRequest{}
	pauseReturnLabelWorkflowRequest.Context = req.GetContext()
	pauseReturnLabelWorkflowRequest.ReturnLabel = &returnsv1.ReturnLabel{
		Counterparty: &returnsv1.InspectionRef{DisplayName: cloned.GetNewOwner().GetDisplayName()},
	}
	pauseReturnLabelWorkflowReply, err := s.returnLabelWorkflowClient.PauseReturnLabelWorkflow(ctx, pauseReturnLabelWorkflowRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.returns.v1.ReturnLabelWorkflowService/PauseReturnLabelWorkflow")
	}
	return &customersv1.SuspendHouseholdResponse{
		Result: &commonv1.OperationResult{
			Message: listPreferenceImportsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Outcome: &customersv1.SuspendHouseholdResponse_Outcome{
			Status:      listPreferenceImportsReply.GetResult().GetStatus(),
			Message:     pauseReturnLabelWorkflowReply.GetOutcome().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						DisplayName: resolveConsentRecordConflictReply.GetConsentRecord().GetSubject().GetDisplayName(),
					},
				}},
				Location: &commonv1.PostalAddress{
					Region: shareSellerPayoutScheduleReportReply.GetSellerPayoutSchedule().GetLocation().GetRegion(),
				},
				Audit: &commonv1.AuditInfo{
					UpdatedBy: pauseReturnLabelWorkflowReply.GetReturnLabel().GetAudit().GetUpdatedBy(),
				},
			}},
			HouseholdId: req.GetHouseholdId(),
		},
	}, nil
}

// ReinstateHousehold reinstates a household.
func (s *HouseholdLifecycleServer) ReinstateHousehold(ctx context.Context, req *customersv1.ReinstateHouseholdRequest) (*customersv1.ReinstateHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ReinstateHousehold", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "reinstate_household"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "reinstate household", "tenant", req.GetContext().GetTenantId())
	ctx = tenancy.With(ctx, "display_name", req.GetDetails().GetNewOwner().GetDisplayName())
	packed, err := anypb.New(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "pack payload")
	}
	snapshot, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullConsentRecordsRequest := &customersv1.PullConsentRecordsRequest{
		Context: req.GetContext(),
		ConsentRecord: &customersv1.ConsentRecord{
			Subject: &customersv1.HouseholdRef{
				DisplayName: tenancy.Value(ctx, "display_name"),
				HouseholdId: req.GetHouseholdId(),
			},
			Detail: packed,
		},
		Details: &customersv1.PullConsentRecordsRequest_Details{Payload: snapshot},
	}
	pullConsentRecordsReply, err := grpcx.NewClient(s.conn, customersv1.NewConsentRecordSyncServiceClient).PullConsentRecords(ctx, pullConsentRecordsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordSyncService/PullConsentRecords")
	}
	watchVideoVisitsStream, err := s.videoVisitClient.WatchVideoVisits(ctx, &telehealthv1.WatchVideoVisitsRequest{
		Context:    req.GetContext(),
		Subscriber: &commonv1.ContactInfo{Email: req.GetDetails().GetNotifyContact().GetEmail()},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.telehealth.v1.VideoVisitService/WatchVideoVisits")
	}
	for {
		received, err := watchVideoVisitsStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, grpcx.Wrap(err, "acme.telehealth.v1.VideoVisitService/WatchVideoVisits")
		}
		_ = received
	}
	step := &householdLifecycleServerReinstateHouseholdCommentPaymentApprovalStep{request: &paymentsv1.CommentPaymentApprovalRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.CommentPaymentApprovalRequest_Details{
			Approver: &identityv1.UserRef{Email: req.GetDetails().GetNotifyContact().GetEmail()},
		},
	}}
	if err := s.runReinstateHouseholdCommentPaymentApproval(ctx, step); err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentApprovalService/CommentPaymentApproval")
	}
	commentPaymentApprovalReply := step.reply
	return &customersv1.ReinstateHouseholdResponse{
		Result: &commonv1.OperationResult{
			Details:      pullConsentRecordsReply.GetResult().GetDetails(),
			ReferenceIds: commentPaymentApprovalReply.GetResult().GetReferenceIds(),
			Status:       "ok",
		},
		Household: &customersv1.Household{HouseholdId: req.GetHouseholdId()},
		Outcome:   &customersv1.ReinstateHouseholdResponse_Outcome{EffectiveAt: req.GetDetails().GetEffectiveAt()},
	}, nil
}

// TerminateHousehold terminates a household.
func (s *HouseholdLifecycleServer) TerminateHousehold(ctx context.Context, req *customersv1.TerminateHouseholdRequest) (*customersv1.TerminateHouseholdResponse, error) {
	if req.GetContext().GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "context.tenant_id is required")
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "terminate_household"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "terminate household", "tenant", req.GetContext().GetTenantId())
	annotations := make(map[string]string, len(req.GetAnnotations()))
	for key, value := range req.GetAnnotations() {
		annotations[key] = value
	}
	countConsentRecordTotalsRequest := &customersv1.CountConsentRecordTotalsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.CountConsentRecordTotalsRequest_Filter{
			CreatedAfter: req.GetDetails().GetEffectiveAt(),
			Labels:       annotations,
			OwnerId:      req.GetDetails().GetNewOwner().GetUserId(),
		},
	}
	countConsentRecordTotalsReply := new(customersv1.CountConsentRecordTotalsResponse)
	if err := s.conn.Invoke(ctx, customersConsentRecordReportingServiceCountConsentRecordTotalsMethod, countConsentRecordTotalsRequest, countConsentRecordTotalsReply); err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.ConsentRecordReportingService/CountConsentRecordTotals")
	}
	return &customersv1.TerminateHouseholdResponse{
		Outcome: &customersv1.TerminateHouseholdResponse_Outcome{
			Message:     countConsentRecordTotalsReply.GetResult().GetMessage(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Result: &commonv1.OperationResult{
			Message: countConsentRecordTotalsReply.GetResult().GetMessage(),
			Status:  "ok",
		},
		Household: &customersv1.Household{HouseholdId: req.GetHouseholdId()},
	}, nil
}

// RenewHousehold renews a household.
func (s *HouseholdLifecycleServer) RenewHousehold(ctx context.Context, req *customersv1.RenewHouseholdRequest) (*customersv1.RenewHouseholdResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected RenewHousehold", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "renew_household"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "renew household", "tenant", req.GetContext().GetTenantId())
	cloned := proto.Clone(req.GetDetails()).(*customersv1.RenewHouseholdRequest_Details)
	validateLedgerAccountRequest := &ledgerv1.ValidateLedgerAccountRequest{
		Context: req.GetContext(),
		LedgerAccount: &ledgerv1.LedgerAccount{
			Contact: &commonv1.ContactInfo{
				PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
				Name:             cloned.GetNotifyContact().GetName(),
				Email:            req.GetDetails().GetNotifyContact().GetEmail(),
			},
		},
	}
	send := s.validateLedgerAccountSender(validateLedgerAccountRequest)
	validateLedgerAccountReply, err := send(ctx)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.ledger.v1.LedgerAccountValidationService/ValidateLedgerAccount")
	}
	encoded, err := protojson.Marshal(req.GetDetails())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.RenewHouseholdRequest_Details
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	var holder struct{ Phone string }
	if record := req.GetDetails().GetNotifyContact(); record != nil {
		if err := mapper.CopyFields(&holder, record, "Phone"); err != nil {
			s.log.WarnContext(ctx, "copy phone", "error", err)
		}
	}
	resumePaymentWorkflowReply, err := s.paymentWorkflowClient.ResumePaymentWorkflow(ctx, &paymentsv1.ResumePaymentWorkflowRequest{
		Context: req.GetContext(),
		Details: &paymentsv1.ResumePaymentWorkflowRequest_Details{
			Notify: &commonv1.ContactInfo{
				Name:             (&decoded).GetNotifyContact().GetName(),
				PreferredChannel: validateLedgerAccountReply.GetLedgerAccount().GetContact().GetPreferredChannel(),
				Phone:            holder.Phone,
			},
		},
		Payment: &paymentsv1.Payment{
			Audit: &commonv1.AuditInfo{
				CreatedAt: validateLedgerAccountReply.GetLedgerAccount().GetAudit().GetCreatedAt(),
			},
		},
	})
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.payments.v1.PaymentWorkflowService/ResumePaymentWorkflow")
	}
	return &customersv1.RenewHouseholdResponse{
		Result: &commonv1.OperationResult{
			Message: validateLedgerAccountReply.GetOutcome().GetMessage(),
			Status:  "ok",
		},
		Outcome: &customersv1.RenewHouseholdResponse_Outcome{
			Message:     resumePaymentWorkflowReply.GetResult().GetMessage(),
			Status:      resumePaymentWorkflowReply.GetOutcome().GetStatus(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Household: &customersv1.Household{HouseholdId: req.GetHouseholdId()},
	}, nil
}

// ExpireHousehold expires a household.
func (s *HouseholdLifecycleServer) ExpireHousehold(ctx context.Context, req *customersv1.ExpireHouseholdRequest) (*customersv1.ExpireHouseholdResponse, error) {
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "expire_household"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "expire household", "tenant", req.GetContext().GetTenantId())
	lintCustomersRequest := &customersv1.LintCustomersRequest{}
	lintCustomersRequest.Context = req.GetContext()
	lintCustomersRequest.Details = &customersv1.LintCustomersRequest_Details{
		Contact: &commonv1.ContactInfo{Name: req.GetDetails().GetNotifyContact().GetName()},
	}
	lintCustomersRequest.Customer = &customersv1.Customer{
		RelationshipManager: &identityv1.UserRef{
			Email:  req.GetDetails().GetNotifyContact().GetEmail(),
			UserId: req.GetDetails().GetNewOwner().GetUserId(),
		},
		Contact: &commonv1.ContactInfo{
			PreferredChannel: req.GetDetails().GetNotifyContact().GetPreferredChannel(),
		},
	}
	lintCustomersReply, err := s.customerValidationClient.LintCustomers(ctx, lintCustomersRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerValidationService/LintCustomers")
	}
	mode := "default"
	if req.GetContext().GetActor().GetServiceAccount() {
		mode = "audited"
	}
	run := householdLifecycleServerFetchHouseholdReportSteps[mode]
	fetchHouseholdReportRequest := &customersv1.FetchHouseholdReportRequest{
		Context:     req.GetContext(),
		HouseholdId: req.GetHouseholdId(),
	}
	fetchHouseholdReportReply, err := run(s, ctx, fetchHouseholdReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdReportingService/FetchHouseholdReport")
	}
	return &customersv1.ExpireHouseholdResponse{
		Outcome: &customersv1.ExpireHouseholdResponse_Outcome{
			NextSteps:   lintCustomersReply.GetOutcome().GetNextSteps(),
			EffectiveAt: req.GetDetails().GetEffectiveAt(),
		},
		Household: &customersv1.Household{
			Children: []*customersv1.Household{&customersv1.Household{
				Location: &commonv1.PostalAddress{
					CountryCode: lintCustomersReply.GetCustomer().GetPrimaryAddress().GetCountryCode(),
				},
				Children: []*customersv1.Household{&customersv1.Household{
					Manager: &identityv1.UserRef{
						DisplayName: fetchHouseholdReportReply.GetHousehold().GetManager().GetDisplayName(),
					},
				}},
			}},
			HouseholdId: req.GetHouseholdId(),
		},
		Result: &commonv1.OperationResult{Status: fetchHouseholdReportReply.GetResult().GetStatus()},
	}, nil
}

// GetHouseholdLifecycle returns a household.
func (s *HouseholdLifecycleServer) GetHouseholdLifecycle(ctx context.Context, req *customersv1.GetHouseholdLifecycleRequest) (*customersv1.GetHouseholdLifecycleResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected GetHouseholdLifecycle", "error", err)
		return nil, err
	}
	if err := s.store.RecordAction(ctx, req.GetContext().GetTenantId(), "household", req.GetHouseholdId(), "get_household_lifecycle"); err != nil {
		return nil, grpcx.Wrap(err, "record action")
	}
	s.log.InfoContext(ctx, "get household lifecycle", "tenant", req.GetContext().GetTenantId())
	encoded, err := protojson.Marshal(req.GetContext())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded commonv1.RequestContext
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	lintCustomerSegmentsRequest := &customersv1.LintCustomerSegmentsRequest{
		Context: req.GetContext(),
		CustomerSegment: &customersv1.CustomerSegment{
			Owner: &identityv1.UserRef{
				Email:  req.GetContext().GetActor().GetEmail(),
				UserId: req.GetContext().GetActor().GetUserId(),
			},
			Audit: &commonv1.AuditInfo{UpdatedAt: req.GetContext().GetDeadline()},
		},
		Details: &customersv1.LintCustomerSegmentsRequest_Details{Locale: (&decoded).GetLocale()},
	}
	lintCustomerSegmentsReply, err := customersv1.CustomerSegmentValidationServiceClient.LintCustomerSegments(s.customerSegmentValidationClient, ctx, lintCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentValidationService/LintCustomerSegments")
	}
	return &customersv1.GetHouseholdLifecycleResponse{
		Household: &customersv1.Household{
			Audit: &commonv1.AuditInfo{
				UpdatedBy: lintCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			},
			HouseholdId: req.GetHouseholdId(),
		},
		Result: &commonv1.OperationResult{Status: "ok"},
	}, nil
}

// ScheduleHouseholdExpiry schedules a household.
func (s *HouseholdLifecycleServer) ScheduleHouseholdExpiry(ctx context.Context, req *customersv1.ScheduleHouseholdExpiryRequest) (*customersv1.ScheduleHouseholdExpiryResponse, error) {
	if err := validateContext(req.GetContext()); err != nil {
		s.log.WarnContext(ctx, "rejected ScheduleHouseholdExpiry", "error", err)
		return nil, err
	}
	if err := s.store.SaveHousehold(ctx, req.GetContext().GetTenantId(), req.GetHousehold()); err != nil {
		return nil, grpcx.Wrap(err, "save household")
	}
	s.log.InfoContext(ctx, "schedule household expiry", "tenant", req.GetContext().GetTenantId(), "etag", req.GetHousehold().GetAudit().GetEtag())
	fetchHouseholdReportRequest := &customersv1.FetchHouseholdReportRequest{
		Context:     req.GetContext(),
		HouseholdId: req.GetHousehold().GetHouseholdId(),
	}
	fetchHouseholdReportReply, err := s.householdReportingLazy().FetchHouseholdReport(ctx, fetchHouseholdReportRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.HouseholdReportingService/FetchHouseholdReport")
	}
	encoded, err := protojson.Marshal(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode request")
	}
	var decoded customersv1.Household
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	countConsentRecordTotalsRequest := &customersv1.CountConsentRecordTotalsRequest{
		Context: req.GetContext(),
		Filter: &customersv1.CountConsentRecordTotalsRequest_Filter{
			Region:       (&decoded).GetLocation().GetRegion(),
			CreatedAfter: req.GetHousehold().GetAudit().GetCreatedAt(),
		},
	}
	type countConsentRecordTotalsResult struct {
		reply *customersv1.CountConsentRecordTotalsResponse
		err   error
	}
	countConsentRecordTotalsResults := make(chan countConsentRecordTotalsResult, 1)
	go func() {
		reply, err := s.consentRecordReportingClient.CountConsentRecordTotals(ctx, countConsentRecordTotalsRequest)
		countConsentRecordTotalsResults <- countConsentRecordTotalsResult{reply, err}
	}()
	var countConsentRecordTotalsReply *customersv1.CountConsentRecordTotalsResponse
	select {
	case result := <-countConsentRecordTotalsResults:
		if result.err != nil {
			return nil, grpcx.Wrap(result.err, "acme.customers.v1.ConsentRecordReportingService/CountConsentRecordTotals")
		}
		countConsentRecordTotalsReply = result.reply
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	snapshot, err := protojson.Marshal(req.GetHousehold())
	if err != nil {
		return nil, grpcx.Wrap(err, "encode snapshot")
	}
	pullCustomerSegmentsRequest := &customersv1.PullCustomerSegmentsRequest{}
	pullCustomerSegmentsRequest.Context = req.GetContext()
	pullCustomerSegmentsRequest.CustomerSegment = &customersv1.CustomerSegment{
		Owner: &identityv1.UserRef{Email: req.GetDetails().GetNotify().GetEmail()},
		Audit: &commonv1.AuditInfo{Etag: req.GetHousehold().GetAudit().GetEtag()},
	}
	pullCustomerSegmentsRequest.Details = &customersv1.PullCustomerSegmentsRequest_Details{Payload: snapshot}
	pullCustomerSegmentsReply, err := s.customerSegmentSyncClient.PullCustomerSegments(ctx, pullCustomerSegmentsRequest)
	if err != nil {
		return nil, grpcx.Wrap(err, "acme.customers.v1.CustomerSegmentSyncService/PullCustomerSegments")
	}
	return &customersv1.ScheduleHouseholdExpiryResponse{
		Household: &customersv1.Household{
			Audit: &commonv1.AuditInfo{
				CreatedAt: fetchHouseholdReportReply.GetHousehold().GetAudit().GetCreatedAt(),
				UpdatedBy: pullCustomerSegmentsReply.GetCustomerSegment().GetAudit().GetUpdatedBy(),
			},
			Manager: &identityv1.UserRef{
				DisplayName: pullCustomerSegmentsReply.GetCustomerSegment().GetOwner().GetDisplayName(),
			},
			HouseholdId: req.GetHouseholdId(),
			Status:      req.GetHousehold().GetStatus(),
		},
		Result: &commonv1.OperationResult{
			Details:      fetchHouseholdReportReply.GetResult().GetDetails(),
			Status:       countConsentRecordTotalsReply.GetResult().GetStatus(),
			ReferenceIds: countConsentRecordTotalsReply.GetResult().GetReferenceIds(),
		},
	}, nil
}

// householdLifecycleServerReinstateHouseholdCommentPaymentApprovalStep carries the request of one step of
// ReinstateHousehold and, once it ran, its reply.
type householdLifecycleServerReinstateHouseholdCommentPaymentApprovalStep struct {
	request *paymentsv1.CommentPaymentApprovalRequest
	reply   *paymentsv1.CommentPaymentApprovalResponse
}

func (s *HouseholdLifecycleServer) runReinstateHouseholdCommentPaymentApproval(ctx context.Context, step *householdLifecycleServerReinstateHouseholdCommentPaymentApprovalStep) error {
	reply, err := s.paymentApprovalClient.CommentPaymentApproval(ctx, step.request)
	if err != nil {
		return err
	}
	step.reply = reply
	return nil
}

// validateLedgerAccountSender returns a function that sends one request to
// acme.ledger.v1.LedgerAccountValidationService/ValidateLedgerAccount.
func (s *HouseholdLifecycleServer) validateLedgerAccountSender(request *ledgerv1.ValidateLedgerAccountRequest) func(context.Context) (*ledgerv1.ValidateLedgerAccountResponse, error) {
	return func(ctx context.Context) (*ledgerv1.ValidateLedgerAccountResponse, error) {
		return s.ledgerAccountValidationClient.ValidateLedgerAccount(ctx, request)
	}
}

// householdLifecycleServerFetchHouseholdReportSteps are the ways to run
// acme.customers.v1.HouseholdReportingService/FetchHouseholdReport, by name.
var householdLifecycleServerFetchHouseholdReportSteps = map[string]func(*HouseholdLifecycleServer, context.Context, *customersv1.FetchHouseholdReportRequest) (*customersv1.FetchHouseholdReportResponse, error){
	"default": (*HouseholdLifecycleServer).fetchHouseholdReportStep,
	"audited": (*HouseholdLifecycleServer).fetchHouseholdReportAuditedStep,
}

func (s *HouseholdLifecycleServer) fetchHouseholdReportStep(ctx context.Context, request *customersv1.FetchHouseholdReportRequest) (*customersv1.FetchHouseholdReportResponse, error) {
	return s.householdReportingClient.FetchHouseholdReport(ctx, request)
}

func (s *HouseholdLifecycleServer) fetchHouseholdReportAuditedStep(ctx context.Context, request *customersv1.FetchHouseholdReportRequest) (*customersv1.FetchHouseholdReportResponse, error) {
	s.log.InfoContext(ctx, "audited call", "method", "acme.customers.v1.HouseholdReportingService/FetchHouseholdReport")
	return s.householdReportingClient.FetchHouseholdReport(ctx, request)
}

// householdReportingLazy dials acme.customers.v1.HouseholdReportingService the first time a handler needs it.
func (s *HouseholdLifecycleServer) householdReportingLazy() customersv1.HouseholdReportingServiceClient {
	s.householdReportingOnce.Do(func() { s.householdReportingCached = customersv1.NewHouseholdReportingServiceClient(s.conn) })
	return s.householdReportingCached
}
